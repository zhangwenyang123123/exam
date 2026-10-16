"""pipeline/crawlers/news.py — 矿业新闻数据源采集器（mining.com 列表页翻页为主 + RSS 辅助）。

采集策略（v5，真实抓取 + 单次 GET 并发，与新版 policy.py 架构对齐，**无 HEAD 预探测、爬虫阶段不做时间过滤**）：
    1. 主路径：mining.com 新闻列表页（首页/分类页）自动翻页，批量发现文章 URL（提前去重，
       候选总量上限 max_candidates=800）；
    2. 辅助：mining.com RSS（feedparser）提供最新条目（自带发布时间与摘要）；
    3. 候选去重后**直接进入正文并发抓取，不再单独发起 HEAD 请求**：
       ThreadPoolExecutor(max_workers=6) 并发 GET，单次请求严格 timeout=5s，不重试、
       子线程内不额外 sleep；4xx/超时/解析异常仅本条标记 fulltext_ok=False，
       content 降级为列表摘要/RSS 摘要/og:description，不中断整体任务；
    4. 时间策略：**爬虫阶段不做发布时间过滤**，不因超出时间窗口丢弃文档；
       发布时间解析失败 published_at=null，文档保留（日志标记"时间解析失败"），
       时间过滤放到后续 clean 清洗模块执行；
    5. 随机 User-Agent；单条异常不影响整体；
    6. doc_id / content_hash / metadata / dedup_status 生成逻辑与 pipeline.clean 完全一致，Schema 不变。

**只做采集与结构化，不做切片 / 向量化 / 入库。**

依赖清单：
    - 必需：Python >= 3.12；feedparser（RSS 解析）；beautifulsoup4（HTML 解析与噪音清洗）
      HTTP 请求使用标准库 urllib（随机 UA / 严格超时）
      并发使用 concurrent.futures.ThreadPoolExecutor（正文 6 线程）
    - 内部复用：pipeline.clean 的 normalize_url / generate_doc_id / compute_content_hash / normalize_text

调用示例见文件末尾 `if __name__ == "__main__":`。
"""

from __future__ import annotations

import calendar
import gzip
import json
import logging
import random
import re
import socket
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Iterable
from urllib.parse import urljoin, urlsplit

import feedparser
from bs4 import BeautifulSoup

# 复用清洗层公共算法，保证主键/哈希一致
from pipeline.clean import (
    compute_content_hash,
    generate_doc_id,
    normalize_text,
    normalize_url,
)

logger = logging.getLogger("pipeline.crawlers.news")

__all__ = ["NewsCrawler", "RSS_FEEDS", "LIST_SEEDS", "collect_news"]

# --------------------------------------------------------------------------- #
# 配置常量
# --------------------------------------------------------------------------- #

# RSS 辅助源（仅 mining.com；sp_global_mining 已 404 废弃）
RSS_FEEDS: list[dict[str, str]] = [
    {
        "feed_url": "https://www.mining.com/feed/",
        "source": "mining_com",
    },
]

# 首页：作为发现分类列表页的 bootstrap 种子
LIST_SEEDS: list[dict[str, str]] = [
    {"url": "https://www.mining.com/", "source": "mining_com"},
]

# mining.com 分类/列表页的首段路径（commodity/copper/、category/xxx/ 等），用于翻页爬取
_CATEGORY_FIRST_SEGS = {"commodity", "category", "segment", "topic", "news"}

# 首页静态导航不暴露、但确实支持 /page/N/ 翻页的“全站新闻时间线”分类页，
# 固定作为翻页种子（仅靠首页发现的 commodity/category 覆盖不到近 30 天的全部文章）。
_KNOWN_CATEGORY_URLS: tuple[str, ...] = (
    "https://www.mining.com/category/news/",
)

# 文章 URL 中需要排除的路径前缀 / 特征（非文章页）
_EXCLUDED_PATH_HINTS = (
    "/category/", "/tag/", "/page/", "/author/", "/feed", "/search",
    "/about", "/contact", "/adverti", "/jobs", "/events", "/webinar",
    "/markets", "/product/", "/gallery/", "/video/", "/live-chart", "/data",
)
_ARTICLE_EXTS = (".jpg", ".jpeg", ".png", ".gif", ".pdf", ".mp4", ".zip")

# 常见浏览器 User-Agent 池，随机选取以做基础反爬
USER_AGENTS: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Edg/123.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
]

# 全文抽取时整体丢弃的噪音容器标签
NOISE_TAGS: tuple[str, ...] = (
    "script", "style", "noscript", "nav", "footer", "header",
    "aside", "form", "iframe", "figure", "figcaption", "button", "svg",
)


# --------------------------------------------------------------------------- #
# 条目模型（RSS 与列表页两条发现路径共用）
# --------------------------------------------------------------------------- #

class FeedEntry:
    """统一后的候选条目：RSS 解析或列表页发现产出的待抓取文章。"""

    __slots__ = ("guid", "link", "title", "published_at", "summary", "author", "tags", "source")

    def __init__(
        self,
        guid: str,
        link: str,
        title: str,
        published_at: datetime | None,
        summary: str,
        author: str | None,
        tags: list[str],
        source: str,
    ) -> None:
        self.guid = guid
        self.link = link
        self.title = title
        self.published_at = published_at
        self.summary = summary
        self.author = author
        self.tags = tags
        self.source = source


# --------------------------------------------------------------------------- #
# 采集器
# --------------------------------------------------------------------------- #

class NewsCrawler:
    """矿业新闻列表页翻页 + RSS 辅助 + 单次 GET 并发正文抓取采集器。

    候选去重后**直接进入并发正文抓取，无 HEAD 预探测环节**；
    **爬虫阶段不做时间过滤**：时间窗口筛选统一放到后续 clean 清洗阶段；
    发布时间解析失败时 published_at=null，文档保留不跳过。

    Args:
        target: 目标采集条数（达到后停止）。
        max_candidates: 候选 URL 总量上限（默认 800，翻页发现达到即提前收敛）。
        max_list_pages: 每个列表种子最多翻页数（可配置，靠此挖到足够深的文章）。
        zero_new_stop: 单分类连续多少页零新增则提前结束该分类翻页。
        request_interval / interval_jitter: 列表页/RSS 发现阶段相邻请求的随机延时下限（秒）及抖动上限，
            默认 1.0 + uniform(0, 1.5) 即 1.0~2.5 秒；正文并发阶段不 sleep。
        max_retries: 发现类请求最大尝试次数（1=不重试，2=失败后重试 1 次；正文 GET 不重试）。
        backoff_base: 发现类请求重试退避基数（秒）。
        timeout: GET 请求超时（秒，默认 5；详情页单次请求严格 5s）。
        max_workers: 正文抓取阶段并发线程数（默认 6）。
        now: 注入的当前时间（UTC），便于测试；默认真实 UTC now。
    """

    def __init__(
        self,
        target: int = 200,
        max_candidates: int = 800,
        max_list_pages: int = 30,
        request_interval: float = 1.0,
        interval_jitter: float = 1.5,
        max_retries: int = 2,
        backoff_base: float = 2.0,
        timeout: int = 5,
        max_workers: int = 6,
        zero_new_stop: int = 4,
        now: datetime | None = None,
    ) -> None:
        self.target = target
        self.max_candidates = max_candidates
        self.max_list_pages = max_list_pages
        self.zero_new_stop = zero_new_stop
        self.request_interval = request_interval
        self.interval_jitter = interval_jitter
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.timeout = timeout
        self.max_workers = max_workers
        self.now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        self._last_request_ts = 0.0
        self._stats_lock = threading.Lock()
        # 采集统计（URL 维度）
        self.stats: dict[str, int] = {
            "list_pages": 0,          # 成功抓取的列表页数
            "article_urls": 0,        # 去重后的候选文章 URL 总数
            "fetched_urls": 0,        # 实际发起过请求的 URL 总数（列表页+文章页+RSS）
            "kept": 0,                # 成功产出的 Document 数
            "fulltext_ok": 0,         # 全文抽取成功数
            "fallback": 0,            # 降级为摘要数
            "date_failed": 0,         # 时间解析失败数（published_at=null，文档保留）
            "failed": 0,              # 无法产出（请求失败/内容空等）数
            "fetch_timeout": 0,       # 正文 GET 超时/失败放弃数
        }

    # ---------------- HTTP 基础：随机 UA / 单次 GET ---------------- #

    def _incr_stat(self, key: str, amount: int = 1) -> None:
        """线程安全地累加统计字段。"""
        with self._stats_lock:
            self.stats[key] += amount

    @staticmethod
    def _random_headers() -> dict[str, str]:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        }

    def _respect_interval(self) -> None:
        """列表页/RSS 发现阶段相邻请求之间的随机延时（1.0~2.5 秒）；正文并发阶段不调用。"""
        wait = self.request_interval + random.uniform(0, self.interval_jitter)
        elapsed = time.monotonic() - self._last_request_ts
        if elapsed < wait:
            time.sleep(wait - elapsed)
        self._last_request_ts = time.monotonic()

    def _fetch(self, url: str) -> str | None:
        """列表页/RSS 等发现类请求：保留随机间隔与原有重试（超时/可恢复失败重试，最多 1 次）。"""
        self._respect_interval()
        self._incr_stat("fetched_urls")
        body, fatal, _ = self._fetch_split(url, self.timeout)
        if body is not None or fatal:
            return body
        delay = self.backoff_base + random.uniform(0, 1)
        time.sleep(delay)
        self._incr_stat("fetched_urls")
        body, _, _ = self._fetch_split(url, self.timeout)
        if body is None:
            logger.warning("抓取 %s 重试 1 次后仍失败，放弃", url)
        return body

    def _fetch_split(self, url: str, timeout: float) -> tuple[str | None, bool, bool]:
        """单次 GET。返回 (正文, 是否确定性失败, 是否超时)：

        - 成功 -> (text, False, False)
        - 4xx（除 429）等确定性失败 -> (None, True, False)，重试无意义
        - 超时 -> (None, False, True)
        - 5xx/网络错误等可恢复失败 -> (None, False, False)
        """
        req = urllib.request.Request(url, headers=self._random_headers())
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                if resp.headers.get("Content-Encoding", "").lower() in ("gzip", "x-gzip"):
                    raw = gzip.decompress(raw)
                charset = resp.headers.get_content_charset() or "utf-8"
                return raw.decode(charset, errors="replace"), False, False
        except urllib.error.HTTPError as exc:
            code = getattr(exc, "code", None)
            if isinstance(code, int) and 400 <= code < 500 and code != 429:
                logger.warning("抓取 %s 返回 %s，丢弃", url, code)
                return None, True, False
            logger.warning("抓取 %s 返回 %s，可恢复失败", url, code)
            return None, False, False
        except (TimeoutError, socket.timeout) as exc:
            logger.debug("抓取 %s 超时: %s", url, exc)
            return None, False, True
        except urllib.error.URLError as exc:
            # urlopen 超时会包装成 URLError(socket.timeout)
            if isinstance(exc.reason, (TimeoutError, socket.timeout)):
                logger.debug("抓取 %s 超时: %s", url, exc)
                return None, False, True
            logger.debug("抓取 %s 网络错误: %s", url, exc)
            return None, False, False
        except OSError as exc:
            logger.debug("抓取 %s 网络错误: %s", url, exc)
            return None, False, False
        except Exception:  # noqa: BLE001 兜底，单条失败不影响整体
            logger.exception("抓取 %s 发生意外错误", url)
            return None, True, False

    # ---------------- RSS 解析（辅助源） ---------------- #

    def fetch_feed(self, feed_url: str, source: str) -> list[FeedEntry]:
        """拉取并解析一个 RSS 源，返回归一化条目列表。"""
        text = self._fetch(feed_url)
        if text is None:
            logger.error("RSS 源抓取失败: %s", feed_url)
            return []
        try:
            return self._parse_with_feedparser(text, source)
        except Exception:  # noqa: BLE001
            logger.exception("解析 RSS 失败: %s", feed_url)
            return []

    def _parse_with_feedparser(self, text: str, source: str) -> list[FeedEntry]:
        """用 feedparser 解析 RSS/Atom，归一化为 FeedEntry（发布时间转 UTC）。"""
        parsed = feedparser.parse(text)
        entries: list[FeedEntry] = []
        for e in parsed.entries:
            published = None
            # feedparser 的 *_parsed 为 UTC struct_time，用 calendar.timegm 准确转秒
            for key in ("published_parsed", "updated_parsed"):
                st = e.get(key)
                if st:
                    published = datetime.fromtimestamp(calendar.timegm(st), tz=timezone.utc)
                    break
            tags = [t.get("term", "") for t in e.get("tags", []) if t.get("term")]
            summary = e.get("summary", "") or e.get("description", "")
            entries.append(
                FeedEntry(
                    guid=e.get("id") or e.get("link", ""),
                    link=e.get("link", ""),
                    title=_strip_tags(e.get("title", "")),
                    published_at=published,
                    summary=summary,
                    author=e.get("author") or e.get("dc_creator") or None,
                    tags=tags,
                    source=source,
                )
            )
        return entries

    # ---------------- 列表页翻页发现 ---------------- #

    @staticmethod
    def _is_article_url(url: str) -> bool:
        """判断是否为 mining.com 文章详情页 URL。"""
        try:
            parts = urlsplit(url)
        except ValueError:
            return False
        host = (parts.netloc or "").lower()
        if "mining.com" not in host:
            return False
        path = parts.path or ""
        if path.endswith(_ARTICLE_EXTS) or any(h in path for h in _EXCLUDED_PATH_HINTS):
            return False
        # 文章 slug 为单段路径：/xxx-yyy/；分类/归档等多段路径排除
        segments = [s for s in path.strip("/").split("/") if s]
        return len(segments) == 1 and bool(re.match(r"^[A-Za-z0-9][\w-]{5,}$", segments[0]))

    def _discover_category_seeds(self, source: str = "mining_com") -> list[dict[str, str]]:
        """从首页 bootstrap 发现分类/列表页（/commodity/xxx/、/category/xxx/ 等）作为翻页种子。

        mining.com 首页自身的 /page/N/ 不返回新内容，但各分类页支持 /page/N/ 翻页。
        """
        seeds: list[dict[str, str]] = []
        seen: set[str] = set()
        # 1) 先纳入已知的全站新闻时间线分类页（首页静态导航抓不到）
        for known in _KNOWN_CATEGORY_URLS:
            norm = normalize_url(known)
            if norm and norm not in seen:
                seen.add(norm)
                seeds.append({"url": known, "source": source})
        # 2) 再从首页 bootstrap 发现 commodity/category 等分类页
        for bootstrap in LIST_SEEDS:
            html_text = self._fetch(bootstrap["url"])
            if html_text is None:
                continue
            soup = BeautifulSoup(html_text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = urljoin(bootstrap["url"], a["href"])
                try:
                    segs = [s for s in urlsplit(href).path.strip("/").split("/") if s]
                except ValueError:
                    continue
                # 分类页为两段路径（commodity/copper），且首段属于已知分类
                if len(segs) != 2 or segs[0] not in _CATEGORY_FIRST_SEGS:
                    continue
                norm = normalize_url(href)
                if not norm or norm in seen:
                    continue
                seen.add(norm)
                seeds.append({"url": href, "source": source})
        logger.info("发现 %d 个分类列表页种子", len(seeds))
        return seeds

    def crawl_list_pages(
        self,
        seeds: Iterable[dict[str, str]] | None = None,
        exclude_links: set[str] | None = None,
        max_candidates: int = 0,
    ) -> list[FeedEntry]:
        """从各分类列表页自动翻页，发现候选文章条目（URL/标题/可能的摘要与日期）。

        Args:
            seeds: 分类页种子；默认调用 _discover_category_seeds 自动发现。
            exclude_links: 已知的 URL（如 RSS 已覆盖），跳过不重复发现。
            max_candidates: 候选总数达到即停止翻页（0=不限制）。
        """
        seeds = list(seeds) if seeds is not None else self._discover_category_seeds()
        exclude_links = exclude_links or set()
        found: dict[str, FeedEntry] = {}  # normalized_url -> entry（保序去重）

        for seed in seeds:
            if max_candidates and len(found) >= max_candidates:
                logger.info("候选已达 %d，停止翻页发现", max_candidates)
                break
            base = seed["url"].rstrip("/")
            source = seed["source"]
            zero_streak = 0  # 当前分类连续“零新增”页数（用于自然收敛早停）
            for page_no in range(1, self.max_list_pages + 1):
                if max_candidates and len(found) >= max_candidates:
                    break
                # WordPress 分类页翻页：首页为 base，后续为 base/page/N/（直接构造，
                # 不依赖列表页内可能已被去噪或结构不一的 pagination 锚点）
                url = base if page_no == 1 else f"{base}/page/{page_no}/"
                html_text = self._fetch(url)
                if html_text is None:
                    logger.warning("列表页抓取失败，停止该分类翻页: %s", url)
                    break
                self.stats["list_pages"] += 1
                try:
                    soup = BeautifulSoup(html_text, "html.parser")
                except Exception:  # noqa: BLE001
                    logger.exception("列表页解析失败: %s", url)
                    break

                page_entries = self._extract_article_links(soup, url, source)
                new_cnt = 0
                for entry in page_entries:
                    key = normalize_url(entry.link) or entry.link
                    if key in found or key in exclude_links:
                        continue
                    found[key] = entry
                    new_cnt += 1
                logger.info("分类页 %s 第 %d 页: 页内 %d 篇，新增 %d 篇（累计候选 %d）",
                            url, page_no, len(page_entries), new_cnt, len(found))
                # 整页无任何文章链接：已到末页/空页，停止该分类。
                if not page_entries:
                    break
                # 连续多页都是“仅重复(RSS/已见)”零新增：说明该分类已翻到陈旧/侧栏区，
                # 早停以节省请求；而“全站新闻时间线”会持续产出新文章，不会触发此早停。
                if new_cnt == 0:
                    zero_streak += 1
                    if zero_streak >= self.zero_new_stop:
                        logger.info("分类 %s 连续 %d 页无新增，提前结束翻页", base, zero_streak)
                        break
                else:
                    zero_streak = 0

        self.stats["article_urls"] += len(found)
        return list(found.values())

    def _extract_article_links(self, soup: BeautifulSoup, page_url: str, source: str) -> list[FeedEntry]:
        """从列表页 HTML 抽取文章链接及其可见元数据（标题/摘要/日期，尽力而为）。

        注意：同一文章常有多个锚点（图片锚点无文本 + 标题文字锚点）。仅当真正产出条目时才
        标记已见，避免空标题的图片锚点抢先遮挡后续带标题的锚点。
        """
        for tag in soup.find_all(NOISE_TAGS):
            if tag.name == "script":  # JSON-LD 可能带日期，保留脚本
                continue
            tag.decompose()

        entries: list[FeedEntry] = []
        added: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = urljoin(page_url, a["href"])
            if not self._is_article_url(href):
                continue
            norm = normalize_url(href) or href
            if norm in added:
                continue

            # 标题：锚点文本 -> img[alt] -> 锚内标题子元素
            title = _clean_inline(a.get_text(" ", strip=True))
            if not title:
                img = a.find("img")
                if img is not None and img.get("alt"):
                    title = _clean_inline(img["alt"])
            if not title:
                h = a.find(["h1", "h2", "h3", "h4"])
                if h is not None:
                    title = _clean_inline(h.get_text(" ", strip=True))

            summary = ""
            published = None
            # 向上找列表项容器（优先 article/li，退到 div），尽力提取摘要与时间
            item = a.find_parent(["article", "li"]) or a.find_parent("div")
            if item is not None:
                if not title:
                    h = item.find(["h2", "h3", "h4"])
                    title = _clean_inline(h.get_text(" ", strip=True)) if h is not None else ""
                p = item.find("p")
                summary = p.get_text(" ", strip=True) if p is not None else ""
                time_tag = item.find("time")
                if time_tag is not None:
                    published = _coerce_dt(time_tag.get("datetime") or time_tag.get_text())

            if not title:  # 无标题：不标记已见，给同一链接的其它锚点补标题的机会
                continue
            added.add(norm)
            entries.append(
                FeedEntry(
                    guid=norm, link=href, title=title,
                    published_at=published, summary=summary,
                    author=None, tags=[], source=source,
                )
            )
        return entries

    # ---------------- 文章页一次请求：正文 + 时间 + 作者 + 标签 ---------------- #

    def fetch_article(self, url: str) -> dict[str, Any] | None:
        """抓取文章页并一次解析：正文 content、发布时间、作者、标签、og:description。

        沿用原 BeautifulSoup 全文抽取逻辑（NOISE_TAGS 去噪 -> article/main/content 容器
        -> 实质性 <p> 段落 -> normalize_text 归一）。失败返回 None。
        """
        html_text, timeout_hit = self._fetch_article_html(url)
        if not html_text:
            return None
        try:
            soup = BeautifulSoup(html_text, "html.parser")
        except Exception:  # noqa: BLE001
            logger.exception("HTML 解析失败: %s", url)
            return None

        # ---- 元数据（需在去噪前读 head / script / <time>）----
        published_at = self._extract_published(soup)
        og_title = _meta_content(soup, "property", "og:title")
        summary_meta = _meta_content(soup, "property", "og:description")
        meta_author = _meta_content(soup, "name", "author")
        tag_names = [
            t for a2 in soup.select('a[href*="/tag/"]')
            if (t := _clean_inline(a2.get_text(" ", strip=True)))
        ]

        # ---- 正文抽取（与原逻辑一致）----
        for tag in soup.find_all(NOISE_TAGS):
            tag.decompose()
        container = (
            soup.find("article")
            or soup.find("main")
            or soup.find(class_=re.compile(r"(article|entry|post|content|body)", re.I))
            or soup
        )
        paragraphs = [p.get_text(" ", strip=True) for p in container.find_all("p")]
        paragraphs = [t for t in paragraphs if len(t) >= 30]
        text = " ".join(paragraphs) if paragraphs else container.get_text(" ", strip=True)
        content = normalize_text(text, for_hash=False)

        return {
            "content": content if len(content) >= 80 else None,
            "published_at": published_at,
            "author": meta_author,
            "tags": tag_names,
            "summary": summary_meta,
            "og_title": og_title,
        }

    def _fetch_article_html(self, url: str) -> tuple[str | None, bool]:
        """正文页专用 GET：**单次请求不重试**，严格 timeout=5s，子线程内不额外 sleep。

        4xx / 超时 / 解析异常一律返回 None，由调用方 build_document 将本条
        标记 fulltext_ok=False、content 降级为摘要；不中断整体采集任务。
        返回 (html, 是否超时)。
        """
        self._incr_stat("fetched_urls")
        body, _fatal, timed_out = self._fetch_split(url, self.timeout)
        if body is not None:
            return body, False
        self._incr_stat("fetch_timeout")
        logger.info("正文抓取失败/超时(%ds)，本条标记 fulltext_ok=false: %s", self.timeout, url)
        return None, timed_out

    @staticmethod
    def _extract_published(soup: BeautifulSoup) -> datetime | None:
        """按可靠度依次取发布时间：meta[article:published_time] -> <time> -> JSON-LD。"""
        val = _meta_content(soup, "property", "article:published_time")
        dt = _coerce_dt(val)
        if dt:
            return dt
        time_tag = soup.find("time")
        if time_tag is not None:
            dt = _coerce_dt(time_tag.get("datetime") or time_tag.get_text())
            if dt:
                return dt
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.get_text())
            except (json.JSONDecodeError, ValueError):
                continue
            dt = _find_date_in_jsonld(data)
            if dt:
                return dt
        return None

    # ---------------- Document 组装（生成逻辑保持不变） ---------------- #

    def build_document(self, entry: FeedEntry) -> dict[str, Any] | None:
        """候选条目 -> Document dict（严格符合 Schema；**爬虫阶段不做时间过滤**）。"""
        if not entry.link:
            return None

        # 文章页单次 GET：正文 + 缺失元数据（无 HEAD 预探测，失败降级在本条内处理）
        article = self.fetch_article(entry.link)
        published_at = entry.published_at or (article or {}).get("published_at")

        # 时间解析失败：**不跳过、不丢弃**，published_at 置 null，时间过滤交给后续 clean 阶段
        if published_at is None:
            self._incr_stat("date_failed")
            logger.info("时间解析失败，published_at=null 保留文档: %s", entry.link)

        # 全文成功 -> 正文；失败/超时 -> 降级摘要（RSS 摘要 / 列表页摘要 / og:description）
        fulltext = (article or {}).get("content")
        fulltext_ok = fulltext is not None
        fallback_summary = normalize_text(entry.summary, for_hash=False) \
            or normalize_text((article or {}).get("summary") or "", for_hash=False)
        content = fulltext if fulltext_ok else fallback_summary
        if not content:
            self._incr_stat("failed")
            logger.info("正文与摘要均为空，跳过: %s", entry.link)
            return None

        title = _clean_inline(entry.title) or (article or {}).get("og_title") \
            or _slug_to_title(entry.link)

        try:
            doc_id = generate_doc_id("news", {"url": entry.link})
        except Exception as exc:  # noqa: BLE001
            logger.warning("生成 doc_id 失败，跳过 %s: %s", entry.link, exc)
            self._incr_stat("failed")
            return None

        language = "zh" if re.search(r"[\u4e00-\u9fff]", content) else "en"

        return {
            "doc_id": doc_id,
            "source_type": "news",
            "source": entry.source,
            "title": title,
            "url": normalize_url(entry.link) or entry.link,
            "content": content,
            "summary": fallback_summary or None,
            "published_at": _iso(published_at) if published_at is not None else None,
            "crawled_at": _iso(self.now),
            "language": language,
            "content_hash": compute_content_hash(content),
            "metadata": {
                "site_domain": _domain(entry.link),
                "author": entry.author or (article or {}).get("author"),
                "tags": entry.tags or (article or {}).get("tags") or [],
                "word_count": len(content),
                "fulltext_ok": fulltext_ok,
            },
            "dedup_status": "kept",
            "duplicate_of": None,
        }

    # ---------------- 并发安全包装 ---------------- #

    def _safe_build_document(self, entry: FeedEntry) -> dict[str, Any] | None:
        """线程安全包装：单条 URL 异常不影响其他任务继续执行。"""
        try:
            return self.build_document(entry)
        except Exception:  # noqa: BLE001
            logger.exception("并发抓取任务异常，跳过: %s", entry.link)
            self._incr_stat("failed")
            return None

    # ---------------- 顶层采集 ---------------- #

    def collect(
        self,
        feeds: Iterable[dict[str, str]] | None = None,
        list_seeds: Iterable[dict[str, str]] | None = None,
    ) -> list[dict[str, Any]]:
        """RSS + 列表页发现 -> URL 去重 -> 并发单次 GET 抓取组装 Document 列表（无 HEAD 预探测）。"""
        documents: list[dict[str, Any]] = []
        seen_urls: set[str] = set()
        seen_ids: set[str] = set()

        # 1) RSS 最新条目优先（自带发布时间与摘要，质量最高）
        entries: list[FeedEntry] = []
        for feed in list(feeds or RSS_FEEDS):
            logger.info("RSS 辅助源: %s (%s)", feed["source"], feed["feed_url"])
            entries.extend(self.fetch_feed(feed["feed_url"], feed["source"]))
        entries.sort(key=lambda e: e.published_at or datetime.min.replace(tzinfo=timezone.utc), reverse=True)

        # 2) 分类列表页自动发现 + 翻页（排除 RSS 已发现的 URL）
        for e in entries:
            seen_urls.add(normalize_url(e.link) or e.link)
        seeds = list(list_seeds) if list_seeds is not None else None
        list_entries = self.crawl_list_pages(seeds, exclude_links=seen_urls,
                                             max_candidates=self.max_candidates)
        candidates = entries + list_entries
        # 已知发布时间倒序（未知时间的排后，抓取时再补日期）
        candidates.sort(key=lambda e: e.published_at or datetime.min.replace(tzinfo=timezone.utc),
                        reverse=True)
        logger.info("候选文章合计 %d 篇（RSS %d + 列表页 %d）",
                    len(candidates), len(entries), len(list_entries))

        # 3) 候选 URL 提前去重，避免重复抓取同一 URL（已采集成功的 URL 也在此拦截）
        unique_candidates: list[FeedEntry] = []
        dedup_keys: set[str] = set()
        for entry in candidates:
            norm = normalize_url(entry.link) or entry.link
            if not norm or norm in dedup_keys:
                continue
            dedup_keys.add(norm)
            unique_candidates.append(entry)
        logger.info("去重后候选 %d 篇（原 %d 篇，上限 %d），开始并发抓取（max_workers=%d, timeout=%ds）",
                    len(unique_candidates), len(candidates), self.max_candidates,
                    self.max_workers, self.timeout)

        # 4) 正文并发抓取组装 Document：单次 GET 严格超时，失败仅本条降级 fulltext_ok=false，
        #    子线程内不额外 sleep，单条异常不中断整体任务
        executor = ThreadPoolExecutor(max_workers=self.max_workers)
        try:
            future_to_entry = {
                executor.submit(self._safe_build_document, entry): entry
                for entry in unique_candidates
            }
            for future in as_completed(future_to_entry):
                if len(documents) >= self.target:
                    break
                doc = future.result()
                if doc is None:
                    continue
                if doc["doc_id"] in seen_ids:
                    continue
                seen_ids.add(doc["doc_id"])
                documents.append(doc)
                self._incr_stat("kept")
                if doc["metadata"]["fulltext_ok"]:
                    self._incr_stat("fulltext_ok")
                else:
                    self._incr_stat("fallback")
                if len(documents) % 25 == 0:
                    logger.info("进度: 已采集 %d / %d", len(documents), self.target)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        if len(documents) < self.target:
            logger.warning("采集数量 %d < 目标 %d，可增大 max_list_pages 或扩充 LIST_SEEDS/分类种子",
                           len(documents), self.target)
        return documents

    # ---------------- 统计输出 ---------------- #

    def log_stats(self) -> None:
        s = self.stats
        logger.info(
            "采集统计 | 列表页 %d 页 | 候选文章 %d 篇 | 总请求 URL %d 个 | "
            "抓取超时/失败 %d | "
            "成功 %d 条 | 全文成功 %d / 降级摘要 %d | 时间解析失败 %d | 失败 %d",
            s["list_pages"], s["article_urls"], s["fetched_urls"],
            s["fetch_timeout"],
            s["kept"], s["fulltext_ok"], s["fallback"], s["date_failed"], s["failed"],
        )


# --------------------------------------------------------------------------- #
# 工具函数
# --------------------------------------------------------------------------- #

def _strip_tags(text: str) -> str:
    return re.sub(r"(?s)<[^>]+>", "", text or "").strip()


def _clean_inline(text: str) -> str:
    """去标签并折叠内部多余空白（用于标题）。"""
    return re.sub(r"\s+", " ", _strip_tags(text)).strip()


def _meta_content(soup: BeautifulSoup, attr: str, value: str) -> str | None:
    """读取 <meta> 的 content（按指定属性名匹配，避开 find() 的 name 保留参数冲突）。"""
    for tag in soup.find_all("meta"):
        if tag.get(attr) == value:
            content = tag.get("content")
            if isinstance(content, str) and content.strip():
                return content.strip()
    return None


def _coerce_dt(value: str | None) -> datetime | None:
    """尽力把字符串解析为 UTC datetime。"""
    if not value:
        return None
    s = str(value).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        for fmt in ("%Y-%m-%d", "%d %b %Y", "%B %d, %Y", "%b %d, %Y"):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except ValueError:
                continue
        else:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _find_date_in_jsonld(data: Any) -> datetime | None:
    """递归搜索 JSON-LD 中的 datePublished / dateCreated。"""
    if isinstance(data, dict):
        for key in ("datePublished", "dateCreated"):
            if key in data:
                dt = _coerce_dt(data[key])
                if dt:
                    return dt
        for v in data.values():
            dt = _find_date_in_jsonld(v)
            if dt:
                return dt
    elif isinstance(data, list):
        for v in data:
            dt = _find_date_in_jsonld(v)
            if dt:
                return dt
    return None


def _slug_to_title(url: str) -> str:
    """从文章 URL slug 兜底生成标题。"""
    path = urlsplit(url).path.strip("/").split("/")[-1] or ""
    return path.replace("-", " ").strip().title()


def _domain(url: str) -> str | None:
    try:
        netloc = urllib.request.urlsplit(url).netloc.lower()
        return netloc[4:] if netloc.startswith("www.") else netloc
    except ValueError:
        return None


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


# --------------------------------------------------------------------------- #
# 便捷入口
# --------------------------------------------------------------------------- #

def collect_news(
    target: int = 200,
    max_candidates: int = 800,
    max_list_pages: int = 30,
    max_workers: int = 6,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """一行调用：采集矿业新闻 Document 列表（采集完成后打印统计；爬虫阶段不做时间过滤）。"""
    crawler = NewsCrawler(target=target, max_candidates=max_candidates,
                          max_list_pages=max_list_pages, max_workers=max_workers, now=now)
    documents = crawler.collect()
    crawler.log_stats()
    return documents


# --------------------------------------------------------------------------- #
# 调用示例（真实网络采集）
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    import json

    # 真实采集：列表页翻页发现 URL（RSS 辅助）→ 去重 → 并发单次 GET 抓取（无 HEAD 预探测）→ 组装 Document
    # 不做发布时间过滤：解析失败 published_at=null，时间筛选交给后续 clean 阶段
    crawler = NewsCrawler(target=200, max_candidates=800, max_list_pages=30, max_workers=6)
    documents = crawler.collect()
    crawler.log_stats()  # 候选 / 总 URL / 超时失败 / 成功 / 全文成功 / 降级摘要 / 时间解析失败 等

    # 导出 news_raw.json
    with open("news_raw.json", "w", encoding="utf-8") as f:
        json.dump(documents, f, ensure_ascii=False, indent=2)
    logger.info("已导出 news_raw.json，共 %d 条", len(documents))

    # 预览前 2 条结构化 Document
    for d in documents[:2]:
        print(json.dumps(d, ensure_ascii=False, indent=2))

    # 返回的 documents 可直接交给 pipeline.clean.clean_documents 做后续去重（本模块不入库/不切片）
