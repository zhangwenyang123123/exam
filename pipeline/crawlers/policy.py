"""pipeline/crawlers/policy.py — 关键矿产政策数据源采集器（真实网页全量抓取，v3）。

数据源（POLICY_SOURCES，全部真实官网列表页翻页）：
    1. 中国稀土集团 regcc.cn（中文）：集团新闻/国资动态/行业动态/基层动态/理论学习/公司公告/
       党建专栏/党建概况/信息公开 等栏目，list_{N}.shtml 翻页；
    2. 自然资源部 mnr.gov.cn（中文）：通知公告 gk/tzgg、部首页要闻 dt/ywbb、政策解读 zcjd，
       index_{N}.html 翻页；详情页 t20YYYYMMDD_编号.htm；
    3. 工信部 miit.gov.cn（中文）：政策文件-文件发布-通知 zwgk/zcwj/wjfb/tz、
       原材料工业司工作动态 jgsj/ycls/gzdt；详情页 /art/YYYY/art_xxx.html。
       注：工信部列表页为 jpaaS 动态渲染，纯 HTTP 下发现不到链接时自动跳过（返 0 候选），
       不对接其 JS 接口前不会产出假数据；
    4. 澳洲 DISR industry.gov.au（英文，language=en）：news / ministerial-media /
       关键矿产专题 topics/mining/critical-minerals，?page=N 翻页。

采集策略（v4，真实抓取 + 单次 GET 并发，**无 HEAD 预探测、爬虫阶段不做时间过滤**）：
    1. 列表页自动翻页发现候选 URL（max_list_pages=60，总量上限 max_candidates=800），
       候选按 normalize_url 提前去重；**不设独立 HEAD 预探测环节**，去重后直接进入正文并发抓取；
    2. ThreadPoolExecutor(max_workers=4) 并发抓取详情页：随机 UA + 单次 GET 严格 timeout=5s，不重试；
       失败（4xx/超时/异常）仅将本条标记 fulltext_ok=False、content 降级为摘要，不中断其他任务；
    3. 发布时间多策略解析：页面 meta -> 标题 -> 正文开头 -> 正文末尾 -> 整页正文 -> URL，
       多组正则依次提取；**解析失败不跳过文档，published_at=null**，
       日志标记"时间解析失败"并计入 date_failed；时间过滤统一放到后续 clean 清洗阶段；
    4. 正文用 BeautifulSoup 清洗：移除 script/nav/侧边栏/面包屑/页脚等噪音容器；
       全文抽取失败 -> fulltext_ok=False，content 降级为列表摘要 / og:description；
    5. doc_id / content_hash / metadata / dedup_status 生成逻辑与 pipeline.clean、
       news.py 完全一致 —— **Document Schema 与输出字段与之前版本严格一致，不得改动**。

**只做真实采集与结构化，不做切片 / 向量化 / 入库，不生成任何模拟文档。**

依赖清单：
    - 必需：Python >= 3.12；beautifulsoup4（HTML 解析与噪音清洗）
      HTTP 请求使用标准库 urllib（随机 UA / 限速 / 单次 GET 严格超时）
      并发使用 concurrent.futures.ThreadPoolExecutor
    - 内部复用：pipeline.clean 的 normalize_url / generate_doc_id / compute_content_hash / normalize_text

调用示例见文件末尾 `if __name__ == "__main__":`。
"""

from __future__ import annotations

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
from typing import Any, Callable, Iterable
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup

# 复用清洗层公共算法，保证主键/哈希与 news.py 完全一致
from pipeline.clean import (
    compute_content_hash,
    generate_doc_id,
    normalize_text,
    normalize_url,
)

logger = logging.getLogger("pipeline.crawlers.policy")

__all__ = ["PolicyCrawler", "POLICY_SOURCES", "collect_policy"]

# --------------------------------------------------------------------------- #
# 配置常量
# --------------------------------------------------------------------------- #

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

# 全文抽取时整体丢弃的噪音容器标签（导航/侧边栏/页脚等）
NOISE_TAGS: tuple[str, ...] = (
    "script", "style", "noscript", "nav", "footer", "header",
    "aside", "form", "iframe", "figure", "figcaption", "button", "svg",
)

# 详情页二次清洗：按 class/id 关键词移除导航、侧边栏、面包屑、相关推荐、页脚等容器
_NOISE_CLASS_RE = re.compile(
    r"(sidebar|side-?bar|nav|crumb|breadcrumb|menu|footer|copyright|related|"
    r"recommend|share|print|download|qrcode|ads|banner|links)",
    re.I,
)

# 中文/年在前数字日期：2025-06-05 / 2025年6月5日 / 2025.06.05
_CN_DATE_RE = re.compile(r"(20\d{2})[-/年.](\d{1,2})[-/月.](\d{1,2})")
# 紧凑数字日期：20250605（常见于 CMS 生成的 URL / 时间戳）
_COMPACT_DATE_RE = re.compile(r"(?<!\d)(20\d{2})(0[1-9]|1[0-2])([0-2]\d|3[01])(?!\d)")
# 斜杠/横杠数字日期：2025/06/05、2025-6-5（年在前）
_NUM_DATE_RE = re.compile(r"(20\d{2})[/\-.](\d{1,2})[/\-.](\d{1,2})")
# 英文日期：5 June 2025 / June 5, 2025
_EN_DATE_RE = re.compile(r"(\d{1,2})\s+([A-Za-z]{3,9})\s+(20\d{2})|([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(20\d{2})")
_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _cnrare_page_url(base: str, page_no: int) -> str:
    """中国稀土集团官网 CMS 翻页：page1 -> list.shtml，pageN -> list_{N}.shtml。"""
    base = base if base.endswith("/") else base + "/"
    name = "list.shtml" if page_no == 1 else f"list_{page_no}.shtml"
    return urljoin(base, name)


def _cngov_page_url(base: str, page_no: int) -> str:
    """自然资源部 / 工信部列表页翻页：page1 -> index.html，pageN -> index_{N-1}.html。"""
    base = base if base.endswith("/") else base + "/"
    name = "index.html" if page_no == 1 else f"index_{page_no - 1}.html"
    return urljoin(base, name)


def _disr_page_url(base: str, page_no: int) -> str:
    """澳洲 DISR（Drupal）翻页：page1 -> base，pageN -> base?page=N-1（0 起始）。"""
    if page_no == 1:
        return base
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}page={page_no - 1}"


# 四个真实政策数据源配置（列表种子 / 翻页函数 / 详情链接判定器 / 语言）
POLICY_SOURCES: list[dict[str, Any]] = [
    {
        "source": "cnrare_policy",
        "region": "CN",
        "language": "zh",
        "matcher": "cnrare",
        "page_url": _cnrare_page_url,
        # 新闻中心 / 企业党建 / 信息公开 等栏目目录（列表页在其下 list_N.shtml）；抓不到的栏目会自动跳过
        "list_seeds": [
            "https://www.regcc.cn/zgxtjt/jtnew/",    # 集团新闻
            "https://www.regcc.cn/zgxtjt/gzdt/",     # 国资动态
            "https://www.regcc.cn/zgxtjt/hydt/",     # 行业动态
            "https://www.regcc.cn/zgxtjt/jcdt/",     # 基层动态
            "https://www.regcc.cn/zgxtjt/ljxx/",     # 理论学习
            "https://www.regcc.cn/zgxtjt/gsgg/",     # 通知公告（公司公告）
            "https://www.regcc.cn/zgxtjt/djgz/",     # 党建专栏（党建工作）
            "https://www.regcc.cn/zgxtjt/djgk/",     # 党建概况
            "https://www.regcc.cn/zgxtjt/xxgkzn/",   # 信息公开（政策信息类）
        ],
    },
    {
        "source": "mnr_policy",
        "region": "CN",
        "language": "zh",
        "matcher": "mnr",
        "page_url": _cngov_page_url,
        # 自然资源部：通知公告 / 首页要闻（自然之声）/ 政策解读；抓不到的栏目自动跳过
        "list_seeds": [
            "https://www.mnr.gov.cn/gk/tzgg/",       # 通知公告
            "https://www.mnr.gov.cn/dt/ywbb/",       # 部首页要闻
            "https://www.mnr.gov.cn/zcjd/",          # 政策解读
        ],
    },
    {
        "source": "miit_policy",
        "region": "CN",
        "language": "zh",
        "matcher": "miit",
        "page_url": _cngov_page_url,
        # 工信部：政策文件-文件发布-通知 / 原材料工业司（ycls）工作动态
        # （列表页 jpaaS 动态渲染，纯 HTTP 发现 0 链接时自动跳过，不生成模拟数据）
        "list_seeds": [
            "https://www.miit.gov.cn/zwgk/zcwj/wjfb/tz/",   # 政策文件（通知）
            "https://www.miit.gov.cn/jgsj/ycls/gzdt/",      # 原材料工业司工作动态
        ],
    },
    {
        "source": "au_disr_policy",
        "region": "AU",
        "language": "en",
        "matcher": "disr",
        "page_url": _disr_page_url,
        # DISR 新闻 / 部长媒体稿 / 关键矿产（Critical Minerals）政策专题列表（英文 policy）
        "list_seeds": [
            "https://www.industry.gov.au/news",
            "https://www.industry.gov.au/ministerial-media",
            "https://www.industry.gov.au/topics/mining/critical-minerals",
        ],
    },
]


# --------------------------------------------------------------------------- #
# 候选条目模型
# --------------------------------------------------------------------------- #

class PolicyEntry:
    """候选政策条目：列表页发现产出的待抓取页面。"""

    __slots__ = ("link", "title", "published_at", "summary", "author", "tags", "source", "region", "page_no")

    def __init__(
        self,
        link: str,
        title: str,
        published_at: datetime | None,
        summary: str,
        author: str | None,
        tags: list[str],
        source: str,
        region: str,
        page_no: int,
    ) -> None:
        self.link = link
        self.title = title
        self.published_at = published_at
        self.summary = summary
        self.author = author
        self.tags = tags
        self.source = source
        self.region = region
        self.page_no = page_no


# --------------------------------------------------------------------------- #
# 采集器
# --------------------------------------------------------------------------- #

class PolicyCrawler:
    """关键矿产政策列表页翻页 + 多线程单次 GET 详情抓取 + 全文抽取采集器。

    候选去重后**直接进入并发正文抓取，无 HEAD 预探测环节**；
    **爬虫阶段不做时间过滤**：时间窗口筛选统一放到后续 clean 清洗阶段；
    发布时间解析失败时 published_at=null，文档保留不跳过。

    Args:
        target: 目标采集条数（达到后停止）。
        max_candidates: 候选 URL 总量上限（默认 800，翻页发现达到即提前收敛）。
        max_list_pages: 每个列表种子最多翻页数（默认 60，增大以保证候选量）。
        max_workers: 详情页并发抓取线程数（默认 4）。
        request_interval / interval_jitter: 列表页相邻请求间隔及随机抖动（秒）。
        max_retries: 列表页发现请求最大尝试次数（默认 2；详情页不重试，失败即降级）。
        backoff_base: 列表页重试退避基数（秒）。
        timeout: GET 请求超时（秒，默认 5；详情页单次请求严格 5s）。
        now: 注入的当前时间（UTC），便于测试；默认真实 UTC now。
    """

    def __init__(
        self,
        target: int = 200,
        max_candidates: int = 800,
        max_list_pages: int = 60,
        max_workers: int = 4,
        request_interval: float = 1.0,
        interval_jitter: float = 1.5,
        max_retries: int = 2,
        backoff_base: float = 2.0,
        timeout: int = 5,
        now: datetime | None = None,
    ) -> None:
        self.target = target
        self.max_candidates = max_candidates
        self.max_list_pages = max_list_pages
        self.max_workers = max_workers
        self.request_interval = request_interval
        self.interval_jitter = interval_jitter
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.timeout = timeout
        self.now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        self._last_request_ts = 0.0
        self._stats_lock = threading.Lock()
        # 分站点详情链接判定器
        self._matchers: dict[str, Callable[[str], bool]] = {
            "cnrare": self._is_cnrare_article,
            "mnr": self._is_mnr_article,
            "miit": self._is_miit_article,
            "disr": self._is_disr_article,
        }
        # 采集统计
        self.stats: dict[str, int] = {
            "list_pages": 0,        # 成功抓取的列表页数
            "article_urls": 0,      # 去重后的候选政策 URL 总数
            "fetched_urls": 0,      # 实际发起请求的 URL 总数（列表页 + 详情页）
            "kept": 0,              # 成功产出的 Document 数
            "fulltext_ok": 0,       # 全文抽取成功数
            "fallback": 0,          # 降级为摘要数
            "date_failed": 0,       # 时间解析失败数（published_at=null，文档保留）
            "failed": 0,            # 无法产出（请求失败/内容空等）数
            "fetch_timeout": 0,     # 详情抓取超时/失败数
        }

    # ---------------- HTTP 基础：随机 UA + 间隔 + 重试 ---------------- #

    def _incr_stat(self, key: str, amount: int = 1) -> None:
        """线程安全地累加统计字段。"""
        with self._stats_lock:
            self.stats[key] += amount

    @staticmethod
    def _random_headers() -> dict[str, str]:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
        }

    def _respect_interval(self) -> None:
        """列表页相邻请求之间的随机延时（约 1.0~2.5 秒），礼貌爬取。"""
        wait = self.request_interval + random.uniform(0, self.interval_jitter)
        elapsed = time.monotonic() - self._last_request_ts
        if elapsed < wait:
            time.sleep(wait - elapsed)
        self._last_request_ts = time.monotonic()

    def _decode(self, resp: Any, raw: bytes) -> str:
        if resp.headers.get("Content-Encoding", "").lower() in ("gzip", "x-gzip"):
            raw = gzip.decompress(raw)
        charset = resp.headers.get_content_charset() or "utf-8"
        return raw.decode(charset, errors="replace")

    def _fetch(self, url: str) -> str | None:
        """列表页等发现类请求：带随机间隔与重试（最多 max_retries 次）。失败返回 None。"""
        for attempt in range(1, self.max_retries + 1):
            self._respect_interval()
            self._incr_stat("fetched_urls")
            req = urllib.request.Request(url, headers=self._random_headers())
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    return self._decode(resp, resp.read())
            except urllib.error.HTTPError as exc:
                code = getattr(exc, "code", None)
                if isinstance(code, int) and 400 <= code < 500 and code != 429:
                    logger.warning("抓取 %s 返回 %s，放弃", url, code)
                    return None
                logger.warning("抓取失败(%d/%d) %s: %s", attempt, self.max_retries, url, exc)
            except (TimeoutError, urllib.error.URLError, OSError) as exc:
                logger.warning("抓取失败(%d/%d) %s: %s", attempt, self.max_retries, url, exc)
            except Exception:  # noqa: BLE001
                logger.exception("抓取 %s 发生意外错误", url)
                return None
            if attempt < self.max_retries:
                time.sleep(self.backoff_base + random.uniform(0, 1))
        logger.error("抓取 %s 达到最大重试次数，放弃", url)
        return None

    def _fetch_detail(self, url: str) -> str | None:
        """详情页专用 GET：**单次请求不重试**，严格 timeout=5s。

        4xx / 超时 / 网络异常一律返回 None，由调用方 build_document 将本条
        标记 fulltext_ok=False、content 降级为摘要；不影响其他任务。
        """
        self._incr_stat("fetched_urls")
        req = urllib.request.Request(url, headers=self._random_headers())
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return self._decode(resp, resp.read())
        except urllib.error.HTTPError as exc:
            logger.warning("详情抓取 %s 返回 %s，本条标记抓取失败(fulltext_ok=false)",
                           url, getattr(exc, "code", exc))
        except (TimeoutError, socket.timeout, urllib.error.URLError, OSError) as exc:
            logger.warning("详情抓取 %s 超时/网络错误(> %ds)，本条标记抓取失败: %s",
                           url, self.timeout, exc)
        except Exception:  # noqa: BLE001 单条异常不中断整体任务
            logger.exception("详情抓取 %s 发生意外错误，本条标记抓取失败", url)
        self._incr_stat("fetch_timeout")
        return None

    # ---------------- 详情链接判定（分站点） ---------------- #

    @staticmethod
    def _is_cnrare_article(url: str) -> bool:
        try:
            parts = urlsplit(url)
        except ValueError:
            return False
        host = (parts.netloc or "").lower()
        if "regcc.cn" not in host:
            return False
        path = parts.path or ""
        if not path.endswith(".shtml"):
            return False
        # 排除列表页 / 首页 / 导航占位页
        last = path.rstrip("/").split("/")[-1]
        if re.match(r"^(list|index|tz|more)", last, re.I):
            return False
        # 详情页需位于栏目目录（/zgxtjt/xxx/）下且非目录根
        return len([s for s in path.strip("/").split("/") if s]) >= 2

    @staticmethod
    def _is_mnr_article(url: str) -> bool:
        """自然资源部详情页：文件名形如 t20260921_2938788.html / content_xxx.htm。"""
        try:
            parts = urlsplit(url)
        except ValueError:
            return False
        host = (parts.netloc or "").lower()
        if "mnr.gov.cn" not in host:
            return False
        path = (parts.path or "").lower()
        if not re.search(r"\.s?html?$", path):
            return False
        last = path.rstrip("/").split("/")[-1]
        # 详情页文件名以 t+8位日期 或 content_ 开头
        if re.match(r"^(t20\d{6}[_0-9]*|content_\d+)\.(s?html?)$", last, re.I):
            return True
        # 兼容 /YYYYMM/ 目录下非 index/list 的其它正文页
        if re.search(r"/20\d{4}/", path) and not re.match(r"^(index|list|shownews)", last, re.I):
            return len([s for s in path.strip("/").split("/") if s]) >= 3
        return False

    @staticmethod
    def _is_miit_article(url: str) -> bool:
        """工信部详情页：路径形如 /zwgk/zcwj/wjfb/tz/art/2024/art_<hash>.html。"""
        try:
            parts = urlsplit(url)
        except ValueError:
            return False
        host = (parts.netloc or "").lower()
        if "miit.gov.cn" not in host:
            return False
        path = (parts.path or "").lower()
        return bool(re.search(r"/art/20\d{2}/art_[0-9a-z]+\.html$", path))

    @staticmethod
    def _is_disr_article(url: str) -> bool:
        try:
            parts = urlsplit(url)
        except ValueError:
            return False
        host = (parts.netloc or "").lower()
        if "industry.gov.au" not in host:
            return False
        path = (parts.path or "").lower().strip("/")
        segs = [s for s in path.split("/") if s]
        # 至少两段（如 /news/xxx、/topics/mining/critical-minerals/xxx），单段多为导航/列表根
        if len(segs) < 2:
            return False
        # 政策/媒体稿/专题常见归属前缀
        if not re.search(
            r"(news|media|ministerial|minister|publication|topic|consultation|"
            r"strategy|critical-minerals|statement|speech|transcript|budget)",
            path,
        ):
            return False
        # 排除纯列表/导航页（不含具体文章 slug）
        listing_roots = (
            "news", "topics", "topics/mining", "topics/mining/critical-minerals",
            "ministerial-media", "about", "search", "groups", "home",
        )
        if path.rstrip("/") in listing_roots:
            return False
        return True

    # ---------------- 列表页发现 + 翻页 ---------------- #

    def crawl_source(self, cfg: dict[str, Any], max_candidates: int = 0) -> list[PolicyEntry]:
        """按单个数据源配置翻页发现候选政策条目（URL/标题/日期/摘要，尽力而为）。"""
        page_url: Callable[[str, int], str] = cfg["page_url"]
        is_article = self._matchers[cfg.get("matcher", "cnrare")]
        found: dict[str, PolicyEntry] = {}  # normalized_url -> entry（保序去重）

        for seed in cfg["list_seeds"]:
            zero_streak = 0
            for page_no in range(1, self.max_list_pages + 1):
                if max_candidates and len(found) >= max_candidates:
                    break
                url = page_url(seed, page_no)
                html_text = self._fetch(url)
                if html_text is None:
                    logger.warning("列表页抓取失败，停止该栏目翻页: %s", url)
                    break
                self._incr_stat("list_pages")
                try:
                    soup = BeautifulSoup(html_text, "html.parser")
                except Exception:  # noqa: BLE001
                    logger.exception("列表页解析失败: %s", url)
                    break

                page_entries = self._extract_list_items(soup, url, cfg, is_article, page_no)
                new_cnt = 0
                for entry in page_entries:
                    key = normalize_url(entry.link) or entry.link
                    if key in found:
                        continue
                    found[key] = entry
                    new_cnt += 1
                logger.info("[%s] 第 %d 页 %s: 页内 %d 篇，新增 %d 篇（累计候选 %d）",
                            cfg["source"], page_no, url, len(page_entries), new_cnt, len(found))
                if not page_entries:
                    break
                # 爬虫阶段不做时间过滤：仅当连续多页无新增（翻页到尽头/重复页）才提前收敛
                if new_cnt == 0:
                    zero_streak += 1
                    if zero_streak >= 6:
                        logger.info("[%s] 连续 %d 页无新增，提前结束翻页", cfg["source"], zero_streak)
                        break
                else:
                    zero_streak = 0

        self._incr_stat("article_urls", len(found))
        return list(found.values())

    def _extract_list_items(
        self,
        soup: BeautifulSoup,
        page_url: str,
        cfg: dict[str, Any],
        is_article: Callable[[str], bool],
        page_no: int,
    ) -> list[PolicyEntry]:
        """从列表页抽取文章链接及可见元数据（标题/日期/摘要）。"""
        for tag in soup.find_all(NOISE_TAGS):
            if tag.name == "script":  # 保留脚本以防内联日期
                continue
            tag.decompose()

        entries: list[PolicyEntry] = []
        added: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = urljoin(page_url, a["href"])
            if not is_article(href):
                continue
            norm = normalize_url(href) or href
            if norm in added:
                continue

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
            item = a.find_parent(["li", "article", "div"]) or a.find_parent("tr")
            if item is not None:
                if not title:
                    h = item.find(["h2", "h3", "h4"])
                    title = _clean_inline(h.get_text(" ", strip=True)) if h is not None else ""
                p = item.find("p")
                summary = p.get_text(" ", strip=True) if p is not None else ""
                time_tag = item.find("time")
                if time_tag is not None:
                    published = _coerce_dt(time_tag.get("datetime") or time_tag.get_text())
                if published is None:
                    published = _scan_date(item.get_text(" ", strip=True))

            # 经上面仍无日期：从标题、URL 再兜底扫一次
            if published is None:
                published = _scan_date(title)
            if published is None:
                published = _scan_date(href)

            if not title:
                continue
            added.add(norm)
            entries.append(
                PolicyEntry(
                    link=href, title=title, published_at=published,
                    summary=summary, author=None, tags=[],
                    source=cfg["source"], region=cfg["region"], page_no=page_no,
                )
            )
        return entries

    # ---------------- 详情页：正文 + 元数据 ---------------- #

    def fetch_detail(self, url: str, title: str = "") -> dict[str, Any] | None:
        """抓取政策详情页并解析：正文 content、发布时间、作者、标签、og:description。"""
        html_text = self._fetch_detail(url)
        if not html_text:
            return None
        try:
            soup = BeautifulSoup(html_text, "html.parser")
        except Exception:  # noqa: BLE001
            logger.exception("HTML 解析失败: %s", url)
            return None

        # ---- 元数据（去噪前读 head / <time>）----
        published_at = self._extract_published(soup, url, title)
        og_title = _meta_content(soup, "property", "og:title")
        summary_meta = _meta_content(soup, "property", "og:description") or _meta_content(soup, "name", "description")
        meta_author = _meta_content(soup, "name", "author") or _meta_content(soup, "name", "SiteName")

        # ---- 正文抽取（与 news.py 一致的容器 + 段落策略）----
        for tag in soup.find_all(NOISE_TAGS):
            tag.decompose()
        # 二次清洗：移除导航 / 侧边栏 / 面包屑 / 相关推荐 / 页脚类容器
        for box in soup.find_all(class_=_NOISE_CLASS_RE):
            box.decompose()
        for box in soup.find_all(id=_NOISE_CLASS_RE):
            box.decompose()
        container = (
            soup.find("article")
            or soup.find(class_=re.compile(r"(content|article|detail|news|TRS_Editor|views|field-body|zoom|Custom_Chart|xxgk_content)", re.I))
            or soup.find("main")
            or soup
        )
        paragraphs = [p.get_text(" ", strip=True) for p in container.find_all("p")]
        paragraphs = [t for t in paragraphs if len(t) >= 20]
        text = " ".join(paragraphs) if paragraphs else container.get_text(" ", strip=True)
        content = normalize_text(text, for_hash=False)

        return {
            "content": content if len(content) >= 50 else None,
            "published_at": published_at,
            "author": meta_author,
            "tags": [],
            "summary": summary_meta,
            "og_title": og_title,
        }

    @staticmethod
    def _extract_published(soup: BeautifulSoup, url: str, title: str = "") -> datetime | None:
        """多策略发布时间解析，依次：meta -> <time> -> 标题 -> 正文开头 -> 正文末尾 ->
        整页正文 -> URL，覆盖多组正则；全部失败返回 None（调用方不跳过文档）。"""
        # 1) 页面 meta
        val = _meta_content(soup, "property", "article:published_time") or _meta_content(soup, "name", "pubdate") \
            or _meta_content(soup, "name", "publishdate") or _meta_content(soup, "name", "PubDate")
        dt = _coerce_dt(val)
        if dt:
            return dt
        # 2) <time> 标签
        time_tag = soup.find("time")
        if time_tag is not None:
            dt = _coerce_dt(time_tag.get("datetime") or time_tag.get_text())
            if dt:
                return dt
        # 3) 页面标题
        dt = _scan_date(title)
        if dt:
            return dt
        og_title = _meta_content(soup, "property", "og:title")
        if og_title:
            dt = _scan_date(og_title)
            if dt:
                return dt
        # 4) 正文开头 / 正文末尾 / 整页兜底（多组正则见 _scan_date）
        body = soup.get_text(" ", strip=True)
        dt = _scan_date(body[:400]) or _scan_date(body[-300:]) or _scan_date(body)
        if dt:
            return dt
        # 5) URL 中的日期段（t20260921 / 202609 / 2026-09-21 等）
        return _scan_date(url)

    # ---------------- Document 组装（严格与 news.py 相同 Schema） ---------------- #

    def build_document(self, entry: PolicyEntry) -> dict[str, Any] | None:
        """候选条目 -> Document dict（字段与之前版本完全一致，不得改动）。

        爬虫阶段不做时间过滤；时间解析失败 -> published_at=null，文档保留。
        """
        if not entry.link:
            return None

        # 详情页一次请求：正文 + 缺失元数据（多策略解析已含标题/正文/URL 兜底）
        detail = self.fetch_detail(entry.link, entry.title)
        published_at = entry.published_at or (detail or {}).get("published_at")

        # 时间解析失败：不跳过文档，published_at=null，日志标记 + 计入统计
        if published_at is None:
            self._incr_stat("date_failed")
            logger.info("时间解析失败，published_at=null 保留文档: %s", entry.link)

        # 全文成功 -> 正文；失败 -> 降级摘要（列表摘要 / og:description）
        fulltext = (detail or {}).get("content")
        fulltext_ok = fulltext is not None
        fallback_summary = normalize_text(entry.summary, for_hash=False) \
            or normalize_text((detail or {}).get("summary") or "", for_hash=False)
        content = fulltext if fulltext_ok else fallback_summary
        if not content:
            self._incr_stat("failed")
            logger.info("正文与摘要均为空，跳过: %s", entry.link)
            return None

        title = _clean_inline(entry.title) or (detail or {}).get("og_title") or _slug_to_title(entry.link)

        try:
            doc_id = generate_doc_id("policy", {"url": entry.link})
        except Exception as exc:  # noqa: BLE001
            logger.warning("生成 doc_id 失败，跳过 %s: %s", entry.link, exc)
            self._incr_stat("failed")
            return None

        # 中文文章 language="zh"，否则按内容判定（与 news.py 规则一致）
        language = "zh" if re.search(r"[\u4e00-\u9fff]", content) else "en"

        return {
            "doc_id": doc_id,
            "source_type": "policy",
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
                "author": entry.author or (detail or {}).get("author"),
                "tags": entry.tags or (detail or {}).get("tags") or [],
                "word_count": len(content),
                "fulltext_ok": fulltext_ok,
            },
            "dedup_status": "kept",
            "duplicate_of": None,
        }

    # ---------------- 并发安全包装 ---------------- #

    def _safe_build_document(self, entry: PolicyEntry) -> dict[str, Any] | None:
        """线程安全包装：单条 URL 异常不影响其他任务继续执行。"""
        try:
            return self.build_document(entry)
        except Exception:  # noqa: BLE001
            logger.exception("并发抓取任务异常，跳过: %s", entry.link)
            self._incr_stat("failed")
            return None

    # ---------------- 顶层采集 ---------------- #

    def collect(self, sources: Iterable[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        """列表页翻页发现 -> URL 去重 -> 多线程并发单次 GET 抓取组装，按 doc_id 去重。"""
        documents: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        # 1) 各数据源分别翻页发现候选（真实网页，无模拟数据；总量不超过 max_candidates）
        candidates: list[PolicyEntry] = []
        for cfg in list(sources or POLICY_SOURCES):
            if len(candidates) >= self.max_candidates:
                logger.info("候选已达上限 %d，跳过后续数据源发现: %s", self.max_candidates, cfg["source"])
                break
            logger.info("开始翻页发现: %s (%s)", cfg["source"], cfg["list_seeds"])
            remaining = self.max_candidates - len(candidates)
            candidates.extend(self.crawl_source(cfg, max_candidates=remaining))
        if len(candidates) > self.max_candidates:
            candidates = candidates[:self.max_candidates]

        # 2) 候选 URL 提前去重（同一文件挂在多栏目时只抓一次）
        unique_candidates: list[PolicyEntry] = []
        dedup_keys: set[str] = set()
        for entry in candidates:
            norm = normalize_url(entry.link) or entry.link
            if not norm or norm in dedup_keys:
                continue
            dedup_keys.add(norm)
            unique_candidates.append(entry)
        # 已知发布时间倒序（新的先抓，无日期的排最后）
        unique_candidates.sort(
            key=lambda e: e.published_at or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        logger.info("候选政策合计 %d 篇（上限 %d），去重后 %d 篇，开始并发抓取详情页（max_workers=%d, timeout=%ds）",
                    len(candidates), self.max_candidates, len(unique_candidates),
                    self.max_workers, self.timeout)

        # 3) 线程池并发抓取详情页并组装 Document（无 HEAD 预探测；单条失败降级 fulltext_ok=false，不中断整体）
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
            logger.warning("采集数量 %d < 目标 %d，可增大 max_list_pages 或扩充 list_seeds 栏目种子",
                           len(documents), self.target)
        return documents

    # ---------------- 统计输出 ---------------- #

    def log_stats(self) -> None:
        s = self.stats
        logger.info(
            "采集统计 | 列表页 %d 页 | 候选政策 %d 篇 | 总请求 URL %d 个 | "
            "成功 %d 条 | 全文成功 %d / 降级摘要 %d | "
            "详情超时/失败 %d | 时间解析失败 %d | 失败 %d",
            s["list_pages"], s["article_urls"], s["fetched_urls"],
            s["kept"], s["fulltext_ok"], s["fallback"],
            s["fetch_timeout"], s["date_failed"], s["failed"],
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
        scanned = _scan_date(s)
        if scanned is not None:
            return scanned
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


def _scan_date(text: str) -> datetime | None:
    """从任意文本中按多套正则兜底扫描第一个可识别日期，返回 UTC datetime（当日 00:00）或 None。

    依次尝试：中文/年在前数字日期 -> 紧凑 8 位数字 -> 英文月名日期，最大化标题/正文/URL 命中。
    """
    if not text:
        return None
    # 1) 2025-06-05 / 2025-6-5 / 2025年6月5日 / 2025.06.05
    m = _CN_DATE_RE.search(text) or _NUM_DATE_RE.search(text)
    if m:
        y, mo, d = (int(x) for x in m.groups())
        try:
            return datetime(y, mo, d, tzinfo=timezone.utc)
        except ValueError:
            pass
    # 2) 紧凑 20250605（URL / 时间戳 / t20260921_xxx.html）
    m = _COMPACT_DATE_RE.search(text)
    if m:
        y, mo, d = (int(x) for x in m.groups())
        try:
            return datetime(y, mo, d, tzinfo=timezone.utc)
        except ValueError:
            pass
    # 3) 5 June 2025 / June 5, 2025
    m = _EN_DATE_RE.search(text)
    if m:
        if m.group(1):  # 5 June 2025
            d, mon_s, y = m.group(1), m.group(2), m.group(3)
        else:           # June 5, 2025
            mon_s, d, y = m.group(4), m.group(5), m.group(6)
        mo = _MONTHS.get(mon_s[:3].lower())
        if mo:
            try:
                return datetime(int(y), mo, int(d), tzinfo=timezone.utc)
            except ValueError:
                pass
    return None


def _slug_to_title(url: str) -> str:
    """从 URL slug 兜底生成标题。"""
    path = urlsplit(url).path.strip("/").split("/")[-1] or ""
    path = re.sub(r"\.s?html?$", "", path)
    return path.replace("-", " ").replace("_", " ").strip().title()


def _domain(url: str) -> str | None:
    try:
        netloc = urlsplit(url).netloc.lower()
        return netloc[4:] if netloc.startswith("www.") else netloc
    except ValueError:
        return None


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


# --------------------------------------------------------------------------- #
# 便捷入口
# --------------------------------------------------------------------------- #

def collect_policy(
    target: int = 200,
    max_list_pages: int = 60,
    max_workers: int = 4,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """一行调用：真实采集关键矿产政策 Document 列表（采集完成后打印统计）。

    爬虫阶段不做时间过滤；时间窗口筛选放到后续 clean 清洗阶段。
    """
    crawler = PolicyCrawler(target=target, max_list_pages=max_list_pages,
                             max_workers=max_workers, now=now)
    documents = crawler.collect()
    crawler.log_stats()
    return documents


# --------------------------------------------------------------------------- #
# 调用示例（真实网络采集）
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    # 真实采集：四源列表页翻页发现 URL -> 去重 -> 并发抓取（无 HEAD 预探测） -> 组装 Document
    # 不做发布时间过滤：解析失败 published_at=null，时间筛选交给后续 clean 阶段
    crawler = PolicyCrawler(target=200, max_list_pages=60, max_workers=4)
    documents = crawler.collect()
    crawler.log_stats()  # 候选 / 成功 / 全文成功 / 降级摘要 / 超时失败 / 时间解析失败 等

    # 导出 policy_raw.json（结构与之前版本完全一致，仅 source_type=policy）
    with open("policy_raw.json", "w", encoding="utf-8") as f:
        json.dump(documents, f, ensure_ascii=False, indent=2)
    logger.info("已导出 policy_raw.json，共 %d 条", len(documents))

    # 预览前 2 条结构化 Document
    for d in documents[:2]:
        print(json.dumps(d, ensure_ascii=False, indent=2))

    # 返回的 documents 可直接交给 pipeline.clean.clean_documents 做后续去重与时间过滤（本模块不入库/不切片）
