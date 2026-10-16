"""pipeline/crawlers/price.py — 商品价格行情数据源采集器（LME / SHFE / GFEX / 上海钢联）。

采集策略（v1，真实抓取，与 news.py v5 / policy.py v4 架构对齐，**爬虫阶段不做时间过滤**）：
    数据源（真实可达性已于 2026-10 核实，不可达源自动跳过、**绝不生成模拟数据**）：
    1. LME 铜/锌/镍：https://www.lme.com/api/prices/closingprices/{metal}/non-ferrous
       （实测被 Cloudflare 403 拦截；保留配置与解析器，本机拿到 403 时仅记失败、不产假数据）；
    2. SHFE 上期所每日行情：https://www.shfe.com.cn/data/tradedata/future/dailydata/kx{YYYYMMDD}.dat
       （官方 JSON 日报，实测可用；按产品组取 铜cu/锌zn/镍ni/锂lc —— 注：碳酸锂当前挂牌于
       广州期货交易所 GFEX，上期所数据暂无 lc 组，保留查找逻辑，未来收录自动生效）；
    3. GFEX 广期所碳酸锂 lc：gx{YYYYMMDD}.dat 同构接口（本机 SSL 证书链校验失败/接口返回 SPA
       时自动跳过，不生成模拟文档）；
    4. 上海钢联 Mysteel 铁矿石频道：https://tks.mysteel.com/ 静态列表页（实测可抓取，
       提取"X月X日Mysteel铁矿石…"行情条目；标题中可解析出价格数字则填充字段，否则置 None）。

    每条行情记录(QuoteRecord) -> 一条 Document（source_type="price"）：
    - 结构化字段：品种名称、交易日期、收盘价、涨跌、成交量 -> 组装为可读 content 正文；
    - published_at 使用行情交易日期；无法解析则 null，文档保留不跳过；
    - 并发 ThreadPoolExecutor(max_workers=2)，单次 GET 严格 timeout=6s，不重试；
      全局共享随机限速（约 2.0~3.5 秒/请求）规避接口频控；单条失败/异常不中断整体任务；
    - doc_id / content_hash / metadata / dedup_status 生成逻辑与 pipeline.clean、
      news.py、policy.py 完全一致 —— **Document Schema 与输出字段严格不变**；
      metadata.tags 携带 [品种, 交易所] 标记商品品种与交易所（Schema 字段不改动）。

**只做采集与结构化，不做清洗 / 切片 / 向量化 / 入库。**

依赖清单：
    - 必需：Python >= 3.12；beautifulsoup4（Mysteel 列表页解析）
      HTTP 请求使用标准库 urllib（随机 UA / 全局限速 / 严格超时）
      并发使用 concurrent.futures.ThreadPoolExecutor（2 线程）
    - 内部复用：pipeline.clean 的 normalize_url / generate_doc_id / compute_content_hash / normalize_text

调用示例见文件末尾 `if __name__ == "__main__":`。
"""

from __future__ import annotations

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
from dataclasses import dataclass, field as dc_field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from urllib.parse import urlsplit

from bs4 import BeautifulSoup

# 复用清洗层公共算法，保证主键/哈希与 news/policy 一致
from pipeline.clean import (
    compute_content_hash,
    generate_doc_id,
    normalize_text,
    normalize_url,
)

logger = logging.getLogger("pipeline.crawlers.price")

__all__ = ["PriceCrawler", "PRICE_SOURCES", "collect_price"]

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

# LME 金属（英文源，language="en"）
_LME_METALS: dict[str, str] = {"copper": "铜", "zinc": "锌", "nickel": "镍"}

# SHFE 产品组 -> 品种名（中文源，language="zh"）
# 注：碳酸锂(lc)当前挂牌于 GFEX 而非 SHFE，SHFE 数据中出现 lc 组时会被自动收录。
_SHFE_GROUPS: dict[str, str] = {"cu": "铜", "zn": "锌", "ni": "镍", "lc": "碳酸锂"}

# GFEX 产品 -> 品种名（本机网络受限时自动跳过）
_GFEX_GROUPS: dict[str, str] = {"lc": "碳酸锂"}

# 数据源注册表：kind 决定任务构造与解析器
PRICE_SOURCES: list[dict[str, Any]] = [
    {
        "kind": "lme",
        "source": "lme_quote",
        "exchange": "LME（伦敦金属交易所）",
        "region": "UK",
        "language": "en",
        "metals": _LME_METALS,
        "api": "https://www.lme.com/api/prices/closingprices/{metal}/non-ferrous",
    },
    {
        "kind": "shfe",
        "source": "shfe_quote",
        "exchange": "SHFE（上期所）",
        "region": "CN",
        "language": "zh",
        "groups": _SHFE_GROUPS,
        "api": "https://www.shfe.com.cn/data/tradedata/future/dailydata/kx{date}.dat",
    },
    {
        "kind": "gfex",
        "source": "gfex_quote",
        "exchange": "GFEX（广期所）",
        "region": "CN",
        "language": "zh",
        "groups": _GFEX_GROUPS,
        "api": "https://www.gfex.com.cn/data/tradedata/future/dailydata/gx{date}.dat",
    },
    {
        "kind": "mysteel",
        "source": "mysteel_iron_ore",
        "exchange": "Mysteel（上海钢联）",
        "region": "CN",
        "language": "zh",
        "symbol": "铁矿石",
        "list_pages": [
            "https://tks.mysteel.com/",   # 铁矿石频道首页（实测 SSR 列表可提取当日全部行情条目）
        ],
    },
]


# --------------------------------------------------------------------------- #
# 行情记录模型
# --------------------------------------------------------------------------- #

@dataclass
class QuoteRecord:
    """一条行情时序记录：转换为一条 source_type="price" 的 Document。"""

    symbol: str                      # 品种名称（铜/锌/镍/碳酸锂/铁矿石）
    exchange: str                    # 交易所/机构（中文展示名）
    url: str                         # 数据来源 URL（列表页或数据文件地址）
    source: str                      # 数据源标识
    region: str                      # CN / UK
    language: str                    # zh / en
    trade_date: datetime | None      # 行情交易日期（解析失败=None，文档保留）
    contract: str | None = None      # 合约代码（cu2510 / 期货指数等）
    close: float | None = None       # 收盘价
    change: float | None = None      # 涨跌（相对前结算/前收）
    volume: float | None = None      # 成交量
    price_type: str = "close"        # 价格类型（close/settlement/spot/index，参与 doc_id 稳定键）
    unit: str = ""                   # 计价单位（元/吨、USD/t 等）
    title: str = ""                  # 条目标题（Mysteel 文章标题等）
    extra: dict[str, Any] = dc_field(default_factory=dict)


# --------------------------------------------------------------------------- #
# 采集器
# --------------------------------------------------------------------------- #

class PriceCrawler:
    """行情任务并发抓取（2 线程 + 全局限速）-> QuoteRecord -> Document 组装。

    Args:
        target: 目标 Document 条数（达到后停止组装）。
        recent_days: 交易所日频接口回退采集的天数窗口（默认 30；仅决定请求哪些
            交易日文件，**不据此丢弃解析失败的记录**）。
        max_workers: 行情请求并发线程数（默认 2，规避接口频控）。
        timeout: 单次 GET 严格超时（秒，默认 6）。
        request_interval / interval_jitter: 全局共享限速间隔下限与随机抖动上限（秒），
            默认 2.0 + uniform(0, 1.5) 即约 2.0~3.5 秒一个请求。
        now: 注入的当前时间（UTC），便于测试；默认真实 UTC now。
    """

    def __init__(
        self,
        target: int = 200,
        recent_days: int = 30,
        max_workers: int = 2,
        timeout: int = 6,
        request_interval: float = 2.0,
        interval_jitter: float = 1.5,
        now: datetime | None = None,
    ) -> None:
        self.target = target
        self.recent_days = recent_days
        self.max_workers = max_workers
        self.timeout = timeout
        self.request_interval = request_interval
        self.interval_jitter = interval_jitter
        self.now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        self._last_request_ts = 0.0
        self._stats_lock = threading.Lock()
        self._rate_lock = threading.Lock()
        # 采集统计（与 news/policy 同风格）
        self.stats: dict[str, int] = {
            "requests_ok": 0,        # 成功的数据请求数（日文件/列表页/API）
            "requests_failed": 0,    # 失败/超时/不可达的请求数
            "records": 0,            # 解析出的候选行情记录数（去重前）
            "kept": 0,               # 成功产出的 Document 数
            "date_failed": 0,        # 交易日期解析失败数（published_at=null，文档保留）
            "no_price": 0,           # 收盘价未解析出数（字段置 None，文档保留）
            "failed": 0,             # 无法产出 Document 的记录数
        }

    # ---------------- HTTP 基础：随机 UA / 全局限速 / 单次 GET ---------------- #

    def _incr_stat(self, key: str, amount: int = 1) -> None:
        """线程安全地累加统计字段。"""
        with self._stats_lock:
            self.stats[key] += amount

    @staticmethod
    def _random_headers() -> dict[str, str]:
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json, text/html, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        }

    def _respect_interval(self) -> None:
        """全局共享限速：任意两次请求之间至少间隔随机 2.0~3.5 秒（跨线程串行化）。"""
        with self._rate_lock:
            wait = self.request_interval + random.uniform(0, self.interval_jitter)
            elapsed = time.monotonic() - self._last_request_ts
            if elapsed < wait:
                time.sleep(wait - elapsed)
            self._last_request_ts = time.monotonic()

    def _fetch_once(self, url: str) -> str | None:
        """单次 GET 不重试，严格 timeout=6s；4xx/超时/异常记 requests_failed 返回 None。"""
        self._respect_interval()
        req = urllib.request.Request(url, headers=self._random_headers())
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
                charset = resp.headers.get_content_charset() or "utf-8"
                self._incr_stat("requests_ok")
                return raw.decode(charset, errors="replace")
        except urllib.error.HTTPError as exc:
            logger.warning("行情请求 %s 返回 %s，本条跳过: %s",
                           url, getattr(exc, "code", exc), str(exc)[:60])
        except (TimeoutError, socket.timeout, urllib.error.URLError, OSError) as exc:
            logger.warning("行情请求 %s 超时/网络错误(> %ds)，本条跳过: %s",
                           url, self.timeout, str(exc)[:80])
        except Exception:  # noqa: BLE001 单条异常不中断整体任务
            logger.exception("行情请求 %s 发生意外错误，本条跳过", url)
        self._incr_stat("requests_failed")
        return None

    # ---------------- 各数据源解析器（任务 -> QuoteRecord 列表） ---------------- #

    def _task_shfe(self, cfg: dict[str, Any], date_str: str) -> list[QuoteRecord]:
        """SHFE/GFEX 日频行情文件：kx/gx{YYYYMMDD}.dat -> 指定产品组的逐合约记录。"""
        url = cfg["api"].format(date=date_str)
        text = self._fetch_once(url)
        if text is None:
            return []
        try:
            data = json.loads(text.strip())
        except (json.JSONDecodeError, ValueError):
            # GFEX 本机受限时返回的是 SPA HTML：解析失败即跳过，不生成模拟数据
            logger.info("行情文件非 JSON（可能被网关/SPA 拦截），跳过: %s", url)
            return []
        trade_day = _coerce_dt(str(data.get("report_date") or "")) or _coerce_dt(date_str)
        records: list[QuoteRecord] = []
        for inst in data.get("o_curinstrument") or []:
            gid = str(inst.get("PRODUCTGROUPID") or "")
            if gid not in cfg["groups"]:
                continue  # 只收指定品种产品组
            contract = f"{gid}{inst.get('DELIVERYMONTH', '')}".replace("_", "")
            close = _to_float(inst.get("CLOSEPRICE"))
            change = _to_float(inst.get("ZD1_CHG"))
            volume = _to_float(inst.get("VOLUME"))
            if close is None:
                self._incr_stat("no_price")
            records.append(QuoteRecord(
                symbol=cfg["groups"][gid],
                exchange=cfg["exchange"],
                url=url,
                source=cfg["source"],
                region=cfg["region"],
                language=cfg["language"],
                trade_date=trade_day,
                contract=contract,
                close=close,
                change=change,
                volume=volume,
                unit="元/吨",
                title=f"{cfg['exchange']}{cfg['groups'][gid]}期货合约{contract} {date_str} 收盘行情",
            ))
        return records

    def _task_lme(self, cfg: dict[str, Any], metal: str) -> list[QuoteRecord]:
        """LME 收盘价 API（英文源）。被 403 拦截时 _fetch_once 已记失败，返回空。"""
        url = cfg["api"].format(metal=metal)
        text = self._fetch_once(url)
        if text is None:
            return []
        try:
            data = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            logger.info("LME API 返回非 JSON，跳过: %s", url)
            return []
        cn_name = _LME_METALS.get(metal, metal)
        records: list[QuoteRecord] = []
        items = data if isinstance(data, list) else (
            data.get("values") or data.get("prices") or data.get("records") or [])
        for it in items:
            if not isinstance(it, dict):
                continue
            day = _coerce_dt(str(it.get("date") or it.get("pricesDate") or ""))
            close = _to_float(it.get("closingPrice") or it.get("close")
                              or it.get("buy") or it.get("seller"))
            change = _to_float(it.get("change") or it.get("closingChange"))
            volume = _to_float(it.get("volume") or it.get("totalOpen"))
            if close is None:
                self._incr_stat("no_price")
            records.append(QuoteRecord(
                symbol=cn_name,
                exchange=cfg["exchange"],
                url=url,
                source=cfg["source"],
                region=cfg["region"],
                language=cfg["language"],
                trade_date=day,
                contract="LME Cash",
                close=close,
                change=change,
                volume=volume,
                unit="USD/t",
                title=f"LME {metal.capitalize()} cash settlement {it.get('date', '')}".strip(),
            ))
        return records

    def _task_mysteel(self, cfg: dict[str, Any], page_url: str) -> list[QuoteRecord]:
        """Mysteel 铁矿石频道列表页：提取"X月X日Mysteel铁矿石…"行情条目。"""
        text = self._fetch_once(page_url)
        if text is None:
            return []
        try:
            soup = BeautifulSoup(text, "html.parser")
        except Exception:  # noqa: BLE001
            logger.exception("Mysteel 列表页解析失败: %s", page_url)
            return []
        year = self.now.year
        records: list[QuoteRecord] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            title = a.get_text(" ", strip=True)
            if not title or title in seen:
                continue
            m = re.match(r"^(\d{1,2})月(\d{1,2})日", title)
            if not m or ("铁矿石" not in title and "铁矿" not in title):
                continue
            seen.add(title)
            # 交易日期：标题"X月X日" + 当前年份（月末回绕保护：月份>当前月则取去年）
            mon, day = int(m.group(1)), int(m.group(2))
            yy = year - 1 if (mon, day) > (self.now.month, self.now.day) else year
            try:
                trade_day = datetime(yy, mon, day, tzinfo=timezone.utc)
            except ValueError:
                trade_day = None
            # 尽力从标题提取数值字段（如 "指数101.2，跌0.05"）
            m = re.search(r"(?:指数|价格|报价)[为:：]?\s*(-?\d[\d,]*\.?\d*)", title) \
                or re.search(r"(-?\d[\d,]*\.\d+)", title)
            close = float(m.group(1).replace(",", "")) if m else None
            chg_m = re.search(r"[涨跌]\s*(-?\d[\d,]*\.?\d*)", title)
            vol_m = re.search(r"(?:成交量|成交)[为:：]?\s*(-?[\d,]+\.?\d*)", title)
            if close is None:
                self._incr_stat("no_price")
            # 类目（去日期前缀后的标题主体）：作为 contract 槽位参与记录/doc_id 去重，
            # 使同日多条不同行情条目（港口指数/远期指数/区域日评等）互不遮挡
            category = re.sub(r"^\d{1,2}月\d{1,2}日Mysteel", "", title).strip(" ：:，,")[:30] or "行情"
            records.append(QuoteRecord(
                symbol=cfg["symbol"],
                exchange=cfg["exchange"],
                url=page_url,
                source=cfg["source"],
                region=cfg["region"],
                language=cfg["language"],
                trade_date=trade_day,
                contract=category,
                close=float(close) if close is not None else None,
                change=float(chg_m.group(1).replace(",", "")) if chg_m else None,
                volume=float(vol_m.group(1).replace(",", "")) if vol_m else None,
                price_type="index" if "指数" in title else "spot",
                unit="元/干吨 或 USD/dmt",
                title=title,
                extra={"article_href": urljoin_safe(page_url, a["href"])},
            ))
        logger.info("Mysteel 列表页 %s 提取铁矿石行情条目 %d 条", page_url, len(records))
        return records

    # ---------------- 任务构造 ---------------- #

    def _build_tasks(self) -> list[tuple[Callable[[], list[QuoteRecord]], str]]:
        """按数据源构造行情请求任务：SHFE/GFEX 逐交易日、LME 逐金属、Mysteel 逐列表页。"""
        tasks: list[tuple[Callable[[], list[QuoteRecord]], str]] = []
        trading_dates = self._recent_trading_dates()
        for cfg in PRICE_SOURCES:
            kind = cfg["kind"]
            if kind == "shfe":
                for ds in trading_dates:
                    tasks.append((lambda c=cfg, d=ds: self._task_shfe(c, d), f"SHFE {ds}"))
            elif kind == "gfex":
                for ds in trading_dates:
                    tasks.append((lambda c=cfg, d=ds: self._task_shfe(c, d), f"GFEX {ds}"))
            elif kind == "lme":
                for metal in cfg["metals"]:
                    tasks.append((lambda c=cfg, m=metal: self._task_lme(c, m), f"LME {metal}"))
            elif kind == "mysteel":
                for page in cfg["list_pages"]:
                    tasks.append((lambda c=cfg, p=page: self._task_mysteel(c, p), f"MYSTEEL {page}"))
        return tasks

    def _recent_trading_dates(self) -> list[str]:
        """近 recent_days 天内的候选交易日（YYYYMMDD，剔除周末；节假日由 404 自然跳过）。"""
        dates: list[str] = []
        for offset in range(0, self.recent_days):
            day = self.now - timedelta(days=offset)
            if day.weekday() < 5:  # 周一~周五
                dates.append(day.strftime("%Y%m%d"))
        return dates

    # ---------------- Document 组装（字段规则与 news/policy 完全一致） ---------------- #

    def build_document(self, rec: QuoteRecord) -> dict[str, Any] | None:
        """行情记录 -> Document dict（严格符合 Schema；爬虫阶段不做时间过滤）。"""
        if not rec.url or not rec.symbol:
            self._incr_stat("failed")
            return None

        published_at = rec.trade_date
        if published_at is None:
            self._incr_stat("date_failed")
            logger.info("时间解析失败，published_at=null 保留文档: %s %s", rec.symbol, rec.title[:40])

        content = self._compose_content(rec)
        if not content:
            self._incr_stat("failed")
            logger.info("行情正文为空，跳过: %s", rec.title[:40])
            return None
        content = normalize_text(content, for_hash=False)
        summary = normalize_text(rec.title, for_hash=False)[:120] or None

        title = rec.title or f"{rec.exchange}{rec.symbol} 行情记录"
        # doc_id 稳定键沿用 pipeline.clean 的 price 规则：source|symbol|trade_date|price_type
        try:
            doc_id = generate_doc_id("price", {
                "source": rec.source,
                "symbol": rec.contract or rec.symbol,
                "trade_date": rec.trade_date.strftime("%Y%m%d") if rec.trade_date else "",
                "price_type": rec.price_type,
            })
        except Exception as exc:  # noqa: BLE001
            logger.warning("生成 doc_id 失败，跳过 %s %s: %s", rec.symbol, rec.title[:40], exc)
            self._incr_stat("failed")
            return None

        has_quote = rec.close is not None
        return {
            "doc_id": doc_id,
            "source_type": "price",
            "source": rec.source,
            "title": title,
            "url": normalize_url(rec.url) or rec.url,
            "content": content,
            "summary": summary,
            "published_at": published_at.astimezone(timezone.utc).isoformat()
            if published_at is not None else None,
            "crawled_at": _iso(self.now),
            "language": rec.language,
            "content_hash": compute_content_hash(content),
            "metadata": {
                "site_domain": _domain(rec.url),
                "author": None,
                # 用 tags 承载"商品品种 + 交易所"标记（Schema 字段结构不变）
                "tags": [rec.symbol, rec.exchange],
                "word_count": len(content),
                "fulltext_ok": bool(has_quote),
            },
            "dedup_status": "kept",
            "duplicate_of": None,
        }

    @staticmethod
    def _compose_content(rec: QuoteRecord) -> str:
        """结构化字段 -> 可读行情正文（缺失字段以"暂无"占位，不留空）。"""
        day = rec.trade_date.strftime("%Y-%m-%d") if rec.trade_date else "日期暂无"
        close_s = f"{rec.close:,.2f}" if rec.close is not None else "暂无"
        chg_s = f"{rec.change:+,.2f}" if rec.change is not None else "暂无"
        vol_s = f"{rec.volume:,.0f}" if rec.volume is not None else "暂无"
        unit = rec.unit or ""
        if rec.language == "en":
            return (
                f"{rec.title}. Exchange: {rec.exchange}. Commodity: {rec.symbol}. "
                f"Trading date: {day}. Closing price: {close_s} {unit}. "
                f"Change: {chg_s}. Volume: {vol_s}."
            ).replace("  ", " ")
        contract = f"（合约 {rec.contract}）" if rec.contract else ""
        return (
            f"{rec.symbol}{contract}，{rec.exchange}，交易日 {day}："
            f"收盘价 {close_s} {unit}，涨跌 {chg_s}，成交量 {vol_s} 手。"
            f"来源页面：{rec.url}"
        )

    # ---------------- 顶层采集 ---------------- #

    def collect(self) -> list[dict[str, Any]]:
        """并发抓取全部行情任务 -> 记录去重 -> 组装 Document（单条失败不中断整体）。"""
        tasks = self._build_tasks()
        logger.info("行情采集任务合计 %d 个（近 %d 交易日窗口，max_workers=%d, timeout=%ds）",
                    len(tasks), self.recent_days, self.max_workers, self.timeout)

        # 1) 线程池并发执行行情请求（全局限速在任务内部串行化请求节奏）
        records: list[QuoteRecord] = []
        executor = ThreadPoolExecutor(max_workers=self.max_workers)
        try:
            future_to_name = {executor.submit(_fn): _name for _fn, _name in tasks}
            done_cnt = 0
            for future in as_completed(future_to_name):
                done_cnt += 1
                try:
                    records.extend(future.result() or [])
                except Exception:  # noqa: BLE001 单任务异常不阻塞整体
                    logger.exception("行情任务异常，跳过: %s", future_to_name[future])
                    self._incr_stat("requests_failed")
                if done_cnt % 25 == 0:
                    logger.info("进度: 行情任务 %d/%d，累计候选记录 %d 条",
                                done_cnt, len(tasks), len(records))
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
        self._incr_stat("records", len(records))

        # 2) 记录去重（交易所+条目唯一标识+交易日）
        unique: list[QuoteRecord] = []
        seen: set[str] = set()
        for rec in records:
            tag = rec.title or rec.contract or rec.symbol
            key = f"{rec.exchange}|{tag}|{rec.trade_date:%Y%m%d}" \
                if rec.trade_date else f"{rec.exchange}|{tag}|{rec.url}"
            if key in seen:
                continue
            seen.add(key)
            unique.append(rec)
        # 已知交易日期倒序（新的先入库；无日期的排最后，但保留不丢弃）
        unique.sort(key=lambda r: r.trade_date or datetime.min.replace(tzinfo=timezone.utc),
                    reverse=True)
        logger.info("候选行情记录合计 %d 条，去重后 %d 条，开始组装 Document",
                    len(records), len(unique))

        # 3) 组装 Document（纯本地操作，不再发请求），达到 target 即停
        documents: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for rec in unique:
            if len(documents) >= self.target:
                break
            doc = self.build_document(rec)
            if doc is None:
                continue
            if doc["doc_id"] in seen_ids:
                continue
            seen_ids.add(doc["doc_id"])
            documents.append(doc)
            self._incr_stat("kept")
            if len(documents) % 50 == 0:
                logger.info("进度: 已采集 %d / %d", len(documents), self.target)

        if len(documents) < self.target:
            logger.warning("采集数量 %d < 目标 %d，可增大 recent_days 或确认 LME/GFEX 网络可达性",
                           len(documents), self.target)
        return documents

    # ---------------- 统计输出 ---------------- #

    def log_stats(self) -> None:
        s = self.stats
        logger.info(
            "采集统计 | 行情请求 成功 %d / 失败 %d | 候选行情 %d 条 | 成功 %d 条 | "
            "收盘价缺失 %d | 时间解析失败 %d | 失败 %d",
            s["requests_ok"], s["requests_failed"], s["records"], s["kept"],
            s["no_price"], s["date_failed"], s["failed"],
        )


# --------------------------------------------------------------------------- #
# 工具函数
# --------------------------------------------------------------------------- #

def _to_float(value: Any) -> float | None:
    """尽力把接口字段值转 float（兼容逗号分隔与空串/字符串数字）。"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().replace(",", "").replace("%", "")
    if not s or s.upper() in ("--", "---", "NIL", "TASVOLUME"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _coerce_dt(value: str | None) -> datetime | None:
    """尽力把日期字符串解析为 UTC datetime（支持 20261015 / 2026-10-15 等格式）。"""
    if not value:
        return None
    s = str(value).strip().replace("Z", "+00:00")
    dt = None
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d", "%d %b %Y", "%B %d, %Y"):
            try:
                dt = datetime.strptime(s[:10] if "%Y%m%d" != fmt else s, fmt)
                break
            except ValueError:
                continue
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _domain(url: str) -> str | None:
    try:
        netloc = urlsplit(url).netloc.lower()
        return netloc[4:] if netloc.startswith("www.") else netloc
    except ValueError:
        return None


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def urljoin_safe(base: str, href: str) -> str:
    """相对链接转绝对（异常回退原值）。"""
    try:
        from urllib.parse import urljoin
        return urljoin(base, href)
    except Exception:  # noqa: BLE001
        return href


# --------------------------------------------------------------------------- #
# 便捷入口
# --------------------------------------------------------------------------- #

def collect_price(
    target: int = 200,
    recent_days: int = 30,
    max_workers: int = 2,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """一行调用：采集近 recent_days 天行情 Document 列表（完成后打印统计）。"""
    crawler = PriceCrawler(target=target, recent_days=recent_days,
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

    # 真实采集：构造近30交易日行情请求任务 -> 2线程限速并发 -> QuoteRecord 去重 -> 组装 Document
    # LME(403)/GFEX(证书受限) 源自动跳过并计入 requests_failed，不生成模拟数据
    crawler = PriceCrawler(target=200, recent_days=30, max_workers=2, timeout=6)
    documents = crawler.collect()
    crawler.log_stats()  # 候选行情 / 成功 / 收盘价缺失 / 时间解析失败 等

    # 导出 price_raw.json（Schema 与 news_raw.json / policy_raw.json 完全一致，仅 source_type=price）
    with open("price_raw.json", "w", encoding="utf-8") as f:
        json.dump(documents, f, ensure_ascii=False, indent=2)
    logger.info("已导出 price_raw.json，共 %d 条", len(documents))

    # 预览前 2 条结构化 Document
    for d in documents[:2]:
        print(json.dumps(d, ensure_ascii=False, indent=2))

    # 返回的 documents 可直接交给 pipeline.clean.clean_documents 做后续清洗去重（本模块不入库/不切片）
