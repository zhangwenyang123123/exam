"""矿业 RAG 三源（news / policy / price）统一清洗 + 去重层。

职责（见 DATA_NOTES.md §3 / §4）：
    1. html 清洗、网页模板噪音行过滤、文本归一化（全角转半角 / 折叠空白）；
    2. URL 归一化 + 稳定 doc_id（{source_type}-{sha1(稳定键)[:12]}）；
    3. 业务事件时间解析：解析失败**不丢弃**，published_at 置 null；
       仅对非 null 值执行 recent_days 时间窗口过滤；
    4. price 专用：build_price_content / build_price_title 组装可读正文，
       crawled_at 读取原始记录，summary 与 content 相同；
    5. metadata 三源统一回填 word_count；
    6. 两级去重：L1 doc_id 精确去重（latest-wins） + L2 content_hash 内容哈希去重；
    7. 顶层入口 clean_documents：合并三源 -> 清洗 -> 去重 -> 导出 kept 文档。

**只负责清洗与去重，不做切片 / 向量化 / 入库。**
"""

from __future__ import annotations

import html
import json
import logging
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from hashlib import sha1, sha256
from typing import Any, Iterable
from urllib.parse import urlsplit, urlunsplit

__all__ = [
    "clean_documents",
    "CleanConfig",
    "CleanError",
    "normalize_text",
    "normalize_url",
    "generate_doc_id",
    "compute_content_hash",
]

logger = logging.getLogger("pipeline.clean")

# --------------------------------------------------------------------------- #
# 常量与配置
# --------------------------------------------------------------------------- #

VALID_SOURCE_TYPES = {"news", "policy", "price"}
SOURCE_TYPES = ("news", "policy", "price")
VALID_LANGUAGE = {"zh", "en"}

# 品种代码 -> 中文显示名（build_price_title 使用）
_SYMBOL_ZH = {
    "CU": "铜", "ZN": "锌", "NI": "镍", "LC": "碳酸锂", "FE": "铁矿石",
}

# 网页模板噪音行（命中即整行丢弃），中英常见样板
_NOISE_PATTERNS = [
    r"^\s*(cookie|cookies)\b.*",
    r".*all rights reserved.*",
    r".*版权所有.*",
    r".*免责声明.*",
    r".*版权声明.*",
    r"^\s*(subscribe|sign up|sign in|log ?in|register)\b.*",
    r"^\s*(follow (us|on)|share (this|on)|newsletter)\b.*",
    r"^\s*(advertisement|sponsored content)\b.*",
    r"^\s*(javascript|enable javascript)\b.*",
    r"^\s*(privacy policy|terms of (use|service))\b.*",
    r"^\s*(联系我们|关于我们|网站导航|首页)\s*$",
]
_NOISE_RE = [re.compile(p, re.IGNORECASE) for p in _NOISE_PATTERNS]

# 需要从 URL query 中剔除的跟踪参数前缀
_TRACKING_PARAMS = ("utm_", "share_", "ref", "referer", "source", "fbclid", "gclid")

# price values 常用字段顺序（content 组装时按此顺序输出）
_PRICE_VALUE_ORDER = ("settlement", "close", "open", "high", "low", "volume", "change", "change_pct")


class CleanError(Exception):
    """清洗过程中的可恢复错误（记录被丢弃，不中断管线）。"""


class CleanConfig:
    """清洗管线配置。

    Args:
        recent_days: 时间窗口天数，published_at 非 null 且早于 (now - recent_days) 丢弃。
        min_content_chars: news/policy 清洗后正文最小字符数，低于此值判为无效丢弃。
        min_price_chars: price 组装正文最小字符数，低于此值丢弃。
        now: 注入的"当前时间"（UTC），便于测试与复现；默认取真实当前 UTC。
    """

    def __init__(
        self,
        recent_days: int = 30,
        min_content_chars: int = 50,
        min_price_chars: int = 20,
        now: datetime | None = None,
    ) -> None:
        self.recent_days = recent_days
        self.min_content_chars = min_content_chars
        self.min_price_chars = min_price_chars
        self.now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        self._cutoff = self.now - timedelta(days=recent_days)

    @property
    def cutoff(self) -> datetime:
        return self._cutoff


# --------------------------------------------------------------------------- #
# 文本 / URL 归一化（爬虫仍从本模块导入，规则不变）
# --------------------------------------------------------------------------- #

def _strip_html(raw: str) -> str:
    """去除噪音容器与所有 HTML 标签，反转义实体，并保留块级换行。

    先整体丢弃 script/style/nav/footer/header/aside 等模板容器，再把块级闭合标签
    与 <br> 转换为换行，使后续 _drop_noise_lines 能按段落/行粒度过滤样板文字。
    """
    if not raw:
        return ""
    text = re.sub(
        r"(?is)<(script|style|noscript|nav|footer|header|aside|form)[^>]*>.*?</\1>",
        " ",
        raw,
    )
    text = re.sub(r"(?i)<\s*br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</\s*(p|div|li|h[1-6]|article|section|blockquote|tr)\s*>", "\n", text)
    text = re.sub(r"(?s)<[^>]+>", "", text)
    text = html.unescape(text)
    return text


def _to_halfwidth(text: str) -> str:
    """全角标点/字符转半角（NFKC 已覆盖大部分，此处补充空格）。"""
    out = []
    for ch in text:
        code = ord(ch)
        if code == 0x3000:            # 全角空格
            out.append(" ")
        elif 0xFF01 <= code <= 0xFF5E:  # 全角 ASCII 区
            out.append(chr(code - 0xFEE0))
        else:
            out.append(ch)
    return "".join(out)


def _collapse_ws(text: str) -> str:
    """折叠连续空白（含 NBSP / 制表 / 换行）为单个空格，去首尾空白。"""
    return re.sub(r"\s+", " ", text.replace("\u00a0", " ")).strip()


def _drop_noise_lines(text: str) -> str:
    """按行剔除模板噪音行。"""
    lines = re.split(r"[\r\n]+", text)
    kept = [ln for ln in lines if ln.strip() and not any(r.match(ln.strip()) for r in _NOISE_RE)]
    return " ".join(kept)


def normalize_text(raw: str, *, for_hash: bool = False) -> str:
    """清洗 / 归一化文本。

    Args:
        raw: 原始 HTML 或纯文本。
        for_hash: True 时额外做小写化，用于生成稳定的 content_hash；
                  False 时保留原始大小写，用于入库展示的 content。
    """
    text = _strip_html(raw)
    text = _drop_noise_lines(text)
    text = unicodedata.normalize("NFKC", text)
    text = _to_halfwidth(text)
    text = _collapse_ws(text)
    if for_hash:
        text = text.lower()
    return text


def normalize_url(url: str) -> str:
    """规范化 URL 用于生成 doc_id。

    规则（见 DATA_NOTES §4）：小写 scheme+host、去 fragment、剔除跟踪 query 参数、
    去尾部斜杠；保留可区分页面的实质 query 参数并按键排序。
    """
    if not url:
        return ""
    parts = urlsplit(url.strip().lower())
    query_pairs = []
    for seg in (parts.query or "").split("&"):
        if not seg:
            continue
        key = seg.split("=", 1)[0]
        if not any(key == tp or key.startswith(tp) for tp in _TRACKING_PARAMS):
            query_pairs.append(seg)
    query = "&".join(sorted(query_pairs))
    path = parts.path.rstrip("/")
    netloc = parts.netloc
    if netloc.startswith("www."):
        netloc = netloc[4:]  # 去 www 前缀，统一站点标识
    return urlunsplit((parts.scheme, netloc, path, query, ""))  # 丢弃 fragment


# --------------------------------------------------------------------------- #
# 主键 / 哈希 / 时间工具
# --------------------------------------------------------------------------- #

def _sha1_12(stable_key: str) -> str:
    return sha1(stable_key.encode("utf-8")).hexdigest()[:12]


def generate_doc_id(source_type: str, record: dict[str, Any]) -> str:
    """按 source_type 生成 doc_id = {source_type}-{sha1(稳定键)[:12]}。

    - news / policy: 稳定键 = normalize_url(url)
    - price:         稳定键 = f"{source}|{symbol}|{trade_date}|{price_type}"
    """
    if source_type not in VALID_SOURCE_TYPES:
        raise CleanError(f"未知 source_type: {source_type!r}")

    if source_type == "price":
        stable = "|".join(
            str(record.get(k) or "")
            for k in ("source", "symbol", "trade_date", "price_type")
        )
    else:
        url = record.get("url") or record.get("link") or ""
        stable = normalize_url(url)
        if not stable:
            raise CleanError(f"{source_type} 记录缺少可用于生成 doc_id 的 url")

    return f"{source_type}-{_sha1_12(stable)}"


def compute_content_hash(content: str) -> str:
    """对归一化正文（小写化）计算 SHA-256，返回 'sha256:<hex>'。"""
    norm = normalize_text(content, for_hash=True)
    digest = sha256(norm.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _coerce_dt(value: Any) -> datetime | None:
    """把多种时间输入解析为 UTC datetime；无法解析返回 None（不抛异常）。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        s = str(value).strip()
        if not s:
            return None
        s = s.replace("Z", "+00:00")
        dt = None
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%Y%m%d"):
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


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _has_cjk(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", text or ""))


# --------------------------------------------------------------------------- #
# price 元数据提取与可读正文组装
# --------------------------------------------------------------------------- #

def _extract_price_meta(raw: dict[str, Any]) -> dict[str, Any]:
    """提取 price 行情结构化元数据；兼容扁平字段与嵌套 values 两种写法。"""
    values = dict(raw.get("values") or {})
    # 单值字段平铺时也纳入 values
    for k in ("close", "settlement", "open", "high", "low", "volume", "change", "change_pct"):
        if k not in values and raw.get(k) is not None:
            values[k] = raw[k]
    src = str(raw.get("source") or "")
    exchange = raw.get("exchange") or ("LME" if "lme" in src else
                                       "SHFE" if "shfe" in src else None)
    return {
        "exchange": exchange,
        "symbol": raw.get("symbol"),
        "trade_date": raw.get("trade_date"),
        "price_type": raw.get("price_type"),
        "currency": raw.get("currency"),
        "unit": raw.get("unit"),
        "values": values,
    }


def build_price_content(meta: dict[str, Any]) -> str:
    """把行情结构化字段组装为可读中文正文（行情以数字为主，语言按来源站点）。"""
    symbol_zh = _SYMBOL_ZH.get(str(meta.get("symbol") or ""), str(meta.get("symbol") or ""))
    values = meta.get("values") or {}
    primary = (values.get("settlement") if values.get("settlement") is not None
               else values.get("close"))
    parts = [
        f"{meta.get('exchange') or '价格源'}",
        f"品种{symbol_zh or '未知'}",
        f"交易日{meta.get('trade_date') or '未知'}",
    ]
    if meta.get("price_type"):
        parts.append(f"价格类型{meta['price_type']}")
    unit = meta.get("unit") or ""
    if primary is not None:
        parts.append(f"收盘价{primary}{unit}")
    # 变化字段：涨跌 / 涨跌幅 / 成交量（含 0 值，用 is not None 判断）
    for key, label, suffix in (("change", "涨跌", unit), ("change_pct", "涨跌幅", "%"),
                               ("volume", "成交量", "")):
        v = values.get(key)
        if v is not None:
            parts.append(f"{label}{v}{suffix}")
    return "，".join(parts) + "."


def build_price_title(meta: dict[str, Any]) -> str:
    """为 price 记录生成结构化标题。"""
    symbol = str(meta.get("symbol") or "")
    symbol_zh = _SYMBOL_ZH.get(symbol, symbol)
    return f"{meta.get('exchange') or ''} {symbol_zh} {meta.get('trade_date') or ''}".strip()


# --------------------------------------------------------------------------- #
# metadata 组装（三源统一回填 word_count）
# --------------------------------------------------------------------------- #

def _build_metadata(source_type: str, raw: dict[str, Any]) -> dict[str, Any]:
    """按源类型提取 metadata；word_count 由 clean_document 统一回填。"""
    if source_type == "news":
        return {
            "site_domain": raw.get("site_domain"),
            "author": raw.get("author"),
            "tags": list(raw.get("tags") or []),
            "word_count": 0,
        }
    if source_type == "policy":
        return {
            "issuer": raw.get("issuer"),
            "doc_number": raw.get("doc_number"),
            "publish_date": raw.get("publish_date"),
            "policy_type": raw.get("policy_type"),
            "region": raw.get("region"),
            "crawl_page_no": raw.get("crawl_page_no"),
            "word_count": 0,
        }
    # price
    meta = _extract_price_meta(raw)
    return {**meta, "word_count": 0}


# --------------------------------------------------------------------------- #
# 单条文档清洗
# --------------------------------------------------------------------------- #

def clean_document(source_type: str, raw: dict[str, Any],
                   config: CleanConfig) -> dict[str, Any]:
    """原始爬取记录 -> 统一 Schema Document dict（未去重）。

    清洗步骤：
      1. 生成稳定 doc_id（news/policy: url 去 query/fragment；price: 多字段拼接）；
      2. 文本清洗 + 归一化生成 content；html 噪音剔除；
      3. 解析业务事件时间 published_at：失败置 null 不丢弃；
         仅非 null 值执行 recent_days 窗口过滤；
      4. metadata 三源统一回填 word_count；price summary = content；
      5. 计算 content_hash，标记 dedup_status='kept'。

    Raises:
        CleanError: 记录无效应被丢弃（正文过短 / 超时间窗口 / 缺关键字段）。
    """
    if source_type not in VALID_SOURCE_TYPES:
        raise CleanError(f"未知 source_type: {source_type!r}")

    # ---- doc_id ----
    doc_id = generate_doc_id(source_type, raw)

    # ---- 业务事件时间（解析失败置 null 保留，仅非 null 做窗口过滤）----
    crawled_at = config.now
    if source_type == "price":
        meta_tmp = _extract_price_meta(raw)
        published_at = _coerce_dt(meta_tmp.get("trade_date"))
        crawled_at = _coerce_dt(raw.get("crawled_at")) or config.now  # price 读取 raw crawled_at
    elif source_type == "policy":
        published_at = _coerce_dt(raw.get("publish_date") or raw.get("published_at"))
    else:
        published_at = _coerce_dt(raw.get("published_at") or raw.get("pub_date"))

    if published_at is not None and published_at < config.cutoff:
        raise CleanError(
            f"[{doc_id}] published_at={published_at.date()} 超出 {config.recent_days} 天窗口，过滤"
        )

    # ---- 正文 / content ----
    metadata = _build_metadata(source_type, raw)
    summary: str | None = None
    if source_type == "price":
        content = build_price_content(metadata)
        if len(content) < config.min_price_chars:
            raise CleanError(
                f"[{doc_id}] 行情正文长度 {len(content)} < {config.min_price_chars}，丢弃"
            )
        title = _collapse_ws(str(raw.get("title") or "")) or build_price_title(metadata)
        url = raw.get("url") or (
            f"{metadata['exchange'] or 'price'}://{metadata['symbol']}"
            f"/{metadata['trade_date']}/{metadata['price_type']}"
        )
        summary = content  # price summary 与 content 相同
        language = (raw.get("language") or "").lower()
        if language not in VALID_LANGUAGE:
            language = "en" if (metadata.get("exchange") or "").upper() == "LME" else "zh"
    else:
        raw_content = raw.get("content") or raw.get("content_html") or raw.get("text") or ""
        content = normalize_text(raw_content, for_hash=False)
        title = _collapse_ws(str(raw.get("title") or ""))
        url = raw.get("url") or raw.get("link") or ""
        language = (raw.get("language") or "").lower()
        if language not in VALID_LANGUAGE:
            language = "zh" if _has_cjk(title + content) else "en"
        if not title:
            raise CleanError(f"[{doc_id}] 标题为空，丢弃")
        if len(content) < config.min_content_chars:
            raise CleanError(
                f"[{doc_id}] 清洗后正文长度 {len(content)} < {config.min_content_chars}，丢弃"
            )
        raw_summary = raw.get("summary")
        summary = normalize_text(str(raw_summary), for_hash=False) if raw_summary else None

    # ---- 三源统一回填 word_count ----
    metadata["word_count"] = len(content)

    return {
        "doc_id": doc_id,
        "source_type": source_type,
        "source": raw.get("source"),
        "title": title,
        "url": url,
        "content": content,
        "summary": summary,
        "published_at": _iso(published_at) if published_at is not None else None,
        "crawled_at": _iso(crawled_at),
        "language": language,
        "content_hash": compute_content_hash(content),
        "metadata": metadata,
        "dedup_status": "kept",
        "duplicate_of": None,
    }


# --------------------------------------------------------------------------- #
# 两级去重
# --------------------------------------------------------------------------- #

def deduplicate(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """两级去重，返回全部记录（kept 与 duplicate 均保留，按 dedup_status 区分）。

    - L1 doc_id 精确去重：同 doc_id 保留 latest-wins（crawled_at 更新者覆盖留档文档，
      旧 content_hash 归属同步回收）；落选者标记 duplicate 后仍输出用于审计。
    - L2 content_hash 内容哈希去重：不同 doc_id 但清洗后正文相同 -> 判重标记 duplicate。
    """
    by_doc_id: dict[str, dict[str, Any]] = {}
    result: list[dict[str, Any]] = []
    hash_owner: dict[str, str] = {}  # content_hash -> doc_id

    for doc in documents:
        doc_id = doc["doc_id"]
        # --- L1: doc_id 精确去重 ---
        if doc_id in by_doc_id:
            existing = by_doc_id[doc_id]
            doc["dedup_status"] = "duplicate"
            doc["duplicate_of"] = doc_id
            if doc["crawled_at"] > existing["crawled_at"]:
                # latest-wins：更新留档文档，并回收旧 content_hash 归属
                old_hash = existing["content_hash"]
                if hash_owner.get(old_hash) == doc_id:
                    hash_owner.pop(old_hash, None)
                existing.update(
                    {k: v for k, v in doc.items() if k not in ("dedup_status", "duplicate_of")}
                )
                hash_owner[existing["content_hash"]] = doc_id
                logger.debug("doc_id 重复，latest-wins 更新: %s", doc_id)
            else:
                logger.debug("doc_id 重复，旧数据标记 duplicate: %s", doc_id)
            result.append(doc)
            continue

        # --- L2: content_hash 内容哈希去重 ---
        ch = doc["content_hash"]
        owner = hash_owner.get(ch)
        if owner is not None and owner != doc_id:
            doc["dedup_status"] = "duplicate"
            doc["duplicate_of"] = owner
            logger.debug("content_hash 命中重复: %s -> %s", doc_id, owner)
            result.append(doc)
            continue

        by_doc_id[doc_id] = doc
        hash_owner[ch] = doc_id
        result.append(doc)

    return result


# --------------------------------------------------------------------------- #
# 顶层入口
# --------------------------------------------------------------------------- #

def _iter_clean(
    raws: Iterable[dict[str, Any]], source_type: str, config: CleanConfig
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """清洗一批原始记录，返回 (documents, rejects)。异常被捕获并记录。

    先物化为 list：raws 可能是生成器，避免统计 input 数量时二次消耗迭代器。
    """
    raws = list(raws)  # 提前转 list，迭代器只消耗一次
    docs: list[dict[str, Any]] = []
    rejects: list[dict[str, Any]] = []
    for idx, raw in enumerate(raws):
        try:
            docs.append(clean_document(source_type, raw, config))
        except CleanError as exc:
            logger.info("丢弃 %s[#%d]: %s", source_type, idx, exc)
            rejects.append({"index": idx, "source_type": source_type, "reason": str(exc)})
        except Exception:  # noqa: BLE001 —— 兜底防御，记录栈信息但不中断管线
            logger.exception("清洗 %s[#%d] 时发生意外错误", source_type, idx)
            rejects.append({"index": idx, "source_type": source_type, "reason": "unexpected"})
    logger.info("[%s] 清洗完成: 输入 %d 条, 有效 %d 条, 丢弃 %d 条",
                source_type, len(raws), len(docs), len(rejects))
    return docs, rejects


def clean_documents(
    news: Iterable[dict[str, Any]] | None = None,
    policy: Iterable[dict[str, Any]] | None = None,
    price: Iterable[dict[str, Any]] | None = None,
    *,
    config: CleanConfig | None = None,
) -> dict[str, Any]:
    """清洗 + 两级去重（doc_id + content_hash）三源原始数据，输出结果。

    Args:
        news / policy / price: 三个爬虫输出的原始字典列表（可为迭代器）。
        config: 清洗配置，默认 recent_days=30。

    Returns:
        {
          "documents": list[Document],   # 含 kept 与 duplicate 全部记录（按 dedup_status 区分）
          "kept": list[Document],        # 仅 dedup_status == "kept"，供下游切片入库
          "rejects": list[dict],         # 被过滤/丢弃记录及原因（审计用）
          "stats": dict,                 # 各源清洗/去重统计
        }
    """
    config = config or CleanConfig()
    logger.info(
        "开始清洗 | 窗口 %d 天 | cutoff=%s | min_content=%d | min_price=%d",
        config.recent_days, config.cutoff.date(),
        config.min_content_chars, config.min_price_chars,
    )

    all_docs: list[dict[str, Any]] = []
    all_rejects: list[dict[str, Any]] = []
    stats: dict[str, dict[str, int]] = {}

    for batch, stype in ((news, "news"), (policy, "policy"), (price, "price")):
        raws = list(batch or [])  # 提前转 list，迭代器只消耗一次
        docs, rejects = _iter_clean(raws, stype, config)
        all_docs.extend(docs)
        all_rejects.extend(rejects)
        stats[stype] = {"input": len(raws), "cleaned": len(docs), "rejected": len(rejects)}

    # 去重
    deduped = deduplicate(all_docs)
    kept = [d for d in deduped if d["dedup_status"] == "kept"]
    dup = [d for d in deduped if d["dedup_status"] == "duplicate"]

    for stype in SOURCE_TYPES:
        stats[stype]["kept"] = sum(1 for d in kept if d["source_type"] == stype)
        stats[stype]["duplicate"] = sum(1 for d in dup if d["source_type"] == stype)

    stats["total"] = {
        "cleaned": len(all_docs),
        "kept": len(kept),
        "duplicate": len(dup),
        "rejected": len(all_rejects),
    }
    logger.info("去重完成 | kept=%d duplicate=%d", len(kept), len(dup))

    return {"documents": deduped, "kept": kept, "rejects": all_rejects, "stats": stats}


# --------------------------------------------------------------------------- #
# 调用示例 / 自测（导出 cleaned_docs.json）
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    import os

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # 固定"当前时间"便于复现（实际使用时不传 now，默认取真实 UTC now）
    cfg = CleanConfig(recent_days=30, now=datetime(2026, 10, 16, 0, 0, tzinfo=timezone.utc))

    news_raw = [
        {
            "url": "https://www.mining.com/rio-tinto-boosts-copper-output/?utm_source=rss",
            "title": "  Rio Tinto   boosts copper output  ",
            "content_html": "<div><script>var x=1;</script>"
                            "<p>Rio Tinto announced a record copper output for Q3 2026, "
                            "citing strong demand from the EV supply chain.</p>"
                            "<footer>All rights reserved. Follow us on Twitter.</footer></div>",
            "published_at": "2026-10-10T08:30:00+00:00",
            "language": "en",
            "source": "mining_com",
            "site_domain": "mining.com",
            "author": "Jane Doe",
            "tags": ["copper", "rio tinto"],
        },
        {
            # 与上面正文（归一化后）相同、URL 不同 -> 触发 L2 content_hash 去重
            "url": "https://mining.com/rio-tinto-boosts-copper-output-dup",
            "title": "Rio Tinto boosts copper output",
            "content_html": "<p>Rio Tinto announced a record copper output for Q3 2026, "
                            "citing strong demand from the EV supply chain.</p>",
            "published_at": "2026-10-10T09:00:00+00:00",
            "source": "mining_com",
        },
        {
            # 超过 30 天窗口 -> 过滤
            "url": "https://www.mining.com/old-news",
            "title": "Old news",
            "content": "This article is far older than the 30 day window and must be filtered out entirely.",
            "published_at": "2026-01-01T00:00:00+00:00",
            "source": "mining_com",
        },
        {
            # 发布时间解析失败 -> published_at=null，保留不丢弃
            "url": "https://www.mining.com/no-date-article",
            "title": "Article without parsable date",
            "content": "An article whose published date can not be parsed, kept with null date.",
            "published_at": "not-a-date",
            "source": "mining_com",
        },
    ]

    policy_raw = [
        {
            "url": "http://www.cnrare-earth.com/news/detail/1234",
            "title": "关于2026年稀土开采总量控制指标的通知",
            "content": "根据国家相关规定，现将二〇二六年度稀土开采总量控制指标下达如下，"
                       "请各单位认真贯彻执行，严格落实安全生产责任，确保指标按时完成。",
            "issuer": "中国稀土集团",
            "doc_number": "中稀发〔2026〕12号",
            "publish_date": "2026-10-05",
            "policy_type": "notice",
            "region": "CN",
            "language": "zh",
            "source": "cnrare_policy",
        },
    ]

    price_raw = [
        {
            "source": "lme_price",
            "exchange": "LME",
            "symbol": "CU",
            "trade_date": "2026-10-14",
            "price_type": "settlement",
            "currency": "USD",
            "unit": "$/t",
            "values": {"settlement": 9872.5, "close": 9875.0, "high": 9910.0, "low": 9750.0,
                       "open": 9800.5, "volume": 12345, "change_pct": 0.31},
            "crawled_at": "2026-10-15T02:11:43Z",
        },
        {
            # 字段平铺写法 + null 日期（trade_date 缺失 -> published_at=null 保留）
            "source": "shfe_price",
            "exchange": "SHFE",
            "symbol": "LC",
            "price_type": "close",
            "currency": "CNY",
            "unit": "元/吨",
            "close": 75650.0,
            "change": -820.0,
            "volume": 9800,
            "crawled_at": "2026-10-15T02:11:43Z",
        },
    ]

    result = clean_documents(news_raw, policy_raw, price_raw, config=cfg)

    print("\n===== STATS =====")
    print(json.dumps(result["stats"], ensure_ascii=False, indent=2))

    print("\n===== KEPT DOCUMENTS =====")
    for d in result["kept"]:
        print(json.dumps({k: d[k] for k in
                          ("doc_id", "source_type", "title", "published_at", "content_hash",
                           "dedup_status")},
                         ensure_ascii=False))
        print("  content:", d["content"][:120])
        print("  word_count:", d["metadata"]["word_count"])

    print("\n===== DUPLICATES =====")
    for d in result["documents"]:
        if d["dedup_status"] == "duplicate":
            print(f"  {d['doc_id']}  ->  duplicate_of {d['duplicate_of']}")

    print("\n===== REJECTS =====")
    for r in result["rejects"]:
        print(" ", r)

    # 导出 kept 文档（UTF-8，中文不转义）
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..",
                            "cleaned_docs.json")
    out_path = os.path.normpath(out_path)
    with open(out_path, "w", encoding="utf-8") as fp:
        json.dump(result["kept"], fp, ensure_ascii=False, indent=2)
    print(f"\n已导出 kept 文档 -> {out_path} (共 {len(result['kept'])} 条)")
