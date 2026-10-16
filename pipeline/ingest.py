"""矿业 RAG 向量入库（ingest.py）：chunk_embeds.json -> Milvus 批量写入。

职责：
    1. 从项目根目录 .env 加载配置（python-dotenv）：
       MILVUS_HOST / MILVUS_PORT / MILVUS_COLLECTION / CHUNK_EMBEDS_PATH；
    2. 读取切片向量化产物 chunk_embeds.json（每条含 chunk_id、chunk 文本、
       embedding 向量及原文档元数据 doc_id/source_type/source/title/
       published_at/metadata/dedup_status）；
    3. 连接 Milvus；collection 不存在时自动建集合：主键 chunk_id、
       向量维度按样本 embedding 长度自动探测、距离度量 COSINE；
    4. 分批插入（batch_size=32），单批异常记录日志并继续，不终止整体任务；
    5. 日志：chunk 总数、每批进度、成功/失败统计。

**只做向量入库，不含检索与问答逻辑。**

运行：
    python -m pipeline.ingest
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterator

from dotenv import load_dotenv
from pymilvus import DataType, MilvusClient

__all__ = [
    "load_config",
    "load_chunks",
    "ensure_collection",
    "build_rows",
    "iter_batches",
    "run_ingest",
]

logger = logging.getLogger("pipeline.ingest")

_PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_PIPELINE_DIR)

# .env 缺失时的回退默认值
DEFAULTS: dict[str, str] = {
    "MILVUS_HOST": "localhost",
    "MILVUS_PORT": "19530",
    "MILVUS_COLLECTION": "mining_rag_collection",
    "CHUNK_EMBEDS_PATH": "pipeline/chunk_embeds.json",
}

# VARCHAR 字段最大长度（Milvus 上限 65535）
_MAX_TEXT_LEN = 65535
_MAX_NAME_LEN = 256

# chunk 文本 -> Milvus text 字段的候选键（chunk_embed.py 输出用 content）
_TEXT_KEYS = ("text", "content")


class IngestError(Exception):
    """入库配置 / 数据不可恢复错误。"""


# --------------------------------------------------------------------------- #
# 配置加载
# --------------------------------------------------------------------------- #

def load_config() -> dict[str, Any]:
    """从项目根目录 .env 读取 Milvus 配置；相对路径按项目根解析。"""
    load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))
    cfg = {
        "host": os.getenv("MILVUS_HOST") or DEFAULTS["MILVUS_HOST"],
        "port": int(os.getenv("MILVUS_PORT") or DEFAULTS["MILVUS_PORT"]),
        "collection": os.getenv("MILVUS_COLLECTION") or DEFAULTS["MILVUS_COLLECTION"],
    }
    raw_path = os.getenv("CHUNK_EMBEDS_PATH") or DEFAULTS["CHUNK_EMBEDS_PATH"]
    if not os.path.isabs(raw_path):
        raw_path = os.path.normpath(os.path.join(_PROJECT_ROOT, raw_path))
    cfg["embeds_path"] = raw_path
    logger.info(
        "配置加载 | %s:%d/%s | 输入=%s",
        cfg["host"], cfg["port"], cfg["collection"], cfg["embeds_path"],
    )
    return cfg


# --------------------------------------------------------------------------- #
# 数据读取与行转换
# --------------------------------------------------------------------------- #

def load_chunks(path: str) -> list[dict[str, Any]]:
    """加载 chunk_embeds.json，校验必备字段（chunk_id / embedding / 文本）。"""
    if not os.path.exists(path):
        raise IngestError(f"输入文件不存在: {path}（请先运行 python -m pipeline.chunk_embed）")
    with open(path, "r", encoding="utf-8") as fp:
        chunks = json.load(fp)
    if not isinstance(chunks, list):
        raise IngestError(f"输入应为 chunk 列表，实际为 {type(chunks).__name__}: {path}")

    valid: list[dict[str, Any]] = []
    skipped = 0
    for c in chunks:
        if (isinstance(c, dict) and c.get("chunk_id")
                and isinstance(c.get("embedding"), list) and c["embedding"]):
            valid.append(c)
        else:
            skipped += 1
    if skipped:
        logger.warning("跳过 %d 条缺失 chunk_id/embedding 的记录", skipped)
    if not valid:
        raise IngestError("无有效 chunk 记录（全部缺失 chunk_id 或 embedding）")
    logger.info("读取 chunk | 总数 %d 条 | 有效 %d 条 | 向量维度 %d",
                len(chunks), len(valid), len(valid[0]["embedding"]))
    return valid


def _truncate(value: Any, max_len: int) -> str:
    return str(value if value is not None else "")[:max_len]


def build_rows(chunk: dict[str, Any]) -> dict[str, Any]:
    """chunk 记录 -> Milvus 行（显式 9 字段，text 取 text/content 键）。"""
    text = next((chunk[k] for k in _TEXT_KEYS if chunk.get(k)), "")
    meta = chunk.get("metadata")
    if not isinstance(meta, dict):
        meta = {}
    return {
        "chunk_id": _truncate(chunk["chunk_id"], _MAX_NAME_LEN),
        "doc_id": _truncate(chunk.get("doc_id") or "", _MAX_NAME_LEN),
        "source_type": _truncate(chunk.get("source_type") or "", 32),
        "source": _truncate(chunk.get("source") or "", _MAX_NAME_LEN),
        "title": _truncate(chunk.get("title") or "", _MAX_NAME_LEN),
        "published_at": _truncate(chunk.get("published_at") or "", 64),
        "text": _truncate(text, _MAX_TEXT_LEN),
        "metadata": meta,
        "dedup_status": _truncate(chunk.get("dedup_status") or "kept", 32),
    }


def iter_batches(items: list[dict[str, Any]], size: int) -> Iterator[list[dict[str, Any]]]:
    """按 batch_size 切分列表，避免一次性构造全部行占用内存。"""
    size = max(1, int(size))
    for start in range(0, len(items), size):
        yield items[start:start + size]


# --------------------------------------------------------------------------- #
# 集合创建
# --------------------------------------------------------------------------- #

def ensure_collection(client: MilvusClient, name: str, dim: int) -> None:
    """collection 不存在则创建：主键 chunk_id，COSINE 向量索引，维度自动探测。"""
    if client.has_collection(collection_name=name):
        logger.info("collection 已存在，直接复用: %s", name)
        return

    schema = client.create_schema(auto_id=False, enable_dynamic_field=True)
    schema.add_field("chunk_id", DataType.VARCHAR, is_primary=True, max_length=_MAX_NAME_LEN)
    schema.add_field("doc_id", DataType.VARCHAR, max_length=_MAX_NAME_LEN)
    schema.add_field("source_type", DataType.VARCHAR, max_length=32)
    schema.add_field("source", DataType.VARCHAR, max_length=_MAX_NAME_LEN)
    schema.add_field("title", DataType.VARCHAR, max_length=_MAX_NAME_LEN)
    schema.add_field("published_at", DataType.VARCHAR, max_length=64)
    schema.add_field("text", DataType.VARCHAR, max_length=_MAX_TEXT_LEN)
    schema.add_field("metadata", DataType.JSON)
    schema.add_field("dedup_status", DataType.VARCHAR, max_length=32)
    schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=dim)

    index_params = client.prepare_index_params()
    index_params.add_index(field_name="embedding", index_type="AUTOINDEX", metric_type="COSINE")

    client.create_collection(collection_name=name, schema=schema, index_params=index_params)
    logger.info("collection 创建完成: %s | 向量维度 dim=%d | metric=COSINE | 主键=chunk_id",
                name, dim)


# --------------------------------------------------------------------------- #
# 入库主流程
# --------------------------------------------------------------------------- #

def run_ingest(batch_size: int = 32) -> dict[str, int]:
    """读取 -> 连接 -> 建集合 -> 分批插入；单批失败记录日志不中断。

    Returns:
        {"total": 读取条数, "success": 入库成功条数, "failed": 失败条数,
         "batches": 批次总数, "failed_batches": 失败批次号列表}
    """
    cfg = load_config()
    chunks = load_chunks(cfg["embeds_path"])
    total = len(chunks)

    client = MilvusClient(
        uri=f"http://{cfg['host']}:{cfg['port']}",
        timeout=10,
    )
    ensure_collection(client, cfg["collection"], dim=len(chunks[0]["embedding"]))

    success = 0
    failed = 0
    failed_batches: list[int] = []
    batch_no = 0
    batches = list(iter_batches(chunks, batch_size))
    for batch_no, batch in enumerate(batches, start=1):
        try:
            rows = [dict(build_rows(c), embedding=c["embedding"]) for c in batch]
            client.insert(collection_name=cfg["collection"], data=rows)
            success += len(rows)
        except Exception as exc:  # noqa: BLE001 —— 单批失败不终止整体任务
            failed += len(batch)
            failed_batches.append(batch_no)
            logger.error("第 %d 批入库失败（%d 条）: %s", batch_no, len(batch), exc)
        logger.info("入库进度: 批次 %d/%d | 累计成功 %d | 累计失败 %d",
                    batch_no, len(batches), success, failed)

    stats = {"total": total, "success": success, "failed": failed,
             "batches": len(batches), "failed_batches": failed_batches}
    logger.info("向量入库完成 | chunk 总数 %d | 成功 %d | 失败 %d | collection=%s",
                total, success, failed, cfg["collection"])
    return stats


# --------------------------------------------------------------------------- #
# main 入口
# --------------------------------------------------------------------------- #

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="chunk_embeds.json 批量写入 Milvus（只入库）")
    parser.add_argument("--batch-size", type=int, default=32, help="每批插入条数，默认 32")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    stats = run_ingest(batch_size=args.batch_size)
    print("\n===== INGEST STATS =====")
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    if stats["failed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
