"""矿业 RAG FastAPI 查询服务（serve/main.py）。

链路：question -> bge embedding -> Milvus COSINE 检索 top-K -> RAG prompt -> DeepSeek 生成回答。

    - 配置全部来自项目根目录 .env（python-dotenv）：
      MILVUS_HOST / MILVUS_PORT / MILVUS_COLLECTION / EMBED_MODEL_NAME /
      EMBED_DEVICE / LLM_BASE_URL / LLM_API_KEY / LLM_MODEL_NAME / RETRIEVE_TOP_K
    - 只实现 RAG 查询，不做数据入库（入库见 pipeline/ingest.py）；
    - 任何异常统一返回标准化错误 JSON：{"error": "..."}。

启动：
    python serve/main.py                     # uvicorn 0.0.0.0:8000
    示例提问: POST /query {"question": "近7天澳洲锂出口政策有何变化?"}
"""

from __future__ import annotations

import logging
import os
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

__all__ = ["app", "run_query"]

logger = logging.getLogger("serve.main")

# --------------------------------------------------------------------------- #
# .env 配置加载（项目根目录）
# --------------------------------------------------------------------------- #

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))
# huggingface.co 官方站在国内网络不可达：默认走 hf-mirror（本地已有缓存时不发网络请求）
os.environ.setdefault("HF_ENDPOINT", "https://hf-mirror.com")

CFG: dict[str, Any] = {
    "milvus_host": os.getenv("MILVUS_HOST", "localhost"),
    "milvus_port": int(os.getenv("MILVUS_PORT", "19530")),
    "collection": os.getenv("MILVUS_COLLECTION", "mining_rag_collection"),
    "embed_model": os.getenv("EMBED_MODEL_NAME", "BAAI/bge-small-en-v1.5"),
    "embed_device": os.getenv("EMBED_DEVICE", "cpu"),
    "llm_base_url": os.getenv("LLM_BASE_URL", ""),
    "llm_api_key": os.getenv("LLM_API_KEY", ""),
    "llm_model": os.getenv("LLM_MODEL_NAME", ""),
    "top_k": int(os.getenv("RETRIEVE_TOP_K", "5")),
}

# 检索返回的字段（与 ingest.py 写入的 schema 对应）
_OUTPUT_FIELDS = ["chunk_id", "doc_id", "source_type", "source", "published_at", "text"]

# --------------------------------------------------------------------------- #
# 模型 / 客户端单例（首次请求时懒加载，进程内复用）
# --------------------------------------------------------------------------- #

_embedder: Any = None
_milvus: Any = None


def get_embedder() -> Any:
    """langchain HuggingFaceEmbeddings 加载 bge 模型（设备由 EMBED_DEVICE 指定）。"""
    global _embedder
    if _embedder is None:
        from langchain_huggingface import HuggingFaceEmbeddings

        logger.info("加载 embedding 模型: %s (device=%s) ...",
                    CFG["embed_model"], CFG["embed_device"])
        _embedder = HuggingFaceEmbeddings(
            model_name=CFG["embed_model"],
            model_kwargs={"device": CFG["embed_device"]},
            encode_kwargs={"normalize_embeddings": True},
        )
        logger.info("embedding 模型加载完成")
    return _embedder


def get_milvus() -> Any:
    """pymilvus MilvusClient 连接（COSINE 索引在建集合时已固定，检索无需再指定）。"""
    global _milvus
    if _milvus is None:
        from pymilvus import MilvusClient

        _milvus = MilvusClient(
            uri=f"http://{CFG['milvus_host']}:{CFG['milvus_port']}", timeout=10,
        )
        logger.info("Milvus 已连接: %s:%d/%s",
                    CFG["milvus_host"], CFG["milvus_port"], CFG["collection"])
    return _milvus


# --------------------------------------------------------------------------- #
# RAG 提示词与 LLM 调用（DeepSeek，OpenAI 协议兼容）
# --------------------------------------------------------------------------- #

_SYSTEM_PROMPT = (
    "你是矿业领域 RAG 问答助手。只依据【检索上下文】回答问题；"
    "上下文不足以回答时必须明确说明“知识库中未检索到相关信息”，禁止编造。"
    "回答使用中文，引用证据时标注对应的 doc_id 或来源。"
)


def build_prompt(question: str, chunks: list[dict[str, Any]]) -> str:
    """把 top-K chunk 原文与元信息拼装为 RAG 上下文块。"""
    blocks = []
    for i, c in enumerate(chunks, start=1):
        blocks.append(
            f"[证据{i}] chunk_id={c['chunk_id']} doc_id={c['doc_id']} "
            f"类型={c['source_type']} 来源={c['source']} "
            f"时间={c['published_at'] or '未知'}\n{c['text']}"
        )
    context = "\n\n".join(blocks) if blocks else "（无召回内容）"
    return f"【检索上下文】\n{context}\n\n【问题】\n{question}"


def call_llm(prompt: str) -> str:
    """调用 DeepSeek 生成最终回答。"""
    if not CFG["llm_api_key"]:
        raise RuntimeError("LLM_API_KEY 未配置，请在项目根目录 .env 中填入 DeepSeek 密钥")
    from openai import OpenAI

    client = OpenAI(base_url=CFG["llm_base_url"] or None, api_key=CFG["llm_api_key"])
    resp = client.chat.completions.create(
        model=CFG["llm_model"] or "deepseek-chat",
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
        timeout=60,
    )
    return (resp.choices[0].message.content or "").strip()


# --------------------------------------------------------------------------- #
# 核心查询链路
# --------------------------------------------------------------------------- #

def run_query(question: str) -> dict[str, Any]:
    """question -> embedding -> Milvus 检索 top-K -> prompt -> LLM 回答。

    Returns:
        {"answer": str, "retrieved_chunks": [{chunk_id, doc_id, source_type,
         source, published_at, text}, ...]}
    """
    vec = get_embedder().embed_query(question)

    res = get_milvus().search(
        collection_name=CFG["collection"],
        data=[vec],
        limit=CFG["top_k"],
        output_fields=_OUTPUT_FIELDS,
    )
    hits = res[0] if res and res[0] else []
    chunks: list[dict[str, Any]] = []
    for h in hits:
        ent = h.get("entity") or {}
        chunks.append({k: ent.get(k) or (h.get(k) if k == "chunk_id" else None)
                       for k in _OUTPUT_FIELDS})
        chunks[-1]["chunk_id"] = str(h.get("id") or ent.get("chunk_id") or "")
    logger.info("检索完成 | query=%r | 召回 %d 条 (recall@%d)", question, len(chunks), CFG["top_k"])

    answer = call_llm(build_prompt(question, chunks))
    return {"answer": answer, "retrieved_chunks": chunks}


# --------------------------------------------------------------------------- #
# FastAPI 应用
# --------------------------------------------------------------------------- #

app = FastAPI(title="Mining RAG Serve", version="1.0.0",
              description="矿业三源聚合 RAG 查询接口（Milvus + bge + DeepSeek）")


class QueryRequest(BaseModel):
    question: str = Field(..., min_length=1, description="用户的自然语言问题",
                          examples=["近7天澳洲锂出口政策有何变化?"])


@app.get("/health")
def health() -> dict[str, Any]:
    """探活：返回配置摘要（不回显密钥）。"""
    return {
        "status": "ok",
        "collection": CFG["collection"],
        "embed_model": CFG["embed_model"],
        "llm_model": CFG["llm_model"],
        "top_k": CFG["top_k"],
        "llm_key_configured": bool(CFG["llm_api_key"]),
    }


@app.post("/query")
def query(req: QueryRequest) -> JSONResponse:
    try:
        result = run_query(req.question)
        return JSONResponse(status_code=200, content=result)
    except Exception as exc:  # noqa: BLE001 —— 标准化错误 JSON，不抛裸栈
        logger.exception("RAG 查询失败: %s", exc)
        return JSONResponse(status_code=500, content={"error": f"{type(exc).__name__}: {exc}"})


@app.exception_handler(Exception)
async def unhandled_exception_handler(_: Any, exc: Exception) -> JSONResponse:
    """全局兜底（含校验之外的意外异常）-> 标准化错误 JSON。"""
    logger.exception("未处理异常")
    return JSONResponse(status_code=500, content={"error": f"{type(exc).__name__}: {exc}"})


# --------------------------------------------------------------------------- #
# uvicorn 启动入口
# --------------------------------------------------------------------------- #

def main() -> None:
    import uvicorn

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger.info("启动 Mining RAG 服务 | Milvus=%s:%d/%s | top_k=%d",
                CFG["milvus_host"], CFG["milvus_port"], CFG["collection"], CFG["top_k"])
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
