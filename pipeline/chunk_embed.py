"""矿业 RAG 文本切片 + 向量化（chunk_embed.py）。

职责（切片向量化阶段）：
    1. 加载清洗产物 cleaned_docs.json（kept 文档列表，14 字段统一 Schema）；
    2. 用 LangChain RecursiveCharacterTextSplitter（chunk_size=800、chunk_overlap=150）
       对每条 Document 的 content 切片；每个 chunk 继承原文档全部元数据，
       新增 chunk_id / chunk_index 字段；
    3. 加载中英双语 embedding 模型（默认 BAAI/bge-small-en-v1.5，
       sentence-transformers 本地推理），分批计算 chunk 文本向量并 L2 归一化；
    4. 输出 pipeline/chunk_embeds.json：[{chunk 文本, 向量, 完整元数据}] 列表；
    5. 日志：原始文档总数、chunk 总数、embedding 生成进度。

**只做切片 + 向量生成，不写入任何向量库。**

运行：
    python -m pipeline.chunk_embed
    python -m pipeline.chunk_embed --input cleaned_docs.json --output pipeline/chunk_embeds.json
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from typing import Any

from langchain_text_splitters import RecursiveCharacterTextSplitter

__all__ = [
    "ChunkEmbedder",
    "load_documents",
    "split_documents",
    "embed_chunks",
    "run_chunk_embed",
]

logger = logging.getLogger("pipeline.chunk_embed")

# 项目根目录 / pipeline 目录（输入输出默认路径基准）
_PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_PIPELINE_DIR)

# 默认模型：sentence-transformers 中英双语小模型（本地推理，首次使用自动下载权重）
DEFAULT_EMBED_MODEL = "BAAI/bge-small-en-v1.5"

# 中英混排友好分隔符：段落 -> 句子（中文句号/分号、英文句点/分号）-> 词 -> 字符
_DEFAULT_SEPARATORS = ["\n\n", "\n", "。", "；", "！", "？", ".", ";", "!", "?", " ", ""]


class ChunkEmbedError(Exception):
    """切片 / 向量化过程中的可恢复错误。"""


# --------------------------------------------------------------------------- #
# 输入加载
# --------------------------------------------------------------------------- #

def load_documents(path: str) -> list[dict[str, Any]]:
    """加载清洗后的 Document 列表；校验顶层为 list 且每条含 content。

    Raises:
        ChunkEmbedError: 文件不存在、JSON 非法或结构不符。
    """
    if not os.path.exists(path):
        raise ChunkEmbedError(f"输入文件不存在: {path}（请先运行 pipeline/clean.py 生成）")
    with open(path, "r", encoding="utf-8") as fp:
        docs = json.load(fp)
    if not isinstance(docs, list):
        raise ChunkEmbedError(f"输入应为 Document 列表，实际为 {type(docs).__name__}: {path}")
    valid = [d for d in docs if isinstance(d, dict) and str(d.get("content") or "").strip()]
    dropped = len(docs) - len(valid)
    if dropped:
        logger.warning("跳过 %d 条缺失 content 的文档", dropped)
    logger.info("加载清洗文档: %s | 共 %d 条（有效 %d 条）", path, len(docs), len(valid))
    return valid


# --------------------------------------------------------------------------- #
# 切片
# --------------------------------------------------------------------------- #

class ChunkEmbedder:
    """RecursiveCharacterTextSplitter 切片 + bge 模型批量向量化封装。

    Args:
        chunk_size: 每 chunk 最大 token/字符数（RecursiveCharacterTextSplitter 默认按字符计）。
        chunk_overlap: 相邻 chunk 重叠长度。
        model_name: sentence-transformers 模型名；None 则跳过向量化（仅切片）。
        batch_size: embedding 批量大小。
        normalize: 是否对向量做 L2 归一化（余弦相似检索友好）。
    """

    def __init__(
        self,
        chunk_size: int = 800,
        chunk_overlap: int = 150,
        model_name: str | None = DEFAULT_EMBED_MODEL,
        batch_size: int = 32,
        normalize: bool = True,
    ) -> None:
        self.splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            separators=list(_DEFAULT_SEPARATORS),
            keep_separator=True,
            length_function=len,
        )
        self.model_name = model_name
        self.batch_size = batch_size
        self.normalize = normalize
        self._model: Any = None

    # ---- 切片：每 chunk 继承原文档全部元数据 + 新增 chunk_id ----
    def split_documents(self, docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        chunks: list[dict[str, Any]] = []
        empty_docs = 0
        for doc in docs:
            pieces = self.splitter.split_text(str(doc["content"]))
            if not pieces:
                empty_docs += 1
                continue
            total = len(pieces)
            for idx, text in enumerate(pieces):
                chunk = dict(doc)  # 继承原文档全部字段（元数据）
                chunk.update({
                    "chunk_id": f"{doc['doc_id']}#c{idx:03d}",
                    "chunk_index": idx,
                    "chunk_total": total,
                    "content": text,          # chunk 文本替换展示正文，其余元数据原样继承
                    "content_hash": None,     # 原 doc 级哈希对 chunk 不再适用
                })
                chunks.append(chunk)
        logger.info(
            "切片完成 | 原始文档 %d 条 -> chunk %d 个 | 空切片文档 %d 条 | "
            "chunk_size=%d overlap=%d",
            len(docs), len(chunks), empty_docs,
            self.splitter._chunk_size, self.splitter._chunk_overlap,
        )
        return chunks

    # ---- 模型加载（懒加载，仅一次）----
    def _load_model(self) -> Any:
        if self._model is not None:
            return self._model
        if not self.model_name:
            raise ChunkEmbedError("未配置 embedding 模型（model_name=None）")
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:  # pragma: no cover
            raise ChunkEmbedError(
                "缺少 sentence-transformers，请安装: uv add sentence-transformers"
            ) from exc
        logger.info("加载 embedding 模型: %s ...", self.model_name)
        self._model = SentenceTransformer(self.model_name)
        logger.info("模型加载完成 | dim=%d", self._model.get_embedding_dimension())
        return self._model

    # ---- 批量向量化（带进度日志）----
    def embed_chunks(self, chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        model = self._load_model()
        texts = [str(c["content"]) for c in chunks]
        total = len(texts)
        bs = max(1, int(self.batch_size))
        for start in range(0, total, bs):
            batch = texts[start:start + bs]
            vecs = model.encode(
                batch,
                batch_size=bs,
                normalize_embeddings=self.normalize,
                show_progress_bar=False,
            )
            for offset, vec in zip(range(start, start + len(batch)), vecs):
                chunks[offset]["embedding"] = [float(x) for x in vec]
            done = min(start + bs, total)
            logger.info("embedding 进度: %d/%d (%.1f%%)", done, total, done * 100.0 / total)
        logger.info("向量化完成 | 共 %d 个 chunk 已生成 embedding", total)
        return chunks


# --------------------------------------------------------------------------- #
# 流程函数（便于测试与复用）
# --------------------------------------------------------------------------- #

def split_documents(docs: list[dict[str, Any]], *, chunk_size: int = 800,
                    chunk_overlap: int = 150) -> list[dict[str, Any]]:
    """便捷函数：仅切片不向量化。"""
    return ChunkEmbedder(chunk_size=chunk_size, chunk_overlap=chunk_overlap,
                         model_name=None).split_documents(docs)


def embed_chunks(chunks: list[dict[str, Any]], *, model_name: str = DEFAULT_EMBED_MODEL,
                 batch_size: int = 32) -> list[dict[str, Any]]:
    """便捷函数：对已切片 chunk 就地补充 embedding 字段。"""
    return ChunkEmbedder(model_name=model_name, batch_size=batch_size).embed_chunks(chunks)


# --------------------------------------------------------------------------- #
# 顶层入口
# --------------------------------------------------------------------------- #

def run_chunk_embed(
    input_path: str,
    output_path: str,
    *,
    chunk_size: int = 800,
    chunk_overlap: int = 150,
    model_name: str | None = DEFAULT_EMBED_MODEL,
    batch_size: int = 32,
) -> dict[str, Any]:
    """加载 -> 切片 -> 向量化 -> 导出 chunk_embeds.json。返回统计信息。"""
    docs = load_documents(input_path)
    embedder = ChunkEmbedder(chunk_size=chunk_size, chunk_overlap=chunk_overlap,
                             model_name=model_name, batch_size=batch_size)
    chunks = embedder.split_documents(docs)
    if not chunks:
        raise ChunkEmbedError("切片结果为空，请检查输入文档 content 字段")
    if model_name:
        chunks = embedder.embed_chunks(chunks)
    else:
        logger.warning("model_name 为空：仅切片，跳过向量化")

    # 输出：chunk 文本 + 向量 + 完整元数据（继承自原文档）
    chunks.sort(key=lambda c: (str(c.get("doc_id") or ""), c["chunk_index"]))
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fp:
        json.dump(chunks, fp, ensure_ascii=False, indent=2)

    stats = {
        "documents": len(docs),
        "chunks": len(chunks),
        "embedding_dim": (len(chunks[0]["embedding"])
                          if chunks and chunks[0].get("embedding") else 0),
        "model": model_name,
    }
    logger.info(
        "切片向量化完成 | 原始文档 %d 条 | 生成 chunk %d 个 | 向量维度 %d | 已导出 -> %s",
        stats["documents"], stats["chunks"], stats["embedding_dim"], output_path,
    )
    return stats


def _default_input() -> str:
    """清洗产物默认路径：优先 pipeline/cleaned_docs.json，回退项目根目录。"""
    in_pipe = os.path.join(_PIPELINE_DIR, "cleaned_docs.json")
    if os.path.exists(in_pipe):
        return in_pipe
    return os.path.join(_PROJECT_ROOT, "cleaned_docs.json")


def main() -> None:
    # huggingface.co 官方站在国内网络不可达，默认走 hf-mirror 镜像（已有本地缓存时不发起网络请求）
    os.environ.setdefault("HF_ENDPOINT", "https://hf-mirror.com")
    parser = argparse.ArgumentParser(description="文档切片 + bge 向量化（不写向量库）")
    parser.add_argument("--input", default=_default_input(),
                        help="清洗产物 JSON（默认 pipeline/cleaned_docs.json，回退项目根目录）")
    parser.add_argument("--output",
                        default=os.path.join(_PIPELINE_DIR, "chunk_embeds.json"),
                        help="输出 chunk+向量文件路径")
    parser.add_argument("--chunk-size", type=int, default=800)
    parser.add_argument("--chunk-overlap", type=int, default=150)
    parser.add_argument("--model", default=DEFAULT_EMBED_MODEL,
                        help="sentence-transformers 模型名；传 '' 表示仅切片")
    parser.add_argument("--batch-size", type=int, default=32)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    stats = run_chunk_embed(
        args.input, args.output,
        chunk_size=args.chunk_size, chunk_overlap=args.chunk_overlap,
        model_name=args.model or None, batch_size=args.batch_size,
    )
    print("\n===== CHUNK EMBED STATS =====")
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    with open(args.output, encoding="utf-8") as fp:
        preview = json.load(fp)[0]
    print("\n===== 首个 chunk 预览 =====")
    print("chunk_id:", preview["chunk_id"], "| doc_id:", preview["doc_id"],
          "| language:", preview.get("language"))
    print("embedding 维度:", len(preview["embedding"]) if preview.get("embedding") else 0)
    print("content:", preview["content"][:120].replace("\n", " "))


if __name__ == "__main__":
    main()
