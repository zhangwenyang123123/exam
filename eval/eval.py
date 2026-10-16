"""矿业 RAG 自动评估脚本（eval/eval.py）。

流程：
    1. 读取同目录 ground_truth.json（20 条 question / expected_answer）；
    2. 从项目根目录 .env 读取 EVAL_QUERY_URL 与 LLM 配置（DeepSeek，OpenAI 协议）；
    3. 逐条 POST 调用 serve 的 /query 接口，拿到 answer 与 retrieved_chunks；
    4. 两个 LLM-as-judge 指标：
       ① recall@5：判断 top5 chunk 是否包含能支撑 expected_answer 的证据，输出 1/0；
       ② answer faithfulness：基于检索上下文对生成答案打 0~1 分（忠于上下文、有无幻觉）；
    5. 汇总平均 recall@5 / 平均 faithfulness，输出逐题明细并保存 eval_result.json；
    6. 单题接口失败/判定失败仅记录错误，不中断整体评估。

运行：
    python eval/eval.py                # 全量 20 条
    python eval/eval.py --limit 3     # 调试：只跑前 3 条
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

import requests
from dotenv import load_dotenv
from openai import OpenAI

logger = logging.getLogger("eval.rag")

_EVAL_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_EVAL_DIR)
load_dotenv(os.path.join(_ROOT, ".env"))

GT_PATH = os.path.join(_EVAL_DIR, "ground_truth.json")
RESULT_PATH = os.path.join(_EVAL_DIR, "eval_result.json")

QUERY_URL = os.getenv("EVAL_QUERY_URL", "http://localhost:8000/query")
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "")
LLM_API_KEY = os.getenv("LLM_API_KEY", "")
LLM_MODEL_NAME = os.getenv("LLM_MODEL_NAME", "deepseek-chat")

_QUERY_TIMEOUT = 180   # /query 含 embedding+检索+LLM，放宽超时
_MAX_CTX = 6000        # 判定 prompt 中 chunk 上下文最大字符数


# --------------------------------------------------------------------------- #
# LLM-as-judge
# --------------------------------------------------------------------------- #

def _llm_text(prompt: str) -> str:
    """调用评审 LLM（temperature=0 保证判定稳定）。"""
    if not LLM_API_KEY:
        raise RuntimeError("LLM_API_KEY 未配置")
    client = OpenAI(base_url=LLM_BASE_URL or None, api_key=LLM_API_KEY)
    resp = client.chat.completions.create(
        model=LLM_MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        timeout=60,
    )
    return (resp.choices[0].message.content or "").strip()


def _format_chunks(chunks: list[dict[str, Any]]) -> str:
    blocks = []
    for i, c in enumerate(chunks, start=1):
        blocks.append(
            f"[chunk{i}] doc_id={c.get('doc_id')} 类型={c.get('source_type')} "
            f"来源={c.get('source')} 时间={c.get('published_at')}\n"
            f"{str(c.get('text') or '')[:1200]}"
        )
    return "\n\n".join(blocks)[:_MAX_CTX]


def judge_recall(expected_answer: str, chunks: list[dict[str, Any]]) -> int:
    """recall@5：top5 chunk 中是否存在能支撑标准答案的原文证据 -> 1/0。"""
    if not chunks:
        return 0
    prompt = (
        "你是RAG检索质量评审。判断下列检索到的文本块中，是否包含能够支撑标准答案的原文证据"
        "（证据不必逐字一致，语义上足以推出标准答案即算命中）。\n\n"
        f"【标准答案】{expected_answer}\n\n"
        f"【检索文本块】\n{_format_chunks(chunks)}\n\n"
        "只输出一个数字：包含证据输出 1，不包含输出 0。不要任何其他文字。"
    )
    out = _llm_text(prompt)
    m = re.search(r"[01]", out)
    if not m:
        raise ValueError(f"recall 判定输出无法解析为 0/1: {out[:50]!r}")
    return int(m.group())


def score_faithfulness(question: str, chunks: list[dict[str, Any]], answer: str) -> float:
    """answer faithfulness：答案是否忠于检索上下文、有无幻觉 -> 0~1 分。"""
    context = _format_chunks(chunks) if chunks else "（无检索上下文）"
    prompt = (
        "你是RAG生成质量评审。请评估【模型回答】对【检索上下文】的忠实度："
        "回答中的每个事实性陈述是否都能从检索上下文找到依据；引入上下文之外的编造信息"
        "（幻觉）应重罚；对上下文的合理归纳、翻译、整理不扣分；"
        "上下文不足时如实说明'未检索到相关信息'属于忠实回答，给高分。\n\n"
        f"【问题】{question}\n\n"
        f"【检索上下文】\n{context}\n\n"
        f"【模型回答】{answer}\n\n"
        "只输出一个 0 到 1 之间的小数（如 0.85），保留两位小数，不要任何其他文字。"
    )
    out = _llm_text(prompt)
    m = re.search(r"0(?:\.\d+)?|1(?:\.0+)?|\.\d+", out)
    if not m:
        raise ValueError(f"faithfulness 判定输出无法解析为分数: {out[:50]!r}")
    return max(0.0, min(1.0, float(m.group())))


# --------------------------------------------------------------------------- #
# 单题评估
# --------------------------------------------------------------------------- #

def query_rag(question: str) -> dict[str, Any]:
    """POST /query 获取 answer 与 retrieved_chunks。"""
    resp = requests.post(QUERY_URL, json={"question": question}, timeout=_QUERY_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if "answer" not in data and "error" in data:
        raise RuntimeError(f"/query 返回错误: {data['error']}")
    return data


def eval_one(idx: int, item: dict[str, Any]) -> dict[str, Any]:
    """单题：调用接口 + 两项指标；任何异常记录 error 后由上层继续下一题。"""
    question = item["question"]
    expected = item["expected_answer"]
    detail: dict[str, Any] = {"index": idx, "question": question,
                              "expected_answer": expected}
    try:
        data = query_rag(question)
        answer = str(data.get("answer") or "")
        chunks = list(data.get("retrieved_chunks") or [])
        detail["answer"] = answer
        detail["retrieved_chunk_ids"] = [c.get("chunk_id") for c in chunks]
        detail["recall@5"] = judge_recall(expected, chunks)
        detail["faithfulness"] = score_faithfulness(question, chunks, answer)
        logger.info("[%d/%d] recall@5=%d faithfulness=%.2f | %s",
                    idx, detail.get("_total", idx), detail["recall@5"],
                    detail["faithfulness"], question[:30])
    except requests.RequestException as exc:
        detail["error"] = f"接口调用失败: {exc}"
        logger.error("[%d] %s | %s", idx, detail["error"], question[:30])
    except Exception as exc:  # noqa: BLE001 —— 单题失败不中断整体评估
        detail["error"] = f"{type(exc).__name__}: {exc}"
        logger.error("[%d] 评估失败: %s | %s", idx, detail["error"], question[:30])
    return detail


# --------------------------------------------------------------------------- #
# 汇总与入口
# --------------------------------------------------------------------------- #

def run_eval(limit: int | None = None) -> dict[str, Any]:
    with open(GT_PATH, "r", encoding="utf-8") as fp:
        gt = json.load(fp)
    if limit:
        gt = gt[:limit]
    logger.info("开始评估 | 数据集 %s | 题目数 %d | 接口 %s", GT_PATH, len(gt), QUERY_URL)

    details: list[dict[str, Any]] = []
    for idx, item in enumerate(gt, start=1):
        d = eval_one(idx, item)
        d["_total"] = len(gt)
        details.append(d)

    ok = [d for d in details if "error" not in d]
    failed = len(details) - len(ok)
    avg_recall = round(sum(d["recall@5"] for d in ok) / len(ok), 4) if ok else 0.0
    avg_faith = round(sum(d["faithfulness"] for d in ok) / len(ok), 4) if ok else 0.0

    summary = {
        "total_questions": len(details),
        "evaluated": len(ok),
        "errors": failed,
        "avg_recall@5": avg_recall,
        "avg_faithfulness": avg_faith,
    }
    report = {"summary": summary,
              "details": [{k: v for k, v in d.items() if k != "_total"} for d in details]}

    with open(RESULT_PATH, "w", encoding="utf-8") as fp:
        json.dump(report, fp, ensure_ascii=False, indent=2)

    print("\n===== RAG 评估汇总报告 =====")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print("\n----- 逐题明细 -----")
    for d in report["details"]:
        if "error" in d:
            print(f"  [{d['index']:>2}] ERROR  {d['question'][:24]}  <- {d['error']}")
        else:
            print(f"  [{d['index']:>2}] recall@5={d['recall@5']} "
                  f"faith={d['faithfulness']:.2f}  {d['question'][:24]}")
    print(f"\n评估数据已保存 -> {RESULT_PATH}")
    return report


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="RAG 自动评估（recall@5 + faithfulness）")
    parser.add_argument("--limit", type=int, default=None, help="只评估前 N 条（调试用）")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    run_eval(limit=args.limit)


if __name__ == "__main__":
    main()
