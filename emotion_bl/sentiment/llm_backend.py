"""
通过 OpenAI 兼容的 Chat Completions API 做情感打分。

默认对接阿里云百炼「通义千问」兼容模式：
  LLM_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
  LLM_API_KEY=<百炼控制台 API-Key>
  请求头：Authorization: Bearer <API-Key>（与 OpenAI 相同）

其它厂商（OpenAI、DeepSeek、Azure 等）改 LLM_BASE_URL / LLM_MODEL 即可。
"""
from __future__ import annotations

import json
import re
import time
from typing import Literal

import httpx

from emotion_bl.config import settings
from emotion_bl.sentiment.analyzer import SentimentResult

SYSTEM_PROMPT = """你是金融文本情绪分析助手。根据给定新闻标题与摘要，判断对标的资产或宏观情绪的倾向强度。
只输出一行 JSON 对象，不要 markdown，不要其它文字。字段：
- score: 浮点数，范围 [-1,1]，-1 强烈负面，0 中性，1 强烈正面
- label: 字符串，必须是 "negative" | "neutral" | "positive" 之一
"""

SYSTEM_PROMPT_WITH_TICKERS = """你是金融文本情绪分析助手。根据给定新闻标题与摘要，完成两件事：（1）情绪倾向；（2）从文中自行识别明确涉及的股票代码（不要依赖外部关键词表）。
只输出一行 JSON 对象，不要 markdown，不要其它文字。字段：
- score: 浮点数，范围 [-1,1]，-1 强烈负面，0 中性，1 强烈正面
- label: 字符串，必须是 "negative" | "neutral" | "positive" 之一
- tickers: 字符串数组。仅填交易所常用代码：美股如 ORCL、AAPL；港股如 0700.HK。文中无明确标的或无法可靠对应代码时填 []。最多 8 个，勿输出公司全名。
"""


def analyze_text_llm(text: str) -> SentimentResult:
    key = (settings.llm_api_key or "").strip()
    if not key:
        raise RuntimeError("使用 LLM 情感需在环境变量或 .env 中设置 LLM_API_KEY（通义为百炼 API-Key）")

    use_tickers = (settings.ticker_mapping_mode or "").strip().lower() == "llm_json"
    system_prompt = SYSTEM_PROMPT_WITH_TICKERS if use_tickers else SYSTEM_PROMPT

    url = settings.llm_base_url.rstrip("/") + "/chat/completions"
    body: dict = {
        "model": settings.llm_model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": (text or "")[:8000]},
        ],
        "temperature": 0.2,
    }
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }

    read_sec = max(5.0, float(settings.llm_per_article_read_sec))
    max_retries = max(0, int(settings.llm_per_article_max_retries))
    attempts = 1 + max_retries
    timeout = httpx.Timeout(
        connect=20.0,
        read=read_sec,
        write=max(60.0, read_sec),
        pool=max(60.0, read_sec),
    )

    t0 = time.perf_counter()
    data = None
    r = None
    for attempt in range(attempts):
        try:
            with httpx.Client(timeout=timeout) as client:
                body_json = {**body, "response_format": {"type": "json_object"}}
                r = client.post(url, headers=headers, json=body_json)
                if r.status_code == 400 and "response_format" in r.text.lower():
                    r = client.post(url, headers=headers, json=body)
                r.raise_for_status()
                data = r.json()
            break
        except httpx.TimeoutException as e:
            if attempt + 1 >= attempts:
                raise RuntimeError(
                    f"单条新闻 LLM 读超时（单次 read 上限 {read_sec:.0f}s，"
                    f"环境变量 LLM_PER_ARTICLE_READ_SEC；已重试 {max_retries} 次仍失败，共 {attempts} 次请求）。"
                    f"可尝试略增大 LLM_PER_ARTICLE_READ_SEC 或降低 LLM_MAX_CONCURRENT。"
                    f" 原始错误: {e}"
                ) from e
            time.sleep(min(4.0, 1.0 * (attempt + 1)))

    assert data is not None and r is not None

    http_s = time.perf_counter() - t0
    content = data["choices"][0]["message"]["content"]
    score, label, tickers = _parse_llm_json(content, extract_tickers=use_tickers)
    return SentimentResult(
        score=score,
        label=label,
        backend=f"llm:{settings.llm_model} · HTTP {http_s:.2f}s · status {r.status_code}",
        mentioned_tickers=tickers,
    )


def _parse_llm_json(
    content: str, *, extract_tickers: bool
) -> tuple[float, Literal["negative", "neutral", "positive"], list[str]]:
    raw = (content or "").strip()
    m = re.search(r"\{[^{}]*\}", raw, re.DOTALL)
    if m:
        raw = m.group(0)
    if not raw:
        raise RuntimeError(
            "大模型返回的正文为空或无可解析的 JSON。"
            " 常见于网关超时后仍返回 200、或 content 被截断；请重试该条。"
        )
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as e:
        preview = (content or "")[:400].replace("\n", " ")
        raise RuntimeError(
            f"大模型返回无法解析为 JSON（{e}）。正文预览: {preview!r}"
        ) from e
    score = float(obj["score"])
    score = max(-1.0, min(1.0, score))
    lab = str(obj.get("label", "neutral")).lower()
    if lab not in ("negative", "neutral", "positive"):
        lab = "neutral"
    tickers: list[str] = []
    if extract_tickers:
        from emotion_bl.tagging import normalize_ticker_symbol

        raw_syms = obj.get("tickers") if "tickers" in obj else obj.get("mentioned_tickers")
        if isinstance(raw_syms, str):
            raw_syms = [raw_syms]
        if isinstance(raw_syms, list):
            for x in raw_syms[:12]:
                nt = normalize_ticker_symbol(str(x))
                if nt and nt not in tickers:
                    tickers.append(nt)
    return score, lab, tickers  # type: ignore[return-value]
