"""
将新闻文本映射到资产代码：基于关键词表，可替换为 NER / 行业分类模型。
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable


DEFAULT_KEYWORD_MAP: dict[str, list[str]] = {
    "AAPL": ["Apple", "苹果", "iPhone", "库克"],
    "MSFT": ["Microsoft", "微软", "Azure", "OpenAI"],
    "GOOGL": ["Google", "谷歌", "Alphabet", "Gemini"],
    "NVDA": ["NVIDIA", "英伟达", "GPU", "CUDA"],
    "TSLA": ["Tesla", "特斯拉", "马斯克", "Musk"],
    "BABA": ["Alibaba", "阿里巴巴", "淘宝", "天猫"],
    "0700.HK": ["腾讯", "Tencent", "微信", "WeChat"],
    # 国际财经新闻里常见公司（RSS 多为英文时，仅靠上表易 0 命中）
    "ORCL": ["Oracle", "甲骨文"],
    "META": ["Meta", "Facebook", "Instagram", "脸书"],
    "AMZN": ["Amazon", "亚马逊", "AWS", "Alexa"],
}


def load_keyword_map(path: Path | None) -> dict[str, list[str]]:
    if path is None or not path.exists():
        return dict(DEFAULT_KEYWORD_MAP)
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    return {str(k): list(v) for k, v in data.items()}


def match_tickers(text: str, keyword_map: dict[str, list[str]]) -> list[str]:
    if not text:
        return []
    found: set[str] = set()
    lower = text.lower()
    for ticker, kws in keyword_map.items():
        for kw in kws:
            if len(kw) <= 2 and kw.isalpha():
                if re.search(rf"\b{re.escape(kw.lower())}\b", lower):
                    found.add(ticker)
                    break
            elif kw.lower() in lower or kw in text:
                found.add(ticker)
                break
    return sorted(found)


def aggregate_scores_by_ticker(
    records: Iterable[dict],
    keyword_map: dict[str, list[str]],
    text_key: str = "text",
) -> dict[str, list[float]]:
    """records: {text, score, ...}"""
    buckets: dict[str, list[float]] = {}
    for r in records:
        text = str(r.get(text_key, ""))
        score = float(r.get("score", 0.0))
        for t in match_tickers(text, keyword_map):
            buckets.setdefault(t, []).append(score)
    return {t: vals for t, vals in buckets.items() if vals}


_TICKER_SYM_RE = re.compile(r"^[\dA-Z][\dA-Z.\-]{0,15}$", re.IGNORECASE)


def normalize_ticker_symbol(s: str) -> str | None:
    """将 LLM/用户输入规范为 ticker 字符串；不合法则 None。"""
    t = (s or "").strip().upper().replace(" ", "")
    if not t or len(t) > 16:
        return None
    if not _TICKER_SYM_RE.match(t):
        return None
    return t


def aggregate_scores_from_news_records(
    records: Iterable[dict],
    *,
    keyword_map: dict[str, list[str]],
    text_key: str = "text",
    mapping_mode: str = "keyword_map",
    llm_json_fallback_keyword: bool = True,
) -> dict[str, list[float]]:
    """
    keyword_map：子串/词边界匹配关键词表。
    llm_json：优先用每条记录里的 mentioned_tickers（来自 LLM）；可选回退关键词表。
    """
    mode = (mapping_mode or "keyword_map").strip().lower()
    if mode != "llm_json":
        return aggregate_scores_by_ticker(records, keyword_map, text_key=text_key)

    buckets: dict[str, list[float]] = {}
    for r in records:
        text = str(r.get(text_key, ""))
        score = float(r.get("score", 0.0))
        raw = r.get("mentioned_tickers")
        ticks: list[str] = []
        if isinstance(raw, list):
            for x in raw:
                nt = normalize_ticker_symbol(str(x))
                if nt and nt not in ticks:
                    ticks.append(nt)
        if not ticks and llm_json_fallback_keyword:
            ticks = match_tickers(text, keyword_map)
        for t in ticks:
            buckets.setdefault(t, []).append(score)
    return {t: vals for t, vals in buckets.items() if vals}
