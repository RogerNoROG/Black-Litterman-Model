#!/usr/bin/env python3
"""
对**单个** ``YYYY-Www.jsonl`` 跑 FinBERT 并写简要聚合（调试用，不做 BL）。
  PYTHONPATH=code python scripts/score_one_week_jsonl.py news-data/by-week/zh/2023-W01.jsonl
"""
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_CODE = _ROOT / "code"
sys.path.insert(0, str(_CODE))

from finbert_bl.aggregate import ScoredNews, aggregate_by_stock, stock_s_values
from finbert_bl.finbert_classifier import FinBertSentiment
from finbert_bl.name_matcher import load_code_names_auto, match_codes_in_text


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: score_one_week_jsonl.py <path.jsonl> [--max-lines N]", file=sys.stderr)
        return 1
    p = Path(sys.argv[1])
    max_lines: int | None = None
    if "--max-lines" in sys.argv:
        i = sys.argv.index("--max-lines")
        max_lines = int(sys.argv[i + 1])
    if not p.is_file():
        print("not a file", p, file=sys.stderr)
        return 1
    codes = load_code_names_auto(None)
    rows: list[str] = []
    with p.open(encoding="utf-8") as f:
        for k, line in enumerate(f):
            if max_lines is not None and k >= max_lines:
                break
            line = line.strip()
            if not line:
                continue
            o = json.loads(line)
            rows.append(str(o.get("content", "") or o.get("text", "")))
    mdl = FinBertSentiment()
    sc = mdl.score_texts(rows) if rows else []
    it: list[ScoredNews] = []
    for body, s in zip(rows, sc, strict=True):
        mc = match_codes_in_text(body, codes)
        it.append(ScoredNews(text_id="", codes=mc, e=s.e, w=s.w))
    agg = aggregate_by_stock(it)
    sv = stock_s_values(agg)
    top = sorted(sv.items(), key=lambda p: p[1], reverse=True)[:15]
    print("n_lines", len(rows), "n_matched_stocks", len(sv))
    print("label hist", Counter(s.label_id for s in sc))
    print("top S_i", top)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
