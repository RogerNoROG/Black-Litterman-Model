#!/usr/bin/env python3
"""
从 Istero 拉取「央视国内要闻 latest」并写入 JSONL（字段与 RSS 爬虫一致，可供周频管线使用）。

配置：项目根 ``.env`` 中 ``ISTEREO_API_TOKEN=``（Bearer，勿带前缀 ``Bearer ``）。

示例::

    .venv/bin/python scripts/fetch_istero_cctv_news.py --append
    .venv/bin/python scripts/fetch_istero_cctv_news.py --out data/cctv_istero.jsonl
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass


def main() -> None:
    ap = argparse.ArgumentParser(description="Istero 央视要闻 → JSONL")
    ap.add_argument(
        "--out",
        type=Path,
        default=ROOT / "data" / "news_items.jsonl",
        help="输出 JSONL 路径",
    )
    ap.add_argument(
        "--append",
        action="store_true",
        help="追加写入；默认覆盖写入（仅本次拉取结果）",
    )
    ap.add_argument(
        "--url",
        type=str,
        default="",
        help="覆盖默认 Istero URL（空则使用 settings.istero_api_url）",
    )
    args = ap.parse_args()

    from emotion_bl.istero_news import fetch_cctv_china_latest_records

    url = args.url.strip() or None
    rows = fetch_cctv_china_latest_records(url=url)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if args.append else "w"
    encoding = "utf-8"
    with args.out.open(mode, encoding=encoding, newline="\n") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"已写入 {len(rows)} 条 → {args.out.resolve()}（append={args.append}）", flush=True)


if __name__ == "__main__":
    main()
