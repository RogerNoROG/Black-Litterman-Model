#!/usr/bin/env python3
"""
周频自动化 Agent：RSS 爬取 → 按周五周线分桶 → LLM/情感生成观点 (P,Q,Ω)
→ 传入 ``code.black_litterman.BlackLitterman`` 与行情先验融合 → 输出每周权重。

实现逻辑见 ``emotion_bl.weekly_pipeline.run_weekly_pipeline``；本脚本仅解析 CLI 并调用。

依赖：仓库根目录 ``.env``（LLM 时）；行情区间 ``AKSHARE_*`` / JSON 路径须覆盖 ``--year``。

示例::

    # 在线拉 AkShare（默认）
    .venv/bin/python scripts/run_weekly_blm_agent.py --year 2025

    # 仅离线 JSON（需先有 code/Data/*.json）
    .venv/bin/python scripts/run_weekly_blm_agent.py --year 2025 --market-source json --skip-crawl

    # 已有新闻 JSONL，跳过爬虫
    .venv/bin/python scripts/run_weekly_blm_agent.py --year 2025 --skip-crawl --jsonl data/news_items.jsonl

    # 浏览器全流程（需先 ``python run_api.py``）
    # ./scripts/one_click_weekly_pipeline_web.sh
"""
from __future__ import annotations

import argparse
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

from emotion_bl.weekly_pipeline import run_weekly_pipeline  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(
        description="爬取新闻 → 按周情感/LLM 观点 → 沪深300前十成分 Black-Litterman 周频权重"
    )
    ap.add_argument("--year", type=int, default=None, help="自然年（默认：structures.BACK_TEST_YEAR）")
    ap.add_argument("--skip-crawl", action="store_true", help="不运行 Scrapy，直接读 --jsonl")
    ap.add_argument(
        "--market-source",
        type=str,
        default="",
        metavar="SOURCE",
        help="覆盖 structures.DATA_SOURCE（如 akshare | json | csv）；空则不改。离线可填 json（需本地 JSON）",
    )
    ap.add_argument(
        "--jsonl",
        type=Path,
        default=ROOT / "data" / "news_items.jsonl",
        help="新闻 JSONL（默认 data/news_items.jsonl）",
    )
    ap.add_argument(
        "--keyword-map",
        type=Path,
        default=ROOT / "data" / "csi300_top10_keywords.json",
        help="代码→关键词 JSON（keyword_map 映射 A 股标的）",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=ROOT / "data" / "weekly_blm_agent_result.json",
        help="输出 JSON（每周多条记录打包在 weeks 数组）",
    )
    ap.add_argument("--truncate-jsonl-before-crawl", action="store_true", help="爬取前清空 --jsonl")
    ap.add_argument("--max-weeks", type=int, default=0, help="仅处理前 N 周（0=全年）")
    ap.add_argument(
        "--feeds",
        type=str,
        default="",
        help="RSS URL，逗号分隔（仅在不加 --skip-crawl 时使用；空则沿用 spider 默认或 news_crawler/settings.py）",
    )
    args = ap.parse_args()

    feeds_arg = (
        [u.strip() for u in args.feeds.split(",") if u.strip()]
        if (args.feeds or "").strip()
        else None
    )

    try:
        run_weekly_pipeline(
            root=ROOT,
            year=args.year,
            skip_crawl=args.skip_crawl,
            market_source=args.market_source,
            jsonl_path=args.jsonl,
            keyword_map_path=args.keyword_map,
            out_path=args.out,
            truncate_jsonl_before_crawl=args.truncate_jsonl_before_crawl,
            max_weeks=args.max_weeks,
            feed_urls=feeds_arg,
        )
    except RuntimeError as e:
        print(str(e), flush=True)
        raise SystemExit(1) from e


if __name__ == "__main__":
    main()
