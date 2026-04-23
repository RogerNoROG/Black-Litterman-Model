#!/usr/bin/env python3
"""
按自然年拉取与主项目一致的周频价/市值 JSON（AkShare，逻辑同 data_akshare.fetch_bl_tables）。

示例::

  .venv/bin/python scripts/fetch_market_year.py --year 2023
  .venv/bin/python scripts/fetch_market_year.py --start 2023-01-01 --end 2023-06-30 --out-suffix 2023h1
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "code"))

from data_akshare import fetch_bl_tables  # noqa: E402
from data_json import save_bl_tables_to_json  # noqa: E402
from structures import AKSHARE_ADJUST  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="拉取指定区间的周频价/市值 JSON")
    ap.add_argument("--year", type=int, default=None, help="自然年，如 2023（等价于该年 1-1 至 12-31）")
    ap.add_argument("--start", type=str, default="", help="开始日 YYYY-MM-DD（与 --year 二选一）")
    ap.add_argument("--end", type=str, default="", help="结束日 YYYY-MM-DD")
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=ROOT / "data",
        help="输出目录（默认项目 data/）",
    )
    ap.add_argument(
        "--out-suffix",
        type=str,
        default="",
        help="文件名后缀，默认用年份或 start_end",
    )
    args = ap.parse_args()

    if args.year is not None:
        start, end = f"{args.year}-01-01", f"{args.year}-12-31"
        suffix = args.out_suffix.strip() or str(args.year)
    else:
        start, end = args.start.strip(), args.end.strip()
        if not start or not end:
            ap.error("请指定 --year 或同时指定 --start 与 --end")
        suffix = args.out_suffix.strip() or f"{start}_{end}".replace("-", "")

    print(f"拉取 {start} ~ {end} …", flush=True)
    price_df, mv_df = fetch_bl_tables(start, end, adjust=AKSHARE_ADJUST)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    pp = args.out_dir / f"price_{suffix}.json"
    mp = args.out_dir / f"market_value_{suffix}.json"
    save_bl_tables_to_json(price_df, mv_df, str(pp), str(mp))
    print(
        f"行数 {len(price_df)}，Date {price_df['Date'].min()} → {price_df['Date'].max()}",
        flush=True,
    )
    print(pp, mp, sep="\n", flush=True)


if __name__ == "__main__":
    main()
