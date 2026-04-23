#!/usr/bin/env python3
"""
打印「沪深300 按官方权重」前 N 只 6 位代码，并可落盘 JSON 供研究/回测引用。

时点一致：见 ``code/csi300_weight_universe.py`` 模块说明；长历史需自建权重快照库。

示例::

  .venv/bin/python scripts/print_csi300_topn_weights.py --top 20
  .venv/bin/python scripts/print_csi300_topn_weights.py --top 20 --save data/csi300_top20_snapshot.json
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "code"))

from csi300_weight_universe import (  # noqa: E402
    fetch_csi300_weights_table,
    save_stocks_and_meta,
    top_n_by_weight,
)


def main() -> None:
    ap = argparse.ArgumentParser(description="沪深300 权重前 N → 代码列表")
    ap.add_argument("--top", type=int, default=20, help="取前 N 只，默认 20")
    ap.add_argument(
        "--save",
        type=Path,
        default=None,
        help="将列表与元信息写入 JSON 路径",
    )
    args = ap.parse_args()

    df = fetch_csi300_weights_table()
    if "日期" in df.columns:
        d0 = df["日期"].iloc[0]
        print("权重表日期(样本):", d0, flush=True)
    rows = top_n_by_weight(df, n=args.top)
    print("前{}只 (代码, 列名):".format(args.top), flush=True)
    for t in rows:
        print(" ", t[0], flush=True)
    if args.save:
        save_stocks_and_meta(rows, args.save, weight_df=df)
        print("已保存:", args.save.resolve(), flush=True)


if __name__ == "__main__":
    main()
