#!/usr/bin/env python3
"""
用 ``yiyanghkust/finbert-tone-chinese`` + 行业相对 (P,Q,Ω) 生成与 ``returns`` 对齐的
``mu_post``（**滞后 1 周**新闻、上一期市值权重），可喂给 ``rolling_bl_expost_loop.run_weekly_rolling_loop``。

示例::

  export PYTHONPATH=code
  python scripts/build_mu_post_finbert.py \\
    --price-json code/Data/price_data.json \\
    --mv-json code/Data/market_value_data.json \\
    --news-dir news-data/by-week/zh \\
    --out code/Data/mu_post_finbert.csv

首次运行会从 Hugging Face 下载约 0.1B 参数模型，需联网。
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# 以仓库内 ``code/`` 为包根
_ROOT = Path(__file__).resolve().parent.parent
_CODE = _ROOT / "code"
if str(_CODE) not in sys.path:
    sys.path.insert(0, str(_CODE))

import numpy as np
import pandas as pd
from data_json import load_market_value_dataframe, load_price_dataframe
from finbert_bl.finbert_classifier import FinBertSentiment
from finbert_bl.industry_map import load_code_to_industry
from finbert_bl.name_matcher import load_code_names_auto
from finbert_bl.pipeline import build_mu_post_dataframe


def _log_ret(df: pd.DataFrame, stock_cols: list[str], *, date_col: str = "Date") -> pd.DataFrame:
    d = pd.to_datetime(df[date_col])
    x = df[stock_cols].astype(float)
    lr = np.log(x) - np.log(x.shift(1))
    return pd.DataFrame(
        data=lr.iloc[1:].to_numpy(),
        index=d.iloc[1:].to_numpy(),
        columns=stock_cols,
    )


def main() -> None:
    p = argparse.ArgumentParser(description="周频 FinBERT+BL 后验 mu_post（滞后新闻）")
    p.add_argument("--price-json", type=Path, required=True)
    p.add_argument("--mv-json", type=Path, required=True)
    p.add_argument("--news-dir", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--code-names", type=Path, default=None, help="含 code+name 的 CSV/JSON；缺省用 AkShare 现成分")
    p.add_argument("--industry", type=Path, default=None, help="无则合成 industry 桶（见 finbert_bl.industry_map）")
    p.add_argument("--window", type=int, default=52)
    p.add_argument("--tau", type=float, default=0.05)
    p.add_argument("--c", type=float, default=3.0)
    p.add_argument("--alpha", type=float, default=0.01, help="Q 中对情绪差的缩放")
    p.add_argument("--pair-k", type=int, default=3)
    p.add_argument("--no-zscore", action="store_true", help="不对行业 bar S 做横截面 z")
    p.add_argument("--device", type=int, default=-1, help="-1=CPU, 0+ = GPU id")
    p.add_argument("--batch-size", type=int, default=8)
    args = p.parse_args()

    price = load_price_dataframe(str(args.price_json))
    mv = load_market_value_dataframe(str(args.mv_json))
    drop = {"Date", "CSI300.GI", "SSE.GI", "SZCI.GI", "sh000300", "sh000001", "sz399001", "Total", "TOTAL", "Total"}
    names = [c for c in price.columns if c not in drop]
    if not names:
        names = [c for c in price.columns if c != "Date"]

    p_log = _log_ret(price, names, date_col="Date")
    mv2 = mv.copy()
    mv2["Date"] = pd.to_datetime(mv2["Date"])
    mv2 = mv2.set_index("Date", drop=True)
    p_log.index = pd.to_datetime(p_log.index)
    mv2.index = pd.to_datetime(mv2.index)
    p_log, mv2 = p_log.align(mv2, join="inner", axis=0)
    mv2 = mv2.reindex(columns=p_log.columns, fill_value=0.0)

    code_tuples = load_code_names_auto(
        str(args.code_names) if args.code_names is not None else None
    )
    code_to_g = load_code_to_industry(str(args.industry) if args.industry else None)

    mdl = FinBertSentiment(device=args.device, batch_size=args.batch_size)
    mu = build_mu_post_dataframe(
        p_log,
        mv2,
        args.news_dir,
        code_tuples,
        list(p_log.columns),
        model=mdl,
        window=int(args.window),
        tau=args.tau,
        c=args.c,
        alpha=args.alpha,
        pair_k=int(args.pair_k),
        zscore_industry=not args.no_zscore,
        code_to_industry=code_to_g,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    mu.to_csv(args.out, encoding="utf-8-sig")
    print(" wrote", args.out, " shape=", mu.shape)


if __name__ == "__main__":
    main()
