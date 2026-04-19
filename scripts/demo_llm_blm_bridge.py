#!/usr/bin/env python3
"""
使用子模块 third_party/LLM-BLM 中已附的 yfinance 收益、market_caps 与 responses JSON，
跑通 ``extras.llm_blm.bridge.run_llm_blm_period``（与上游 evaluate_multiple 同构）。

在仓库根目录执行（需已安装项目依赖与 venv）::

    .venv/bin/python scripts/demo_llm_blm_bridge.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from extras.llm_blm.bridge import (  # noqa: E402
    load_llm_blm_response_json,
    run_llm_blm_period,
)

import pandas as pd  # noqa: E402


def main() -> None:
    sub = ROOT / "third_party" / "LLM-BLM"
    if not sub.is_dir():
        print("未找到子模块 third_party/LLM-BLM，请执行: git submodule update --init --recursive")
        raise SystemExit(1)

    start, end = "2024-06-01", "2024-06-30"
    ret_path = sub / f"yfinance/returns_{start}_{end}.csv"
    resp_path = sub / f"responses/gemma_{start}_{end}.json"
    cap_path = sub / "market_caps.json"
    for p in (ret_path, resp_path, cap_path):
        if not p.exists():
            print(f"缺少上游数据文件: {p}")
            raise SystemExit(1)

    returns = pd.read_csv(ret_path, index_col=0)
    returns.index = pd.to_datetime(returns.index)

    with cap_path.open(encoding="utf-8") as f:
        market_caps = json.load(f)

    data_dict = load_llm_blm_response_json(resp_path)

    w, mu_bl, tickers = run_llm_blm_period(
        returns=returns,
        market_caps=market_caps,
        data_dict=data_dict,
        tau=0.025,
        rf=0.02,
        risk_aversion=0.1,
    )

    nz = [(t, float(x)) for t, x in zip(tickers, w) if x > 1e-6]
    nz.sort(key=lambda z: z[1], reverse=True)
    print(f"标的数 n={len(tickers)}, 权重>1e-6 的个数={len(nz)}")
    print("前10大权重:")
    for t, x in nz[:10]:
        print(f"  {t}: {x:.6f}")
    print("mu_bl 前5维:", mu_bl[:5])


if __name__ == "__main__":
    main()
