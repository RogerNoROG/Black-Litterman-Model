#!/usr/bin/env python3
"""
在**不修改** code/structures.py 的前提下，用指定 JSON 跑与 code/main.py 相同流程：

加载行情 → BlackLitterman（CC 收益、市值权、观点）→ 按自然年回测 → 保存 code/Plot 图。

用法::

  .venv/bin/python scripts/run_bl_main_json.py \\
    --price data/price_bl_2018_2023.json \\
    --mv data/market_value_bl_2018_2023.json \\
    --year 2023

要求：价/市值表须覆盖 [BACK_TEST_T 周, 回测年]；否则请拉长 JSON 或改小 structures.BACK_TEST_T 后本脚本同步。
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CODE = ROOT / "code"
sys.path.insert(0, str(CODE))
sys.path.insert(0, str(ROOT))

import importlib  # noqa: E402

import structures  # noqa: E402

os.chdir(CODE)  # 与在 code/ 下运行 main 时 cwd 一致（若需相对路径）


def _apply_structures(
    *,
    price_path: str,
    mv_path: str,
    year: int,
    backtest_t: int | None,
) -> None:
    """覆盖配置；并 reload data_providers，避免其 import 时拷贝的 DATA_SOURCE 仍为旧值。"""
    structures.DATA_SOURCE = "json"
    structures.PRICE_DATA_PATH = price_path
    structures.MV_DATA_PATH = mv_path
    structures.BACK_TEST_YEAR = int(year)
    structures.BACK_TEST_PERIOD_NAME = str(year)
    if backtest_t is not None:
        structures.BACK_TEST_T = int(backtest_t)
    import data_providers  # noqa: WPS433

    importlib.reload(data_providers)


def main() -> None:
    ap = argparse.ArgumentParser(description="main.py 等效：JSON 行情 + 指定回测年")
    ap.add_argument(
        "--price",
        type=Path,
        default=ROOT / "data" / "price_bl_2018_2023.json",
    )
    ap.add_argument(
        "--mv",
        type=Path,
        default=ROOT / "data" / "market_value_bl_2018_2023.json",
    )
    ap.add_argument("--year", type=int, default=2023)
    ap.add_argument(
        "--backtest-t",
        type=int,
        default=150,
        help="估计窗周数。200 周时数据须很早起点；2018-2023 全样本下首个 2023 周 iloc 约 153，默认 150。",
    )
    args = ap.parse_args()

    rp = str(args.price.resolve())
    mp = str(args.mv.resolve())
    for p, label in ((args.price, "price"), (args.mv, "mv")):
        if not p.is_file():
            raise SystemExit(f"文件不存在: {p}（{label}）")

    _apply_structures(
        price_path=rp, mv_path=mp, year=args.year, backtest_t=args.backtest_t
    )

    from back_test import BackTest  # noqa: WPS433
    from black_litterman import BlackLitterman  # noqa: WPS433
    from data_providers import fetch_price_market_pair  # noqa: WPS433

    try:
        sys.stdout.reconfigure(line_buffering=True)
    except (AttributeError, OSError, ValueError):
        pass

    print("-" * 30, "Black-Litterman (JSON year=%s)" % args.year, "-" * 30, flush=True)
    print("View type:", structures.VIEW_TYPE_NAME[structures.VIEW_TYPE], flush=True)
    print("BACK_TEST_T:", structures.BACK_TEST_T, flush=True)
    print("价:", rp, flush=True)
    print("市值:", mp, flush=True)
    print("正在加载行情（DATA_SOURCE=json）…", flush=True)

    price_df, mv_df, src_used = fetch_price_market_pair()
    print("Data source:", src_used, flush=True)
    print(
        f"数据就绪: price {price_df.shape}, mv {mv_df.shape}",
        flush=True,
    )
    bl = BlackLitterman(price_df=price_df, mv_df=mv_df)
    bl.price_path = rp
    bl.mv_path = mp
    bl.get_cc_return()
    bl.get_market_value_weight()

    print("-" * 30, "Back test", "-" * 30, flush=True)
    if structures.BACK_TEST_YEAR is not None:
        s_ix, e_ix = bl.backtest_iloc_range_for_year(structures.BACK_TEST_YEAR)
        print(
            f"Year {structures.BACK_TEST_YEAR}, iloc [{s_ix}, {e_ix}]",
            flush=True,
        )
        bt = BackTest(start_index=s_ix, end_index=e_ix)
    else:
        bt = BackTest()
    bt.back_test(bl)
    print("完成。", flush=True)


if __name__ == "__main__":
    main()
