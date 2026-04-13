"""
将旧版「美股 Wind」Excel（Weekly 工作表）转为项目使用的 JSON。
依赖：openpyxl、pandas；输出路径由 code/structures.py 的 PRICE_DATA_PATH / MV_DATA_PATH 决定。

用法（在仓库根目录）：
  .venv/bin/python extras/migrate_xlsx_to_json.py

默认读取 extras/legacy_data/wind_us_xlsx/ 下的 Price_Data.xlsx、Market_Value.xlsx；
若不存在则尝试 code/Data/ 下同名文件。
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CODE = ROOT / "code"
sys.path.insert(0, str(CODE))

import pandas as pd

from data_json import save_bl_tables_to_json
from structures import MV_DATA_PATH, PRICE_DATA_PATH


def _resolve_xlsx_paths() -> tuple[Path, Path]:
    legacy = ROOT / "extras" / "legacy_data" / "wind_us_xlsx"
    cand_price = [
        legacy / "Price_Data.xlsx",
        CODE / "Data" / "Price_Data.xlsx",
    ]
    cand_mv = [
        legacy / "Market_Value.xlsx",
        CODE / "Data" / "Market_Value.xlsx",
    ]
    px = next((p for p in cand_price if p.is_file()), None)
    mv = next((p for p in cand_mv if p.is_file()), None)
    if px is None or mv is None:
        print("未找到 xlsx。请将文件放入 extras/legacy_data/wind_us_xlsx/ 或 code/Data/")
        sys.exit(1)
    return px, mv


def main() -> None:
    xlsx_price, xlsx_mv = _resolve_xlsx_paths()
    sheet = "Weekly"

    price_df = pd.read_excel(xlsx_price, sheet_name=sheet)
    mv_df = pd.read_excel(xlsx_mv, sheet_name=sheet)
    price_df["Date"] = pd.to_datetime(price_df["Date"])
    mv_df["Date"] = pd.to_datetime(mv_df["Date"])
    for alias in ("TOTAL", "total"):
        if "Total" not in mv_df.columns and alias in mv_df.columns:
            mv_df = mv_df.rename(columns={alias: "Total"})
            break

    def _under_code(p: str) -> Path:
        q = p[2:] if p.startswith("./") else p
        return CODE / q

    out_price = _under_code(PRICE_DATA_PATH)
    out_mv = _under_code(MV_DATA_PATH)
    out_price.parent.mkdir(parents=True, exist_ok=True)

    save_bl_tables_to_json(
        price_df, mv_df, str(out_price), str(out_mv)
    )
    print("已写入:", out_price, out_mv)


if __name__ == "__main__":
    main()
