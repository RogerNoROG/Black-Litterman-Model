"""周频价/市值 JSON：`version`、`kind`、`records`；市值表含 `Total`。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

JSON_VERSION = 1
KIND_PRICE = "bl_weekly_prices"
KIND_MV = "bl_weekly_market_values"


def _load_doc(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"数据文件不存在: {p.resolve()}")
    with p.open(encoding="utf-8") as f:
        return json.load(f)


def _normalize_mv_columns(df: pd.DataFrame) -> pd.DataFrame:
    """与旧 Excel 一致：总市值列统一为 Total（兼容 TOTAL / total）。"""
    for alias in ("TOTAL", "total"):
        if "Total" not in df.columns and alias in df.columns:
            return df.rename(columns={alias: "Total"})
    return df


def _records_to_dataframe(records: List[Dict[str, Any]], *, require_total: bool) -> pd.DataFrame:
    if not records:
        raise ValueError("JSON records 为空")
    df = pd.DataFrame(records)
    if "Date" not in df.columns:
        raise ValueError("JSON 每条记录须包含 Date 字段")
    if require_total:
        df = _normalize_mv_columns(df)
    if require_total and "Total" not in df.columns:
        raise ValueError("市值 JSON 须包含 Total 列（或 TOTAL）")
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    return df


def load_price_dataframe(path: str) -> pd.DataFrame:
    doc = _load_doc(path)
    ver = doc.get("version", 1)
    if ver != JSON_VERSION:
        raise ValueError(f"不支持的 JSON version: {ver}（期望 {JSON_VERSION}）")
    records = doc.get("records")
    if not isinstance(records, list):
        raise ValueError("JSON 根对象须包含 records 数组")
    return _records_to_dataframe(records, require_total=False)


def load_market_value_dataframe(path: str) -> pd.DataFrame:
    doc = _load_doc(path)
    ver = doc.get("version", 1)
    if ver != JSON_VERSION:
        raise ValueError(f"不支持的 JSON version: {ver}（期望 {JSON_VERSION}）")
    records = doc.get("records")
    if not isinstance(records, list):
        raise ValueError("JSON 根对象须包含 records 数组")
    return _records_to_dataframe(records, require_total=True)


def save_price_dataframe(df: pd.DataFrame, path: str, *, kind: str = KIND_PRICE) -> None:
    out = df.copy()
    out["Date"] = pd.to_datetime(out["Date"]).dt.strftime("%Y-%m-%d")
    records = out.to_dict(orient="records")
    doc = {"version": JSON_VERSION, "kind": kind, "records": records}
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)


def save_market_value_dataframe(df: pd.DataFrame, path: str) -> None:
    save_price_dataframe(df, path, kind=KIND_MV)


def save_bl_tables_to_json(
    price_df: pd.DataFrame,
    mv_df: pd.DataFrame,
    price_path: str,
    mv_path: str,
) -> None:
    save_price_dataframe(price_df, price_path, kind=KIND_PRICE)
    save_market_value_dataframe(mv_df, mv_path)
