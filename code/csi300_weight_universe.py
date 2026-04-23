"""
沪深300：按**官方成分权重**取前 N 只（用于可投资股票池 / 与 BL 维数一致）。

**时点一致（point-in-time）说明**
------------------------------
1. 权威数据源为**中证指数**发布的样本权重（`closeweight` xls 内带 ``日期`` 列，示样本权重适用参考日）。
2. ``ak.index_stock_cons_weight_csindex("000300")`` 在多数环境下返回**当前官网最新**一期权重，**不能**在接口里传入 `2018-12-31` 这类历史日一次性拉全历史。若做**长历史回测**，需任选其一：  
   - **自建快照库**：在每周/月调仓前运行拉取，将返回表（含``日期``、``成分券代码``、``权重``）**落盘**，回测时对该调仓日 **选取 ``权重表日期`` ≤ 调仓日** 的**最近**一期；或  
   - 购买/使用带历史截面的**中证/终端**数据；或  
   - **近似**：在回测日 T 用当日成分 + 日末总市值占比近似权重（实现更重，需另写管线）。

3. 本模块提供的 ``fetch_csi300_weights_table`` / ``top_n_by_weight`` 对**任意**含 ``成分券代码``、``权重`` 的 DataFrame 有效，便于你读**自己存的历史快照**后复用相同排序逻辑。

与 ``data_akshare.STOCKS`` 一致：元组为 ``(6位码, 列名)``，列名默认与代码相同。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Tuple

import pandas as pd

CSI300_INDEX_CODE = "000300"


def fetch_csi300_weights_table() -> pd.DataFrame:
    """拉取**最新**一期沪深300官方成分权重表（经 AkShare 中证接口）。"""
    try:
        import akshare as ak  # type: ignore
    except ImportError as e:
        raise ImportError(
            "需安装 AkShare: pip install akshare"
        ) from e
    df = ak.index_stock_cons_weight_csindex(symbol=CSI300_INDEX_CODE)
    if df is None or df.empty:
        raise ValueError("未获取到沪深300权重表")
    return df


def _norm_code(s: str) -> str:
    c = str(s).strip().zfill(6)
    if len(c) != 6 or not c.isdigit():
        raise ValueError(f"非6位股票代码: {s!r}")
    return c


def top_n_by_weight(
    weight_df: pd.DataFrame,
    n: int = 20,
    *,
    code_col: str = "成分券代码",
    weight_col: str = "权重",
) -> List[Tuple[str, str]]:
    """
    按 ``权重`` 降序取前 ``n`` 只，返回 ``data_akshare.STOCKS`` 风格的列表。

    ``weight_df`` 须为 ``fetch_csi300_weights_table`` 或自存历史快照的同类表结构。
    """
    if n < 1 or n > 300:
        raise ValueError("n 应在 1~300 之间")
    dfc = weight_df.copy()
    if code_col not in dfc.columns or weight_col not in dfc.columns:
        raise KeyError(
            f"表须含列 {code_col!r} 与 {weight_col!r}，实际: {dfc.columns.tolist()}"
        )
    dfc[weight_col] = pd.to_numeric(dfc[weight_col], errors="coerce")
    dfc = dfc.dropna(subset=[weight_col])
    dfc = dfc.sort_values(weight_col, ascending=False)
    out: List[Tuple[str, str]] = []
    for _, row in dfc.head(n).iterrows():
        code = _norm_code(str(row[code_col]))
        out.append((code, code))
    return out


def snapshot_info(weight_df: pd.DataFrame) -> dict:
    """从权重表中取出样本日期（若存在）及行数，便于存 JSON 元信息。"""
    d: dict = {"index": CSI300_INDEX_CODE, "n_rows": len(weight_df)}
    if "日期" in weight_df.columns:
        s = weight_df["日期"]
        s = pd.to_datetime(s, errors="coerce")
        if s.notna().any():
            d["weight_file_date"] = str(s.max())[:10]
    return d


def save_stocks_and_meta(
    stocks: List[Tuple[str, str]],
    path: Path,
    *,
    weight_df: pd.DataFrame | None = None,
) -> None:
    """将前 N 只列表与可选元信息写入 JSON。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    meta: dict = {"stocks": [[a, b] for a, b in stocks], "k": len(stocks)}
    if weight_df is not None:
        meta["snapshot"] = snapshot_info(weight_df)
    with path.open("w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def load_stocks_from_json(path: str | Path) -> List[Tuple[str, str]]:
    with open(path, encoding="utf-8") as f:
        doc = json.load(f)
    raw = doc.get("stocks")
    if not raw:
        raise ValueError("JSON 中缺少 stocks 数组")
    return [(str(a).zfill(6), str(b)) for a, b in raw]
