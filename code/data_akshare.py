"""AkShare：沪深300相关指数 + 前十权重股周频收盘价；市值优先东财现货总市值。"""

from __future__ import annotations

import time
from typing import Dict, List, Optional, Tuple

import pandas as pd

# 三大指数列名（与 black_litterman 中 INDEX_NUMBER 0/1/2 对应）
INDEX_SINA: List[Tuple[str, str]] = [
    ("sh000300", "CSI300.GI"),
    ("sh000001", "SSE.GI"),
    ("sz399001", "SZCI.GI"),
]

# 沪深300 权重前列 10 只（代码与列名一致；顺序与 get_views_P_Q_matrix 中 P 的下标绑定）
STOCKS: List[Tuple[str, str]] = [
    ("300750", "300750"),  # 宁德时代
    ("600519", "600519"),  # 贵州茅台
    ("300308", "300308"),  # 中际旭创
    ("601318", "601318"),  # 中国平安
    ("601899", "601899"),  # 紫金矿业
    ("600036", "600036"),  # 招商银行
    ("300502", "300502"),  # 新易盛
    ("000333", "000333"),  # 美的集团
    ("600900", "600900"),  # 长江电力
    ("601166", "601166"),  # 兴业银行
]

_FETCH_PAUSE_SEC = 0.2


def _pause() -> None:
    time.sleep(_FETCH_PAUSE_SEC)


def _import_akshare():
    try:
        import akshare as ak  # type: ignore
    except ImportError as e:
        raise ImportError(
            "使用 AkShare 数据源请先安装：pip install akshare（建议在项目 venv 中安装）"
        ) from e
    return ak


def _daily_index_close_cn(ak, sina_symbol: str) -> pd.Series:
    df = ak.stock_zh_index_daily(symbol=sina_symbol)
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    s = df.set_index("date")["close"].sort_index().astype("float64")
    return s


def _daily_stock_close_cn(
    ak, symbol: str, adjust: str, start_d: pd.Timestamp, end_d: pd.Timestamp
) -> pd.Series:
    sd = (start_d - pd.Timedelta(days=420)).strftime("%Y%m%d")
    ed = end_d.strftime("%Y%m%d")
    df = ak.stock_zh_a_hist(
        symbol=symbol, period="daily", start_date=sd, end_date=ed, adjust=adjust
    )
    df = df.copy()
    df["date"] = pd.to_datetime(df["日期"])
    s = df.set_index("date")["收盘"].sort_index().astype("float64")
    return s


def _to_weekly_close(series: pd.Series) -> pd.Series:
    return series.resample("W-FRI").last().dropna()


def _fetch_spot_mcap_map_cn(ak) -> Optional[Dict[str, float]]:
    try:
        spot = ak.stock_zh_a_spot_em()
    except Exception:
        return None
    if spot is None or spot.empty:
        return None
    code_col = "代码" if "代码" in spot.columns else None
    cap_col = "总市值" if "总市值" in spot.columns else None
    if not (code_col and cap_col):
        return None
    out: Dict[str, float] = {}
    for _, row in spot.iterrows():
        code = str(row[code_col]).strip().zfill(6)
        cap = pd.to_numeric(row[cap_col], errors="coerce")
        if code and pd.notna(cap):
            out[code] = float(cap)
    return out or None


def _infer_shares(
    weekly_panel: pd.DataFrame,
    mcap_map: Optional[Dict[str, float]],
    stock_codes: List[str],
    col_names: List[str],
) -> Dict[str, float]:
    shares: Dict[str, float] = {}
    last_row = weekly_panel.iloc[-1]
    for code, col in zip(stock_codes, col_names):
        px = float(last_row[col])
        mcap = mcap_map.get(code) if mcap_map else None
        if mcap is not None and px > 0:
            shares[col] = mcap / px
        else:
            shares[col] = 1.0
    return shares


def fetch_bl_tables(
    start_date: str,
    end_date: str,
    *,
    adjust: str = "qfq",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    拉取并返回 (price_df, market_value_df)，均含 `Date` 列。
    """
    ak = _import_akshare()
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)

    weekly_parts: Dict[str, pd.Series] = {}
    for sina_sym, col in INDEX_SINA:
        s = _to_weekly_close(_daily_index_close_cn(ak, sina_sym))
        weekly_parts[col] = s
        _pause()

    stock_codes = [t[0] for t in STOCKS]
    stock_cols = [t[1] for t in STOCKS]
    for code, col in STOCKS:
        s = _to_weekly_close(_daily_stock_close_cn(ak, code, adjust, start, end))
        weekly_parts[col] = s
        _pause()

    panel = pd.DataFrame(weekly_parts).sort_index()
    panel = panel.loc[(panel.index >= start) & (panel.index <= end)]
    panel = panel.sort_index().ffill(limit=8).dropna(how="any")

    ordered_cols = [c for _, c in INDEX_SINA] + stock_cols
    panel = panel[ordered_cols]

    price_df = panel.reset_index()
    price_df.columns = ["Date"] + ordered_cols
    price_df["Date"] = pd.to_datetime(price_df["Date"])

    mcap_map = _fetch_spot_mcap_map_cn(ak)
    _pause()
    shares = _infer_shares(panel, mcap_map, stock_codes, stock_cols)

    mv_data = {col: panel[col] * shares[col] for col in stock_cols}
    mv_panel = pd.DataFrame(mv_data, index=panel.index)
    mv_panel["Total"] = mv_panel.sum(axis=1)
    mv_df = mv_panel.reset_index()
    mv_df.columns = ["Date"] + stock_cols + ["Total"]
    mv_df["Date"] = pd.to_datetime(mv_df["Date"])

    return price_df, mv_df


if __name__ == "__main__":
    import os
    import sys

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from data_json import save_bl_tables_to_json
    from structures import (
        AKSHARE_ADJUST,
        AKSHARE_END_DATE,
        AKSHARE_SAVE_JSON_AFTER_FETCH,
        AKSHARE_START_DATE,
        MV_DATA_PATH,
        PRICE_DATA_PATH,
    )

    p, m = fetch_bl_tables(AKSHARE_START_DATE, AKSHARE_END_DATE, adjust=AKSHARE_ADJUST)
    print("price:", p.shape, "mv:", m.shape)
    print(p.head(2))
    if AKSHARE_SAVE_JSON_AFTER_FETCH:
        os.makedirs(os.path.dirname(os.path.abspath(PRICE_DATA_PATH)) or ".", exist_ok=True)
        save_bl_tables_to_json(p, m, PRICE_DATA_PATH, MV_DATA_PATH)
        print("已写入", PRICE_DATA_PATH, MV_DATA_PATH)
