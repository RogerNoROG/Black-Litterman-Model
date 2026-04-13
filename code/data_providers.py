"""
按 structures.DATA_SOURCE 加载 (price_df, mv_df)，列结构与 AkShare/JSON 一致。
支持：akshare、json、csv、baostock、tushare。
主源失败时按 DATA_SOURCE_FALLBACK 自动切换（见 fetch_price_market_pair）。
"""

from __future__ import annotations

import os
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd

from data_akshare import INDEX_SINA, STOCKS, fetch_bl_tables
from data_json import save_bl_tables_to_json
from structures import (
    AKSHARE_ADJUST,
    AKSHARE_END_DATE,
    AKSHARE_SAVE_JSON_AFTER_FETCH,
    AKSHARE_START_DATE,
    DATA_SOURCE,
    DATA_SOURCE_FALLBACK,
    MV_CSV_PATH,
    MV_DATA_PATH,
    PRICE_CSV_PATH,
    PRICE_DATA_PATH,
    BAOSTOCK_ADJUSTFLAG,
    TUSHARE_REQUEST_PAUSE_SEC,
    TUSHARE_TOKEN,
)

# Baostock 证券代码（与 INDEX_SINA / STOCKS 列顺序一致）
INDEX_BAOSTOCK: List[Tuple[str, str]] = [
    ("sh.000300", "CSI300.GI"),
    ("sh.000001", "SSE.GI"),
    ("sz.399001", "SZCI.GI"),
]

# Tushare ts_code（与 INDEX_SINA / STOCKS 列顺序一致）
INDEX_TUSHARE: List[Tuple[str, str]] = [
    ("000300.SH", "CSI300.GI"),
    ("000001.SH", "SSE.GI"),
    ("399001.SZ", "SZCI.GI"),
]


def _ts_date_bounds() -> Tuple[str, str]:
    s = pd.Timestamp(AKSHARE_START_DATE).strftime("%Y%m%d")
    e = pd.Timestamp(AKSHARE_END_DATE).strftime("%Y%m%d")
    return s, e


def _ts_stock_ts_code(code: str) -> str:
    c = str(code).zfill(6)
    if c.startswith(("6", "9")):
        return f"{c}.SH"
    return f"{c}.SZ"


def _tushare_stock_adj_param(ak_adj: str) -> Optional[str]:
    a = (ak_adj or "").strip().lower()
    if a == "qfq":
        return "qfq"
    if a == "hfq":
        return "hfq"
    return None


def _tushare_token() -> str:
    t = os.environ.get("TUSHARE_TOKEN", "").strip()
    if t:
        return t
    return (TUSHARE_TOKEN or "").strip()


def _to_weekly_close(series: pd.Series) -> pd.Series:
    return series.resample("W-FRI").last().dropna()


def _tushare_pause() -> None:
    time.sleep(max(0.0, float(TUSHARE_REQUEST_PAUSE_SEC or 0)))


def _normalize_mv_total(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for alias in ("TOTAL", "total"):
        if "Total" not in df.columns and alias in df.columns:
            return df.rename(columns={alias: "Total"})
    return df


def _load_json_tables() -> Tuple[pd.DataFrame, pd.DataFrame]:
    from data_json import load_market_value_dataframe, load_price_dataframe

    return (
        load_price_dataframe(PRICE_DATA_PATH),
        load_market_value_dataframe(MV_DATA_PATH),
    )


def _load_csv_tables() -> Tuple[pd.DataFrame, pd.DataFrame]:
    pp = PRICE_CSV_PATH
    mp = MV_CSV_PATH
    if not os.path.isfile(pp) or not os.path.isfile(mp):
        raise FileNotFoundError(
            f"CSV 数据源需同时存在: {pp} 与 {mp}（UTF-8，首列为 Date）"
        )
    price = pd.read_csv(pp, encoding="utf-8-sig")
    mv = pd.read_csv(mp, encoding="utf-8-sig")
    price["Date"] = pd.to_datetime(price["Date"])
    mv["Date"] = pd.to_datetime(mv["Date"])
    mv = _normalize_mv_total(mv)
    if "Total" not in mv.columns:
        raise ValueError("市值 CSV 须含 Total 列（或 TOTAL）")
    return price, mv


def _bs_stock_symbol(code: str) -> str:
    c = str(code).zfill(6)
    return f"sh.{c}" if c.startswith("6") else f"sz.{c}"


def _bs_weekly_series(
    bs, symbol: str, start: str, end: str, adjustflag: str
) -> pd.Series:
    rs = bs.query_history_k_data_plus(
        symbol,
        "date,close",
        start_date=start,
        end_date=end,
        frequency="w",
        adjustflag=adjustflag,
    )
    if rs.error_code != "0":
        raise RuntimeError(f"baostock {symbol}: {rs.error_msg}")
    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        raise RuntimeError(f"baostock 无数据: {symbol}")
    df = pd.DataFrame(rows, columns=rs.fields)
    df["date"] = pd.to_datetime(df["date"])
    s = pd.to_numeric(df["close"], errors="coerce")
    s.index = df["date"]
    return s.sort_index().astype("float64")


def _load_baostock_tables() -> Tuple[pd.DataFrame, pd.DataFrame]:
    try:
        import baostock as bs
    except ImportError as e:
        raise ImportError(
            "DATA_SOURCE='baostock' 需要安装: pip install baostock"
        ) from e

    start = pd.Timestamp(AKSHARE_START_DATE).strftime("%Y-%m-%d")
    end = pd.Timestamp(AKSHARE_END_DATE).strftime("%Y-%m-%d")
    adj = BAOSTOCK_ADJUSTFLAG

    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"baostock 登录失败: {lg.error_msg}")
    try:
        parts: Dict[str, pd.Series] = {}
        for sym, col in INDEX_BAOSTOCK:
            parts[col] = _bs_weekly_series(bs, sym, start, end, adj)
        for code, col in STOCKS:
            parts[col] = _bs_weekly_series(
                bs, _bs_stock_symbol(code), start, end, adj
            )

        panel = pd.DataFrame(parts).sort_index()
        panel = panel.sort_index().ffill(limit=8).dropna(how="any")
        start_ts = pd.Timestamp(AKSHARE_START_DATE)
        end_ts = pd.Timestamp(AKSHARE_END_DATE)
        panel = panel.loc[(panel.index >= start_ts) & (panel.index <= end_ts)]

        ordered = [c for _, c in INDEX_SINA] + [t[1] for t in STOCKS]
        panel = panel[ordered]

        price_df = panel.reset_index()
        price_df.columns = ["Date"] + ordered
        price_df["Date"] = pd.to_datetime(price_df["Date"])

        stock_cols = [t[1] for t in STOCKS]
        mv_panel = pd.DataFrame(
            {c: panel[c] * 1.0 for c in stock_cols}, index=panel.index
        )
        mv_panel["Total"] = mv_panel.sum(axis=1)
        mv_df = mv_panel.reset_index()
        mv_df.columns = ["Date"] + stock_cols + ["Total"]
        mv_df["Date"] = pd.to_datetime(mv_df["Date"])
        return price_df, mv_df
    finally:
        bs.logout()


def _tushare_series_from_df(df: pd.DataFrame, label: str) -> pd.Series:
    if df is None or df.empty:
        raise RuntimeError(f"Tushare 无数据: {label}")
    d = df.copy()
    d["trade_date"] = pd.to_datetime(d["trade_date"], format="%Y%m%d", errors="coerce")
    if d["trade_date"].isna().all():
        d["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    s = pd.to_numeric(d["close"], errors="coerce")
    s.index = d["trade_date"]
    return s.sort_index().dropna().astype("float64")


def _tushare_index_daily_series(pro, ts_code: str, start: str, end: str) -> pd.Series:
    _tushare_pause()
    df = pro.index_daily(ts_code=ts_code, start_date=start, end_date=end)
    return _tushare_series_from_df(df, f"index_daily {ts_code}")


def _tushare_stock_daily_series(
    pro, ts_code: str, start: str, end: str, adj: Optional[str]
) -> pd.Series:
    _tushare_pause()
    if adj:
        df = pro.pro_bar(
            ts_code=ts_code, start_date=start, end_date=end, freq="D", adj=adj
        )
    else:
        df = pro.daily(ts_code=ts_code, start_date=start, end_date=end)
    return _tushare_series_from_df(df, f"stock {'pro_bar' if adj else 'daily'} {ts_code}")


def _infer_share_tushare_basic(
    pro,
    ts_code: str,
    start: str,
    end: str,
    daily_close: pd.Series,
) -> float:
    _tushare_pause()
    db = pro.daily_basic(
        ts_code=ts_code,
        start_date=start,
        end_date=end,
        fields="trade_date,total_mv",
    )
    if db is None or db.empty:
        return 1.0
    db = db.copy()
    db["trade_date"] = pd.to_datetime(db["trade_date"], format="%Y%m%d", errors="coerce")
    if db["trade_date"].isna().all():
        db["trade_date"] = pd.to_datetime(db["trade_date"], errors="coerce")
    db["total_mv"] = pd.to_numeric(db["total_mv"], errors="coerce")
    db = db.sort_values("trade_date").dropna(subset=["total_mv", "trade_date"])
    if db.empty:
        return 1.0
    last = db.iloc[-1]
    td = pd.Timestamp(last["trade_date"])
    mv_wan = float(last["total_mv"])
    if mv_wan <= 0:
        return 1.0
    sub = daily_close.loc[daily_close.index <= td]
    if sub.empty:
        return 1.0
    close = float(sub.iloc[-1])
    if close <= 0:
        return 1.0
    return (mv_wan * 10000.0) / close


def _load_tushare_tables() -> Tuple[pd.DataFrame, pd.DataFrame]:
    try:
        import tushare as ts
    except ImportError as e:
        raise ImportError(
            "DATA_SOURCE='tushare' 需要安装: pip install tushare"
        ) from e

    token = _tushare_token()
    if not token:
        raise ValueError(
            "Tushare 需设置环境变量 TUSHARE_TOKEN 或在 structures.TUSHARE_TOKEN 填写 token（https://tushare.pro）"
        )

    start_d = pd.Timestamp(AKSHARE_START_DATE)
    end_d = pd.Timestamp(AKSHARE_END_DATE)
    start_s, end_s = _ts_date_bounds()
    adj = _tushare_stock_adj_param(AKSHARE_ADJUST)

    pro = ts.pro_api(token)

    weekly_parts: Dict[str, pd.Series] = {}
    for ts_code, col in INDEX_TUSHARE:
        s = _to_weekly_close(_tushare_index_daily_series(pro, ts_code, start_s, end_s))
        weekly_parts[col] = s

    stock_codes = [t[0] for t in STOCKS]
    stock_cols = [t[1] for t in STOCKS]
    daily_by_col: Dict[str, pd.Series] = {}
    for code, col in STOCKS:
        tsym = _ts_stock_ts_code(code)
        dseries = _tushare_stock_daily_series(pro, tsym, start_s, end_s, adj)
        daily_by_col[col] = dseries
        weekly_parts[col] = _to_weekly_close(dseries)

    panel = pd.DataFrame(weekly_parts).sort_index()
    panel = panel.loc[(panel.index >= start_d) & (panel.index <= end_d)]
    panel = panel.sort_index().ffill(limit=8).dropna(how="any")

    ordered = [c for _, c in INDEX_TUSHARE] + stock_cols
    panel = panel[ordered]

    price_df = panel.reset_index()
    price_df.columns = ["Date"] + ordered
    price_df["Date"] = pd.to_datetime(price_df["Date"])

    shares: Dict[str, float] = {}
    for code, col in zip(stock_codes, stock_cols):
        tsym = _ts_stock_ts_code(code)
        shares[col] = _infer_share_tushare_basic(
            pro, tsym, start_s, end_s, daily_by_col[col]
        )

    mv_data = {col: panel[col] * shares[col] for col in stock_cols}
    mv_panel = pd.DataFrame(mv_data, index=panel.index)
    mv_panel["Total"] = mv_panel.sum(axis=1)
    mv_df = mv_panel.reset_index()
    mv_df.columns = ["Date"] + stock_cols + ["Total"]
    mv_df["Date"] = pd.to_datetime(mv_df["Date"])

    return price_df, mv_df


def _fallback_source_list() -> List[str]:
    raw = DATA_SOURCE_FALLBACK
    if raw is None:
        return []
    if isinstance(raw, str):
        return [x.strip().lower() for x in raw.split(",") if x.strip()]
    return [str(x).strip().lower() for x in raw if str(x).strip()]


def _source_try_chain() -> List[str]:
    primary = (DATA_SOURCE or "akshare").strip().lower()
    seen = {primary}
    chain = [primary]
    for s in _fallback_source_list():
        if s not in seen:
            seen.add(s)
            chain.append(s)
    return chain


def _load_from_source(src: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """单一数据源；失败时抛异常，由 fetch_price_market_pair 决定是否换源。"""
    if src == "akshare":
        price_df, mv_df = fetch_bl_tables(
            AKSHARE_START_DATE, AKSHARE_END_DATE, adjust=AKSHARE_ADJUST
        )
        if AKSHARE_SAVE_JSON_AFTER_FETCH:
            ddir = os.path.dirname(os.path.abspath(PRICE_DATA_PATH))
            if ddir:
                os.makedirs(ddir, exist_ok=True)
            save_bl_tables_to_json(price_df, mv_df, PRICE_DATA_PATH, MV_DATA_PATH)
            print("AkShare → JSON:", PRICE_DATA_PATH, MV_DATA_PATH)
        return price_df, mv_df
    if src == "json":
        return _load_json_tables()
    if src == "csv":
        return _load_csv_tables()
    if src == "baostock":
        return _load_baostock_tables()
    if src == "tushare":
        price_df, mv_df = _load_tushare_tables()
        if AKSHARE_SAVE_JSON_AFTER_FETCH:
            ddir = os.path.dirname(os.path.abspath(PRICE_DATA_PATH))
            if ddir:
                os.makedirs(ddir, exist_ok=True)
            save_bl_tables_to_json(price_df, mv_df, PRICE_DATA_PATH, MV_DATA_PATH)
            print("Tushare → JSON:", PRICE_DATA_PATH, MV_DATA_PATH)
        return price_df, mv_df
    raise ValueError(
        f"未知数据源 {src!r}，可选: akshare, json, csv, baostock, tushare"
    )


def fetch_price_market_pair() -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    按 DATA_SOURCE 与 DATA_SOURCE_FALLBACK 依次尝试，返回 (price_df, mv_df, 实际使用的源名)。
    """
    chain = _source_try_chain()
    primary = chain[0]
    errors: List[Tuple[str, BaseException]] = []
    for src in chain:
        try:
            price_df, mv_df = _load_from_source(src)
            if src != primary:
                print(
                    f"[data] 主源 {primary!r} 不可用，已改用 {src!r}。"
                    f"（可调整 structures.DATA_SOURCE / DATA_SOURCE_FALLBACK）"
                )
            return price_df, mv_df, src
        except Exception as e:
            errors.append((src, e))
            continue
    parts = [f"{s}: {type(e).__name__}: {e}" for s, e in errors]
    raise RuntimeError(
        "所有数据源均失败，尝试顺序: "
        + " → ".join(chain)
        + " | "
        + " | ".join(parts)
    ) from errors[-1][1]
