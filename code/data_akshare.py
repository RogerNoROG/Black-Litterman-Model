"""AkShare：沪深三大指数（东财指数日线）+ 前十权重股（A 股日线）周频收盘价；市值来自东财现货「总市值」。

本模块接口选型与参数对齐 AKShare 文档（股票数据）：
- **A 股历史日线**：``AKSHARE_STOCK_HIST_SOURCES`` 为优先级列表；**同一次** ``fetch_bl_tables`` 内
  首次成功的源会作为**粘性源**优先用于后续股票，该源彻底失败后再按列表重新探测。
  东财：`ak.stock_zh_a_hist`；腾讯：`ak.stock_zh_a_hist_tx`（``sz``/``sh`` 前缀由本模块补全）。
  腾讯接口在 AkShare 内部常按**年份**循环请求，终端可能出现 **tqdm 进度条**（非本仓库实现）。
  参数：`symbol` 为 6 位股票代码；`period='daily'`；`start_date` / `end_date` 为 `YYYYMMDD`；
  `adjust`：`''` 不复权，`'qfq'` 前复权，`'hfq'` 后复权（与 `structures.AKSHARE_ADJUST` 一致）。
  文档：https://akshare.akfamily.xyz/data/stock/stock.html （历史行情数据-东财）
- **沪深京 A 股实时行情（东财）**：`ak.stock_zh_a_spot_em` — 取「总市值」等字段推算股本。
  文档：同上（实时行情数据-东财 / stock_zh_a_spot_em）
- **指数日线（东财）**：`ak.stock_zh_index_daily_em` — `symbol` 须带市场前缀：`sh`/`sz`/`csi`/`bj`。
  文档：东方财富网-股票指数数据（与 stock_zh_a_hist 同属官方维护接口体系）

指数列仍使用新浪风格代码 `sh000300` 等，与 `BlackLitterman.INDEX_NUMBER` 一致；拉取时直接传入东财接口。

说明：新浪 `stock_zh_a_daily` 多次请求易封 IP，故个股历史不走新浪；东财不可用时自动换腾讯接口。
"""

from __future__ import annotations

import time
import warnings
from typing import Callable, Dict, List, Optional, Sequence, Tuple, TypeVar, Union

import pandas as pd

from structures import (
    AKSHARE_HTTP_RETRIES,
    AKSHARE_RETRY_BASE_SEC,
    AKSHARE_STOCK_HIST_SOURCES,
)

T = TypeVar("T")

# 三大指数：元组 (东财/新浪通用代码, DataFrame 列名)；代码须含 sh/sz 前缀供 stock_zh_index_daily_em 使用
INDEX_SINA: List[Tuple[str, str]] = [
    ("sh000300", "CSI300.GI"),
    ("sh000001", "SSE.GI"),
    ("sz399001", "SZCI.GI"),
]

# 沪深300 权重前列 10 只（列名与 get_views_P_Q_matrix 下标一致）
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


def _retry_network(fn: Callable[[], T], *, desc: str) -> T:
    """应对 RemoteDisconnected / ConnectionError 等瞬时网络错误。"""
    import requests

    retriable = (
        requests.exceptions.RequestException,
        ConnectionError,
        OSError,
        TimeoutError,
    )
    last_err: BaseException | None = None
    for attempt in range(AKSHARE_HTTP_RETRIES):
        try:
            return fn()
        except retriable as e:
            last_err = e
            if attempt >= AKSHARE_HTTP_RETRIES - 1:
                raise RuntimeError(
                    f"AkShare 请求失败（{desc}），已重试 {AKSHARE_HTTP_RETRIES} 次。"
                    " 可检查网络/代理；若配置了 DATA_SOURCE_FALLBACK，将自动尝试备用数据源。"
                ) from e
            delay = AKSHARE_RETRY_BASE_SEC * (2**attempt)
            time.sleep(delay)
    assert last_err is not None
    raise RuntimeError("unreachable") from last_err


def _import_akshare():
    try:
        import akshare as ak  # type: ignore
    except ImportError as e:
        raise ImportError(
            "使用 AkShare 数据源请先安装：pip install akshare（建议在项目 venv 中安装）"
        ) from e
    return ak


def _series_from_hist_df(df: pd.DataFrame, *, desc: str) -> pd.Series:
    """东财 K 线 DataFrame → 按日收盘 Series（列名兼容 date/日期）。"""
    if df is None or df.empty:
        raise ValueError(f"{desc}: 返回空表")
    dfc = df.copy()
    date_col = "date" if "date" in dfc.columns else "日期"
    close_col = "close" if "close" in dfc.columns else "收盘"
    if date_col not in dfc.columns or close_col not in dfc.columns:
        raise ValueError(f"{desc}: 缺少日期/收盘列，实际列={dfc.columns.tolist()}")
    dfc[date_col] = pd.to_datetime(dfc[date_col])
    return dfc.set_index(date_col)[close_col].sort_index().astype("float64")


def _index_daily_close_em(
    ak,
    symbol_market: str,
    start_d: pd.Timestamp,
    end_d: pd.Timestamp,
) -> pd.Series:
    """
    东方财富指数日线收盘价序列。
    `symbol_market` 形如 ``sh000300``、``sz399001``（见 ``stock_zh_index_daily_em`` 文档）。
    东财失败或空表时回退新浪 ``stock_zh_index_daily``（文档提示大量采集易封 IP）。
    """
    sd = start_d.strftime("%Y%m%d")
    ed = end_d.strftime("%Y%m%d")

    def _call_em():
        df = ak.stock_zh_index_daily_em(
            symbol=symbol_market, start_date=sd, end_date=ed
        )
        return _series_from_hist_df(df, desc=f"指数(EM) {symbol_market}")

    try:
        return _retry_network(_call_em, desc=f"指数(EM) {symbol_market}")
    except (RuntimeError, ValueError, KeyError):
        pass

    def _call_sina():
        df = ak.stock_zh_index_daily(symbol=symbol_market)
        s = _series_from_hist_df(df, desc=f"指数(新浪) {symbol_market}")
        s.index = pd.to_datetime(s.index)
        mask = (s.index >= start_d.normalize()) & (s.index <= end_d.normalize())
        s = s.loc[mask]
        if s.empty:
            raise ValueError(f"指数(新浪) {symbol_market} 在区间内无数据")
        return s

    return _retry_network(_call_sina, desc=f"指数(新浪备用) {symbol_market}")


def _qq_tx_symbol_for_a_code(code: str) -> str:
    """腾讯 ``stock_zh_a_hist_tx`` 要求 ``sz000001`` / ``sh600519`` 形式。"""
    c = str(code).zfill(6)
    if c.startswith(("6", "9")):
        return f"sh{c}"
    return f"sz{c}"


def _canonical_stock_hist_source(name: str) -> Optional[str]:
    n = (name or "").strip().lower()
    if n in ("eastmoney", "em", "east"):
        return "eastmoney"
    if n in ("tencent", "tx", "qq"):
        return "tencent"
    return None


def _normalize_stock_hist_sources(
    raw: Union[str, Sequence[str], None],
) -> Tuple[str, ...]:
    """解析 ``AKSHARE_STOCK_HIST_SOURCES``：支持 tuple / list / 逗号分隔字符串。"""
    default: Tuple[str, ...] = ("eastmoney", "tencent")
    if raw is None:
        return default
    if isinstance(raw, str):
        parts = [p.strip() for p in raw.split(",") if p.strip()]
    else:
        parts = [str(p).strip() for p in raw if str(p).strip()]
    seen: set[str] = set()
    out: List[str] = []
    for p in parts:
        c = _canonical_stock_hist_source(p)
        if c is None:
            warnings.warn(
                f"未知的 A 股日 K 数据源标识 {p!r}，已跳过。"
                " 有效值：eastmoney / em、tencent / tx / qq。",
                UserWarning,
                stacklevel=2,
            )
            continue
        if c not in seen:
            seen.add(c)
            out.append(c)
    return tuple(out) if out else default


class _StockHistTaskSession:
    """
    单次 ``fetch_bl_tables`` 内的 A 股日 K 拉取：粘性源优先，失效后按 ``AKSHARE_STOCK_HIST_SOURCES`` 重探测。
    """

    __slots__ = ("_ak", "_adjust", "_sources", "_sticky", "_warned_non_primary", "_sd", "_ed")

    def __init__(self, ak, adjust: str, start_d: pd.Timestamp, end_d: pd.Timestamp) -> None:
        self._ak = ak
        self._adjust = adjust if adjust is not None else ""
        self._sources = _normalize_stock_hist_sources(AKSHARE_STOCK_HIST_SOURCES)
        self._sticky: Optional[str] = None
        self._warned_non_primary = False
        self._sd = (start_d - pd.Timedelta(days=420)).strftime("%Y%m%d")
        self._ed = end_d.strftime("%Y%m%d")

    def _fetch_one(self, symbol: str, src: str) -> pd.Series:
        ak = self._ak
        sd, ed, adj = self._sd, self._ed, self._adjust

        def _call_em() -> pd.Series:
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=sd,
                end_date=ed,
                adjust=adj,
            )
            return _series_from_hist_df(df, desc=f"A股(stock_zh_a_hist) {symbol}")

        tx_sym = _qq_tx_symbol_for_a_code(symbol)

        def _call_tx() -> pd.Series:
            df = ak.stock_zh_a_hist_tx(
                symbol=tx_sym,
                start_date=sd,
                end_date=ed,
                adjust=adj,
            )
            return _series_from_hist_df(
                df, desc=f"A股(stock_zh_a_hist_tx) {tx_sym}"
            )

        if src == "eastmoney":
            s = _retry_network(_call_em, desc=f"A股(EM) {symbol}")
        else:
            s = _retry_network(_call_tx, desc=f"A股(TX) {symbol}")
        if s is None or s.empty:
            raise ValueError("序列为空")
        return s

    def fetch_daily_close(self, symbol: str) -> pd.Series:
        preferred = self._sources[0]
        if self._sticky is not None:
            try:
                return self._fetch_one(symbol, self._sticky)
            except (RuntimeError, ValueError, KeyError):
                self._sticky = None

        errs: List[str] = []
        for i, src in enumerate(self._sources):
            try:
                s = self._fetch_one(symbol, src)
                self._sticky = src
                if src != preferred and not self._warned_non_primary:
                    warnings.warn(
                        f"A股日 K：首选源 {preferred} 不可用，本任务内将优先使用 {src}；"
                        f"若 {src} 再失败将按 {self._sources} 重新探测。",
                        UserWarning,
                        stacklevel=2,
                    )
                    self._warned_non_primary = True
                return s
            except (RuntimeError, ValueError, KeyError) as e:
                errs.append(f"{src}: {e}")

        raise RuntimeError(
            f"A股 {symbol} 日 K 全部数据源失败（已按顺序尝试 {self._sources}）："
            + " | ".join(errs)
        ) from None


def _to_weekly_close(series: pd.Series) -> pd.Series:
    return series.resample("W-FRI").last().dropna()


def _fetch_spot_mcap_map_cn(ak) -> Optional[Dict[str, float]]:
    """东财沪深京 A 股现货；总市值单位：元（文档 stock_zh_a_spot_em）。"""
    try:
        spot = _retry_network(
            lambda: ak.stock_zh_a_spot_em(),
            desc="A股现货市值 stock_zh_a_spot_em",
        )
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

    流程摘要
    --------
    1. 指数：``stock_zh_index_daily_em(sh/sz...)`` → 日收盘 → ``W-FRI`` 周收盘。
    2. 个股：东财/腾讯日 K（同一次拉取内**粘性源**优先，该源整段失败后再按列表重探测）→ 周收盘。
    3. 市值：``stock_zh_a_spot_em`` 的「总市值」÷ 最近一周收盘价得股本，再 × 周线得到市值序列。
    """
    ak = _import_akshare()
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)

    weekly_parts: Dict[str, pd.Series] = {}
    for market_sym, col in INDEX_SINA:
        s = _to_weekly_close(_index_daily_close_em(ak, market_sym, start, end))
        weekly_parts[col] = s
        _pause()

    stock_codes = [t[0] for t in STOCKS]
    stock_cols = [t[1] for t in STOCKS]
    stock_hist_sess = _StockHistTaskSession(ak, adjust, start, end)
    for code, col in STOCKS:
        s = _to_weekly_close(stock_hist_sess.fetch_daily_close(code))
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
