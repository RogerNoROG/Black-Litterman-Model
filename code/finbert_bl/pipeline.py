"""单周 JSONL → 聚合情绪 → (P,Q,Ω) → BL 后验收益（与 ``column_order`` 对齐）。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Sequence, Tuple

import numpy as np
import pandas as pd

from .aggregate import ScoredNews, aggregate_by_stock, industry_bar, stock_s_values
from .bl_posterior import bl_posterior_combined_return, implied_excess_equilibrium_return
from .finbert_classifier import FinBertSentiment
from .industry_map import ensure_group_for_universe
from .lagged_align import previous_iso_week_file_key
from .name_matcher import match_codes_in_text
from .pq_omega import build_top_bottom_pair_PQ, he_litterman_omega, zscore_bars


def _load_jsonl_texts(path: Path) -> List[Tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            tid = str(obj.get("id", ""))
            body = str(obj.get("content", "") or obj.get("text", "") or "")
            if not body:
                continue
            rows.append((tid, body))
    return rows


def posterior_from_week_jsonl(
    jsonl_path: str | Path,
    code_names: Sequence[Tuple[str, str]],
    column_order: Sequence[str],
    stock_ret_window: pd.DataFrame,
    w_mkt_row: np.ndarray,
    *,
    code_to_industry: dict[str, str] | None = None,
    model: FinBertSentiment | None = None,
    tau: float = 0.05,
    c: float = 3.0,
    alpha: float = 0.01,
    pair_k: int = 3,
    zscore_industry: bool = True,
    fill_neutral_for_no_news: bool = True,
) -> np.ndarray:
    """
    对**单**个 ``t-1`` 周新闻文件做 FinBERT、行业相对观点与 BL 后验，返回与 ``column_order`` 同序的
    :math:`\\pi^{\\text{post}}`（周对数超额收益的**观点**尺度，与 ``black_litterman`` 一致）。

    Parameters
    ----------
    stock_ret_window
        估计窗 **T×N** 周对数收益（**不含**当周），列名须为 6 位码或与 ``column_order`` 一致。
    w_mkt_row
        该调仓日 N 维市值权重（与 ``column_order`` 一致）。
    """
    path = Path(jsonl_path)
    model = model or FinBertSentiment()
    order = [str(x).strip() for x in column_order]
    universe = set(str(x).strip().zfill(6) for x in order)

    cti: dict[str, str] = {}
    if code_to_industry is not None:
        cti = dict(code_to_industry)
    code_to_group = ensure_group_for_universe(cti, sorted(universe))

    rows = _load_jsonl_texts(path)
    bodies = [r[1] for r in rows]
    scored = model.score_texts(bodies) if bodies else []

    news_items: list[ScoredNews] = []
    for (tid, body), sc in zip(rows, scored):
        codes = match_codes_in_text(body, code_names)
        codes = [c for c in codes if c in universe]
        news_items.append(
            ScoredNews(
                text_id=tid,
                codes=codes,
                e=sc.e,
                w=sc.w,
            )
        )

    agg = aggregate_by_stock(news_items)
    s_map = stock_s_values(agg)
    if fill_neutral_for_no_news:
        for c in universe:
            if c not in s_map:
                s_map[c] = 0.0

    bars = industry_bar(s_map, code_to_group, universe=universe)
    if zscore_industry and len(bars) > 1:
        bars = zscore_bars(bars)

    P, Q = build_top_bottom_pair_PQ(
        bars,
        code_to_group,
        order,
        k=pair_k,
        alpha=alpha,
    )

    mkt_cov = np.array(stock_ret_window.cov())
    implied, _ = implied_excess_equilibrium_return(stock_ret_window, w_mkt_row)

    if P.size == 0:
        return implied

    omega_d = he_litterman_omega(P, mkt_cov, tau, c)
    omega_d = np.maximum(omega_d, 1e-18)
    post = bl_posterior_combined_return(
        implied,
        mkt_cov,
        P,
        Q,
        omega_d,
        tau=tau,
    )
    return post


def build_mu_post_dataframe(
    returns: pd.DataFrame,
    mv: pd.DataFrame,
    news_dir: str | Path,
    code_names: Sequence[Tuple[str, str]],
    stock_cols: Sequence[str],
    *,
    model: FinBertSentiment | None = None,
    window: int = 52,
    tau: float = 0.05,
    c: float = 3.0,
    alpha: float = 0.01,
    pair_k: int = 3,
    zscore_industry: bool = True,
    code_to_industry: dict[str, str] | None = None,
) -> pd.DataFrame:
    """
    对 ``returns`` 每一行（第 ``t`` 周）用 **上一 ISO 周** 新闻构造 :math:`\\pi^{\\text{post}}_t` ，
    行索引与 ``returns`` 一致；**前 ``window`` 行**无足够历史则为 NaN。

    ``returns`` / ``mv`` 须具**相同** ``Date`` 索引（或可对齐）；``stock_cols`` 为「仅个股」列名列表。
    ``mv`` 为按周、含 ``Total`` 的市值表，用于 ``t-1`` 周市值权重，与 ``black_litterman.get_market_value_weight`` 类似。
    """
    news_dir = Path(news_dir)
    r = returns.copy()
    mv = mv.reindex(r.index)
    stock_cols = [str(s).strip() for s in stock_cols]
    n = len(stock_cols)
    out = pd.DataFrame(index=r.index, columns=stock_cols, dtype=float)
    model = model or FinBertSentiment()
    w_all = mv.reindex(r.index)
    w_all = w_all.reindex(columns=stock_cols, fill_value=0.0)
    for c2 in stock_cols:
        w_all[c2] = pd.to_numeric(w_all[c2], errors="coerce")
    for t_idx, dt in enumerate(r.index):
        if t_idx < max(window, 1):
            continue
        # 与 ``get_post_weight`` 一致：用上一期市值权重
        dt_w = r.index[t_idx - 1]
        if dt_w not in w_all.index:
            continue
        w_row = w_all.loc[dt_w, stock_cols].to_numpy(dtype=float)
        tsum = float(np.nansum(w_row))
        if tsum and np.isfinite(tsum) and tsum > 0:
            wn = w_row / tsum
        else:
            wn = np.full(n, 1.0 / n, dtype=float)
        # 新闻：t-1 周（相对 dt 的 ISO 周；无前视）
        prev_key = previous_iso_week_file_key(dt)
        jp = news_dir / f"{prev_key}.jsonl"
        if not jp.is_file():
            continue
        hist = r.iloc[t_idx - window : t_idx][stock_cols]
        try:
            pi = posterior_from_week_jsonl(
                jp,
                code_names,
                stock_cols,
                hist,
                wn,
                model=model,
                tau=tau,
                c=c,
                alpha=alpha,
                pair_k=pair_k,
                zscore_industry=zscore_industry,
                code_to_industry=code_to_industry,
            )
        except (FileNotFoundError, ValueError, np.linalg.LinAlgError, OSError):
            continue
        out.loc[dt, stock_cols] = pi
    return out
