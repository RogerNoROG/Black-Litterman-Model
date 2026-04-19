"""
将 [youngandbin/LLM-BLM](https://github.com/youngandbin/LLM-BLM) 中的观点定义与后验，
对接本仓库 ``emotion_bl.bl.black_litterman.BlackLittermanEngine``（与上游 ``evaluate_multiple.py``
中矩阵公式一致，便于与本项目情绪管线统一校验）。

上游要点（节选）：
- ``Q_i = mean(LLM 多次 sampled expected_return)``
- ``Omega_ii = var(同上)``
- ``P = I``
- 先验均衡收益 ``Pi``：CAPM 隐含（β × 市场风险溢价），见 ``capm_equilibrium_returns``
- 权重：在样本协方差下做多-only 最小化 ``w'Σw - λ w'μ``（``evaluate_multiple.black_litterman_LLM``）

本模块不调用上游仓库内 Python（避免其 ``run.py`` 占位 API）；逻辑与其 ``evaluate_multiple.py`` 对齐。
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
from scipy.optimize import minimize

from emotion_bl.bl.black_litterman import BLInputs, BlackLittermanEngine


def load_llm_blm_response_json(path: Path | str) -> dict[str, Any]:
    """读取上游 ``responses/{model}_{start}_{end}.json`` 格式的字典。"""
    with Path(path).open(encoding="utf-8") as f:
        return json.load(f)


def llm_blm_absolute_views(
    data_dict: Mapping[str, Any],
    tickers: Sequence[str],
    *,
    empty_view_variance: float = 1e6,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    复现上游对每只股票的绝对观点：P=I，Q 为预测均值，Ω 为多次采样方差。

    ``data_dict[ticker]`` 须含 ``expected_return``: list[float]（上游为 30 次 LLM 采样）。
    """
    q_list: list[float] = []
    o_list: list[float] = []
    for t in tickers:
        block = data_dict.get(t) or {}
        exp = block.get("expected_return") or []
        if not exp:
            q_list.append(0.0)
            o_list.append(float(empty_view_variance))
            continue
        arr = np.asarray(exp, dtype=float)
        arr = arr[~np.isnan(arr)]
        if arr.size == 0:
            q_list.append(0.0)
            o_list.append(float(empty_view_variance))
            continue
        q_list.append(float(np.mean(arr)))
        o_list.append(float(np.var(arr)) if arr.size > 1 else 1e-8)

    n = len(tickers)
    P = np.eye(n)
    Q = np.asarray(q_list, dtype=float)
    omega = np.asarray(o_list, dtype=float)
    return P, Q, omega


def capm_equilibrium_returns(
    returns: pd.DataFrame,
    market_caps: Mapping[str, float],
    *,
    rf: float = 0.02,
) -> tuple[np.ndarray, list[str]]:
    """
    与 ``evaluate_multiple.process_period`` 一致：用市值加权市场组合收益估计 β，
    ``Pi_i = beta_i * (E[R_m - rf])``（此处对 ``R_m - rf`` 取序列均值得到溢价标量）。

    Parameters
    ----------
    returns
        列名为 ticker；行为时间序（与上游 CSV returns 相同）。
    market_caps
        各 ticker 市值；缺失的列会在权重估计前丢弃。
    """
    cols = [c for c in returns.columns if c in market_caps and market_caps[c] is not None]
    if not cols:
        raise ValueError("市值与收益列无交集")
    r = returns[cols].copy()
    r = r.dropna(axis=1, how="any")
    cols = list(r.columns)
    mc = pd.Series({k: float(market_caps[k]) for k in cols})
    mc = mc.replace([np.inf, -np.inf], np.nan).dropna()
    valid = [c for c in cols if c in mc.index]
    if not valid:
        raise ValueError("有效市值为空")
    r = r[valid]
    w = mc.loc[valid]
    w = w / w.sum()
    mkt = (r * w).sum(axis=1)
    m_var = float(mkt.var())
    if m_var < 1e-18:
        raise ValueError("市场组合收益方差过小，无法估计 beta")
    premium_series = mkt - rf
    market_risk_premium = float(premium_series.mean())
    beta = r.apply(lambda col: col.cov(mkt) / m_var)
    pi = (beta * market_risk_premium).to_numpy(dtype=float)
    return pi, valid


def solve_posterior_mu_with_engine(
    *,
    sigma: np.ndarray,
    pi: np.ndarray,
    P: np.ndarray,
    Q: np.ndarray,
    omega: np.ndarray,
    tau: float,
) -> np.ndarray:
    """调用本仓库 ``BlackLittermanEngine`` 得到后验期望收益 ``mu_bl``。"""
    eng = BlackLittermanEngine()
    res = eng.solve(
        BLInputs(sigma=sigma, pi=pi, P=P, Q=Q, omega=omega, tau=float(tau))
    )
    return res.mu_bl


def long_only_weights_min_variance(
    posterior_returns: np.ndarray,
    cov: np.ndarray,
    *,
    risk_aversion: float = 0.1,
) -> np.ndarray:
    """
    与上游 ``black_litterman_LLM`` 末尾相同：长约束、全额投资下最小化
    ``w'Σw - λ w'μ``。
    """
    mu = np.asarray(posterior_returns, dtype=float).reshape(-1)
    sigma = np.asarray(cov, dtype=float)
    n = mu.shape[0]

    def objective_function(w: np.ndarray, lam: float) -> float:
        return float(w.T @ sigma @ w - lam * (w @ mu))

    cons = (
        {"type": "eq", "fun": lambda x: np.sum(x) - 1.0},
        {"type": "ineq", "fun": lambda x: x},
    )
    bounds = tuple((0.0, 1.0) for _ in range(n))
    x0 = np.ones(n) / n
    result = minimize(
        objective_function,
        x0,
        args=(risk_aversion,),
        constraints=cons,
        bounds=bounds,
    )
    if not result.success:
        raise RuntimeError(f"scipy.minimize 未收敛: {result.message}")
    return result.x


def run_llm_blm_period(
    *,
    returns: pd.DataFrame,
    market_caps: Mapping[str, float],
    data_dict: Mapping[str, Any],
    tau: float = 0.025,
    rf: float = 0.02,
    risk_aversion: float = 0.1,
) -> tuple[np.ndarray, np.ndarray, list[str]]:
    """
    单期端到端：CAPM ``Pi`` + LLM 观点 → 后验 ``mu_bl`` → 多头权重。

    Returns
    -------
    weights, mu_bl, tickers
    """
    pi, tickers = capm_equilibrium_returns(returns, market_caps, rf=rf)
    r = returns[tickers].to_numpy(dtype=float)
    sigma = np.cov(r.T)
    P, Q, omega = llm_blm_absolute_views(data_dict, tickers)
    mu_bl = solve_posterior_mu_with_engine(
        sigma=sigma, pi=pi, P=P, Q=Q, omega=omega, tau=tau
    )
    w = long_only_weights_min_variance(mu_bl, sigma, risk_aversion=risk_aversion)
    return w, mu_bl, tickers
