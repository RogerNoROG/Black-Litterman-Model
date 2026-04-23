"""
周频滚动主循环：等权 (EW) vs 前视 BL/均值-方差 vs 后视 ex-post 上界（**分析用，不可实盘中再造**）

设计要点
--------
- 第 ``t`` 周 **持有期收益** 为 ``returns`` 第 ``t`` 行（与 ``DataFrame`` 行索引一一对应）。
- **协方差** :math:`\\Sigma_t` 仅用 **过去 ``window`` 周、且不包含** 第 ``t`` 行：
  ``returns.iloc[t - window : t]``，即行 ``t-window, ..., t-1``，避免用当周收益参与协方差估计（严格前视）。
- **Ex-post 权重** 在「已知**当周** :math:`r_t`（``returns`` 第 ``t`` 行）」后构造，仅用于**事后**与 BL 的对比；不反馈到 :math:`t+1` 的前视决策。
- **BL 前视** 需由外部先得到各周的后验预期收益 :math:`\\pi^{\\text{post}}_t`（如 StructBERT+观点矩阵），以 ``mu_post`` 传入；本模块不计算 BL 后验，只做**同一** :math:`\\Sigma_t` 下的**长仓、净多头** 均值-方差解。

与论文表述一致时，可称 ex-post 为「**上帝视角、与 BL 同 :math:`\\Sigma_t` 与约束**下的对照」。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import numpy as np
import pandas as pd
from scipy.optimize import minimize

try:
    from sklearn.covariance import LedoitWolf
except ImportError as e:  # pragma: no cover
    raise ImportError(
        "rolling_bl_expost_loop 需要 scikit-learn：pip install scikit-learn"
    ) from e


def _as_array_2d(x: Any) -> np.ndarray:
    a = np.asarray(x, dtype=float)
    if a.ndim == 1:
        a = a.reshape(1, -1)
    return a


def estimate_sigma_ledoit_wolf(returns_window: np.ndarray) -> np.ndarray:
    """
    对形状 ``(T_win, N)`` 的收益率样本做 Ledoit–Wolf 协方差；样本过少时退回 ``np.cov`` + 岭。
    """
    r = _as_array_2d(returns_window)
    n, p = r.shape
    if n < 2:
        raise ValueError("估计协方差至少需要 2 个有效周度观测")
    sigma = None
    if n >= 2:
        try:
            lw = LedoitWolf().fit(r)
            sigma = np.asarray(lw.covariance_, dtype=float)
        except (ValueError, np.linalg.LinAlgError):
            pass
    if sigma is None or not np.isfinite(sigma).all():
        sigma = np.cov(r, rowvar=False, ddof=0)
    # 对称 + 小岭，防数值上非 PD
    sigma = (sigma + sigma.T) * 0.5
    tr = max(float(np.trace(sigma) / p), 1e-12)
    sigma = sigma + 1e-8 * tr * np.eye(p, dtype=float)
    return sigma


def solve_longonly_mean_variance(
    mu: np.ndarray,
    sigma: np.ndarray,
    *,
    risk_aversion: float = 1.0,
) -> np.ndarray:
    """
    长仓、净和为 1，最大化 w'@mu - (lambda/2) w'@Sigma@w 。

    使用 ``SLSQP`` ；若解不稳定，会返回 ``NaN`` 等权上屏弃（调用方应检查）。
    """
    mu = np.asarray(mu, dtype=float).ravel()
    sigma = np.asarray(sigma, dtype=float)
    n = mu.size
    if sigma.shape != (n, n):
        raise ValueError(f"sigma 须为 (N,N)={n}，得 {sigma.shape}")
    lmbd = max(float(risk_aversion), 1e-12)

    def neg_u(w: np.ndarray) -> float:
        w = w.astype(float)
        p = w @ mu
        v = w @ sigma @ w
        return -p + 0.5 * lmbd * v

    w0 = np.full(n, 1.0 / n, dtype=float)
    bnds = tuple((0.0, 1.0) for _ in range(n))
    cons = ({"type": "eq", "fun": lambda w: np.sum(w) - 1.0},)
    res = minimize(
        neg_u,
        w0,
        method="SLSQP",
        bounds=bnds,
        constraints=cons,
        options={"maxiter": 500, "ftol": 1e-9},
    )
    w = res.x
    s = w.sum()
    if s > 0:
        w = w / s
    if not (np.all(w >= -1e-8) and abs(w.sum() - 1) < 1e-4):
        return np.full(n, 1.0 / n)  # 退回等权
    w = np.clip(w, 0, None)
    w = w / w.sum()
    return w


def solve_longonly_max_sharpe(
    r_realized: np.ndarray,
    sigma: np.ndarray,
) -> np.ndarray:
    """
    长仓、净和为 1，在「无风险=0」下近似最大化 Sharpe: (w'r) / sqrt(w' Sigma w) 。

    后视、仅作分析上界时，:math:`r` 为**当周已实现**各资产周收益；:math:`\\Sigma` 与 BL 用同一滚动估计。

    若优化失败，退回等权。
    """
    r = np.asarray(r_realized, dtype=float).ravel()
    sigma = np.asarray(sigma, dtype=float)
    n = r.size
    if sigma.shape != (n, n):
        raise ValueError("sigma 维数与 r 不一致")

    def neg_sharpe(w: np.ndarray) -> float:
        w = w.astype(float)
        p = w @ r
        v = w @ sigma @ w
        return -p / (np.sqrt(max(v, 1e-20)))

    w0 = np.full(n, 1.0 / n, dtype=float)
    bnds = tuple((0.0, 1.0) for _ in range(n))
    cons = ({"type": "eq", "fun": lambda w: np.sum(w) - 1.0},)
    res = minimize(
        neg_sharpe,
        w0,
        method="SLSQP",
        bounds=bnds,
        constraints=cons,
        options={"maxiter": 500, "ftol": 1e-9},
    )
    w = res.x
    s = w.sum()
    if s > 0:
        w = w / s
    w = np.clip(w, 0, None)
    if w.sum() <= 0:
        w = np.full(n, 1.0 / n)
    else:
        w = w / w.sum()
    return w


@dataclass
class RollingBacktestResult:
    """主循环结果。"""

    table: pd.DataFrame
    """主表：行与 ``returns`` 的 ``[window : T)`` 对齐。"""

    weights_ew: list[np.ndarray]
    weights_bl: list[np.ndarray]
    weights_expost: list[np.ndarray]
    week_labels: list[Any]
    n_assets: int
    window: int


def run_weekly_rolling_loop(
    returns: pd.DataFrame,
    mu_post: pd.DataFrame,
    *,
    window: int = 10,
    risk_aversion: float = 1.0,
) -> RollingBacktestResult:
    """
    每周 $t$（从 ``window`` 到 ``T-1`` 循环）在 **t 周初** 用信息 **不含** 第 $t$ 行收益 估计 :math:`\\Sigma_t$，
    用 **当周** 已实现收益 ``returns.iloc[t]`` 与相同 :math:`\\Sigma_t$ 构造 ex-post 权重，仅用于**对照**。

    Parameters
    ----------
    returns
        周收益，形状 ``(T, N)``。第 ``t`` 行为**第 t 个持有期**的 **N 维** 资产收益，列名即资产名。
    mu_post
        与 ``returns`` 同索引、同列的**后验预期周收益**（前视，来自你方的 BL/情绪链）。
        若某周为 ``NaN`` 整行，当周的 ``w_bl`` 退为等权并可在表里标记。

    window
        协方差滚动长度（**不包含** 当前周）。

    risk_aversion
        长仓均值-方差中 :math:`\\lambda`，BL 前视子问题使用。

    Returns
    -------
    RollingBacktestResult
        ``table`` 中 ``r_ew, r_bl, r_expost`` 均为**该周**组合收益
        :math:`w' r_t`（:math:`r_t=\\texttt{returns.iloc\\[t\\]}$）；``ret_diff_expost_minus_bl = r_expost - r_bl`` 等。

    说明
    ----
    - 换手约束、GMV 先验 等可在外层用另一套 ``mu_post``/约束优化扩展；本模块保持**可复现的最小可解释核**。
    """
    if window < 2:
        raise ValueError("window 至少为 2")
    R = returns.copy()
    M = mu_post.reindex(R.index, columns=R.columns)
    T, n = R.shape
    if M.shape != R.shape:
        raise ValueError("mu_post 须与 returns 同索引、同列")
    if n < 1:
        raise ValueError("至少需要 1 只资产")
    if T <= window:
        return RollingBacktestResult(
            table=pd.DataFrame(),
            weights_ew=[],
            weights_bl=[],
            weights_expost=[],
            week_labels=[],
            n_assets=n,
            window=window,
        )

    rows: list[dict[str, Any]] = []
    w_ew = np.full(n, 1.0 / n, dtype=float)
    weights_ew: list[np.ndarray] = []
    weights_bl: list[np.ndarray] = []
    weights_expost: list[np.ndarray] = []
    week_labels: list[Any] = []

    for t in range(window, T):
        lab = R.index[t]
        r_t = R.iloc[t].to_numpy(dtype=float)
        R_hist = R.iloc[t - window : t].to_numpy(dtype=float)
        if R_hist.shape[0] < window or np.isnan(r_t).all():
            continue
        if np.isnan(R_hist).any():
            R_hist = np.where(np.isfinite(R_hist), R_hist, 0.0)  # 粗补零；可改为删行
        try:
            sigma = estimate_sigma_ledoit_wolf(R_hist)
        except Exception as e:  # noqa: BLE001
            rows.append(
                {
                    "week": lab,
                    "r_ew": np.nan,
                    "r_bl": np.nan,
                    "r_expost": np.nan,
                    "ret_diff_expost_minus_bl": np.nan,
                    "l2_w_bl_vs_w_ex": np.nan,
                    "error": str(e),
                }
            )
            continue

        w_bl = w_ew.copy()
        pi_t = M.iloc[t].to_numpy(dtype=float)
        if np.isfinite(pi_t).all() and not np.allclose(pi_t, 0.0):
            w_bl = solve_longonly_mean_variance(
                pi_t, sigma, risk_aversion=risk_aversion
            )
        w_ex = solve_longonly_max_sharpe(r_t, sigma)

        r_ = float(w_ew @ r_t)
        r_b = float(w_bl @ r_t)
        r_e = float(w_ex @ r_t)

        rows.append(
            {
                "week": lab,
                "r_ew": r_,
                "r_bl": r_b,
                "r_expost": r_e,
                "ret_diff_expost_minus_bl": r_e - r_b,
                "l2_w_bl_vs_w_ex": float(np.linalg.norm(w_bl - w_ex)),
                "error": None,
            }
        )
        week_labels.append(lab)
        weights_ew.append(w_ew.copy())
        weights_bl.append(w_bl)
        weights_expost.append(w_ex)

    out = pd.DataFrame(rows)
    if not out.empty and "week" in out.columns:
        out = out.set_index("week", drop=True)

    return RollingBacktestResult(
        table=out,
        weights_ew=weights_ew,
        weights_bl=weights_bl,
        weights_expost=weights_expost,
        week_labels=week_labels,
        n_assets=n,
        window=window,
    )


def summarize_cumulative_pnl(weekly_returns: pd.Series) -> dict[str, float]:
    """
    简单累计对数/简单收益和（不年化）；用于快速报表。
    """
    s = np.asarray(weekly_returns.dropna(), dtype=float)
    if s.size == 0:
        return {"sum": 0.0, "mean": 0.0, "std": 0.0}
    return {
        "sum": float(s.sum()),
        "mean": float(s.mean()),
        "std": float(s.std()),
    }
