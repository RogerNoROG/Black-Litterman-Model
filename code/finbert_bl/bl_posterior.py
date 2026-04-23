"""与 ``black_litterman.BlackLitterman`` 同式的隐含均衡收益与后验，便于在无 DataFrame 侧独立调用。"""

from __future__ import annotations

import numpy as np
import pandas as pd


def implied_excess_equilibrium_return(
    stock_cc_ret: pd.DataFrame,
    w_mkt: np.ndarray,
    *,
    rf_annual: float = 0.025,
) -> tuple[np.ndarray, float]:
    """
    对数超额均衡收益（与 ``BlackLitterman.get_implied_excess_equilibrium_return`` 相同）。
    ``stock_cc_ret`` 为**估计窗**内周对数收益，列为个股。
    """
    w_mkt = np.asarray(w_mkt, dtype=float).ravel()
    rf = float(np.log(1.0 + rf_annual) / 52.0)
    mkt_cov = np.array(stock_cc_ret.cov())
    mu = np.array(stock_cc_ret.mean())
    wmw = w_mkt @ mkt_cov @ w_mkt.T
    if wmw <= 0:
        wmw = 1e-12
    lambd = (float(np.dot(w_mkt, mu)) - rf) / wmw
    implied_ret = lambd * np.dot(mkt_cov, w_mkt)
    return implied_ret, float(lambd)


def bl_posterior_combined_return(
    implied_ret: np.ndarray,
    mkt_cov: np.ndarray,
    P: np.ndarray,
    Q: np.ndarray,
    omega: np.ndarray,
    *,
    tau: float,
) -> np.ndarray:
    """
    Black–Litterman 后验均值，与 ``get_posterior_combined_return`` 一致。

    ``omega`` 为 K×K（通常为对角）观点协方差。
    """
    implied_ret = np.asarray(implied_ret, dtype=float).ravel()
    mkt_cov = np.asarray(mkt_cov, dtype=float)
    P = np.asarray(P, dtype=float)
    Q = np.asarray(Q, dtype=float).reshape(-1, 1)
    omega = np.asarray(omega, dtype=float)
    if P.size == 0:
        return implied_ret
    if omega.ndim == 1:
        omega = np.diag(omega)
    n = implied_ret.size
    ts = float(tau) * mkt_cov
    a = np.linalg.inv(ts) + P.T @ np.linalg.inv(omega) @ P
    b = (
        np.linalg.inv(ts) @ implied_ret.reshape(n, 1)
        + P.T @ np.linalg.inv(omega) @ Q
    )
    kmat = np.linalg.inv(a)
    return (kmat @ b).ravel()
