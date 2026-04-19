"""
Black-Litterman 后验期望收益，用于将情绪观点 Q 与均衡收益 Pi 融合。

参考: He & Litterman (1999); 矩阵形式与 Idzorek 表述一致。
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
from numpy.linalg import inv


@dataclass
class BLInputs:
    """资产维度 n；k 个观点。"""

    sigma: np.ndarray
    pi: np.ndarray
    P: np.ndarray
    Q: np.ndarray
    omega: np.ndarray
    tau: float = 0.05


@dataclass
class BLResult:
    mu_bl: np.ndarray
    sigma_bl: np.ndarray
    pi: np.ndarray
    P: np.ndarray
    Q: np.ndarray
    omega: np.ndarray


class BlackLittermanEngine:
    """
    E[R_BL] = M^{-1} * Br
    M = (tau*Sigma)^{-1} + P' Omega^{-1} P
    Br = (tau*Sigma)^{-1} Pi + P' Omega^{-1} Q

    后验协方差（常用近似）:
    Sigma_BL = inv(M)
    """

    def solve(self, inp: BLInputs) -> BLResult:
        n = inp.pi.shape[0]
        sigma = np.asarray(inp.sigma, dtype=float)
        pi = np.asarray(inp.pi, dtype=float).reshape(-1)
        P = np.asarray(inp.P, dtype=float)
        Q = np.asarray(inp.Q, dtype=float).reshape(-1)
        omega = np.asarray(inp.omega, dtype=float)
        if omega.ndim == 1:
            omega_mat = np.diag(omega)
        else:
            omega_mat = omega

        if sigma.shape != (n, n):
            raise ValueError(f"sigma 形状应为 ({n},{n})")
        if P.shape[1] != n:
            raise ValueError("P 列数应等于资产数 n")
        if P.shape[0] != Q.shape[0]:
            raise ValueError("P 行数应等于 Q 长度")
        if omega_mat.shape[0] != P.shape[0]:
            raise ValueError("Omega 维度与观点数 k 不一致")

        tau_sigma = inp.tau * sigma
        ts_inv = inv(tau_sigma)
        o_inv = inv(omega_mat)
        M = ts_inv + P.T @ o_inv @ P
        Br = ts_inv @ pi + P.T @ o_inv @ Q
        mu_bl = inv(M) @ Br
        sigma_bl = inv(M)

        return BLResult(
            mu_bl=mu_bl,
            sigma_bl=sigma_bl,
            pi=pi,
            P=P,
            Q=Q,
            omega=np.diag(omega_mat) if omega_mat.ndim == 2 else omega,
        )


def views_from_sentiment(
    asset_scores: dict[str, float],
    tickers: list[str],
    view_scale: float = 0.02,
    base_uncertainty: float = 1e-4,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    将每只资产的情绪均值映射为相对观点收益率 Q（超额收益尺度），
    不确定性 omega_i 与 |score| 成反比（越极端越「自信」可改为常数）。
    """
    n = len(tickers)
    idx = {t: i for i, t in enumerate(tickers)}
    rows = []
    q_list = []
    omega_list = []
    eps = 1e-8
    for t, score in asset_scores.items():
        if t not in idx:
            continue
        if abs(float(score)) < eps:
            continue
        row = np.zeros(n)
        row[idx[t]] = 1.0
        rows.append(row)
        q_list.append(float(score) * view_scale)
        omega_list.append(base_uncertainty + (1.0 - min(1.0, abs(score))) * base_uncertainty * 10)

    if not rows:
        P = np.zeros((0, n))
        Q = np.zeros(0)
        omega = np.zeros(0)
        return P, Q, omega

    P = np.vstack(rows)
    Q = np.array(q_list, dtype=float)
    omega = np.array(omega_list, dtype=float)
    return P, Q, omega
