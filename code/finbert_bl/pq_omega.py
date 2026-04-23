"""行业 top/bottom 相对观点 (P, Q) 与 He–Litterman 比例型 Omega 对角元。"""

from __future__ import annotations

from typing import Dict, List, Sequence, Set, Tuple

import numpy as np

PQT = Tuple[np.ndarray, np.ndarray]


def _codes_in_group(code_to_g: dict[str, str], target_g: str) -> list[str]:
    return sorted([c for c, g in code_to_g.items() if g == target_g])


def build_top_bottom_pair_PQ(
    industry_bar: dict[str, float],
    code_to_group: dict[str, str],
    column_order: Sequence[str],
    *,
    k: int = 3,
    alpha: float = 0.01,
) -> PQT:
    """
    对情绪**最高**的 k 个行业与**最低**的 k 个行业一一配对，共 k 行观点。

    第 i 行：等权多第 i 高兴行业、等权空第 i 低情绪行业（组合权重**净敞口为 0**）。

    **Q** 为周度**超额**组合收益观点：``alpha * (bar_S_long - bar_S_short)``（与
    :math:`\\bar{S}` 同量纲，约 -2~2；**alpha 须按历史回测标定**）。

    Parameters
    ----------
    industry_bar
        行业 :math:`\\to \\bar{S}_g`（已标准化与否由调用方决定）。
    code_to_group
        6 位码到行业名（需覆盖 ``column_order`` 中需映射者）。
    column_order
        与收益向量、协方差列顺序**一致**的 6 位码列表（长度 N）。

    Returns
    -------
    P, Q
        ``P`` 形状 ``(k, N)`` ；``Q`` 形状 ``(k,)`` 。
    """
    order = [str(c).strip().zfill(6) for c in column_order]
    n = len(order)
    if k < 1:
        return np.zeros((0, n)), np.zeros(0)
    valid = {g: v for g, v in industry_bar.items() if np.isfinite(v)}
    if len(valid) < 2 * k:
        k = min(k, max(1, len(valid) // 2))
    items = sorted(valid.items(), key=lambda x: -x[1])
    if len(items) < 2 * k or k < 1:
        return np.zeros((0, n)), np.zeros(0)
    longs = [items[i][0] for i in range(k)]
    shorts = [items[-(i + 1)][0] for i in range(k)]
    P = np.zeros((k, n))
    Qv = np.zeros(k)
    code_set: Set[str] = set(order)
    idx: dict[str, int] = {c: j for j, c in enumerate(order)}

    for i in range(k):
        gl, gs = longs[i], shorts[i]
        L = [c for c in _codes_in_group(code_to_group, gl) if c in code_set]
        S = [c for c in _codes_in_group(code_to_group, gs) if c in code_set]
        if not L or not S:
            # 无成分在该行业
            P[i, :] = 0.0
            Qv[i] = 0.0
            continue
        wl, ws = 1.0 / len(L), 1.0 / len(S)
        for c in L:
            P[i, idx[c]] += wl
        for c in S:
            P[i, idx[c]] -= ws
        Qv[i] = float(alpha) * (float(industry_bar[gl]) - float(industry_bar[gs]))
    return P, Qv


def he_litterman_omega(
    P: np.ndarray,
    sigma: np.ndarray,
    tau: float,
    c: float,
) -> np.ndarray:
    """
    返回 **对角** 不确定度 :math:`\\mathrm{diag}( c \\cdot \\mathrm{diag}(P \\, \\tau\\Sigma P') )` ，
    即对 He–Litterman 比例式取对角，与本仓库 :meth:`black_litterman.BlackLitterman.get_views_omega` 的
    方向一致并增加信心系数 ``c`` 。

    若需完整 K×K 矩阵，可另建 ``P @ (tau*Sigma) @ P.T * c`` ；此处仅返回
    供 ``get_posterior_combined_return`` 的 ``omega`` 对角或 ``np.diag`` 用。
    """
    P = np.asarray(P, dtype=float)
    sigma = np.asarray(sigma, dtype=float)
    if P.size == 0:
        return np.zeros(0)
    m = P @ (float(tau) * sigma) @ P.T
    d = np.maximum(np.diag(m), 0.0) * float(c)
    return d


def zscore_bars(bars: dict[str, float]) -> dict[str, float]:
    """横截面 z-score，键不变。"""
    v = list(bars.values())
    a = np.asarray(v, dtype=float)
    m = float(np.mean(a)) if a.size else 0.0
    s = float(np.std(a, ddof=0)) or 1.0
    keys = list(bars.keys())
    return {keys[i]: (a[i] - m) / s for i in range(len(keys))}
