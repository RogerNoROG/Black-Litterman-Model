"""按新闻聚合 S_i = sum(w*e) / sum(w) 与行业平均 bar{S}。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Set


@dataclass
class ScoredNews:
    text_id: str
    codes: List[str]  # 与该股相关则计入
    e: float
    w: float


def aggregate_by_stock(
    items: Iterable[ScoredNews],
) -> dict[str, tuple[float, float]]:
    """
    返回 ``code -> (sum_w_times_e, sum_w)``；最终 :math:`S_i = a/b` 。
    若某股无新闻，不出现在 dict 中。
    """
    acc: dict[str, list[float]] = {}
    for it in items:
        if it.w <= 0:
            w = 1e-9
        else:
            w = it.w
        for c in it.codes:
            if c not in acc:
                acc[c] = [0.0, 0.0]  # num, den
            acc[c][0] += w * it.e
            acc[c][1] += w
    return {k: (v[0], v[1]) for k, v in acc.items()}


def stock_s_values(
    w_e_pairs: dict[str, tuple[float, float]],
) -> dict[str, float]:
    out: dict[str, float] = {}
    for c, (num, den) in w_e_pairs.items():
        if den <= 0:
            out[c] = 0.0
        else:
            out[c] = num / den
    return out


def industry_bar(
    s_by_code: Mapping[str, float],
    code_to_group: Mapping[str, str],
    universe: Set[str] | None = None,
) -> dict[str, float]:
    """
    计算每个组（如申万/中信/合成桶）的等权平均情绪。

    若 ``universe`` 提供，只对该集合中有行业标签的代码等权到组；无新闻的股票对组无贡献
    （即组内只平均**有 S** 的样本，或你可在调用前对无新闻填 0 并先合并入 ``s_by_code``）。
    """
    from collections import defaultdict

    gsum: dict[str, list[float]] = defaultdict(list)
    for code, sv in s_by_code.items():
        if universe is not None and code not in universe:
            continue
        g = code_to_group.get(code)
        if g is None:
            continue
        gsum[g].append(float(sv))
    return {g: (sum(v) / len(v)) for g, v in gsum.items() if v}
