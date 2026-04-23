"""行业/分组：读 CSV/JSON，无表时用合成 30 桶（回测占位，非真申万/中信）。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Mapping, MutableMapping


def load_code_to_industry(
    path: str | None,
    *,
    synthetic_groups: int = 30,
) -> dict[str, str]:
    """
    返回 ``6位码 -> 行业/桶名``。

    - 若未提供 path：返回空映射；在 ``ensure_group_for_universe`` 内为每只股票
      分配 ``synth_XX`` 合成行业（**非**真实申万/中信，生产请自供 CSV/JSON）。
    - CSV：列 ``code,industry`` 或 ``代码,行业`` 等（自动探测）。
    - JSON: ``{ "600519": "食品饮料", ... }`` 或 ``{ "mapping": { ... } }``。
    """
    if not path:
        return {}

    p = Path(path)
    if p.suffix.lower() == ".json":
        with p.open(encoding="utf-8") as f:
            doc = json.load(f)
        if isinstance(doc, dict) and "mapping" in doc:
            doc = doc["mapping"]
        if not isinstance(doc, dict):
            raise ValueError("行业 JSON 须为 代码->行业 的 object")
        out: dict[str, str] = {}
        for k, v in doc.items():
            c = str(k).strip().zfill(6)[:6]
            if c.isdigit() and len(c) == 6:
                out[c] = str(v).strip()
        return out

    import pandas as pd

    df = pd.read_csv(p)
    code_col, ind_col = None, None
    for a, b in (("code", "industry"), ("代码", "行业"), ("成分券代码", "行业")):
        if a in df.columns and b in df.columns:
            code_col, ind_col = a, b
            break
    if not code_col:
        raise KeyError("行业 CSV 须含 code/industry 或 代码/行业 列对")
    out2: dict[str, str] = {}
    for _, r in df.iterrows():
        c = str(r[code_col]).strip().zfill(6)[:6]
        if not (c.isdigit() and len(c) == 6):
            continue
        out2[c] = str(r[ind_col]).strip()
    return out2


def ensure_group_for_universe(
    code_to_group: MutableMapping[str, str] | None,
    universe: list[str],
    *,
    synthetic_groups: int = 30,
) -> dict[str, str]:
    if code_to_group is None:
        code_to_group = {}
    out: dict[str, str] = {}
    for c in universe:
        c6 = c.strip().zfill(6)[:6]
        if c6 in code_to_group and code_to_group[c6]:
            out[c6] = code_to_group[c6]
        else:
            g = f"synth_{int(c6) % max(1, synthetic_groups):02d}"
            out[c6] = g
    return out
