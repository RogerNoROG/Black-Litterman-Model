"""将新闻正文关联到沪深300成分股：简称/全称子串匹配（长名优先以减轻误匹配）。"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

CodeName = Tuple[str, str]  # (6 位, 显示名)


def norm_code(s: str) -> str:
    c = str(s).strip().zfill(6)
    if not (len(c) == 6 and c.isdigit()):
        raise ValueError(f"非 6 位 A 股代码: {s!r}")
    return c


def load_code_names_from_csv(path: str | Path) -> List[CodeName]:
    import pandas as pd

    p = Path(path)
    df = pd.read_csv(p)
    for code_col, name_col in (("code", "name"), ("代码", "名称"), ("成分券代码", "成分券名称")):
        if code_col in df.columns and name_col in df.columns:
            out: list[CodeName] = []
            for _, r in df.iterrows():
                try:
                    out.append((norm_code(str(r[code_col])), str(r[name_col]).strip()))
                except ValueError:
                    continue
            return out
    raise KeyError("CSV 须含 (code,name) 或 (代码,名称) 等列对")


def load_code_names_from_json(path: str | Path) -> List[CodeName]:
    p = Path(path)
    with p.open(encoding="utf-8") as f:
        doc = json.load(f)
    if isinstance(doc, list):
        return [(norm_code(str(x[0])), str(x[1])) for x in doc]
    if isinstance(doc, dict) and "stocks" in doc:
        return [
            (norm_code(str(x["code"])), str(x.get("name", x["code"])).strip())
            for x in doc["stocks"]
        ]
    raise ValueError("JSON 格式须为 [ [code, name], ... ] 或 {stocks: [{code,name}] }")


def load_csi300_code_names() -> List[CodeName]:
    """
    从 AkShare 拉取**当前**沪深300成分（代码 + 证券简称），用于词表匹配。

    注意：与 ``csi300_weight_universe`` 一致，非历史点截面；长回测应自建**时点一致**名单 JSON/CSV。
    """
    import akshare as ak  # type: ignore

    # 多接口兼容列名
    last_err: BaseException | None = None
    for fn in (lambda: ak.index_stock_cons_weight_csindex(symbol="000300"),):
        try:
            df = fn()
        except Exception as e:  # noqa: BLE001
            last_err = e
            continue
        if df is None or df.empty:
            continue
        code_col = None
        for c in ("成分券代码", "品种代码", "证券代码", "code"):
            if c in df.columns:
                code_col = c
                break
        name_col = None
        for c in ("成分券名称", "股票名称", "证券简称", "股票简称", "名称", "name"):
            if c in df.columns:
                name_col = c
                break
        if not code_col or not name_col:
            continue
        out: list[CodeName] = []
        for _, r in df.iterrows():
            try:
                out.append(
                    (norm_code(str(r[code_col])), str(r[name_col]).strip())
                )
            except ValueError:
                continue
        if out:
            return out
    err = last_err or RuntimeError("无可用列")
    raise RuntimeError("无法从 AkShare 获取沪深300成分及简称，可改用 --code-names 指定 CSV/JSON") from err


def load_code_names_auto(path: str | None) -> List[CodeName]:
    if not path:
        return load_csi300_code_names()
    p = Path(path)
    suf = p.suffix.lower()
    if suf in (".csv", ".tsv", ".txt"):
        return load_code_names_from_csv(p)
    if suf in (".json",):
        return load_code_names_from_json(p)
    raise ValueError(f"不支持的 code-names 文件: {p}")


def _strip_noise(text: str) -> str:
    t = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)  # markdown 链接
    return t


def match_codes_in_text(
    text: str,
    code_names: Sequence[CodeName],
    *,
    min_name_len: int = 2,
) -> list[str]:
    """
    在正文中子串匹配**证券简称**；按名称长度**降序**以优先更长实体。

    返回去重后、按**首次出现**顺序的 6 位码列表（仅出现在 ``code_names`` 表中的）。"""
    t = _strip_noise(str(text))
    name_by_len = sorted(
        ((n, c) for c, n in code_names if n and len(n) >= min_name_len),
        key=lambda x: -len(x[0]),
    )
    found: list[str] = []
    for name, code in name_by_len:
        if name in t:
            found.append(code)
    # 去重保序
    return list(dict.fromkeys(found))
