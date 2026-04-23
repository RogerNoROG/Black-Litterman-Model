"""周频与 ISO 周新闻文件对齐；投资周 t 使用 t-1 周新闻（**滞后，无前视**）。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def previous_iso_week_file_key(week_end_or_label: Any) -> str:
    """
    将当前行的**周标签**换为**上一 ISO 周**的 ``YYYY-Www`` 文件名键。

    约定：``week_end_or_label`` 为 ``Datetime``/``Timestamp``/可解析的日期，表示
    **本持有期**结束日或该周代表日；上一自然周的新闻对应 ``date - 7 days`` 的 ISO 周。
    """
    d = pd.to_datetime(week_end_or_label, errors="coerce")
    if pd.isna(d):
        raise ValueError(f"无法解析为日期: {week_end_or_label!r}")
    prev = d - pd.Timedelta(days=7)
    y, w, _ = prev.isocalendar()
    return f"{int(y)}-W{int(w):02d}"


def find_news_jsonl_path(news_dir: str | Path, iso_key: str) -> Path:
    p = Path(news_dir) / f"{iso_key}.jsonl"
    if not p.is_file():
        raise FileNotFoundError(f"滞后新闻文件不存在: {p}")
    return p
