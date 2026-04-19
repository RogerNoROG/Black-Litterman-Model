"""
按「周线」切开新闻：与 AkShare / 行情侧常用的周五收盘周线对齐。

- 使用 pandas ``freq='W-FRI'``：每周区间以周五为锚点；
- ``pd.Grouper(..., label='right', closed='right')``：分组标签为该周最后一个周五（右闭）；
- 时间统一到 ``Asia/Shanghai``（A 股语境；JSONL 里 RFC3339 +00:00 会先解析再转换）。

产出字典的键为当周「周五」的日历日 ``YYYY-MM-DD``，便于与 ``BlackLitterman._price_row_dates`` 等周线日历对照。
"""

from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

DEFAULT_PUBLISHED_KEYS = ("published", "pubDate", "date", "published_at")


def parse_published_timestamp(
    value: Any,
    *,
    timezone: str = "Asia/Shanghai",
) -> pd.Timestamp:
    """
    解析单条新闻时间；无法解析返回 ``pd.NaT``。
    无时区信息的字符串按 ``timezone`` 本地化（视作该时区墙上时间）。
    """
    if value is None:
        return pd.NaT
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize(timezone)
    else:
        ts = ts.tz_convert(timezone)
    return ts


def pick_published_raw(record: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for k in keys:
        if k in record and record[k] not in (None, ""):
            return record[k]
    return None


def bucket_news_by_week_w_fri(
    records: Sequence[Mapping[str, Any]],
    *,
    published_keys: Sequence[str] = DEFAULT_PUBLISHED_KEYS,
    timezone: str = "Asia/Shanghai",
    annotate_week_key: bool = False,
    week_key_field: str = "_week_end_friday",
) -> tuple[OrderedDict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    """
    将新闻列表按周五结束的周线（``W-FRI``）分桶。

    Parameters
    ----------
    records :
        JSONL 读入后的一串 dict（须含可解析的发布时间字段之一）。
    published_keys :
        依次尝试的字段名。
    timezone :
        交易周对齐用的时区（默认上海）。
    annotate_week_key :
        为 ``True`` 时，每桶内每条记录写入 ``week_key_field``（当周周五 ``YYYY-MM-DD``），
        便于下游调试或落盘。
    week_key_field :
        写入记录上的字段名（仅 ``annotate_week_key=True`` 时）。

    Returns
    -------
    buckets :
        键为该周周五日期字符串 ``YYYY-MM-DD``，按时间升序。
    skipped :
        无法解析 ``published`` 的条目（原 dict 浅拷贝引用包装为列表）。
    """
    skipped: list[dict[str, Any]] = []
    indices: list[int] = []
    parsed: list[pd.Timestamp] = []

    for i, rec in enumerate(records):
        raw = pick_published_raw(rec, published_keys)
        ts = parse_published_timestamp(raw, timezone=timezone)
        if pd.isna(ts):
            skipped.append(dict(rec))
            continue
        indices.append(i)
        parsed.append(ts)

    if not indices:
        return OrderedDict(), skipped

    df = pd.DataFrame({"_i": indices, "_dt": parsed})
    g = df.groupby(
        pd.Grouper(key="_dt", freq="W-FRI", label="right", closed="right")
    )
    out: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()

    for week_end, sub in g:
        if pd.isna(week_end) or sub.empty:
            continue
        label = pd.Timestamp(week_end).strftime("%Y-%m-%d")
        rows: list[dict[str, Any]] = []
        for j in sub["_i"].tolist():
            item = dict(records[j])
            if annotate_week_key:
                item[week_key_field] = label
            rows.append(item)
        out[label] = rows

    return out, skipped


def format_bucket_summary(buckets: Mapping[str, Sequence[Any]]) -> str:
    """便于 CLI / 日志的一行摘要。"""
    lines = [f"{wk}: {len(items)} 条" for wk, items in buckets.items()]
    return " | ".join(lines) if lines else "(空)"


def _read_jsonl(path: Path | str) -> list[dict[str, Any]]:
    """CLI 用轻量读取，避免依赖 agent_service 全量依赖。"""
    p = Path(path)
    if not p.exists():
        return []
    rows: list[dict[str, Any]] = []
    with p.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _cli() -> None:
    import argparse

    ap = argparse.ArgumentParser(
        description="按周线 W-FRI（周五标签）切分 JSONL 新闻；时区默认 Asia/Shanghai。"
    )
    ap.add_argument("jsonl", type=Path, help="JSONL 路径（含 published 等时间字段）")
    ap.add_argument(
        "--timezone",
        default="Asia/Shanghai",
        help="对齐交易周的时区（默认 Asia/Shanghai）",
    )
    ap.add_argument(
        "--annotate",
        action="store_true",
        help="为每条记录写入 _week_end_friday 字段（当周周五 YYYY-MM-DD）",
    )
    args = ap.parse_args()
    items = _read_jsonl(args.jsonl)
    buckets, skipped = bucket_news_by_week_w_fri(
        items,
        timezone=args.timezone,
        annotate_week_key=args.annotate,
    )
    print(f"总条数: {len(items)} | 有效时间: {len(items) - len(skipped)} | 无法解析时间: {len(skipped)}")
    print(f"周数: {len(buckets)}")
    for wk, rows in buckets.items():
        print(f"  {wk}: {len(rows)} 条")
    if skipped:
        print(f"（无 published）示例最多 3 条 title: {[s.get('title', '')[:40] for s in skipped[:3]]}")


if __name__ == "__main__":
    _cli()
