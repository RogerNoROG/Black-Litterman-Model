#!/usr/bin/env python3
"""
将 news-data 下 JSONL 按** ISO 公历周** 拆成多文件，文件名：``{ISO年}-W{周序号:02d}.jsonl``（例：``2023-W48``）。

- 时间字段：依次尝试 ``date``、``published``、``pubDate``、``date_published``、``published_at``；无时区时按
  ``Asia/Shanghai`` 理解。对历史上夏令时等导致的**不存在/歧义**时刻，使用 ``nonexistent``/``ambiguous`` 安全策略。
- ``--jobs N``：默认 **多线程**（`ThreadPoolExecutor`，每个 ``part-*.jsonl`` 一任务），先写入
  ``by-week/{lang}/.shards/`` 再按周**合并**到最终文件；``--use-processes`` 时改多进程。
- ``--date-from`` / ``--date-to``：只保留**日历日**落在该闭区间内的记录（对 ``YYYY-MM-DD`` 字符串做快路径加速）。

示例::

  .venv/bin/python scripts/split_news_data_by_iso_week.py \\
    --in-root news-data --out-root news-data/by-week --langs zh \\
    --date-from 2018-01-01 --date-to 2023-12-31 --jobs 8
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from collections import OrderedDict, defaultdict
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
)
from datetime import date
from pathlib import Path
from typing import Any, TextIO

import pandas as pd

# 与 emotion_bl.news_weekly 中 DEFAULT_PUBLISHED_KEYS 对齐并含 OpenNews 的 date
PUBLISHED_KEYS = ("date", "published", "pubDate", "date_published", "published_at")


def _parse_time(rec: dict[str, Any], *, tz: str) -> pd.Timestamp | None:
    for k in PUBLISHED_KEYS:
        v = rec.get(k)
        if v is None or v == "":
            continue
        if isinstance(v, (int, float)):
            v = int(v) / (1000.0 if v > 1e12 else 1.0)
        ts = pd.to_datetime(v, errors="coerce")
        if pd.isna(ts):
            continue
        ts = pd.Timestamp(ts)
        if ts.tzinfo is None:
            # 午夜时刻在部分历史日（夏令时/调整）上可能「不存在」；先对齐到日再用正午
            if ts.hour == 0 and ts.minute == 0 and ts.second == 0:
                ts = ts.normalize() + pd.Timedelta(hours=12)
            t2: pd.Timestamp | None = None
            for ne in ("shift_forward", "shift_backward", "NaT"):
                try:
                    t2 = ts.tz_localize(
                        tz,
                        ambiguous="NaT",
                        nonexistent=ne,
                    )  # type: ignore[arg-type]
                except (ValueError, TypeError, OSError):
                    t2 = None
                    continue
                if t2 is not None and not pd.isna(t2):
                    return t2.tz_convert(tz) if t2.tzinfo else t2
            return None
        return ts.tz_convert(tz)
    return None


def _ts_in_date_range(
    ts: pd.Timestamp,
    *,
    date_from: date | None,
    date_to: date | None,
) -> bool:
    """``date_from``/``date_to`` 为 ``None`` 表示不限制；否则按 **Asia/Shanghai 日历日** 闭区间。"""
    if date_from is None and date_to is None:
        return True
    day = ts.tz_convert("Asia/Shanghai").date()
    if date_from is not None and day < date_from:
        return False
    if date_to is not None and day > date_to:
        return False
    return True


def _fast_iso_date_in_range(
    rec: dict[str, Any],
    *,
    date_from: date | None,
    date_to: date | None,
) -> bool | None:
    """
    若 ``date`` 为 ``YYYY-MM-DD`` 字符串，可用字符串与边界比较，省掉部分解析。

    返回 ``None`` 表示无法快筛，应走完整时间解析；``True``/``False`` 表示在/不在范围内。
    """
    if date_from is None and date_to is None:
        return None
    v = rec.get("date")
    if not isinstance(v, str) or len(v) < 10:
        return None
    s = v[:10]
    if not s[0:4].isdigit() or s[4] != "-" or s[7] != "-":
        return None
    if date_from is not None and s < date_from.isoformat():
        return False
    if date_to is not None and s > date_to.isoformat():
        return False
    return True


def _iso_filename(ts: pd.Timestamp) -> str:
    ic = ts.isocalendar()
    if isinstance(ic, tuple):
        y, w, _ = ic[0], ic[1], ic[2]
    else:
        y, w = int(ic.year), int(ic.week)
    return f"{y}-W{w:02d}"


def _wfri_filename(ts: pd.Timestamp) -> str:
    dfc = pd.DataFrame({"_dt": [ts]})
    g = dfc.groupby(
        pd.Grouper(key="_dt", freq="W-FRI", label="right", closed="right")
    )
    for week_end, _ in g:
        if pd.isna(week_end):
            continue
        return pd.Timestamp(week_end).strftime("%Y-%m-%d")
    return ts.strftime("%Y-%m-%d")


def _open_out(
    out_dir: Path,
    week_id: str,
    handles: OrderedDict[str, TextIO],
    *,
    max_open: int,
) -> TextIO:
    if week_id in handles:
        handles.move_to_end(week_id)
        return handles[week_id]
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{week_id}.jsonl"
    f = path.open("a", encoding="utf-8", newline="\n")
    handles[week_id] = f
    while len(handles) > max_open:
        old_k, old_f = handles.popitem(last=False)
        old_f.close()
    return f


def _process_one_jsonl(
    in_path: Path,
    out_dir: Path,
    *,
    tz: str,
    bucket: str,
    max_open: int,
    date_from: date | None = None,
    date_to: date | None = None,
) -> tuple[int, int, int]:
    """
    单文件流式切分。返回 (ok, skip, bad_json)。
    若设 ``date_from``/``date_to``，区间外记录计入 skip、不输出。
    """
    n_ok = 0
    n_skip = 0
    n_bad = 0
    handles: OrderedDict[str, TextIO] = OrderedDict()
    name_fn = _iso_filename if bucket == "iso" else _wfri_filename
    with in_path.open("r", encoding="utf-8", errors="replace") as inf:
        for line in inf:
            line = line.rstrip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                n_bad += 1
                continue
            fr = _fast_iso_date_in_range(
                rec, date_from=date_from, date_to=date_to
            )
            if fr is False:
                n_skip += 1
                continue
            ts = _parse_time(rec, tz=tz)
            if ts is None:
                n_skip += 1
                continue
            if fr is not True and not _ts_in_date_range(
                ts, date_from=date_from, date_to=date_to
            ):
                n_skip += 1
                continue
            wid = name_fn(ts)
            outf = _open_out(out_dir, wid, handles, max_open=max_open)
            outf.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n_ok += 1
    for f in handles.values():
        f.close()
    handles.clear()
    return n_ok, n_skip, n_bad


def _split_dir_sequential(
    in_dir: Path,
    out_dir: Path,
    *,
    tz: str,
    bucket: str,
    max_open: int,
    date_from: date | None = None,
    date_to: date | None = None,
) -> tuple[int, int, int]:
    files = sorted(in_dir.glob("*.jsonl"))
    if not files:
        return 0, 0, 0
    t_ok = t_skip = t_bad = 0
    for fp in files:
        o, s, b = _process_one_jsonl(
            fp,
            out_dir,
            tz=tz,
            bucket=bucket,
            max_open=max_open,
            date_from=date_from,
            date_to=date_to,
        )
        t_ok += o
        t_skip += s
        t_bad += b
    return t_ok, t_skip, t_bad


def _merge_shard_dirs(
    out_lang_dir: Path,
    shard_root: Path,
) -> None:
    """将 ``shard_root`` 下各子目录中同名 ``*.jsonl`` 流式合并到 ``out_lang_dir``。"""
    by_name: dict[str, list[Path]] = defaultdict(list)
    for p in shard_root.rglob("*.jsonl"):
        by_name[p.name].append(p)
    out_lang_dir.mkdir(parents=True, exist_ok=True)
    for name, parts in by_name.items():
        dest = out_lang_dir / name
        with dest.open("w", encoding="utf-8", newline="\n") as w:
            for part in sorted(parts):
                with part.open("r", encoding="utf-8", errors="replace") as r:
                    shutil.copyfileobj(r, w)


def _parallel_worker(
    item: tuple[str, str, str, str, int, str | None, str | None],
) -> tuple[str, int, int, int]:
    """
    并行子任务。``dfrom_str``/``dto_str`` 为 ``YYYY-MM-DD`` 或 ``None``（字符串便于线程/进程传参一致）。
    """
    in_p, sh_p, tz, bucket, mopen, dfrom_str, dto_str = item
    d0 = date.fromisoformat(dfrom_str) if dfrom_str else None
    t0 = date.fromisoformat(dto_str) if dto_str else None
    ok, sk, bad = _process_one_jsonl(
        Path(in_p),
        Path(sh_p),
        tz=tz,
        bucket=bucket,
        max_open=mopen,
        date_from=d0,
        date_to=t0,
    )
    return in_p, ok, sk, bad


def _split_dir_parallel(
    in_dir: Path,
    out_dir: Path,
    *,
    tz: str,
    bucket: str,
    max_open: int,
    jobs: int,
    date_from: date | None = None,
    date_to: date | None = None,
    use_processes: bool = False,
) -> tuple[int, int, int]:
    files = sorted(in_dir.glob("*.jsonl"))
    if not files:
        return 0, 0, 0
    n_workers = min(max(1, jobs), len(files))
    if n_workers == 1:
        return _split_dir_sequential(
            in_dir,
            out_dir,
            tz=tz,
            bucket=bucket,
            max_open=max_open,
            date_from=date_from,
            date_to=date_to,
        )

    dfrom_str = date_from.isoformat() if date_from else None
    dto_str = date_to.isoformat() if date_to else None

    shard_root = out_dir / ".shards"
    if shard_root.is_dir():
        shutil.rmtree(shard_root)
    shard_root.mkdir(parents=True, exist_ok=True)

    tasks: list[tuple[str, str, str, str, int, str | None, str | None]] = []
    for i, fp in enumerate(files):
        sh = shard_root / f"{i:04d}_{fp.stem}"
        sh.mkdir(parents=True, exist_ok=True)
        tasks.append(
            (
                str(fp.resolve()),
                str(sh.resolve()),
                tz,
                bucket,
                max_open,
                dfrom_str,
                dto_str,
            )
        )

    t_ok = t_skip = t_bad = 0
    Executor = ProcessPoolExecutor if use_processes else ThreadPoolExecutor
    with Executor(max_workers=n_workers) as ex:
        futs = [ex.submit(_parallel_worker, t) for t in tasks]
        for fut in as_completed(futs):
            _p, o, s, b = fut.result()
            t_ok += o
            t_skip += s
            t_bad += b

    _merge_shard_dirs(out_dir, shard_root)
    shutil.rmtree(shard_root, ignore_errors=True)
    return t_ok, t_skip, t_bad


def main() -> None:
    ap = argparse.ArgumentParser(
        description="将 JSONL 新闻按周拆到独立文件（ISO 年周或周五标签）"
    )
    ap.add_argument(
        "--in-root",
        type=Path,
        default=Path("news-data"),
        help="根目录，其下为 zh/、en/ 子目录及各自 part-*.jsonl",
    )
    ap.add_argument(
        "--out-root",
        type=Path,
        default=Path("news-data") / "by-week",
        help="输出根目录，将创建 zh/、en/",
    )
    ap.add_argument(
        "--langs",
        type=str,
        default="zh,en",
        help="要处理的子目录，逗号分隔，默认 zh,en",
    )
    ap.add_argument(
        "--bucket",
        choices=("iso", "w-fri"),
        default="iso",
        help="iso=文件名 {年}-W{周}；w-fri=周五 YYYY-MM-DD",
    )
    ap.add_argument(
        "--timezone",
        default="Asia/Shanghai",
        help="解析无时区时间时使用的 IANA 时区",
    )
    ap.add_argument(
        "--max-open",
        type=int,
        default=300,
        help="单进程内同时打开的周文件数上限",
    )
    ap.add_argument(
        "--jobs",
        type=int,
        default=0,
        help="并行线程数；0=自动（min(8, CPU, 分片数)）；1=纯单进程顺序读所有文件",
    )
    ap.add_argument(
        "--date-from",
        type=str,
        default="",
        help="仅保留此日（含）之后的新闻，如 2018-01-01；空表示不限制",
    )
    ap.add_argument(
        "--date-to",
        type=str,
        default="",
        help="仅保留此日（含）之前的新闻，如 2023-12-31；空表示不限制",
    )
    ap.add_argument(
        "--use-processes",
        action="store_true",
        help="用多进程替代多线程（I/O+GIL 场景一般线程更快）",
    )
    args = ap.parse_args()
    root: Path = args.in_root
    if not root.is_dir():
        raise SystemExit(f"输入根目录不存在: {root.resolve()}")

    d_from: date | None = None
    d_to: date | None = None
    if (args.date_from or "").strip():
        d_from = date.fromisoformat((args.date_from or "").strip()[:10])
    if (args.date_to or "").strip():
        d_to = date.fromisoformat((args.date_to or "").strip()[:10])
    if d_from is not None and d_to is not None and d_from > d_to:
        raise SystemExit("--date-from 不能晚于 --date-to")

    ncpu = os.cpu_count() or 4

    langs = [x.strip() for x in args.langs.split(",") if x.strip()]
    total_ok = 0
    for lang in langs:
        d = root / lang
        if not d.is_dir():
            print(f"[skip] 无子目录: {d}", file=sys.stderr)
            continue
        odir = args.out_root / lang
        if odir.is_dir():
            n_exist = len(list(odir.glob("*.jsonl")))
            n_dot = (odir / ".shards").is_dir()
            if n_exist > 0 or n_dot:
                print(
                    f"警告: {odir} 下已有输出或 .shards；会清空 .shards 并合并，"
                    f"**最终**周文件以本次合并结果为准。建议先 `rm -rf` 该目录。",
                    file=sys.stderr,
                )
        n_files = len(list(d.glob("*.jsonl")))
        if args.jobs <= 0:
            j = min(8, ncpu, max(1, n_files))
        else:
            j = int(args.jobs)
        mopen = args.max_open
        if j > 1:
            mopen = max(64, min(mopen, 400 // j))

        mode = "process" if args.use_processes else "thread"
        dr = f"{d_from} ~ {d_to}" if (d_from or d_to) else "（未限制）"
        print(
            f"处理 {d} -> {odir} … (jobs={j}, {mode}, 日期: {dr}, max_open={mopen})",
            flush=True,
        )
        if j <= 1:
            ok, skip, bad = _split_dir_sequential(
                d,
                odir,
                tz=args.timezone,
                bucket=args.bucket,
                max_open=mopen,
                date_from=d_from,
                date_to=d_to,
            )
        else:
            ok, skip, bad = _split_dir_parallel(
                d,
                odir,
                tz=args.timezone,
                bucket=args.bucket,
                max_open=mopen,
                jobs=j,
                date_from=d_from,
                date_to=d_to,
                use_processes=args.use_processes,
            )
        print(
            f"  写入 {ok} 行 | 无时间: {skip} | JSON 坏行: {bad}",
            flush=True,
        )
        total_ok += ok
    print(f"合计成功写入: {total_ok} 行", flush=True)


if __name__ == "__main__":
    main()
