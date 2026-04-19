"""
周频 BLM Agent 全流程（可被 CLI 与 FastAPI 流式端点共用）。

阶段索引（与网页流程条一致）：
0 配置 · 1 中文新闻采集（默认 Istero API；可选 Scrapy RSS）· 2 新闻分桶 · 3 行情数据
· 4 样本协方差/收益 · 5 市值隐含先验权重 · 6 回测窗口 · 7 逐周情感观点并入 BL · 8 落盘
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from emotion_bl.agent_service import pipeline_analyze, read_jsonl, run_scrapy_rss
from emotion_bl.config import settings
from emotion_bl.news_weekly import bucket_news_by_week_w_fri


def _utc_iso_z() -> str:
    u = datetime.now(timezone.utc)
    return u.strftime("%Y-%m-%dT%H:%M:%S") + f".{u.microsecond // 1000:03d}Z"


def week_end_key_for_iloc(bl: Any, start_idx: int) -> str:
    """
    与 ``news_weekly.bucket_news_by_week_w_fri`` 的周线标签对齐。

    行情侧 ``Date`` 已由 ``W-FRI`` 重采样为当周**最后一个周五**的日历日，
    因此直接用该日期的上海时区「日」作为键，与按 ``published`` 做 ``Grouper(W-FRI)``
    得到的周五标签一致；避免 ``to_period`` 的时区告警与边界歧义。
    """
    cal = bl._price_row_dates
    if cal is None or len(cal) < 2:
        raise RuntimeError("缺少价格日历")
    end_dates = pd.to_datetime(cal.iloc[1:]).reset_index(drop=True)
    if start_idx < 0 or start_idx >= len(end_dates):
        raise IndexError(f"start_idx={start_idx} 超出周线范围 [0,{len(end_dates)})")
    d = pd.Timestamp(end_dates.iloc[start_idx])
    if d.tzinfo is None:
        d = d.tz_localize("Asia/Shanghai")
    else:
        d = d.tz_convert("Asia/Shanghai")
    return d.normalize().strftime("%Y-%m-%d")


def _parse_tickers_from_stocks() -> list[str]:
    from data_akshare import STOCKS

    return [t[0] for t in STOCKS]


def _ensure_paths(root: Path) -> Path:
    code_dir = (root / "code").resolve()
    if not code_dir.is_dir():
        raise FileNotFoundError(f"未找到 code 目录: {code_dir}")
    if str(code_dir) not in sys.path:
        sys.path.insert(0, str(code_dir))
    if str(root.resolve()) not in sys.path:
        sys.path.insert(0, str(root.resolve()))
    return code_dir


def run_weekly_pipeline(
    *,
    root: Path,
    year: int | None = None,
    skip_crawl: bool = False,
    market_source: str = "",
    jsonl_path: Path | None = None,
    keyword_map_path: Path | None = None,
    out_path: Path | None = None,
    truncate_jsonl_before_crawl: bool = False,
    max_weeks: int = 0,
    feed_urls: list[str] | None = None,
    news_source: str = "api",
    api_append: bool = False,
    on_log: Callable[[str], None] | None = None,
    on_step: Callable[[int, str], None] | None = None,
    on_progress: Callable[..., None] | None = None,
) -> tuple[dict[str, Any], Path]:
    """
    执行与 ``scripts/run_weekly_blm_agent.py`` 相同的业务逻辑。

    返回 ``(payload, 输出文件路径)``。payload 可 JSON 序列化（不含路径元数据）。

    回调均为可选：``on_step(idx, label)``；``on_progress(kind, current, total, **kw)``。
    """
    root = root.resolve()
    jp = (jsonl_path or (root / "data" / "news_items.jsonl")).resolve()
    kp = (keyword_map_path or (root / "data" / "csi300_top10_keywords.json")).resolve()
    outp = (out_path or (root / "data" / "weekly_blm_agent_result.json")).resolve()

    def log(msg: str) -> None:
        if on_log:
            on_log(msg)
        else:
            print(msg, flush=True)

    def step(idx: int, label: str) -> None:
        if on_step:
            on_step(idx, label)
        log(f"[阶段 {idx}] {label}")

    def prog(kind: str, current: int, total: int, **kw: Any) -> None:
        if on_progress:
            on_progress(kind, current, total, **kw)

    try:
        from dotenv import load_dotenv

        load_dotenv(root / ".env")
    except ImportError:
        pass

    code_dir = _ensure_paths(root)
    _prev_cwd = os.getcwd()
    os.chdir(code_dir)
    try:
        import structures as st

        step(0, "加载 structures，准备路径与参数")
        log(f"项目根: {root}")
        log(f"新闻 JSONL: {jp} · 关键词表: {kp} · 输出: {outp}")

        if (market_source or "").strip():
            st.DATA_SOURCE = (market_source or "").strip().lower()
            log(f"[配置] DATA_SOURCE 覆盖为: {st.DATA_SOURCE}")

        from black_litterman import BlackLitterman
        from data_providers import fetch_price_market_pair

        y = int(year if year is not None else st.BACK_TEST_YEAR)
        tau_bl = float(st.TAU)
        log(f"[配置] 回测年 year={y} · TAU={tau_bl} · BACK_TEST_T={st.BACK_TEST_T}")

        news_mode = (news_source or "api").strip().lower()
        if not skip_crawl:
            jp.parent.mkdir(parents=True, exist_ok=True)
            if truncate_jsonl_before_crawl and jp.exists():
                jp.unlink()
                log(f"已按选项删除旧 JSONL: {jp}")

            if news_mode == "api":
                step(1, "Istero API 中文要闻 → 写入 JSONL")
                from emotion_bl.istero_news import fetch_cctv_china_latest_records

                rows = fetch_cctv_china_latest_records()
                mode = "a" if api_append else "w"
                with jp.open(mode, encoding="utf-8", newline="\n") as f:
                    for row in rows:
                        f.write(json.dumps(row, ensure_ascii=False) + "\n")
                log(
                    f"Istero API 写入 {len(rows)} 条 → {jp}（mode={mode}，append={api_append}）"
                )
            elif news_mode == "rss":
                step(1, "Scrapy 采集 RSS → 写入 JSONL")
                rc, log_tail = run_scrapy_rss(root, feed_urls=feed_urls, capture_log=True)
                if rc != 0:
                    raise RuntimeError(f"Scrapy 退出码 {rc}\n{log_tail}")
                log(
                    f"Scrapy 完成，退出码 0。日志尾部:\n{log_tail[-2000:]}"
                    if len(log_tail) > 2000
                    else f"Scrapy 完成。\n{log_tail}"
                )
            else:
                raise ValueError(
                    f"未知 news_source={news_source!r}，请使用 api（默认）或 rss"
                )
        else:
            step(1, "跳过新闻采集（--skip-crawl，直接读已有 JSONL）")
            log("未运行 API/RSS，直接读取已有 JSONL。")

        step(2, "读取 JSONL，按 W-FRI（周五周线）分桶")
        all_news = read_jsonl(jp)
        buckets, skipped = bucket_news_by_week_w_fri(all_news, timezone="Asia/Shanghai")
        log(
            f"新闻总计 {len(all_news)} 条；有效周桶 {len(buckets)} 个；"
            f"无日期跳过 {len(skipped)} 条。"
        )
        if buckets:
            bk = list(buckets.keys())
            log(f"新闻桶键（周五）范围: {bk[0]} … {bk[-1]}（按 published 聚合，与行情周线键须同年才能匹配）")
            in_year = [k for k in bk if k.startswith(f"{y}-")]
            n_in_year = sum(len(buckets[k]) for k in in_year)
            log(
                f"回测年 {y} 内周桶数: {len(in_year)}，条数合计: {n_in_year}。"
                f"（其余新闻落在其它年的周五桶，不会参与本年周频匹配）"
            )
            if n_in_year == 0 and len(all_news) - len(skipped) > 0:
                log(
                    f"[警告] 有 {len(all_news) - len(skipped)} 条带日期的新闻，但没有任何一条落在 "
                    f"「{y}」年的周五桶；请改用该年附近的新闻、或将 --year 改为与 published 年份一致。"
                )

        step(3, "加载行情：指数 + 成分股周收盘价 / 市值（structures.DATA_SOURCE）")
        log(f"调用 fetch_price_market_pair()，DATA_SOURCE={st.DATA_SOURCE} …")
        price_df, mv_df, src = fetch_price_market_pair()
        log(
            f"行情就绪：实际数据源标识「{src}」· price_df {price_df.shape} · "
            f"mv_df {mv_df.shape}；列含 Date 与 CSI300.GI 等。"
        )

        step(4, "BlackLitterman：周收益样本、协方差估计（get_cc_return）")
        bl = BlackLitterman(price_df=price_df, mv_df=mv_df)
        bl.get_cc_return()
        log(
            f"样本协方差与收益统计已估计；股票数 stock_number={bl.stock_number}，"
            f"列名 stock_names={list(bl.stock_names)}。"
        )

        step(5, "先验：市值隐含均衡权重（get_market_value_weight）")
        bl.get_market_value_weight()
        log("市值加权先验权重 π 已就绪。")

        step(6, f"解析回测年 {y} 在周线日历上的 iloc 切片")
        s_ix, e_ix = bl.backtest_iloc_range_for_year(y)
        n_weeks_total = e_ix - s_ix + 1
        log(f"回测年 {y} 对应周线索引范围 [{s_ix}, {e_ix}]，共 {n_weeks_total} 周。")

        tickers = _parse_tickers_from_stocks()
        results: list[dict[str, Any]] = []
        n_done = 0

        step(7, "逐周：情感/LLM → 观点 (P,Q,Ω) → get_post_weight_with_sentiment_views")
        log(
            f"[配置] 情感后端 SENTIMENT_BACKEND={settings.sentiment_backend!r}；"
            "仅当为 llm 时才调用大模型 API（通义等）。snownlp/bert 不走 LLM。"
        )

        def pipe_log(msg: str) -> None:
            log(f"  [管线] {msg}")

        def pipe_step(_i: int) -> None:
            pass

        def pipe_llm(cur: int, tot: int) -> None:
            prog("llm_articles", cur, tot)

        for start_idx in range(s_ix, e_ix + 1):
            if max_weeks and n_done >= max_weeks:
                log(f"已达 max_weeks={max_weeks}，提前结束。")
                break

            week_key = week_end_key_for_iloc(bl, start_idx)
            week_items = buckets.get(week_key, [])
            prog("blm_week", n_done + 1, n_weeks_total, week_end=week_key, start_idx=start_idx)

            if not week_items:
                log(
                    f"  周 {week_key} (idx={start_idx})：桶内无新闻 → 零观点矩阵，"
                    f"仅先验 + 协方差求后验权重。"
                )
                w_bl, real_ret = bl.get_post_weight_with_sentiment_views(
                    start_idx,
                    np.zeros((0, bl.stock_number)),
                    np.array([]),
                    np.array([]),
                )
                row = {
                    "year": y,
                    "week_end": week_key,
                    "start_idx": start_idx,
                    "n_news": 0,
                    "skipped_no_news": True,
                    "weights": {t: float(x) for t, x in zip(bl.stock_names, w_bl)},
                    "realized_stock_returns": {
                        t: float(x) for t, x in zip(bl.stock_names, real_ret)
                    },
                    "views": None,
                }
                results.append(row)
                n_done += 1
                continue

            log(
                f"  周 {week_key} (idx={start_idx})：本桶 {len(week_items)} 条新闻 → "
                f"pipeline_analyze（情感/映射/观点）…"
            )
            pipe = pipeline_analyze(
                keyword_map_path=kp,
                tickers=tickers,
                tau=tau_bl,
                records=week_items,
                internal_bl_fusion=False,
                on_log=pipe_log,
                on_step=pipe_step,
                on_llm_progress=pipe_llm,
            )
            P = np.asarray(pipe["views"]["P"], dtype=float)
            Q = np.asarray(pipe["views"]["Q"], dtype=float)
            Om = np.asarray(pipe["views"]["omega"], dtype=float)
            log(
                f"  观点矩阵形状 P={P.shape} Q={Q.shape} Ω={Om.shape}；"
                f"articles_analyzed={pipe.get('articles_analyzed', 0)}"
            )

            w_bl, real_ret = bl.get_post_weight_with_sentiment_views(start_idx, P, Q, Om)
            row = {
                "year": y,
                "week_end": week_key,
                "start_idx": start_idx,
                "n_news": len(week_items),
                "articles_analyzed": pipe.get("articles_analyzed", 0),
                "asset_sentiment_mean": pipe.get("asset_sentiment_mean", {}),
                "weights": {t: float(x) for t, x in zip(bl.stock_names, w_bl)},
                "realized_stock_returns": {
                    t: float(x) for t, x in zip(bl.stock_names, real_ret)
                },
                "views": pipe.get("views"),
            }
            results.append(row)
            n_done += 1
            log(
                f"  → 后验权重和={sum(row['weights'].values()):.6f}；"
                f"已实现股票周收益已写入行。"
            )

        step(8, "序列化并写入 weekly JSON")
        outp.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, Any] = {
            "generated_at": _utc_iso_z(),
            "market_data_source": src,
            "structures_data_source_used": st.DATA_SOURCE,
            "tau": tau_bl,
            "universe": tickers,
            "year": y,
            "weeks": results,
        }
        outp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"已写入: {outp}")

        return payload, outp
    finally:
        os.chdir(_prev_cwd)
