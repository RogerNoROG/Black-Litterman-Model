"""
编排：读 JSONL → 情感分析 → 按资产聚合 → 生成 BL 观点 (P, Q, Omega)。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import numpy as np

from emotion_bl.bl.black_litterman import BlackLittermanEngine, BLInputs, views_from_sentiment
from emotion_bl.config import settings
from emotion_bl.sentiment import SentimentAnalyzer
from emotion_bl.sentiment.analyzer import _resolve_backend
from emotion_bl.tagging import aggregate_scores_from_news_records, load_keyword_map


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def run_scrapy_rss(
    project_root: Path,
    feed_urls: list[str] | None = None,
    *,
    capture_log: bool = False,
    subprocess_timeout: float | None = None,
) -> tuple[int, str]:
    """在项目根目录执行 Scrapy，写入 settings 中的 JSONL。capture_log=True 时返回 (returncode, 合并日志尾部)。"""
    cmd = [sys.executable, "-m", "scrapy", "crawl", "rss", "-s", "LOG_LEVEL=INFO"]
    if feed_urls:
        cmd.extend(["-a", "feeds=" + ",".join(feed_urls)])
    env = {**os.environ}
    sep = os.pathsep
    root_s = str(project_root)
    prev_pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = root_s + sep + prev_pp if prev_pp else root_s
    run_kw: dict[str, Any] = {}
    if subprocess_timeout is not None and subprocess_timeout > 0:
        run_kw["timeout"] = subprocess_timeout
    try:
        if capture_log:
            r = subprocess.run(
                cmd,
                cwd=project_root,
                env=env,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                **run_kw,
            )
            merged = (r.stdout or "") + ("\n" + r.stderr if r.stderr else "")
            tail = merged.strip()[-6000:] if merged.strip() else "(无控制台输出)"
            return r.returncode, tail
        r = subprocess.run(cmd, cwd=project_root, env=env, **run_kw)
        return r.returncode, ""
    except subprocess.TimeoutExpired:
        raise TimeoutError("RSS 爬取子进程超时，已中止（与 ANALYSIS_DEADLINE_SEC 剩余预算有关）") from None


def articles_to_text_rows(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for it in items:
        title = it.get("title") or ""
        summary = it.get("summary") or ""
        text = f"{title}\n{summary}".strip()
        out.append({**it, "text": text})
    return out


def _check_deadline(deadline_monotonic: float | None) -> None:
    if deadline_monotonic is not None and time.monotonic() > deadline_monotonic:
        raise TimeoutError(
            "分析已超过配置的总时限（环境变量 ANALYSIS_DEADLINE_SEC，见 .env.example）。"
            " 设为 0 可关闭；或增大该值 / 减少 max_articles / 改用 snownlp。"
        )


def pipeline_analyze(
    jsonl_path: Path | None = None,
    keyword_map_path: Path | None = None,
    tickers: list[str] | None = None,
    tau: float | None = None,
    view_scale: float | None = None,
    sigma: np.ndarray | None = None,
    pi: np.ndarray | None = None,
    max_articles: int | None = None,
    *,
    records: list[dict[str, Any]] | None = None,
    internal_bl_fusion: bool = True,
    on_log: Callable[[str], None] | None = None,
    on_step: Callable[[int], None] | None = None,
    on_llm_progress: Callable[[int, int], None] | None = None,
    deadline_monotonic: float | None = None,
) -> dict[str, Any]:
    def log(msg: str) -> None:
        _check_deadline(deadline_monotonic)
        if on_log:
            on_log(msg)

    def step(idx: int) -> None:
        _check_deadline(deadline_monotonic)
        if on_step:
            on_step(idx)

    path = jsonl_path or settings.news_jsonl_path
    step(1)
    analyzer = SentimentAnalyzer()
    kmap = load_keyword_map(keyword_map_path)
    if records is not None:
        items = records
        log(f"[内存输入] 新闻条数: {len(items)}（跳过 JSONL 读取）")
    else:
        log(f"[落盘/读取] 数据文件: {path.resolve()}")
        items = read_jsonl(path)
        log(f"[落盘/读取] JSONL 原始条数: {len(items)}")
    rows = articles_to_text_rows(items)
    if max_articles is not None and max_articles > 0:
        rows = rows[:max_articles]
        log(f"[落盘/读取] 按 max_articles 截取后待分析: {len(rows)} 条")

    step(2)
    eff = _resolve_backend(analyzer.use_bert)
    model_hint = (
        settings.llm_model
        if eff == "llm"
        else (settings.bert_model_id if eff == "bert" else "SnowNLP")
    )
    log(f"[情感] 实际后端: {eff}，引擎: {model_hint}，待分析条数={len(rows)}")
    if eff == "llm":
        log(
            "[情感] 说明：LLM 为每条新闻独立 HTTP；已启用并发以缩短墙钟时间。"
            " 单条单次读超时见 LLM_PER_ARTICLE_READ_SEC，超时自动重试最多 LLM_PER_ARTICLE_MAX_RETRIES 次。"
            " 若遇 429/限流，请降低 LLM_MAX_CONCURRENT。"
        )
    t_sent0 = time.perf_counter()
    scored: list[dict[str, Any]] = []

    if eff == "llm":
        max_w = max(1, min(int(getattr(settings, "llm_max_concurrent", 8) or 8), 32))
        work = [(i, r) for i, r in enumerate(rows) if r.get("text")]
        log(f"[情感] 并发: max_workers={max_w}，待请求条数={len(work)}")
        results_by_idx: dict[int, dict[str, Any]] = {}
        log_lock = threading.Lock()

        def run_llm_one(item: tuple[int, dict[str, Any]]) -> tuple[int, dict[str, Any]]:
            idx, r = item
            _check_deadline(deadline_monotonic)
            n_chars = len(r.get("text") or "")
            t0 = time.perf_counter()
            sr = analyzer.analyze(r["text"])
            dt = time.perf_counter() - t0
            row = {
                **r,
                "score": sr.score,
                "label": sr.label,
                "backend": sr.backend,
                "mentioned_tickers": list(sr.mentioned_tickers),
            }
            title = (r.get("title") or "")[:60]
            with log_lock:
                log(
                    f"[情感] (完成 {idx + 1}/{len(rows)}) 本条耗时 {dt:.2f}s · 文本 {n_chars} 字 · {title}… → "
                    f"{sr.label} score={sr.score:.3f} · {sr.backend}"
                )
            return idx, row

        total_llm = len(work)
        with ThreadPoolExecutor(max_workers=max_w) as ex:
            futures = [ex.submit(run_llm_one, p) for p in work]
            done_llm = 0
            for fut in as_completed(futures):
                idx, row = fut.result()
                results_by_idx[idx] = row
                done_llm += 1
                if on_llm_progress:
                    on_llm_progress(done_llm, total_llm)
        scored = [results_by_idx[i] for i in sorted(results_by_idx)]
    else:
        for idx, r in enumerate(rows):
            _check_deadline(deadline_monotonic)
            if not r.get("text"):
                continue
            n_chars = len(r.get("text") or "")
            t0 = time.perf_counter()
            sr = analyzer.analyze(r["text"])
            dt = time.perf_counter() - t0
            scored.append(
                {
                    **r,
                    "score": sr.score,
                    "label": sr.label,
                    "backend": sr.backend,
                    "mentioned_tickers": list(sr.mentioned_tickers),
                }
            )
            title = (r.get("title") or "")[:60]
            log(
                f"[情感] ({idx + 1}/{len(rows)}) 本条耗时 {dt:.2f}s · 文本 {n_chars} 字 · {title}… → "
                f"{sr.label} score={sr.score:.3f} · {sr.backend}"
            )

    if scored:
        sent_total = time.perf_counter() - t_sent0
        avg_note = "（墙钟总时长/条数；LLM 并发时远小于逐条串行之和）" if eff == "llm" else ""
        log(
            f"[情感] 阶段小结：共 {len(scored)} 条，累计 {sent_total:.1f}s，"
            f"均摊 {sent_total / len(scored):.2f}s/条{avg_note}"
        )

    step(3)
    map_mode = (settings.ticker_mapping_mode or "keyword_map").strip().lower()
    if map_mode == "llm_json" and eff != "llm":
        log(
            "[映射] TICKER_MAPPING_MODE=llm_json 需要 SENTIMENT_BACKEND=llm；"
            "当前为其他后端，本次回退为 keyword_map。"
        )
        map_mode = "keyword_map"
    log(f"[映射] 模式: {map_mode}")
    buckets = aggregate_scores_from_news_records(
        scored,
        keyword_map=kmap,
        text_key="text",
        mapping_mode=map_mode,
        llm_json_fallback_keyword=bool(
            getattr(settings, "llm_ticker_json_fallback_keyword", True)
        ),
    )
    asset_scores = {t: float(np.mean(v)) for t, v in buckets.items()}
    article_counts_per_ticker = {t: len(v) for t, v in buckets.items()}
    log(f"[映射] 命中标的数: {len(buckets)}，标的: {list(buckets.keys()) or '（无）'}")
    if not buckets:
        if map_mode == "llm_json":
            log(
                "[映射] 未命中：各条 LLM 返回的 tickers 为空，且关键词回退仍未匹配。"
                " 可检查模型是否按 JSON 输出 tickers，或关闭回退仅依赖抽取。"
            )
        else:
            log(
                "[映射] 未命中原因：当前新闻正文里没有出现关键词表中的公司/品牌词，"
                "无法把情绪聚合到 ticker，故 k=0、无 BL 观点。"
                " 可改用 TICKER_MAPPING_MODE=llm_json + SENTIMENT_BACKEND=llm 由模型从正文抽代码，"
                "或在 emotion_bl/tagging.py / keyword_map_path 扩充映射。"
            )

    if tickers:
        universe = list(tickers)
    else:
        universe = sorted(asset_scores.keys()) or sorted(kmap.keys())
    full_scores = {t: asset_scores.get(t, 0.0) for t in universe}

    step(4)
    P, Q, omega = views_from_sentiment(
        full_scores,
        universe,
        view_scale=view_scale if view_scale is not None else settings.default_view_scale,
    )
    log(f"[预设观点] 非零绝对视图条数 k={len(Q)}，universe 维度 n={len(universe)}")

    n = len(universe)
    if sigma is None:
        sigma = np.eye(n) * 0.04**2
    if pi is None:
        pi = np.zeros(n)

    mu_bl = pi
    sigma_bl = sigma
    step(5)
    if internal_bl_fusion and P.shape[0] > 0:
        engine = BlackLittermanEngine()
        res = engine.solve(
            BLInputs(
                sigma=sigma,
                pi=pi,
                P=P,
                Q=Q,
                omega=omega,
                tau=tau if tau is not None else settings.default_tau,
            )
        )
        mu_bl = res.mu_bl
        sigma_bl = res.sigma_bl
        log("[BL] 已融合先验与观点，得到后验 μ_BL。")
    elif not internal_bl_fusion:
        log("[BL] internal_bl_fusion=False：仅输出观点矩阵，不在此融合（交由周频 BL 回测）。")
    else:
        log("[BL] 无有效观点，跳过融合（μ_BL 等于先验 Π）。")

    return {
        "articles_analyzed": len(scored),
        "article_counts_per_ticker": article_counts_per_ticker,
        "per_article": [
            {
                "title": x.get("title", "")[:200],
                "score": x["score"],
                "label": x["label"],
                "link": x.get("link", ""),
            }
            for x in scored[:100]
        ],
        "asset_sentiment_mean": asset_scores,
        "views": {
            "tickers": universe,
            "P": P.tolist(),
            "Q": Q.tolist(),
            "omega": omega.tolist() if omega.size else [],
        },
        "black_litterman": {
            "mu_bl": mu_bl.tolist(),
            "sigma_bl_diag": np.diag(sigma_bl).tolist() if sigma_bl.ndim == 2 else [],
        },
    }
