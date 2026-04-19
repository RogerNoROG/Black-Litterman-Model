from __future__ import annotations

import json
import queue
import threading
import time
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from emotion_bl.agent_service import pipeline_analyze, run_scrapy_rss
from emotion_bl.config import settings
from emotion_bl.dashboard import build_dashboard_payload
from emotion_bl.weekly_pipeline import run_weekly_pipeline

app = FastAPI(
    title="情绪量化 Black-Litterman Agent",
    description="爬虫数据 → 情感分析 → BL 后验收益（模块化 API）",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class AnalyzeRequest(BaseModel):
    jsonl_path: str | None = None
    keyword_map_path: str | None = None
    tickers: list[str] | None = None
    tau: float | None = Field(default=None, ge=1e-6, le=1.0)
    view_scale: float | None = Field(default=None, description="情绪 score 映射为观点收益的缩放")
    max_articles: int | None = Field(default=None, ge=1, le=2000)


class CrawlRequest(BaseModel):
    project_root: str = "."
    feed_urls: list[str] | None = None


class AnalyzeCovRequest(BaseModel):
    analyze: AnalyzeRequest
    sigma: list[list[float]]
    pi: list[float]


class RunDashboardRequest(BaseModel):
    """一键：可选先爬 RSS，再分析并返回前端展示用 dashboard。"""

    crawl: bool = False
    project_root: str = "."
    feed_urls: list[str] | None = None
    jsonl_path: str | None = None
    keyword_map_path: str | None = None
    tickers: list[str] | None = None
    max_articles: int | None = Field(
        default=40,
        ge=1,
        le=500,
        description="最多分析的新闻条数（控制大模型调用次数）",
    )
    tau: float | None = Field(default=None, ge=1e-6, le=1.0)
    view_scale: float | None = None


class WeeklyPipelineStreamRequest(BaseModel):
    """周频 BLM：RSS/JSONL → 周线分桶 → 行情 → 逐周情感与 Black-Litterman（与 CLI 等价，NDJSON 流式日志）。"""

    year: int | None = Field(default=None, description="默认 structures.BACK_TEST_YEAR")
    skip_crawl: bool = False
    project_root: str = "."
    market_source: str = Field(
        "",
        description="非空则覆盖 structures.DATA_SOURCE（如 json、akshare）",
    )
    jsonl_path: str | None = None
    keyword_map_path: str | None = None
    out_path: str | None = None
    truncate_jsonl_before_crawl: bool = False
    max_weeks: int = Field(
        0,
        ge=0,
        le=500,
        description="0=不限制；否则仅处理前 N 个周线窗口",
    )
    feed_urls: list[str] | None = None


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
WEB_DIR = PROJECT_ROOT / "web"
STATIC_DIR = WEB_DIR / "static"


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/meta")
def api_meta():
    """自检：确认当前进程加载的是本仓库 API（含流式端点）。"""
    here = Path(__file__).resolve()
    stream_paths = [
        "/stream/ndjson",
        "/api/run/stream",
        "/api/run-stream",
        "/api/weekly-pipeline/stream",
    ]
    return {
        "package": "emotion_bl.api.main",
        "main_file": str(here),
        "stream_post_paths": stream_paths,
        "sentiment_backend": settings.sentiment_backend,
        "ticker_mapping_mode": settings.ticker_mapping_mode,
        "llm_ticker_json_fallback_keyword": settings.llm_ticker_json_fallback_keyword,
        "llm_model": settings.llm_model,
        "llm_per_article_read_sec": settings.llm_per_article_read_sec,
        "llm_per_article_max_retries": settings.llm_per_article_max_retries,
        "llm_max_concurrent": settings.llm_max_concurrent,
        "analysis_deadline_sec": settings.analysis_deadline_sec,
        "hint": "若 stream 仍 404，说明端口上不是本进程；请关闭旧 uvicorn 后在本项目根目录执行 python run_api.py",
    }


@app.post("/crawl/rss")
def crawl_rss(body: CrawlRequest):
    root = Path(body.project_root).resolve()
    run_scrapy_rss(root, body.feed_urls)
    return {"message": "crawl triggered", "project_root": str(root)}


@app.post("/analyze")
def analyze(body: AnalyzeRequest) -> dict[str, Any]:
    jp = Path(body.jsonl_path) if body.jsonl_path else None
    kp = Path(body.keyword_map_path) if body.keyword_map_path else None
    try:
        return pipeline_analyze(
            jsonl_path=jp,
            keyword_map_path=kp,
            tickers=body.tickers,
            tau=body.tau,
            view_scale=body.view_scale,
            max_articles=body.max_articles,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/analyze/with_cov")
def analyze_with_cov(body: AnalyzeCovRequest):
    """传入样本协方差与先验均衡收益 Pi（如由市值加权隐含）。"""
    a = body.analyze
    jp = Path(a.jsonl_path) if a.jsonl_path else None
    kp = Path(a.keyword_map_path) if a.keyword_map_path else None
    S = np.array(body.sigma, dtype=float)
    p = np.array(body.pi, dtype=float)
    try:
        return pipeline_analyze(
            jsonl_path=jp,
            keyword_map_path=kp,
            tickers=a.tickers,
            tau=a.tau,
            view_scale=a.view_scale,
            sigma=S,
            pi=p,
            max_articles=a.max_articles,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/")
def index_page():
    index = WEB_DIR / "index.html"
    if not index.is_file():
        raise HTTPException(status_code=404, detail="web/index.html 缺失")
    return FileResponse(index)


@app.get("/pipeline-full")
def pipeline_full_page():
    """一键全流程：新闻采集 → 数据处理 → BL 拟合（NDJSON 流 + 日志）。"""
    page = WEB_DIR / "pipeline-full.html"
    if not page.is_file():
        raise HTTPException(status_code=404, detail="web/pipeline-full.html 缺失")
    return FileResponse(page)


@app.get("/weekly-agent")
def weekly_agent_dashboard_page():
    """周频 BLM Agent 结果可视化（读 ``data/weekly_blm_agent_result.json``）。"""
    page = WEB_DIR / "weekly-agent.html"
    if not page.is_file():
        raise HTTPException(status_code=404, detail="web/weekly-agent.html 缺失")
    return FileResponse(page)


@app.get("/api/weekly-blm-agent-result")
def weekly_blm_agent_result(
    relative_path: str = Query(
        "",
        max_length=240,
        description="相对于项目下 data/ 的 JSON 路径，如 weekly_blm_agent_result.json；空则默认该文件",
    ),
) -> dict[str, Any]:
    """供看板拉取 ``scripts/run_weekly_blm_agent.py`` 等写出的 JSON（仅允许 data/ 目录内）。"""
    data_root = (PROJECT_ROOT / "data").resolve()
    rel = (relative_path or "").strip().replace("\\", "/").lstrip("/")
    if not rel:
        p = data_root / "weekly_blm_agent_result.json"
    else:
        if ".." in rel or rel.startswith("/"):
            raise HTTPException(status_code=400, detail="非法路径")
        p = (data_root / rel).resolve()
        try:
            p.relative_to(data_root)
        except ValueError as e:
            raise HTTPException(
                status_code=400, detail="path 必须位于项目 data/ 目录下"
            ) from e
    if p.suffix.lower() != ".json":
        raise HTTPException(status_code=400, detail="仅支持 .json 文件")
    if not p.is_file():
        raise HTTPException(
            status_code=404,
            detail=f"未找到文件（请先运行 scripts/run_weekly_blm_agent.py 或指定 data 下已有 JSON）",
        )
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"JSON 解析失败: {e}") from e


@app.post("/api/run")
def run_dashboard(body: RunDashboardRequest) -> dict[str, Any]:
    root = Path(body.project_root).resolve()
    try:
        if body.crawl:
            run_scrapy_rss(root, body.feed_urls)
        jp = Path(body.jsonl_path) if body.jsonl_path else None
        kp = Path(body.keyword_map_path) if body.keyword_map_path else None
        raw = pipeline_analyze(
            jsonl_path=jp,
            keyword_map_path=kp,
            tickers=body.tickers,
            tau=body.tau,
            view_scale=body.view_scale,
            max_articles=body.max_articles,
        )
        dashboard = build_dashboard_payload(raw)
        return {
            "dashboard": dashboard,
            "recent_articles": (raw.get("per_article") or [])[:15],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


def _stream_run_dashboard(body: RunDashboardRequest):
    """后台线程执行管线，经队列向主线程推送 NDJSON 事件（step / log / done / error）。"""
    q: queue.Queue = queue.Queue()

    def work() -> None:
        t_run = time.perf_counter()
        deadline: float | None = None
        ads = float(getattr(settings, "analysis_deadline_sec", 0) or 0)
        if ads > 0:
            deadline = time.monotonic() + ads

        def emit_log(msg: str) -> None:
            elapsed = time.perf_counter() - t_run
            q.put({"type": "log", "message": f"[+{elapsed:7.1f}s] {msg}"})

        try:
            q.put({"type": "step", "index": 0})
            if deadline is not None:
                emit_log(f"[计时] 已启用整次分析时限 ANALYSIS_DEADLINE_SEC={ads}s（到点将中止）")
            else:
                emit_log("[计时] 未设置整次分析时限（ANALYSIS_DEADLINE_SEC=0），将跑完全部条数。")
            root = Path(body.project_root).resolve()
            if body.crawl:
                emit_log("[采集] 启动 Scrapy RSS …")
                crawl_timeout = None
                if deadline is not None:
                    crawl_timeout = max(5.0, deadline - time.monotonic())
                code, slog = run_scrapy_rss(
                    root,
                    body.feed_urls,
                    capture_log=True,
                    subprocess_timeout=crawl_timeout,
                )
                emit_log(f"[采集] Scrapy 结束，退出码 {code}。\n{slog}")
            else:
                emit_log("[采集] 未勾选爬取，跳过 RSS。")

            jp = Path(body.jsonl_path) if body.jsonl_path else None
            kp = Path(body.keyword_map_path) if body.keyword_map_path else None

            raw = pipeline_analyze(
                jsonl_path=jp,
                keyword_map_path=kp,
                tickers=body.tickers,
                tau=body.tau,
                view_scale=body.view_scale,
                max_articles=body.max_articles,
                on_log=emit_log,
                on_step=lambda i: q.put({"type": "step", "index": i}),
                on_llm_progress=lambda cur, tot: q.put(
                    {
                        "type": "progress",
                        "kind": "llm_articles",
                        "current": cur,
                        "total": tot,
                    }
                ),
                deadline_monotonic=deadline,
            )
            dashboard = build_dashboard_payload(raw)
            q.put({"type": "step", "index": 6})
            emit_log("[呈现] 已生成时效性摘要与预设观点表。")
            q.put(
                {
                    "type": "done",
                    "dashboard": dashboard,
                    "recent_articles": (raw.get("per_article") or [])[:15],
                }
            )
        except TimeoutError as e:
            q.put({"type": "error", "message": str(e)})
        except Exception as e:
            q.put({"type": "error", "message": str(e)})
        finally:
            q.put(None)

    th = threading.Thread(target=work, daemon=True)
    th.start()
    while True:
        item = q.get()
        if item is None:
            break
        yield json.dumps(item, ensure_ascii=False) + "\n"


def _run_dashboard_stream_response(body: RunDashboardRequest) -> StreamingResponse:
    return StreamingResponse(
        _stream_run_dashboard(body),
        media_type="application/x-ndjson; charset=utf-8",
    )


@app.post("/api/run/stream")
def run_dashboard_stream(body: RunDashboardRequest):
    """NDJSON 流：与真实执行同步的 step / log，最后一条 type=done 含 dashboard。总时限由 ANALYSIS_DEADLINE_SEC（0=不限制）。"""
    return _run_dashboard_stream_response(body)


@app.post("/api/run-stream")
def run_dashboard_stream_alias(body: RunDashboardRequest):
    """备用路径（少数反向代理对 `/api/run/stream` 处理异常时可改用此地址）。"""
    return _run_dashboard_stream_response(body)


@app.post("/stream/ndjson")
def run_dashboard_stream_ndjson(body: RunDashboardRequest):
    """短路径流式端点，避免与 `/api/run` 在个别环境下的路由冲突。"""
    return _run_dashboard_stream_response(body)


def _resolve_project_path(root: Path, p: str | None, default_rel: str) -> Path:
    if not p or not str(p).strip():
        return (root / default_rel).resolve()
    pp = Path(p.strip())
    if pp.is_absolute():
        return pp.resolve()
    return (root / pp).resolve()


def _stream_weekly_pipeline(body: WeeklyPipelineStreamRequest):
    q: queue.Queue = queue.Queue()

    def work() -> None:
        t0 = time.perf_counter()

        def emit(obj: dict[str, Any]) -> None:
            q.put(obj)

        def tlog(msg: str) -> None:
            elapsed = time.perf_counter() - t0
            emit({"type": "log", "message": f"[+{elapsed:7.1f}s] {msg}"})

        def tstep(idx: int, label: str = "") -> None:
            emit({"type": "step", "index": idx, "label": label})

        def tprog(kind: str, cur: int, tot: int, **kw: Any) -> None:
            row: dict[str, Any] = {
                "type": "progress",
                "kind": kind,
                "current": cur,
                "total": tot,
            }
            for k, v in kw.items():
                if isinstance(v, (str, int, float, bool)) or v is None:
                    row[k] = v
            emit(row)

        try:
            root = Path(body.project_root).expanduser().resolve()
            jp = _resolve_project_path(root, body.jsonl_path, "data/news_items.jsonl")
            kp = _resolve_project_path(
                root, body.keyword_map_path, "data/csi300_top10_keywords.json"
            )
            outp = _resolve_project_path(
                root, body.out_path, "data/weekly_blm_agent_result.json"
            )

            payload, outp_written = run_weekly_pipeline(
                root=root,
                year=body.year,
                skip_crawl=body.skip_crawl,
                market_source=body.market_source or "",
                jsonl_path=jp,
                keyword_map_path=kp,
                out_path=outp,
                truncate_jsonl_before_crawl=body.truncate_jsonl_before_crawl,
                max_weeks=body.max_weeks,
                feed_urls=body.feed_urls,
                on_log=tlog,
                on_step=tstep,
                on_progress=tprog,
            )
            try:
                rel_out = str(outp_written.relative_to(root))
            except ValueError:
                rel_out = str(outp_written)
            emit(
                {
                    "type": "done",
                    "generated_at": payload.get("generated_at"),
                    "year": payload.get("year"),
                    "weeks_count": len(payload.get("weeks", [])),
                    "universe": payload.get("universe"),
                    "market_data_source": payload.get("market_data_source"),
                    "result_relative": rel_out,
                    "dashboard_url": "/weekly-agent",
                }
            )
        except Exception as e:
            emit({"type": "error", "message": str(e)})
        finally:
            q.put(None)

    th = threading.Thread(target=work, daemon=True)
    th.start()
    while True:
        item = q.get()
        if item is None:
            break
        yield json.dumps(item, ensure_ascii=False) + "\n"


def _weekly_pipeline_stream_response(body: WeeklyPipelineStreamRequest) -> StreamingResponse:
    return StreamingResponse(
        _stream_weekly_pipeline(body),
        media_type="application/x-ndjson; charset=utf-8",
    )


@app.post("/api/weekly-pipeline/stream")
def weekly_pipeline_stream(body: WeeklyPipelineStreamRequest):
    """NDJSON：step（0–8）/ log / progress（blm_week、llm_articles）/ done | error。"""
    return _weekly_pipeline_stream_response(body)


if STATIC_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
