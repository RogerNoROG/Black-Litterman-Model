"""
将 pipeline 输出整理为「时效性总结 + 预设观点 + 可信度」展示结构。
可信度：在同类 Ω 下做相对归一（Ω 越小 → BL 中观点越「尖锐」、此处显示可信度越高）。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import numpy as np


def _credibility_from_omega(omegas: list[float]) -> list[float]:
    """Ω 越小 → BL 观点越「尖锐」→ 本批次内相对可信度越高。"""
    if not omegas:
        return []
    xs = [float(o) for o in omegas]
    lo, hi = min(xs), max(xs)
    if hi <= lo + 1e-18:
        return [1.0] * len(xs)
    return [round((hi - x) / (hi - lo), 4) for x in xs]


def _label(c: float) -> str:
    if c >= 0.67:
        return "较高"
    if c >= 0.34:
        return "中等"
    return "较低"


def build_dashboard_payload(raw: dict[str, Any]) -> dict[str, Any]:
    views = raw.get("views") or {}
    tickers: list[str] = list(views.get("tickers") or [])
    P = np.asarray(views.get("P") or [], dtype=float)
    Q = np.asarray(views.get("Q") or [], dtype=float)
    omega = np.asarray(views.get("omega") or [], dtype=float)
    asset_sent = raw.get("asset_sentiment_mean") or {}
    counts = raw.get("article_counts_per_ticker") or {}

    k = len(Q)
    cred_list = _credibility_from_omega([float(omega[i]) for i in range(k)]) if k else []

    preset_views: list[dict[str, Any]] = []
    for i in range(k):
        row = P[i] if P.ndim == 2 and i < P.shape[0] else np.array([])
        j = int(np.argmax(row)) if row.size else 0
        t = tickers[j] if j < len(tickers) else "?"
        q = float(Q[i])
        om = float(omega[i]) if i < len(omega) else 0.0
        c = cred_list[i] if i < len(cred_list) else 0.0
        preset_views.append(
            {
                "ticker": t,
                "sentiment_mean": round(float(asset_sent.get(t, 0.0)), 4),
                "view_q": round(q, 6),
                "view_q_bps": round(q * 10000, 2),
                "omega": round(om, 8),
                "credibility": c,
                "credibility_percent": round(c * 100, 1),
                "credibility_label": _label(c),
                "supporting_articles": int(counts.get(t, 0)),
                "direction": "偏多" if q > 0 else ("偏空" if q < 0 else "中性"),
            }
        )

    n_art = int(raw.get("articles_analyzed", 0))
    lines = [
        f"本次共纳入 **{n_art}** 条时效资讯并完成情感量化。",
        f"在 Black-Litterman 框架下生成 **{k}** 条「绝对视图」预设观点（每只标的对应超额收益预期 Q 与不确定性 Ω）。",
        "**可信度**由当前批次内 Ω 的相对大小换算：Ω 越小，表示该情绪观点在模型中被赋予越强的置信权重（相对同批其它观点）。",
    ]
    if preset_views:
        top = ", ".join(f"{v['ticker']}({v['direction']}, 可信度{v['credibility_label']})" for v in preset_views[:8])
        lines.append(f"**要点：** {top}" + ("…" if len(preset_views) > 8 else ""))
    else:
        lines.append("**要点：** 未形成非零预设观点（可能与关键词未命中或情绪接近中性有关）。")

    summary_md = "\n\n".join(lines)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary_markdown": summary_md,
        "articles_analyzed": n_art,
        "preset_views": preset_views,
        "black_litterman": raw.get("black_litterman"),
    }
