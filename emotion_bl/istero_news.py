"""
Istero 开放数据：央视国内要闻 ``latest`` 接口 → 与本仓库 JSONL 对齐的新闻 dict。

官方说明摘要
------------
- 地址：默认 ``https://api.istero.com/resource/v1/cctv/china/latest/news``（GET/POST，JSON）。
- 鉴权（推荐）：``Authorization: Bearer <token>``；环境变量 ``ISTEREO_API_TOKEN``。
- 可选：``X-Signature``、``X-Timestamp``、``X-Nonce``（动态签名，见服务商《开发文档》）。
- 成功时通常 ``code == 200``，列表在 ``data``；失败时读 ``message``。

``time`` 字段为 ``YYYY-MM-DD HH:mm:ss``，本模块按 **Asia/Shanghai** 解析再输出 RFC3339 ``published``，
以便 ``emotion_bl.news_weekly`` 分桶。
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx
import pandas as pd

from emotion_bl.config import settings


def _shanghai_published_iso(time_str: str) -> str:
    s = (time_str or "").strip()
    if not s:
        return ""
    ts = pd.to_datetime(s, errors="coerce")
    if pd.isna(ts):
        return ""
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("Asia/Shanghai")
    else:
        ts = ts.tz_convert("Asia/Shanghai")
    return ts.isoformat()


def cctv_item_to_news_record(item: dict[str, Any]) -> dict[str, Any]:
    """单条 API ``data[]`` → 与 RSS 管线兼容的 JSONL 行结构。"""
    published = _shanghai_published_iso(str(item.get("time") or ""))
    return {
        "title": (item.get("title") or "").strip(),
        "link": (item.get("url") or "").strip(),
        "summary": (item.get("description") or "")[:2000],
        "published": published,
        "source": "istero:api.istero.com/resource/v1/cctv/china/latest/news",
        "poster": (item.get("poster") or "").strip(),
        "keywords": (item.get("keywords") or "").strip(),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_cctv_china_latest_raw(
    *,
    token: str | None = None,
    url: str | None = None,
    timeout_sec: float = 60.0,
    use_signature_headers: bool | None = None,
    sign_secret: str | None = None,
) -> dict[str, Any]:
    """
    调用 Istero 接口，返回完整 JSON（含 ``code`` / ``data`` / ``message`` 等）。

    ``use_signature_headers=True`` 时仅附加时间戳与 nonce（占位）；**具体 Sign 算法须按服务商文档实现**，
    当前仓库未内置签名计算，若 401/403 请联系 Istero 或在其文档中实现 ``X-Signature`` 后扩展本函数。
    """
    tok = (token if token is not None else settings.istero_api_token).strip()
    if not tok:
        raise ValueError(
            "缺少 Istero Token：请在 .env 设置 ISTERO_API_TOKEN，或传入 token=..."
        )
    endpoint = (url or settings.istero_api_url or "").strip()
    if not endpoint:
        raise ValueError("ISTEREO_API_URL 为空")

    headers: dict[str, str] = {
        "Authorization": f"Bearer {tok}",
        "Accept": "application/json",
    }
    use_sig = (
        settings.istero_use_signature_headers
        if use_signature_headers is None
        else use_signature_headers
    )
    if use_sig:
        # 占位：nonce / timestamp 可按文档参与签名；未配置 sign 算法时不设置 X-Signature
        headers["X-Timestamp"] = str(int(time.time() * 1000))
        headers["X-Nonce"] = uuid.uuid4().hex[:16]
        sec = (sign_secret if sign_secret is not None else settings.istero_sign_secret)
        if sec.strip():
            raise NotImplementedError(
                "已开启 istero_use_signature_headers 且配置了 istero_sign_secret，"
                "但本仓库尚未实现 Istero《开发文档》中的 Sign 算法；请关闭签名或自行扩展 emotion_bl/istero_news.py"
            )

    with httpx.Client(timeout=timeout_sec) as client:
        r = client.get(endpoint, headers=headers)
        r.raise_for_status()
        body = r.json()

    code = body.get("code")
    if code != 200:
        msg = body.get("message") or body.get("msg") or json.dumps(body, ensure_ascii=False)[:500]
        raise RuntimeError(f"Istero 业务错误 code={code}: {msg}")
    return body


def fetch_cctv_china_latest_records(
    *,
    token: str | None = None,
    url: str | None = None,
    timeout_sec: float = 60.0,
) -> list[dict[str, Any]]:
    """返回已映射好的新闻 dict 列表（可直接写入 JSONL）。"""
    raw = fetch_cctv_china_latest_raw(
        token=token, url=url, timeout_sec=timeout_sec
    )
    data = raw.get("data")
    if not isinstance(data, list):
        raise RuntimeError(f"Istero 返回 data 非列表: {type(data)}")
    return [cctv_item_to_news_record(x) for x in data if isinstance(x, dict)]
