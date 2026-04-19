"""
可扩展的时效性内容获取：用 httpx 拉取 JSON/HTML 搜索 API 结果，
解析后写入与 Scrapy 相同结构的 dict 列表，供 pipeline_analyze 使用。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx


def fetch_url(url: str, timeout: float = 30.0) -> bytes:
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        r = client.get(url)
        r.raise_for_status()
        return r.content


def rows_from_json_placeholder(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """示例：期望 payload 含 {\"items\": [{\"title\",\"url\",\"snippet\"}]}"""
    out = []
    now = datetime.now(timezone.utc).isoformat()
    for it in payload.get("items", []):
        out.append(
            {
                "title": it.get("title", ""),
                "link": it.get("url", ""),
                "summary": it.get("snippet", ""),
                "published": it.get("published", ""),
                "source": "search_api",
                "fetched_at": now,
            }
        )
    return out
