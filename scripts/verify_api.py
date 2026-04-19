"""全量自检（不启动网络）：导入、路由、流式管线一轮。

用法（在项目根目录）:
  python scripts/verify_api.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    from fastapi.testclient import TestClient

    import emotion_bl.api.main as main_mod

    paths = {getattr(r, "path", None) for r in main_mod.app.routes if getattr(r, "path", None)}
    need = {
        "/health",
        "/api/meta",
        "/api/run",
        "/api/run/stream",
        "/api/run-stream",
        "/stream/ndjson",
        "/",
    }
    missing = need - paths
    if missing:
        print("FAIL missing routes:", missing)
        return 1
    print("OK routes:", sorted(need))

    c = TestClient(main_mod.app)
    r = c.get("/api/meta")
    if r.status_code != 200:
        print("FAIL /api/meta", r.status_code)
        return 1
    meta = r.json()
    print("OK /api/meta main_file:", meta.get("main_file", "")[:80], "…")

    body_path = ROOT / "data" / "stream_smoke_request.example.json"
    if not body_path.is_file():
        print("WARN skip stream test, missing", body_path)
        return 0
    body = json.loads(body_path.read_text(encoding="utf-8"))
    r = c.post("/stream/ndjson", json=body)
    if r.status_code != 200:
        print("FAIL stream", r.status_code, r.text[:500])
        return 1
    lines = [x for x in r.text.strip().split("\n") if x.strip()]
    last = json.loads(lines[-1])
    if last.get("type") != "done":
        print("FAIL last frame", last)
        return 1
    print("OK stream/ndjson lines=", len(lines), "last=done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
