"""启动 FastAPI：python run_api.py

无论从哪个当前目录启动，都会把本项目根目录插入 sys.path 最前，
避免误导入其它路径下同名 emotion_bl（表现为 /api/run/stream 一直 404）。
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import uvicorn

from emotion_bl.config import settings

if __name__ == "__main__":
    uvicorn.run(
        "emotion_bl.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
    )
