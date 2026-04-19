#!/usr/bin/env bash
# 结束占用 API 端口的进程并启动 FastAPI（与 Windows 下 restart_server.ps1 对应）
#
# 用法:
#   ./restart_server.sh              # 默认端口 8000，前台运行（Ctrl+C 退出）
#   PORT=8001 ./restart_server.sh    # 指定端口（需与 .env 中 API_PORT 一致）
#   BACKGROUND=1 ./restart_server.sh # 后台启动，追加日志到项目根目录 api_server.log

set -euo pipefail

PORT="${PORT:-8000}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*) _pp_sep=";" ;;
  *) _pp_sep=":" ;;
esac
if [[ -n "${PYTHONPATH:-}" ]]; then
  export PYTHONPATH="${ROOT}${_pp_sep}${PYTHONPATH}"
else
  export PYTHONPATH="${ROOT}"
fi

if command -v fuser >/dev/null 2>&1; then
  fuser -k "${PORT}/tcp" 2>/dev/null || true
elif command -v lsof >/dev/null 2>&1; then
  lsof -ti ":${PORT}" 2>/dev/null | xargs -r kill -9 2>/dev/null || true
else
  echo "[restart] 未找到 fuser/lsof，无法自动释放端口 ${PORT}；请手动结束占用进程" >&2
fi

sleep 1

# 优先使用项目虚拟环境（系统 python 通常未装 requirements）
if [[ -x "${ROOT}/.venv/bin/python" ]]; then
  PY="${ROOT}/.venv/bin/python"
elif [[ -x "${ROOT}/venv/bin/python" ]]; then
  PY="${ROOT}/venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PY=python3
  echo "[restart] 提示: 未找到 .venv，使用系统 python3。若报 ModuleNotFoundError，请执行: python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt" >&2
elif command -v python >/dev/null 2>&1; then
  PY=python
  echo "[restart] 提示: 未找到 .venv，使用系统 python。" >&2
else
  echo "[restart] 未找到 python3 或 python" >&2
  exit 1
fi

if [[ "${BACKGROUND:-0}" == "1" ]]; then
  echo "[restart] 后台启动 ${PY} run_api.py，日志 ${ROOT}/api_server.log"
  nohup "${PY}" run_api.py >>"${ROOT}/api_server.log" 2>&1 &
  echo "[restart] PID $!"
  exit 0
fi

echo "[restart] 前台启动 ${PY} run_api.py（Ctrl+C 退出）..."
exec "${PY}" run_api.py
