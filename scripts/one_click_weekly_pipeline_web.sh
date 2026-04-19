#!/usr/bin/env bash
# 一键：尽量启动本仓库 API，并在浏览器打开「全流程」页（周频 BLM 可视化执行过程）。
# 用法：在项目根执行  ./scripts/one_click_weekly_pipeline_web.sh
# 环境变量：HOST（默认 127.0.0.1）、PORT（与 run_api / settings 一致，常见 8000）

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ -f ".venv/bin/activate" ]]; then
  # shellcheck source=/dev/null
  source ".venv/bin/activate"
fi

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8000}"
URL="http://${HOST}:${PORT}/pipeline-full"
HEALTH="http://${HOST}:${PORT}/health"

if curl -sf "$HEALTH" >/dev/null 2>&1; then
  echo "检测到 API 已在运行: $HEALTH"
else
  echo "未检测到 API，将在后台启动: python run_api.py（日志: /tmp/blm_run_api.log）"
  nohup python run_api.py > /tmp/blm_run_api.log 2>&1 &
  echo "$!" > /tmp/blm_run_api.pid
  for _ in $(seq 1 60); do
    if curl -sf "$HEALTH" >/dev/null 2>&1; then
      echo "API 已就绪。"
      break
    fi
    sleep 0.25
  done
  if ! curl -sf "$HEALTH" >/dev/null 2>&1; then
    echo "启动超时，请查看 /tmp/blm_run_api.log 后手动执行: python run_api.py"
    exit 1
  fi
fi

echo "打开页面: $URL"
if command -v xdg-open >/dev/null 2>&1; then
  xdg-open "$URL" >/dev/null 2>&1 || true
elif command -v open >/dev/null 2>&1; then
  open "$URL" >/dev/null 2>&1 || true
else
  echo "（未找到 xdg-open/open）请手动在浏览器打开上述 URL。"
fi

echo ""
echo "说明：页面内点击「一键运行全流程」才会真正跑周频管线（可能较久）。"
echo "若端口不是 ${PORT}，请设置环境变量 PORT 后重试。"
