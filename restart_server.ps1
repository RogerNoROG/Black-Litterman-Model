# 结束占用 API 端口的进程并启动 FastAPI（run_api.py）
# 用法:
#   .\restart_server.ps1              # 前台运行，Ctrl+C 停止
#   .\restart_server.ps1 -Port 8001   # 指定端口（需与 .env 中 API_PORT 一致）
#   .\restart_server.ps1 -Background  # 新开窗口后台启动

param(
    [int] $Port = 8000,
    [switch] $Background
)

$ErrorActionPreference = "Continue"
$ProjectRoot = $PSScriptRoot
Set-Location $ProjectRoot

$sep = [IO.Path]::PathSeparator
if ([string]::IsNullOrEmpty($env:PYTHONPATH)) {
    $env:PYTHONPATH = $ProjectRoot
} elseif ($env:PYTHONPATH -notlike "*$ProjectRoot*") {
    $env:PYTHONPATH = "$ProjectRoot$sep$env:PYTHONPATH"
}

$listeners = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if ($listeners) {
    $pids = $listeners.OwningProcess | Sort-Object -Unique
    foreach ($procId in $pids) {
        Write-Host "[restart] 停止占用端口 $Port 的进程 PID=$procId"
        Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 1
} else {
    Write-Host "[restart] 端口 $Port 当前无监听"
}

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Error "未找到 python，请先安装并加入 PATH。"
    exit 1
}

if ($Background) {
    Write-Host "[restart] 在新窗口中启动 python run_api.py ..."
    Start-Process -FilePath "python" -ArgumentList "run_api.py" -WorkingDirectory $ProjectRoot
} else {
    Write-Host "[restart] 前台启动 python run_api.py（Ctrl+C 退出）..."
    python run_api.py
}
