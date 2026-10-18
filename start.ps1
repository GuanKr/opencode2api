#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
chcp 65001 | Out-Null
$dir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -LiteralPath $dir

$exe = Join-Path $dir "opencode2api.exe"
$cfg = Join-Path $dir "config.json"

if (-not (Test-Path -LiteralPath $exe)) {
  Write-Host "[错误] 找不到 $exe" -ForegroundColor Red
  Write-Host "请先执行: go build -o opencode2api.exe ./cmd/opencode2api"
  if ([Environment]::UserInteractive) { pause }
  exit 1
}
if (-not (Test-Path -LiteralPath $cfg)) {
  Write-Host "[错误] 找不到 $cfg" -ForegroundColor Red
  Write-Host "请先: Copy-Item config.example.json config.json 并修改 server_keys/zen_keys/proxies/webui.password"
  if ([Environment]::UserInteractive) { pause }
  exit 1
}

# 看门狗默认关闭：只有显式传 -Watchdog 才启动，避免默认多一个常驻进程与自动切换。
# （旧 -NoWatchdog 已废弃，仅保留兼容提示；见下方 elseif 分支。）
# 注意只有经本脚本（Ctrl+C 退出时连带停止看门狗）启动才有父子托管；点 X 关窗口可能残留，需任务管理器收尾；
# 直接 .\watchdog-clash.ps1 启动则是独立进程，关窗口不影响、可单独启停。
$watchdogRequested = $args -contains "-Watchdog"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " opencode2api 启动中..."
Write-Host " 目录: $dir"
Write-Host " 配置: $cfg"
Write-Host " API : http://127.0.0.1:19880/healthz（以 config.json 为准）"
Write-Host " WebUI: http://127.0.0.1:19881（以 config.json 为准）"
if ($watchdogRequested) {
  Write-Host " 看门狗: watchdog-clash.ps1 (Clash 节点自动切换，被动巡检 15s)"
  Write-Host " 提示: Ctrl+C 退出会连带停止看门狗；直接点 X 关窗口可能残留最小化的"
  Write-Host "       看门狗进程，需在任务管理器结束对应的 pwsh 进程"
} else {
  Write-Host " 看门狗: 默认关闭 (加 -Watchdog 启动 Clash 节点自动切换)"
}
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

# 看门狗只跑在 pwsh 7+ 下：watchdog 是无 BOM UTF-8 + 中文，
# powershell 5.1 会按 ANSI 解码导致过滤失效，故不回退 5.1。
$watchdog = Join-Path $dir "watchdog-clash.ps1"
$watchdogProc = $null
$watchdogSkipReason = ''
$watchdogEnabled = $watchdogRequested -and (Test-Path -LiteralPath $watchdog)
if ($watchdogEnabled) {
  try {
    $logDir = Join-Path $dir ".tmp"
    if (-not (Test-Path -LiteralPath $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }
  } catch {
    Write-Host "[看门狗] 日志目录创建失败，仅启动网关：$($_.Exception.Message)" -ForegroundColor Yellow
    $watchdogEnabled = $false
    $watchdogSkipReason = 'logdir'
  }
}
if ($watchdogEnabled) {
  $pwshExe = $null
  try { $pwshExe = (Get-Command pwsh -ErrorAction SilentlyContinue).Source } catch { }
  if ([string]::IsNullOrWhiteSpace($pwshExe)) {
    Write-Host "[看门狗] 找不到 pwsh 7+（不回退 powershell 5.1：中文脚本会被误解码），仅启动网关" -ForegroundColor Yellow
    Write-Host ""
    $watchdogEnabled = $false
    $watchdogSkipReason = 'no-pwsh'
  } else {
    try {
      # 带空格路径必须整体加引号再传 -File，否则子进程会把路径切散。
      $watchdogArg = '"' + $watchdog + '"'
      $watchdogProc = Start-Process -FilePath $pwshExe -ArgumentList @("-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", $watchdogArg) -WorkingDirectory $dir -PassThru -WindowStyle Minimized
      Write-Host "[看门狗] 已启动，PID $($watchdogProc.Id)，日志 .tmp\clash-watchdog.log" -ForegroundColor Green
      Write-Host "[看门狗] 调参请直调 watchdog-clash.ps1 (-PollSeconds/-MaxCandidates/...)" -ForegroundColor Gray
      Write-Host "[看门狗] 先跑一次只读试运行确认链路：" -ForegroundColor Gray
      Write-Host "  pwsh -NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"watchdog-clash.ps1`" -DryRun" -ForegroundColor Gray
      Write-Host ""
    } catch {
      Write-Host "[看门狗] 启动失败，仅启动网关：$($_.Exception.Message)" -ForegroundColor Yellow
      Write-Host ""
      $watchdogProc = $null
    }
  }
} elseif (-not $watchdogRequested) {
  # 默认关闭：上面横幅已说明 "-Watchdog 启动"，正常情况这里不再重复提示。
  if ($args -contains "-NoWatchdog") {
    Write-Host "[看门狗] -NoWatchdog 已废弃：默认即不启动看门狗（如需启动请改用 -Watchdog）" -ForegroundColor Yellow
    Write-Host ""
  }
} elseif ($watchdogSkipReason -eq 'logdir') {
  # 已在上面打印过日志目录失败的原因，这里不再重复“未找到文件”的误导信息。
} elseif ($watchdogSkipReason -eq 'no-pwsh') {
  # 已在上面打印过找不到 pwsh 的原因。
} else {
  Write-Host "[看门狗] 未找到 watchdog-clash.ps1，仅启动网关" -ForegroundColor Yellow
  Write-Host ""
}

$code = 1
try {
  # 用绝对路径，空格目录安全；把输出直接透传到窗口
  # start.ps1 只消费 -Watchdog；网关的 -config/-listen/-web-listen 请改 config.json 或直调 exe。
  $extraArgs = @($args | Where-Object { $_ -ne "-Watchdog" -and $_ -ne "-NoWatchdog" })
  if (@($extraArgs).Count -gt 0) {
    Write-Host "[提示] start.ps1 不透传网关参数，已忽略：$($extraArgs -join ' ')" -ForegroundColor Yellow
  }
  & $exe -config $cfg
  $code = $LASTEXITCODE
} catch {
  $code = 1
  Write-Host "[错误] 网关启动失败：$($_.Exception.Message)" -ForegroundColor Red
} finally {
  if ($watchdogProc -ne $null -and -not $watchdogProc.HasExited) {
    try { Stop-Process -InputObject $watchdogProc -Force } catch { }
    Write-Host "[看门狗] 已随网关退出停止" -ForegroundColor Gray
  } elseif ($watchdogProc -ne $null -and $watchdogProc.HasExited -and $watchdogProc.ExitCode -ne 0) {
    Write-Host "[看门狗] 运行期间已提前退出(ExitCode $($watchdogProc.ExitCode))，请看 .tmp\clash-watchdog.log" -ForegroundColor Red
  }
}
Write-Host ""
Write-Host "[已退出] 返回码 $code" -ForegroundColor Yellow
if ([Environment]::UserInteractive) { pause }
exit $code
