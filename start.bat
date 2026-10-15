@echo off
title opencode2api
cd /d "%~dp0"

set "EXE=%~dp0opencode2api.exe"
set "CFG=%~dp0config.json"

if not exist "%EXE%" (
  echo [ERROR] not found: %EXE%
  echo Run: go build -o opencode2api.exe ./cmd/opencode2api
  pause
  exit /b 1
)

if not exist "%CFG%" (
  echo [ERROR] not found: %CFG%
  echo Copy: copy config.example.json config.json
  pause
  exit /b 1
)

echo ==========================================
echo  opencode2api starting...
echo  Dir: %~dp0
echo  Config: %CFG%
echo  API: http://127.0.0.1:19880/healthz
echo  WebUI: http://127.0.0.1:19881
echo  Keep window open, Ctrl+C to stop
echo  NOTE: watchdog auto-failover (watchdog-clash.ps1) only starts via start.ps1
echo ==========================================
echo.

:: quoted for space in "Program Files"
"%EXE%" -config "%CFG%"

echo.
echo [exited] code %ERRORLEVEL%
pause
