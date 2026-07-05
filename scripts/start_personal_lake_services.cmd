@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%..") do set "REPO_ROOT=%%~fI"

"C:\Program Files\PowerShell\7\pwsh.exe" ^
  -NoProfile ^
  -ExecutionPolicy Bypass ^
  -File "%SCRIPT_DIR%start_personal_lake_services.ps1" ^
  -RepoRoot "%REPO_ROOT%" ^
  -EnvFile "%REPO_ROOT%\deploy\personal-lake\.env" ^
  -DockerExe "C:\Program Files\Docker\Docker\resources\bin\docker.exe" ^
  -DockerDesktopExe "C:\Program Files\Docker\Docker\Docker Desktop.exe" ^
  -TailscaleExe "C:\Program Files\Tailscale\tailscale.exe" ^
  -DockerRetryAttempts 24 ^
  -DockerRetryDelaySeconds 10
