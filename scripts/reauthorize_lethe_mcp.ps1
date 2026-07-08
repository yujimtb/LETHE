param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("IdlePrecheck", "AbsoluteRenewal")]
    [string]$Mode
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ([string]::IsNullOrWhiteSpace($PSScriptRoot)) {
    throw "PSScriptRoot is required."
}

if ([string]::IsNullOrWhiteSpace($env:TEMP)) {
    throw "TEMP is required."
}

$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$notePath = Join-Path $env:TEMP "lethe-mcp-reauth-$Mode-$timestamp.txt"

switch ($Mode) {
    "IdlePrecheck" {
        $reason = "ChatGPT/Codex refresh-token idle expiry is expected around 2026-07-23 JST if unused."
        $deadline = "2026-07-23 JST"
    }
    "AbsoluteRenewal" {
        $reason = "ChatGPT/Codex refresh-token absolute expiry is expected around 2026-08-07 JST."
        $deadline = "2026-08-07 JST"
    }
}

$note = @"
LETHE MCP reauthentication

Reason:
$reason

Deadline:
$deadline

Use the existing Auth0 tenant:
https://lethe-mcp.jp.auth0.com/

Required consent scopes:
- mcp:read
- write:supplemental
- offline_access

ChatGPT.com:
1. Open ChatGPT settings.
2. Open Apps / Connectors or Developer mode apps.
3. Select LETHE Personal Lake.
4. Run Refresh on the draft app details.
5. Run Reconnect.
6. On Auth0 consent, verify mcp:read, write:supplemental, and offline_access.

Codex:
Run:
codex mcp login lethe-personal-lake

Claude Code:
Run:
claude mcp login "claude.ai LETHE Personal Lake"

After reauthentication, verify:
codex mcp list
claude mcp list

Repo:
$repoRoot
"@

Set-Content -LiteralPath $notePath -Value $note -Encoding UTF8

Start-Process -FilePath "notepad.exe" -ArgumentList @($notePath)
Start-Process -FilePath "https://chatgpt.com/"
Start-Process -FilePath "https://claude.ai/settings/connectors"

$terminalLines = @(
    '$ErrorActionPreference = "Stop"',
    'Write-Host "LETHE MCP reauthentication"',
    'Write-Host "Use Auth0 tenant: https://lethe-mcp.jp.auth0.com/"',
    'Write-Host "Required consent scopes: mcp:read, write:supplemental, offline_access"',
    'Write-Host ""',
    'Write-Host "Starting Codex MCP login..."',
    'codex mcp login lethe-personal-lake',
    'Write-Host ""',
    'Write-Host "Starting Claude Code MCP login..."',
    'claude mcp login "claude.ai LETHE Personal Lake"',
    'Write-Host ""',
    'Write-Host "Verification commands:"',
    'Write-Host "codex mcp list"',
    'Write-Host "claude mcp list"'
)
$terminalCommand = $terminalLines -join "; "

Start-Process -FilePath "powershell.exe" -ArgumentList @(
    "-NoExit",
    "-ExecutionPolicy",
    "Bypass",
    "-Command",
    $terminalCommand
)
