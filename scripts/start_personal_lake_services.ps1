param(
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot,

    [Parameter(Mandatory = $true)]
    [string]$EnvFile,

    [Parameter(Mandatory = $true)]
    [string]$DockerExe,

    [Parameter(Mandatory = $true)]
    [string]$DockerDesktopExe,

    [Parameter(Mandatory = $true)]
    [string]$TailscaleExe,

    [Parameter(Mandatory = $true)]
    [int]$DockerRetryAttempts,

    [Parameter(Mandatory = $true)]
    [int]$DockerRetryDelaySeconds
)

$ErrorActionPreference = "Stop"

function Require-NonBlank {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [Parameter(Mandatory = $true)]
        [string]$Value
    )
    if ([string]::IsNullOrWhiteSpace($Value)) {
        throw "$Name must not be blank"
    }
}

function Read-RequiredEnvValue {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $values = @(Get-Content -LiteralPath $Path | ForEach-Object {
        if ($_ -match "^\s*$([regex]::Escape($Name))\s*=\s*(.+?)\s*$") {
            $Matches[1].Trim().Trim('"').Trim("'")
        }
    })
    if ($values.Count -ne 1) {
        throw "$Name must be defined exactly once in $Path"
    }
    if ([string]::IsNullOrWhiteSpace($values[0])) {
        throw "$Name must not be blank in $Path"
    }
    return $values[0]
}

Require-NonBlank -Name "RepoRoot" -Value $RepoRoot
Require-NonBlank -Name "EnvFile" -Value $EnvFile
Require-NonBlank -Name "DockerExe" -Value $DockerExe
Require-NonBlank -Name "DockerDesktopExe" -Value $DockerDesktopExe
Require-NonBlank -Name "TailscaleExe" -Value $TailscaleExe

if ($DockerRetryAttempts -lt 1) {
    throw "DockerRetryAttempts must be at least 1"
}
if ($DockerRetryDelaySeconds -lt 1) {
    throw "DockerRetryDelaySeconds must be at least 1"
}

$repo = (Resolve-Path -LiteralPath $RepoRoot).Path
$envPath = (Resolve-Path -LiteralPath $EnvFile).Path
$dockerPath = (Resolve-Path -LiteralPath $DockerExe).Path
$dockerDesktopPath = (Resolve-Path -LiteralPath $DockerDesktopExe).Path
$tailscalePath = (Resolve-Path -LiteralPath $TailscaleExe).Path
$composeFile = Join-Path $repo "deploy/personal-lake/compose.yaml"

if (-not (Test-Path -LiteralPath $composeFile)) {
    throw "compose file does not exist: $composeFile"
}

$mcpHostPortText = Read-RequiredEnvValue -Path $envPath -Name "LETHE_MCP_HOST_PORT"
try {
    $mcpHostPort = [int]::Parse($mcpHostPortText, [System.Globalization.CultureInfo]::InvariantCulture)
} catch {
    throw "LETHE_MCP_HOST_PORT must be an integer in $envPath"
}
if ($mcpHostPort -lt 1 -or $mcpHostPort -gt 65535) {
    throw "LETHE_MCP_HOST_PORT must be a valid TCP port"
}

$dockerReady = $false
for ($attempt = 1; $attempt -le $DockerRetryAttempts; $attempt++) {
    & $dockerPath version --format "{{.Server.Version}}" *> $null
    if ($LASTEXITCODE -eq 0) {
        $dockerReady = $true
        break
    }
    if ($attempt -eq 1) {
        Start-Process -FilePath $dockerDesktopPath -WindowStyle Hidden | Out-Null
    }
    Start-Sleep -Seconds $DockerRetryDelaySeconds
}
if (-not $dockerReady) {
    throw "Docker daemon did not become ready after $DockerRetryAttempts attempts"
}

Set-Location -LiteralPath $repo
& $dockerPath compose --env-file $envPath -f $composeFile up --build -d
if ($LASTEXITCODE -ne 0) {
    throw "docker compose up failed with exit code $LASTEXITCODE"
}

& $tailscalePath funnel --bg --yes $mcpHostPort
if ($LASTEXITCODE -ne 0) {
    throw "tailscale funnel failed with exit code $LASTEXITCODE"
}
