param(
    [Parameter(Mandatory = $true)]
    [string]$EnvFile,

    [Parameter(Mandatory = $true)]
    [string]$ArchiveRoot,

    [Parameter(Mandatory = $true)]
    [ValidateSet("claude-code", "codex")]
    [string]$Tool,

    [Parameter(Mandatory = $true)]
    [string]$SourceInstance,

    [Parameter(Mandatory = $true)]
    [string]$BaseUrl,

    [Parameter(Mandatory = $true)]
    [string]$ApiTokenEnv,

    [ValidateSet("1", "2")]
    [string]$ApiVersion,

    [int]$AdmissionGeneration,

    [Parameter(Mandatory = $true)]
    [string]$ReportDir
)

$ErrorActionPreference = "Stop"

function Require-Command {
    param([Parameter(Mandatory = $true)][string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "$Name command is not available"
    }
}

function Require-Env {
    param([Parameter(Mandatory = $true)][string]$Name)
    $value = [Environment]::GetEnvironmentVariable($Name)
    if ([string]::IsNullOrWhiteSpace($value)) {
        throw "missing required environment variable $Name"
    }
}

function Import-EnvFile {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "EnvFile not found: $Path"
    }
    $lineNumber = 0
    foreach ($line in Get-Content -LiteralPath $Path) {
        $lineNumber += 1
        $trimmed = $line.Trim()
        if ($trimmed -eq "" -or $trimmed.StartsWith("#")) {
            continue
        }
        $match = [regex]::Match($trimmed, "^([A-Za-z_][A-Za-z0-9_]*)=(.*)$")
        if (-not $match.Success) {
            throw "invalid env file line $lineNumber in $Path"
        }
        $name = $match.Groups[1].Value
        $value = $match.Groups[2].Value
        if ($value.Length -ge 2 -and (
            ($value.StartsWith('"') -and $value.EndsWith('"')) -or
            ($value.StartsWith("'") -and $value.EndsWith("'"))
        )) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        [Environment]::SetEnvironmentVariable($name, $value, "Process")
    }
}

function Write-JsonFile {
    param(
        [Parameter(Mandatory = $true)]$Value,
        [Parameter(Mandatory = $true)][string]$Path
    )
    $directory = Split-Path -Parent $Path
    New-Item -ItemType Directory -Force -Path $directory | Out-Null
    $Value | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath $Path -Encoding utf8NoBOM
}

function Parse-ImportReport {
    param(
        [Parameter(Mandatory = $true)][string]$ToolName,
        [Parameter(Mandatory = $true)][string]$Output
    )
    $match = [regex]::Match(
        $Output,
        "$ToolName import complete: ingested=(\d+), duplicates=(\d+), quarantined=(\d+)"
    )
    if (-not $match.Success) {
        throw "$ToolName import output did not contain a completion summary: $Output"
    }
    [pscustomobject]@{
        ingested = [int]$match.Groups[1].Value
        duplicates = [int]$match.Groups[2].Value
        quarantined = [int]$match.Groups[3].Value
    }
}

Require-Command "cargo"

Import-EnvFile -Path $EnvFile
Require-Env $ApiTokenEnv

if (-not (Test-Path -LiteralPath $ArchiveRoot -PathType Container)) {
    throw "ArchiveRoot not found: $ArchiveRoot"
}
if ([string]::IsNullOrWhiteSpace($SourceInstance)) {
    throw "SourceInstance must not be blank"
}
if ([string]::IsNullOrWhiteSpace($BaseUrl)) {
    throw "BaseUrl must not be blank"
}
if ($PSBoundParameters.ContainsKey("AdmissionGeneration") -and $AdmissionGeneration -le 0) {
    throw "AdmissionGeneration must be positive"
}

$packageName = "lethe-import-$Tool"
if ($Tool -eq "claude-code") {
    $archiveArg = "--archive-root=$ArchiveRoot"
} else {
    $archiveArg = "--archive=$ArchiveRoot"
}

$startedAt = (Get-Date).ToUniversalTime()
$stamp = $startedAt.ToString("yyyyMMdd-HHmmss")
$reportPath = Join-Path $ReportDir "agent-source-import-$Tool-$stamp.json"
$failureReportPath = Join-Path $ReportDir "agent-source-import-$Tool-$stamp.failed.json"

$importArguments = @(
    $archiveArg,
    "--source-instance=$SourceInstance",
    "--base-url=$BaseUrl",
    "--api-token-env=$ApiTokenEnv"
)
if ($PSBoundParameters.ContainsKey("ApiVersion")) {
    $importArguments += "--api-version=$ApiVersion"
}
if ($PSBoundParameters.ContainsKey("AdmissionGeneration")) {
    $importArguments += "--admission-generation=$AdmissionGeneration"
}

try {
    $output = cargo run -q -p $packageName -- @importArguments | Out-String
    if ($LASTEXITCODE -ne 0) {
        throw "$packageName failed: $output"
    }
    $report = Parse-ImportReport -ToolName $Tool -Output $output
    if ($report.quarantined -ne 0) {
        throw "$Tool import quarantined $($report.quarantined) observations"
    }

    $result = [pscustomobject]@{
        status = "ok"
        job = "agent-source-lake-import-$Tool"
        started_at = $startedAt.ToString("o")
        finished_at = (Get-Date).ToUniversalTime().ToString("o")
        archive_root = (Resolve-Path -LiteralPath $ArchiveRoot).Path
        source_instance = $SourceInstance
        base_url = $BaseUrl
        import_report = $report
    }
    Write-JsonFile -Value $result -Path $reportPath
    $result | ConvertTo-Json -Depth 10
}
catch {
    $message = $_.Exception.Message
    $failureReport = [pscustomobject]@{
        status = "failed"
        job = "agent-source-lake-import-$Tool"
        started_at = $startedAt.ToString("o")
        finished_at = (Get-Date).ToUniversalTime().ToString("o")
        error = $message
    }
    Write-JsonFile -Value $failureReport -Path $failureReportPath
    throw $message
}
