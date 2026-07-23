param(
    [Parameter(Mandatory = $true)]
    [string]$TaskName,

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
    [string]$ReportDir,

    [Parameter(Mandatory = $true)]
    [string]$DailyAt
)

$ErrorActionPreference = "Stop"

function Require-Path {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][ValidateSet("Leaf", "Container")][string]$PathType
    )
    if (-not (Test-Path -LiteralPath $Path -PathType $PathType)) {
        throw "$PathType path not found: $Path"
    }
}

function Quote-Argument {
    param([Parameter(Mandatory = $true)][string]$Value)
    return '"' + ($Value -replace '"', '\"') + '"'
}

if ($PSBoundParameters.ContainsKey("AdmissionGeneration") -and $AdmissionGeneration -le 0) {
    throw "AdmissionGeneration must be positive"
}

Require-Path -Path $EnvFile -PathType Leaf
Require-Path -Path $ArchiveRoot -PathType Container

New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null

$runner = Join-Path $PSScriptRoot "run_agent_source_lake_import.ps1"
Require-Path -Path $runner -PathType Leaf

$arguments = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", (Quote-Argument $runner),
    "-EnvFile", (Quote-Argument (Resolve-Path -LiteralPath $EnvFile).Path),
    "-ArchiveRoot", (Quote-Argument (Resolve-Path -LiteralPath $ArchiveRoot).Path),
    "-Tool", (Quote-Argument $Tool),
    "-SourceInstance", (Quote-Argument $SourceInstance),
    "-BaseUrl", (Quote-Argument $BaseUrl),
    "-ApiTokenEnv", (Quote-Argument $ApiTokenEnv),
    "-ReportDir", (Quote-Argument (Resolve-Path -LiteralPath $ReportDir).Path)
)
if ($PSBoundParameters.ContainsKey("ApiVersion")) {
    $arguments += "-ApiVersion"
    $arguments += (Quote-Argument $ApiVersion)
}
if ($PSBoundParameters.ContainsKey("AdmissionGeneration")) {
    $arguments += "-AdmissionGeneration"
    $arguments += $AdmissionGeneration
}

$action = New-ScheduledTaskAction `
    -Execute "pwsh.exe" `
    -Argument ($arguments -join " ") `
    -WorkingDirectory (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$trigger = New-ScheduledTaskTrigger -Daily -At ([datetime]::ParseExact(
    $DailyAt,
    "HH:mm",
    [System.Globalization.CultureInfo]::InvariantCulture
))
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description "Daily import of $Tool archive JSONL into the LETHE personal lake via the online API." `
    -Force | Out-Null

[pscustomobject]@{
    registered = $true
    task_name = $TaskName
    tool = $Tool
    daily_at = $DailyAt
    runner = (Resolve-Path -LiteralPath $runner).Path
} | ConvertTo-Json -Depth 5
