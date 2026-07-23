param(
    [Parameter(Mandatory = $true)]
    [string]$TaskName,

    [Parameter(Mandatory = $true)]
    [string]$EnvFile,

    [Parameter(Mandatory = $true)]
    [string]$ArchiveRepo,

    [Parameter(Mandatory = $true)]
    [string]$ConversationDir,

    [Parameter(Mandatory = $true)]
    [string]$DatabasePath,

    [Parameter(Mandatory = $true)]
    [string]$BaseUrl,

    [Parameter(Mandatory = $true)]
    [string]$ApiTokenEnv,

    [Parameter(Mandatory = $true)]
    [string]$SourceInstance,

    [ValidateSet("1", "2")]
    [string]$ApiVersion,

    [int]$AdmissionGeneration,

    [Parameter(Mandatory = $true)]
    [string]$BrowserProfileDir,

    [Parameter(Mandatory = $true)]
    [string]$DownloadDir,

    [Parameter(Mandatory = $true)]
    [string]$ReportDir,

    [Parameter(Mandatory = $true)]
    [string]$PlaywrightNodeModulesPath,

    [Parameter(Mandatory = $true)]
    [ValidateSet("All", "30 days", "90 days")]
    [string]$ExportPeriod,

    [Parameter(Mandatory = $true)]
    [string]$DailyAt,

    [Parameter(Mandatory = $true)]
    [int]$BrowserTimeoutMinutes,

    [switch]$Headless,

    [switch]$NotifyOnFailure,

    [string]$SlackWebhookEnvName
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

if ($BrowserTimeoutMinutes -le 0) {
    throw "BrowserTimeoutMinutes must be positive"
}
if ($PSBoundParameters.ContainsKey("AdmissionGeneration") -and $AdmissionGeneration -le 0) {
    throw "AdmissionGeneration must be positive"
}
if ([string]::IsNullOrWhiteSpace($SlackWebhookEnvName) -and $NotifyOnFailure) {
    throw "SlackWebhookEnvName is required when NotifyOnFailure is set"
}

Require-Path -Path $EnvFile -PathType Leaf
Require-Path -Path $ArchiveRepo -PathType Container
Require-Path -Path (Split-Path -Parent $DatabasePath) -PathType Container
Require-Path -Path $PlaywrightNodeModulesPath -PathType Container

New-Item -ItemType Directory -Force -Path $BrowserProfileDir | Out-Null
New-Item -ItemType Directory -Force -Path $DownloadDir | Out-Null
New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null

$runner = Join-Path $PSScriptRoot "run_claude_personal_lake_daily_export.ps1"
Require-Path -Path $runner -PathType Leaf

$arguments = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", (Quote-Argument $runner),
    "-EnvFile", (Quote-Argument (Resolve-Path -LiteralPath $EnvFile).Path),
    "-ArchiveRepo", (Quote-Argument (Resolve-Path -LiteralPath $ArchiveRepo).Path),
    "-ConversationDir", (Quote-Argument $ConversationDir),
    "-DatabasePath", (Quote-Argument (Resolve-Path -LiteralPath $DatabasePath).Path),
    "-BaseUrl", (Quote-Argument $BaseUrl),
    "-ApiTokenEnv", (Quote-Argument $ApiTokenEnv),
    "-SourceInstance", (Quote-Argument $SourceInstance),
    "-BrowserProfileDir", (Quote-Argument (Resolve-Path -LiteralPath $BrowserProfileDir).Path),
    "-DownloadDir", (Quote-Argument (Resolve-Path -LiteralPath $DownloadDir).Path),
    "-ReportDir", (Quote-Argument (Resolve-Path -LiteralPath $ReportDir).Path),
    "-PlaywrightNodeModulesPath", (Quote-Argument (Resolve-Path -LiteralPath $PlaywrightNodeModulesPath).Path),
    "-ExportPeriod", (Quote-Argument $ExportPeriod),
    "-BrowserTimeoutMinutes", $BrowserTimeoutMinutes
)
if ($PSBoundParameters.ContainsKey("ApiVersion")) {
    $arguments += "-ApiVersion"
    $arguments += (Quote-Argument $ApiVersion)
}
if ($PSBoundParameters.ContainsKey("AdmissionGeneration")) {
    $arguments += "-AdmissionGeneration"
    $arguments += $AdmissionGeneration
}
if ($Headless) {
    $arguments += "-Headless"
}
if ($NotifyOnFailure) {
    $arguments += "-NotifyOnFailure"
    $arguments += "-SlackWebhookEnvName"
    $arguments += (Quote-Argument $SlackWebhookEnvName)
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
    -ExecutionTimeLimit (New-TimeSpan -Hours 4)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description "Daily Claude.ai export request, download, archive, and LETHE import." `
    -Force | Out-Null

[pscustomobject]@{
    registered = $true
    task_name = $TaskName
    daily_at = $DailyAt
    runner = (Resolve-Path -LiteralPath $runner).Path
} | ConvertTo-Json -Depth 5
