param(
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
    [int]$BrowserTimeoutMinutes,

    [switch]$Headless,

    [switch]$RequireFreshConversation,

    [switch]$NotifyOnFailure,

    [string]$SlackWebhookEnvName
)

$ErrorActionPreference = "Stop"

if ($PSBoundParameters.ContainsKey("AdmissionGeneration") -and $AdmissionGeneration -le 0) {
    throw "AdmissionGeneration must be positive"
}

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

function Assert-FreshClaudeConversation {
    param(
        [Parameter(Mandatory = $true)][string]$ArchiveRepo,
        [Parameter(Mandatory = $true)][string]$ConversationDir,
        [Parameter(Mandatory = $true)][datetime]$SinceUtc
    )
    $conversationPath = Join-Path $ArchiveRepo $ConversationDir
    if (-not (Test-Path -LiteralPath $conversationPath -PathType Container)) {
        throw "conversation archive directory not found: $conversationPath"
    }
    $files = Get-ChildItem -LiteralPath $conversationPath -Filter "*.json" -File
    foreach ($file in $files) {
        $conversation = Get-Content -LiteralPath $file.FullName -Raw | ConvertFrom-Json -Depth 100
        $timestamps = @()
        foreach ($propertyName in @("created_at", "updated_at")) {
            if ($conversation.PSObject.Properties.Name -contains $propertyName) {
                $timestamps += [string]$conversation.$propertyName
            }
        }
        foreach ($message in @($conversation.messages)) {
            foreach ($propertyName in @("created_at", "updated_at")) {
                if ($message.PSObject.Properties.Name -contains $propertyName) {
                    $timestamps += [string]$message.$propertyName
                }
            }
        }
        foreach ($timestamp in $timestamps) {
            if ([string]::IsNullOrWhiteSpace($timestamp)) {
                continue
            }
            $parsed = [datetime]::Parse(
                $timestamp,
                [System.Globalization.CultureInfo]::InvariantCulture,
                [System.Globalization.DateTimeStyles]::AssumeUniversal -bor [System.Globalization.DateTimeStyles]::AdjustToUniversal
            )
            if ($parsed.ToUniversalTime() -ge $SinceUtc) {
                return [pscustomobject]@{
                    found = $true
                    file = $file.FullName
                    timestamp = $parsed.ToUniversalTime().ToString("o")
                }
            }
        }
    }
    throw "no Claude conversation in $conversationPath has a timestamp on or after $($SinceUtc.ToString("o"))"
}

function Invoke-FailureNotification {
    param(
        [Parameter(Mandatory = $true)][string]$ReportPath,
        [Parameter(Mandatory = $true)][string]$Message
    )
    if (-not $NotifyOnFailure) {
        return
    }
    if ([string]::IsNullOrWhiteSpace($SlackWebhookEnvName)) {
        throw "SlackWebhookEnvName is required when NotifyOnFailure is set"
    }
    & (Join-Path $PSScriptRoot "notify_personal_lake_job_failure.ps1") `
        -JobName "claude-personal-lake-daily-export" `
        -ExitCode 1 `
        -ReportPath $ReportPath `
        -Message $Message `
        -WebhookEnvName $SlackWebhookEnvName | Out-Null
}

if ($BrowserTimeoutMinutes -le 0) {
    throw "BrowserTimeoutMinutes must be positive"
}
if ([string]::IsNullOrWhiteSpace($SlackWebhookEnvName) -and $NotifyOnFailure) {
    throw "SlackWebhookEnvName is required when NotifyOnFailure is set"
}

Require-Command "node"
Require-Command "git"
Require-Command "cargo"
Require-Command "python"

Import-EnvFile -Path $EnvFile
Require-Env $ApiTokenEnv
if ($NotifyOnFailure) {
    Require-Env $SlackWebhookEnvName
}

if (-not (Test-Path -LiteralPath $PlaywrightNodeModulesPath -PathType Container)) {
    throw "PlaywrightNodeModulesPath not found: $PlaywrightNodeModulesPath"
}

$startedAt = (Get-Date).ToUniversalTime()
$stamp = $startedAt.ToString("yyyyMMdd-HHmmss")
$reportPath = Join-Path $ReportDir "claude-daily-export-$stamp.json"
$failureReportPath = Join-Path $ReportDir "claude-daily-export-$stamp.failed.json"
$browserScript = Join-Path $PSScriptRoot "claude_export_browser.mjs"
$browserTimeoutMs = $BrowserTimeoutMinutes * 60 * 1000
$headlessValue = if ($Headless) { "true" } else { "false" }

try {
    New-Item -ItemType Directory -Force -Path $DownloadDir | Out-Null
    New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null

    $previousNodePath = [Environment]::GetEnvironmentVariable("NODE_PATH", "Process")
    [Environment]::SetEnvironmentVariable("NODE_PATH", $PlaywrightNodeModulesPath, "Process")
    try {
        $browserOutput = node $browserScript `
            --mode request-and-download `
            --profile-dir $BrowserProfileDir `
            --download-dir $DownloadDir `
            --timeout-ms $browserTimeoutMs `
            --export-period $ExportPeriod `
            --headless $headlessValue | Out-String
        if ($LASTEXITCODE -ne 0) {
            throw "claude_export_browser.mjs failed"
        }
    }
    finally {
        [Environment]::SetEnvironmentVariable("NODE_PATH", $previousNodePath, "Process")
    }

    $browserReport = $browserOutput | ConvertFrom-Json -Depth 20
    if ($browserReport.status -ne "ok") {
        throw "Claude browser export did not return status=ok"
    }
    if ([string]::IsNullOrWhiteSpace($browserReport.zip_path)) {
        throw "Claude browser export did not produce zip_path"
    }
    if (-not (Test-Path -LiteralPath $browserReport.zip_path -PathType Leaf)) {
        throw "Claude export zip does not exist: $($browserReport.zip_path)"
    }

    $commitMessage = "Archive claude.ai export $($startedAt.ToString("yyyy-MM-dd"))"
    $importParameters = @{
        ZipPath = $browserReport.zip_path
        ArchiveRepo = $ArchiveRepo
        ConversationDir = $ConversationDir
        CommitMessage = $commitMessage
        DatabasePath = $DatabasePath
        BaseUrl = $BaseUrl
        ApiTokenEnv = $ApiTokenEnv
        SourceInstance = $SourceInstance
    }
    if ($PSBoundParameters.ContainsKey("ApiVersion")) {
        $importParameters.ApiVersion = $ApiVersion
    }
    if ($PSBoundParameters.ContainsKey("AdmissionGeneration")) {
        $importParameters.AdmissionGeneration = $AdmissionGeneration
    }
    $importOutput = & (Join-Path $PSScriptRoot "run_claude_personal_lake_import.ps1") @importParameters | Out-String

    $freshConversation = $null
    if ($RequireFreshConversation) {
        $freshConversation = Assert-FreshClaudeConversation `
            -ArchiveRepo $ArchiveRepo `
            -ConversationDir $ConversationDir `
            -SinceUtc $startedAt.AddHours(-24)
    }

    $report = [pscustomobject]@{
        status = "ok"
        job = "claude-personal-lake-daily-export"
        started_at = $startedAt.ToString("o")
        finished_at = (Get-Date).ToUniversalTime().ToString("o")
        export_period = $ExportPeriod
        archive_repo = (Resolve-Path -LiteralPath $ArchiveRepo).Path
        conversation_dir = $ConversationDir
        source_instance = $SourceInstance
        zip_path = (Resolve-Path -LiteralPath $browserReport.zip_path).Path
        browser_report = $browserReport
        import_report = ($importOutput | ConvertFrom-Json -Depth 20)
        fresh_conversation = $freshConversation
    }
    Write-JsonFile -Value $report -Path $reportPath
    $report | ConvertTo-Json -Depth 20
}
catch {
    $message = $_.Exception.Message
    $failureReport = [pscustomobject]@{
        status = "failed"
        job = "claude-personal-lake-daily-export"
        started_at = $startedAt.ToString("o")
        finished_at = (Get-Date).ToUniversalTime().ToString("o")
        export_period = $ExportPeriod
        error = $message
    }
    Write-JsonFile -Value $failureReport -Path $failureReportPath
    Invoke-FailureNotification -ReportPath $failureReportPath -Message $message
    throw $message
}
