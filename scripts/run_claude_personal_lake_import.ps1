param(
    [Parameter(Mandatory = $true)]
    [string]$ZipPath,

    [Parameter(Mandatory = $true)]
    [string]$ArchiveRepo,

    [Parameter(Mandatory = $true)]
    [string]$ConversationDir,

    [Parameter(Mandatory = $true)]
    [string]$CommitMessage,

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

    [int]$AdmissionGeneration
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

function Parse-ImportReport {
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$Output
    )
    $match = [regex]::Match(
        $Output,
        "claude import complete: ingested=(\d+), duplicates=(\d+), quarantined=(\d+)"
    )
    if (-not $match.Success) {
        throw "$Label output did not contain a claude import report: $Output"
    }
    [pscustomobject]@{
        ingested = [int]$match.Groups[1].Value
        duplicates = [int]$match.Groups[2].Value
        quarantined = [int]$match.Groups[3].Value
    }
}

Require-Command "cargo"
Require-Command "git"
Require-Command "python"
Require-Env $ApiTokenEnv

if (-not (Test-Path -LiteralPath $ZipPath -PathType Leaf)) {
    throw "Claude export zip not found: $ZipPath"
}
if ([string]::IsNullOrWhiteSpace($SourceInstance)) {
    throw "SourceInstance must not be blank"
}
if ([string]::IsNullOrWhiteSpace($BaseUrl)) {
    throw "BaseUrl must not be blank"
}
if ([string]::IsNullOrWhiteSpace($ApiTokenEnv)) {
    throw "ApiTokenEnv must not be blank"
}
if ($PSBoundParameters.ContainsKey("AdmissionGeneration") -and $AdmissionGeneration -le 0) {
    throw "AdmissionGeneration must be positive"
}

& (Join-Path $PSScriptRoot "archive_claude_export.ps1") `
    -ZipPath $ZipPath `
    -ArchiveRepo $ArchiveRepo `
    -ConversationDir $ConversationDir `
    -CommitMessage $CommitMessage

$importArguments = @(
    "--zip=$ZipPath",
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

$firstOutput = cargo run -q -p lethe-import-claude -- @importArguments | Out-String
if ($LASTEXITCODE -ne 0) {
    throw "first lethe-import-claude failed"
}
$firstReport = Parse-ImportReport "first import" $firstOutput
if ($firstReport.quarantined -ne 0) {
    throw "first import quarantined $($firstReport.quarantined) observations"
}

$secondOutput = cargo run -q -p lethe-import-claude -- @importArguments | Out-String
if ($LASTEXITCODE -ne 0) {
    throw "second lethe-import-claude failed"
}
$secondReport = Parse-ImportReport "second import" $secondOutput
if ($secondReport.ingested -ne 0) {
    throw "second import ingested $($secondReport.ingested) observations; expected complete no-op"
}
if ($secondReport.quarantined -ne 0) {
    throw "second import quarantined $($secondReport.quarantined) observations"
}

$conversationPath = Join-Path $ArchiveRepo $ConversationDir
python (Join-Path $PSScriptRoot "personal_lake_sanity.py") `
    --db $DatabasePath `
    --claude-conversations-dir $conversationPath `
    --claude-source-instance $SourceInstance
if ($LASTEXITCODE -ne 0) {
    throw "personal_lake_sanity.py failed for claude source"
}

[pscustomobject]@{
    archive_repo = (Resolve-Path -LiteralPath $ArchiveRepo).Path
    conversation_dir = (Resolve-Path -LiteralPath $conversationPath).Path
    source_instance = $SourceInstance
    first_import = $firstReport
    second_import = $secondReport
} | ConvertTo-Json -Depth 10
