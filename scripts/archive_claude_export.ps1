param(
    [Parameter(Mandatory = $true)]
    [string]$ZipPath,

    [Parameter(Mandatory = $true)]
    [string]$ArchiveRepo,

    [string]$ConversationDir = "conversations",

    [string]$CommitMessage = "Archive claude.ai export"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    throw "git command is not available"
}

if (-not (Test-Path -LiteralPath $ZipPath -PathType Leaf)) {
    throw "Claude export zip not found: $ZipPath"
}

if (Test-Path -LiteralPath $ArchiveRepo -PathType Leaf) {
    throw "ArchiveRepo is a file: $ArchiveRepo"
}

New-Item -ItemType Directory -Force -Path $ArchiveRepo | Out-Null

$gitDir = Join-Path $ArchiveRepo ".git"
if (-not (Test-Path -LiteralPath $gitDir -PathType Container)) {
    git -C $ArchiveRepo init | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "git init failed for $ArchiveRepo"
    }
}

$targetDir = Join-Path $ArchiveRepo $ConversationDir
& (Join-Path $PSScriptRoot "expand_claude_export.ps1") -ZipPath $ZipPath -OutputDir $targetDir

$status = git -C $ArchiveRepo status --porcelain | Out-String
if ([string]::IsNullOrWhiteSpace($status)) {
    Write-Output "archive_no_changes=true"
    exit 0
}

git -C $ArchiveRepo add -- $ConversationDir
if ($LASTEXITCODE -ne 0) {
    throw "git add failed for $ArchiveRepo"
}

git -C $ArchiveRepo commit -m $CommitMessage
if ($LASTEXITCODE -ne 0) {
    throw "git commit failed for $ArchiveRepo"
}

Write-Output "archive_commit_created=true"
