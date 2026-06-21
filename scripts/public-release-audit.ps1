param(
    [switch]$CheckHistory
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-RepoRoot {
    $root = git rev-parse --show-toplevel 2>$null
    if (-not $root) {
        throw 'This script must run inside a git repository.'
    }

    return ($root | Select-Object -First 1).Trim()
}

function Get-NormalizedPath([string]$path) {
    return $path.Replace('\\', '/').Trim()
}

function Test-ForbiddenTrackedPath([string]$path) {
    $normalized = Get-NormalizedPath $path

    if ($normalized -eq '.env') {
        return $true
    }

    if ($normalized -eq 'client_secret.json') {
        return $true
    }

    if ($normalized.StartsWith('target/')) {
        return $true
    }

    if ($normalized.StartsWith('data/')) {
        return $normalized -notin @('data/README.md', 'data/.gitkeep')
    }

    return $false
}

function Add-Violation {
    param(
        [System.Collections.Generic.List[string]]$Bucket,
        [string]$Message
    )

    $Bucket.Add($Message)
}

function Assert-NoForbiddenTrackedPaths {
    param([System.Collections.Generic.List[string]]$Violations)

    $trackedFiles = @(git ls-files)
    foreach ($file in $trackedFiles) {
        if (Test-ForbiddenTrackedPath $file) {
            Add-Violation $Violations "Tracked forbidden path: $file"
        }
    }
}

function Assert-HistoryClean {
    param([System.Collections.Generic.List[string]]$Violations)

    $historyNames = @(git log --all --name-only --pretty=format: -- . client_secret.json data target)
    foreach ($entry in $historyNames) {
        $normalized = Get-NormalizedPath $entry
        if (-not $normalized) {
            continue
        }

        if (Test-ForbiddenTrackedPath $normalized) {
            Add-Violation $Violations "Forbidden path appears in git history: $normalized"
        }
    }
}

function Assert-EnvExampleSafe {
    param(
        [string]$RepoRoot,
        [System.Collections.Generic.List[string]]$Violations
    )

    $envExamplePath = Join-Path $RepoRoot '.env.example'
    if (-not (Test-Path $envExamplePath)) {
        Add-Violation $Violations 'Missing .env.example'
        return
    }

    $expectedValues = @{
        'LETHE_BIND_ADDR' = '127.0.0.1:8080'
        'LETHE_DATABASE_PATH' = './data/lethe.sqlite3'
        'LETHE_BLOB_DIR' = './data/blobs'
        'LETHE_POLL_SECONDS' = '300'
        'LETHE_API_TOKENS' = 'read-token:read:persons+read:timeline,sync-token:admin:sync'
        'LETHE_MAX_BLOB_BYTES' = '10485760'
        'LETHE_MAX_PAYLOAD_BYTES' = '1048576'
        'LETHE_MAX_SYNC_ITEMS' = '10000'
        'LETHE_MAX_PAGE_SIZE' = '100'
        'LETHE_SLACK_BOT_TOKEN' = 'xoxb-your-slack-bot-token'
        'LETHE_SLACK_THREAD_TOKEN' = 'xoxp-your-thread-read-token'
        'LETHE_SLACK_CHANNEL_IDS' = 'C01234567,C08999999'
        'LETHE_GOOGLE_ACCESS_TOKEN' = ''
        'LETHE_GOOGLE_CLIENT_ID' = ''
        'LETHE_GOOGLE_CLIENT_SECRET' = ''
        'LETHE_GOOGLE_REFRESH_TOKEN' = ''
        'LETHE_GEMINI_API_KEY' = 'your-gemini-api-key'
        'LETHE_GEMINI_MODEL' = 'gemini-3-flash-preview'
        'LETHE_GOOGLE_SLIDE_ANALYSIS_LIMIT' = '10'
        'LETHE_GOOGLE_PRESENTATION_IDS' = 'your-presentation-id'
    }

    $content = Get-Content -Path $envExamplePath
    $actualValues = @{}
    foreach ($line in $content) {
        if ($line -notmatch '^[A-Z0-9_]+=' ) {
            continue
        }

        $parts = $line -split '=', 2
        $actualValues[$parts[0]] = $parts[1]
    }

    foreach ($key in $expectedValues.Keys) {
        if (-not $actualValues.ContainsKey($key)) {
            Add-Violation $Violations ".env.example is missing expected key: $key"
            continue
        }

        if ($actualValues[$key] -ne $expectedValues[$key]) {
            Add-Violation $Violations ".env.example contains unexpected value for $key"
        }
    }
}

function Assert-NoSecretPatterns {
    param([System.Collections.Generic.List[string]]$Violations)

    $patternMap = @{
        'Slack token' = 'xox[baprs]-[0-9A-Za-z-]{10,}'
        'Google access token' = 'ya29\.[0-9A-Za-z._-]+'
        'Google OAuth client secret' = 'GOCSPX-[0-9A-Za-z_-]{10,}'
        'Google refresh token' = '1//[0-9A-Za-z._-]+'
        'Google API key' = 'AIza[0-9A-Za-z_-]{20,}'
        'Gemini API key' = 'AQ\.[A-Za-z0-9._-]{20,}'
        'Private key block' = '-----BEGIN (RSA|EC|OPENSSH|DSA|PGP) PRIVATE KEY-----'
    }

    foreach ($name in $patternMap.Keys) {
        $pattern = $patternMap[$name]
        $matches = @(git grep -n -I -E -e $pattern -- . ':(exclude).env.example' ':(exclude)README.md' ':(exclude)SECURITY.md' ':(exclude)tests/**' ':(exclude)src/self_host/app.rs' ':(exclude)scripts/public-release-audit.ps1' 2>$null)
        foreach ($match in $matches) {
            Add-Violation $Violations "$name match in tracked file: $match"
        }
    }
}

$repoRoot = Get-RepoRoot
Set-Location $repoRoot

$violations = [System.Collections.Generic.List[string]]::new()

Assert-NoForbiddenTrackedPaths -Violations $violations
if ($CheckHistory) {
    Assert-HistoryClean -Violations $violations
}
Assert-EnvExampleSafe -RepoRoot $repoRoot -Violations $violations
Assert-NoSecretPatterns -Violations $violations

$uniqueViolations = @($violations | Sort-Object -Unique)
if ($uniqueViolations.Count -gt 0) {
    Write-Host 'Public release audit failed:' -ForegroundColor Red
    foreach ($violation in $uniqueViolations) {
        Write-Host " - $violation"
    }
    exit 1
}

Write-Host 'Public release audit passed.' -ForegroundColor Green
if (-not $CheckHistory) {
    Write-Host 'History scan skipped. Use -CheckHistory before making the repository public.' -ForegroundColor Yellow
}
