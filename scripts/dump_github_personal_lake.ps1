param(
    [Parameter(Mandatory = $true)]
    [string]$OutputPath
)

$ErrorActionPreference = "Stop"

function Invoke-GhJson {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [string[]]$Headers = @()
    )

    $ghArgs = @("api")
    foreach ($header in $Headers) {
        $ghArgs += @("-H", $header)
    }
    $ghArgs += $Path
    $output = & gh @ghArgs 2>&1
    if ($LASTEXITCODE -ne 0) {
        $message = $output | Out-String
        throw "gh api failed for $Path`: $message"
    }
    $json = $output | Out-String
    $json | ConvertFrom-Json -Depth 100
}

function Invoke-GhArray {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [string[]]$Headers = @(),
        [switch]$AllowEmptyRepository
    )

    $ghArgs = @("api", "--paginate", "--slurp")
    foreach ($header in $Headers) {
        $ghArgs += @("-H", $header)
    }
    $ghArgs += $Path
    $output = & gh @ghArgs 2>&1
    if ($LASTEXITCODE -ne 0) {
        $message = $output | Out-String
        if ($AllowEmptyRepository -and $message.Contains("Git Repository is empty") -and $message.Contains("HTTP 409")) {
            return @()
        }
        throw "gh api pagination failed for $Path`: $message"
    }
    $json = $output | Out-String

    $pages = $json | ConvertFrom-Json -Depth 100 -NoEnumerate
    $items = @()
    foreach ($page in $pages) {
        foreach ($item in @($page)) {
            if ($item -is [array]) {
                foreach ($nested in $item) {
                    $items += $nested
                }
            }
            else {
                $items += $item
            }
        }
    }
    $items
}

function New-RepositoryDump {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FullName
    )

    Write-Host "dumping_repo=$FullName"

    $allIssues = Invoke-GhArray "/repos/$FullName/issues?state=all&per_page=100"
    $issues = @($allIssues | Where-Object { $null -eq $_.pull_request })
    $pulls = Invoke-GhArray "/repos/$FullName/pulls?state=all&per_page=100"
    $issueComments = Invoke-GhArray "/repos/$FullName/issues/comments?per_page=100"
    $commitsLight = Invoke-GhArray -Path "/repos/$FullName/commits?per_page=100" -AllowEmptyRepository

    $reviews = @()
    $reviewComments = @()
    foreach ($pull in $pulls) {
        foreach ($review in Invoke-GhArray "/repos/$FullName/pulls/$($pull.number)/reviews?per_page=100") {
            $reviews += $review
        }
        foreach ($comment in Invoke-GhArray "/repos/$FullName/pulls/$($pull.number)/comments?per_page=100") {
            $reviewComments += $comment
        }
    }

    $timelineEvents = @()
    foreach ($issue in $allIssues) {
        foreach ($event in Invoke-GhArray "/repos/$FullName/issues/$($issue.number)/timeline?per_page=100" @("Accept: application/vnd.github+json")) {
            $timelineEvents += $event
        }
    }

    $commits = @()
    foreach ($commit in $commitsLight) {
        $commits += Invoke-GhJson "/repos/$FullName/commits/$($commit.sha)"
    }

    [ordered]@{
        full_name = $FullName
        issues = @($issues)
        issue_comments = @($issueComments)
        pull_requests = @($pulls)
        pull_request_reviews = @($reviews)
        pull_request_review_comments = @($reviewComments)
        commits = @($commits)
        timeline_events = @($timelineEvents)
    }
}

if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    throw "gh command is not available"
}

$parent = Split-Path -Parent $OutputPath
if (-not [string]::IsNullOrWhiteSpace($parent)) {
    New-Item -ItemType Directory -Force -Path $parent | Out-Null
}

$repos = Invoke-GhArray "/user/repos?affiliation=owner&visibility=all&per_page=100"
if ($repos.Count -eq 0) {
    throw "no owned GitHub repositories returned by gh api"
}

$repoDumps = @()
foreach ($repo in $repos) {
    if ([string]::IsNullOrWhiteSpace($repo.full_name)) {
        throw "GitHub repository response is missing full_name"
    }
    $repoDumps += New-RepositoryDump $repo.full_name
}

$bundle = [ordered]@{
    dumped_at = (Get-Date).ToUniversalTime().ToString("o")
    repositories = @($repoDumps)
}

$bundle | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $OutputPath -Encoding utf8NoBOM
Write-Output "github_dump=$OutputPath"
