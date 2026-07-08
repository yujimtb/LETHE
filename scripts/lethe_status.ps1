param(
    [string]$BaseUrl = "http://127.0.0.1:8080",
    [string]$Token,
    [string]$TokenEnv = "LETHE_API_SYNC_TOKEN",
    [string]$ReadToken,
    [string]$ReadTokenEnv = "LETHE_API_READ_TOKEN"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Resolve-Token {
    param(
        [string]$Value,
        [string]$EnvName,
        [Parameter(Mandatory = $true)]
        [string]$Purpose,
        [bool]$Required
    )

    if (-not [string]::IsNullOrWhiteSpace($Value)) {
        return $Value
    }
    if (-not [string]::IsNullOrWhiteSpace($EnvName)) {
        $envValue = [Environment]::GetEnvironmentVariable($EnvName)
        if (-not [string]::IsNullOrWhiteSpace($envValue)) {
            return $envValue
        }
        if ($Required) {
            throw "$Purpose could not find token environment variable $EnvName. Set $EnvName or pass -Token."
        }
    }
    if ($Required) {
        throw "$Purpose could not find a token. Pass -Token or provide -TokenEnv with an environment variable containing the token."
    }
    return $null
}

function Invoke-LetheJson {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Uri,
        [Parameter(Mandatory = $true)]
        [string]$BearerToken,
        [Parameter(Mandatory = $true)]
        [string]$Purpose
    )

    try {
        Invoke-RestMethod -Method Get -Uri $Uri -Headers @{ Authorization = "Bearer $BearerToken" }
    }
    catch {
        $statusCode = $null
        if ($_.Exception.Response -and $_.Exception.Response.StatusCode) {
            $statusCode = [int]$_.Exception.Response.StatusCode
        }
        if ($statusCode -eq 401 -or $statusCode -eq 403) {
            throw "$Purpose is unauthorized for $Uri. Check that the token has the required scope."
        }
        if ($_.Exception.Message -match "Unable to connect|No connection could be made|actively refused|NameResolutionFailure") {
            throw "$Purpose could not reach $Uri. The selfhost server may be stopped or BaseUrl may be wrong."
        }
        throw "$Purpose failed for $Uri`: $($_.Exception.Message)"
    }
}

function Write-Section {
    param([Parameter(Mandatory = $true)][string]$Title)
    Write-Output ""
    Write-Output "== $Title =="
}

function Format-Value {
    param($Value)
    if ($null -eq $Value -or [string]::IsNullOrWhiteSpace([string]$Value)) {
        return "-"
    }
    return [string]$Value
}

function Get-OptionalProperty {
    param(
        $Object,
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    if ($null -eq $Object) {
        return $null
    }
    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) {
        return $null
    }
    return $property.Value
}

function Format-ObservedValue {
    param($Value)
    if ($null -eq $Value -or [string]::IsNullOrWhiteSpace([string]$Value)) {
        return "unobserved"
    }
    return [string]$Value
}

function Format-ObservedAge {
    param($Seconds)
    if ($null -eq $Seconds) {
        return "unobserved"
    }
    return Format-Age $Seconds
}

function Format-Age {
    param($Seconds)
    if ($null -eq $Seconds) {
        return "-"
    }
    $total = [int64]$Seconds
    if ($total -lt 120) {
        return "$total sec"
    }
    $minutes = [math]::Floor($total / 60)
    if ($minutes -lt 120) {
        return "$minutes min"
    }
    $hours = [math]::Floor($minutes / 60)
    if ($hours -lt 72) {
        return "$hours hr"
    }
    $days = [math]::Floor($hours / 24)
    return "$days day"
}

if ([string]::IsNullOrWhiteSpace($BaseUrl)) {
    throw "BaseUrl must not be blank"
}

$base = $BaseUrl.TrimEnd("/")

try {
    $healthToken = Resolve-Token -Value $Token -EnvName $TokenEnv -Purpose "/health/deep" -Required $true
    $health = Invoke-LetheJson -Uri "$base/health/deep" -BearerToken $healthToken -Purpose "deep health"
}
catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 2
}

$exitCode = 0
if ($health.status -ne "ok") {
    $exitCode = 1
}

Write-Output "LETHE status: $base"
Write-Output "overall=$($health.status) version=$($health.version)"

Write-Section "Sync"
$lastSync = $health.last_sync
$metrics = $health.metrics
Write-Output "completed_at=$(Format-Value $lastSync.completed_at)"
Write-Output "error=$(Format-Value $lastSync.error)"
Write-Output "fetched=$($metrics.fetched) ingested=$($metrics.ingested) skipped=$($metrics.skipped) failed(dead-letter)=$($metrics.failed) quarantined=$($metrics.quarantined) latency_ms=$($metrics.latency_ms)"
if ($metrics.failed -gt 0 -or $metrics.quarantined -gt 0) {
    $exitCode = 1
}

Write-Section "Dependencies"
if (@($health.dependencies).Count -eq 0) {
    Write-Output "none"
}
else {
    foreach ($dependency in $health.dependencies) {
        Write-Output "$($dependency.name): $($dependency.status) detail=$(Format-Value $dependency.detail)"
        if ($dependency.status -ne "ok") {
            $exitCode = 1
        }
    }
}

Write-Section "Projections"
foreach ($projection in $health.projections) {
    Write-Output "$($projection.id): status=$($projection.status) health=$($projection.health)"
    if ($projection.health -ne "healthy") {
        $exitCode = 1
    }
}

Write-Section "Source Freshness"
$resolvedReadToken = Resolve-Token -Value $ReadToken -EnvName $ReadTokenEnv -Purpose "/projections/freshness" -Required $false
if ($null -eq $resolvedReadToken) {
    Write-Output "skipped: pass -ReadToken or -ReadTokenEnv with read:corpus scope to show per-source freshness"
}
else {
    try {
        $freshnessEnvelope = Invoke-LetheJson -Uri "$base/projections/freshness" -BearerToken $resolvedReadToken -Purpose "source freshness"
        $sources = @($freshnessEnvelope.data.sources)
        $missing = @($freshnessEnvelope.data.missing)
        Write-Output "sources=$($sources.Count) missing_or_unobserved=$($missing.Count)"
        foreach ($source in $sources) {
            $sourceId = Format-Value (Get-OptionalProperty -Object $source -Name "source_id")
            $status = Format-Value (Get-OptionalProperty -Object $source -Name "status")
            $age = Format-ObservedAge (Get-OptionalProperty -Object $source -Name "age_seconds")
            $max = Format-Age (Get-OptionalProperty -Object $source -Name "max_age_seconds")
            $lastObserved = Format-ObservedValue (Get-OptionalProperty -Object $source -Name "last_observed_at")
            Write-Output "$($sourceId): status=$status age=$age max=$max last_observed=$lastObserved"
        }
        if ($missing.Count -gt 0) {
            $exitCode = 1
        }
    }
    catch {
        Write-Output "unavailable: $($_.Exception.Message)"
        $exitCode = 1
    }
}

exit $exitCode
