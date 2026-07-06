param(
    [Parameter(Mandatory = $true)]
    [string]$JobName,

    [Parameter(Mandatory = $true)]
    [int]$ExitCode,

    [Parameter(Mandatory = $true)]
    [string]$ReportPath,

    [Parameter(Mandatory = $true)]
    [string]$Message,

    [Parameter(Mandatory = $true)]
    [string]$WebhookEnvName
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($JobName)) {
    throw "JobName must not be blank"
}
if ([string]::IsNullOrWhiteSpace($ReportPath)) {
    throw "ReportPath must not be blank"
}
if ([string]::IsNullOrWhiteSpace($Message)) {
    throw "Message must not be blank"
}
if ([string]::IsNullOrWhiteSpace($WebhookEnvName)) {
    throw "WebhookEnvName must not be blank"
}

$webhook = [Environment]::GetEnvironmentVariable($WebhookEnvName)
if ([string]::IsNullOrWhiteSpace($webhook)) {
    throw "missing required Slack webhook environment variable $WebhookEnvName"
}

$payload = [pscustomobject]@{
    text = "[LETHE] $JobName failed with exit code $ExitCode. Report: $ReportPath`n$Message"
}

Invoke-RestMethod `
    -Method Post `
    -Uri $webhook `
    -ContentType "application/json" `
    -Body ($payload | ConvertTo-Json -Depth 5) | Out-Null

[pscustomobject]@{
    notified = $true
    job_name = $JobName
    report_path = $ReportPath
} | ConvertTo-Json -Depth 5
