param(
    [Parameter(Mandatory = $true)]
    [datetime]$IdlePrecheckAt,

    [Parameter(Mandatory = $true)]
    [datetime]$AbsoluteRenewalAt
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ([string]::IsNullOrWhiteSpace($PSScriptRoot)) {
    throw "PSScriptRoot is required."
}

if ($IdlePrecheckAt -ge $AbsoluteRenewalAt) {
    throw "IdlePrecheckAt must be earlier than AbsoluteRenewalAt."
}

$reauthScript = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "reauthorize_lethe_mcp.ps1")).Path
$userId = "$env:USERDOMAIN\$env:USERNAME"

if ([string]::IsNullOrWhiteSpace($env:USERDOMAIN)) {
    throw "USERDOMAIN is required."
}

if ([string]::IsNullOrWhiteSpace($env:USERNAME)) {
    throw "USERNAME is required."
}

function Register-LetheMcpReauthTask {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TaskName,

        [Parameter(Mandatory = $true)]
        [datetime]$RunAt,

        [Parameter(Mandatory = $true)]
        [ValidateSet("IdlePrecheck", "AbsoluteRenewal")]
        [string]$Mode,

        [Parameter(Mandatory = $true)]
        [string]$Description
    )

    $argument = "-NoProfile -ExecutionPolicy Bypass -File `"$reauthScript`" -Mode $Mode"
    $action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $argument
    $trigger = New-ScheduledTaskTrigger -Once -At $RunAt
    $settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -WakeToRun
    $principal = New-ScheduledTaskPrincipal `
        -UserId $userId `
        -LogonType Interactive `
        -RunLevel Limited

    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Principal $principal `
        -Description $Description `
        -Force | Out-Null
}

Register-LetheMcpReauthTask `
    -TaskName "LETHE MCP Reauth Idle Precheck" `
    -RunAt $IdlePrecheckAt `
    -Mode "IdlePrecheck" `
    -Description "Open LETHE MCP reauthentication flow one day before ChatGPT/Codex idle refresh-token expiry."

Register-LetheMcpReauthTask `
    -TaskName "LETHE MCP Reauth Absolute Renewal" `
    -RunAt $AbsoluteRenewalAt `
    -Mode "AbsoluteRenewal" `
    -Description "Open LETHE MCP reauthentication flow one day before ChatGPT/Codex absolute refresh-token expiry."

Get-ScheduledTask -TaskName "LETHE MCP Reauth*" | ForEach-Object {
    $info = Get-ScheduledTaskInfo -TaskName $_.TaskName -TaskPath $_.TaskPath
    [pscustomobject]@{
        TaskName = $_.TaskName
        State = $_.State
        NextRunTime = $info.NextRunTime
        TaskPath = $_.TaskPath
    }
}
