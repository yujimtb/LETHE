param(
    [Parameter(Mandatory = $true)]
    [string]$CredentialTarget,

    [Parameter(Mandatory = $true)]
    [string]$CredentialUser,

    [Parameter(Mandatory = $true)]
    [string]$EnvironmentVariableName,

    [switch]$Force
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command cmdkey -ErrorAction SilentlyContinue)) {
    throw "cmdkey command is not available"
}
if ([string]::IsNullOrWhiteSpace($CredentialTarget)) {
    throw "CredentialTarget must not be blank"
}
if ([string]::IsNullOrWhiteSpace($CredentialUser)) {
    throw "CredentialUser must not be blank"
}
if ([string]::IsNullOrWhiteSpace($EnvironmentVariableName)) {
    throw "EnvironmentVariableName must not be blank"
}

$existing = cmdkey /list:$CredentialTarget | Out-String
if (($existing -notmatch "\* NONE \*") -and ($existing -match [regex]::Escape($CredentialTarget))) {
    if (-not $Force) {
        throw "credential target already exists: $CredentialTarget"
    }
    cmdkey /delete:$CredentialTarget | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "failed to delete existing credential target: $CredentialTarget"
    }
}

$buffer = [byte[]]::new(32)
[System.Security.Cryptography.RandomNumberGenerator]::Fill($buffer)
$key = -join ($buffer | ForEach-Object { $_.ToString("x2") })

cmdkey /generic:$CredentialTarget /user:$CredentialUser /pass:$key | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "failed to store credential target: $CredentialTarget"
}

[Environment]::SetEnvironmentVariable($EnvironmentVariableName, $key, "User")
[Environment]::SetEnvironmentVariable($EnvironmentVariableName, $key, "Process")

$listed = cmdkey /list:$CredentialTarget | Out-String
if (($listed -match "\* NONE \*") -or ($listed -notmatch [regex]::Escape($CredentialTarget))) {
    throw "credential target was not listed after storing: $CredentialTarget"
}

[pscustomobject]@{
    credential_target = $CredentialTarget
    credential_user = $CredentialUser
    environment_variable = $EnvironmentVariableName
    user_environment_set = $true
    process_environment_set = $true
} | ConvertTo-Json
