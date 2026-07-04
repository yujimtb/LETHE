$ErrorActionPreference = "Stop"

function New-HexSecret {
    param([Parameter(Mandatory = $true)][int]$Bytes)

    $buffer = [byte[]]::new($Bytes)
    [System.Security.Cryptography.RandomNumberGenerator]::Fill($buffer)
    -join ($buffer | ForEach-Object { $_.ToString("x2") })
}

Write-Output "LETHE_STORAGE_ENCRYPTION_KEY=$(New-HexSecret -Bytes 32)"
Write-Output "LETHE_API_READ_TOKEN=$(New-HexSecret -Bytes 32)"
Write-Output "LETHE_API_SYNC_TOKEN=$(New-HexSecret -Bytes 32)"
