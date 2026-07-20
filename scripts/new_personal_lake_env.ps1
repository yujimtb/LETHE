$ErrorActionPreference = "Stop"

function New-HexSecret {
    param([Parameter(Mandatory = $true)][int]$Bytes)

    $buffer = [byte[]]::new($Bytes)
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $generator.GetBytes($buffer)
    } finally {
        $generator.Dispose()
    }
    -join ($buffer | ForEach-Object { $_.ToString("x2") })
}

Write-Output "LETHE_STORAGE_ENCRYPTION_KEY=$(New-HexSecret -Bytes 32)"
Write-Output "LETHE_OPERATIONAL_STORAGE_ENCRYPTION_KEY=$(New-HexSecret -Bytes 32)"
Write-Output "LETHE_API_READ_TOKEN=$(New-HexSecret -Bytes 32)"
Write-Output "LETHE_API_WRITE_TOKEN=$(New-HexSecret -Bytes 32)"
Write-Output "LETHE_API_SYNC_TOKEN=$(New-HexSecret -Bytes 32)"
Write-Output "LETHE_NANIHOLD_TOKEN=$(New-HexSecret -Bytes 32)"
Write-Output "LETHE_HTTP_HOST_PORT=8080"
Write-Output "LETHE_MCP_HOST_PORT=8090"
