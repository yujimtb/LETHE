[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $EnvFile
)

$ErrorActionPreference = "Stop"
$resolved = (Resolve-Path -LiteralPath $EnvFile).Path
if (-not (Test-Path -LiteralPath $resolved -PathType Leaf)) {
    throw "Personal Lake environment file not found: $resolved"
}

$content = [IO.File]::ReadAllText($resolved)
$keys = @{}
foreach ($line in ($content -split "\r?\n")) {
    if (-not $line -or $line.StartsWith("#")) {
        continue
    }
    $parts = $line.Split("=", 2)
    if ($parts.Count -ne 2 -or -not $parts[0] -or -not $parts[1]) {
        throw "Invalid Personal Lake environment entry"
    }
    if ($keys.ContainsKey($parts[0])) {
        throw "Duplicate Personal Lake environment key: $($parts[0])"
    }
    $keys[$parts[0]] = $true
}
if ($keys.ContainsKey("LETHE_NANIHOLD_TOKEN")) {
    throw "LETHE_NANIHOLD_TOKEN already exists; refusing to replace it"
}

$bytes = [byte[]]::new(32)
$generator = [Security.Cryptography.RandomNumberGenerator]::Create()
try {
    $generator.GetBytes($bytes)
} finally {
    $generator.Dispose()
}
$token = -join ($bytes | ForEach-Object { $_.ToString("x2") })
$prefix = if ($content.EndsWith("`n")) { "" } else { [Environment]::NewLine }
$line = (
    $prefix +
    "LETHE_NANIHOLD_TOKEN=" +
    $token +
    [Environment]::NewLine
)
$encoding = [Text.UTF8Encoding]::new($false)
[IO.File]::AppendAllText($resolved, $line, $encoding)
Write-Output "Added a dedicated Nanihold token without printing its value."
