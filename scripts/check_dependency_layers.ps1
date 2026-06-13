$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$cargoFiles = Get-ChildItem -LiteralPath (Join-Path $root "crates") -Recurse -Filter Cargo.toml

$rules = @{
    "lethe-core" = @("tokio", "reqwest", "rusqlite", "axum")
    "lethe-policy" = @("tokio", "reqwest", "rusqlite", "axum")
    "lethe-storage-api" = @("tokio", "reqwest", "rusqlite", "axum")
    "lethe-adapter-slack" = @("lethe-adapter-gslides")
    "lethe-adapter-gslides" = @("lethe-adapter-slack")
    "lethe-storage-sqlite" = @("lethe-adapter-slack", "lethe-adapter-gslides")
}

$violations = @()
foreach ($file in $cargoFiles) {
    $content = Get-Content -LiteralPath $file.FullName -Raw
    if ($content -notmatch '(?m)^name\s*=\s*"([^"]+)"') {
        continue
    }
    $name = $Matches[1]
    if (-not $rules.ContainsKey($name)) {
        continue
    }
    foreach ($forbidden in $rules[$name]) {
        if ($content -match "(?m)^\s*$([regex]::Escape($forbidden))\s*=") {
            $violations += "$name must not depend on $forbidden"
        }
    }
}

if ($violations.Count -gt 0) {
    $violations | ForEach-Object { Write-Error $_ }
    exit 1
}

Write-Host "dependency layer check passed"
