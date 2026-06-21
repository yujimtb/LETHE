$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$rootManifest = Get-Content -LiteralPath (Join-Path $root "Cargo.toml") -Raw
if ($rootManifest -match '(?m)^\[package\]') {
    throw "workspace root must be a virtual manifest"
}
if (Test-Path -LiteralPath (Join-Path $root "src")) {
    throw "workspace root must not contain src/"
}

$metadata = cargo metadata --no-deps --format-version 1 |
    ConvertFrom-Json -Depth 100
$workspacePackages = @{}
foreach ($package in $metadata.packages) {
    $workspacePackages[$package.name] = $package
}

$allowedLocalDependencies = @{
    "lethe-core" = @()
    "lethe-policy" = @("lethe-core")
    "lethe-registry" = @("lethe-core")
    "lethe-storage-api" = @("lethe-core")
    "lethe-engine" = @("lethe-core", "lethe-policy", "lethe-registry", "lethe-storage-api")
    "lethe-api" = @("lethe-core", "lethe-engine")
    "lethe-runtime" = @("lethe-core")
    "lethe-profile-model" = @("lethe-core")
    "lethe-adapter-api" = @("lethe-core")
    "lethe-adapter-slack" = @("lethe-adapter-api", "lethe-core")
    "lethe-adapter-gslides" = @("lethe-adapter-api", "lethe-core")
    "lethe-adapter-claude" = @("lethe-adapter-api", "lethe-core")
    "lethe-adapter-notion" = @("lethe-adapter-api", "lethe-profile-model")
    "lethe-derivation-gemini" = @(
        "lethe-adapter-api",
        "lethe-adapter-gslides",
        "lethe-core",
        "lethe-engine",
        "lethe-profile-model"
    )
    "lethe-projection-person" = @(
        "lethe-core",
        "lethe-engine",
        "lethe-policy",
        "lethe-profile-model"
    )
    "lethe-storage-sqlite" = @("lethe-core", "lethe-runtime")
}

$violations = [System.Collections.Generic.List[string]]::new()
foreach ($name in $allowedLocalDependencies.Keys) {
    $package = $workspacePackages[$name]
    if ($null -eq $package) {
        $violations.Add("missing workspace package: $name")
        continue
    }
    $allowed = $allowedLocalDependencies[$name]
    foreach ($dependency in $package.dependencies) {
        if ($null -eq $dependency.path) {
            continue
        }
        if ($dependency.name -notin $allowed) {
            $violations.Add("$name must not depend on $($dependency.name)")
        }
    }
}

$rustFiles = Get-ChildItem -LiteralPath $root -Recurse -File -Filter *.rs |
    Where-Object {
        $_.FullName -notmatch '[\\/](target|\.git)[\\/]'
    }
foreach ($file in $rustFiles) {
    $content = Get-Content -LiteralPath $file.FullName -Raw
    if ($content -match '#\[path\s*=') {
        $violations.Add("cross-directory source ownership is forbidden: $($file.FullName)")
    }
}

if ($violations.Count -gt 0) {
    $violations | ForEach-Object { Write-Error $_ }
    exit 1
}

Write-Output "dependency layer check passed"
