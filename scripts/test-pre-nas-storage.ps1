[CmdletBinding()]
param(
    [string]$DeploymentMode = 'test'
)

$ErrorActionPreference = 'Stop'
if ($DeploymentMode -cne 'test') {
    throw "pre-NAS storage topology only accepts DeploymentMode=test"
}

$repository = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repository 'deploy\pre-nas-storage-test\compose.yaml'
$environmentFile = Join-Path $repository 'deploy\pre-nas-storage-test\test.env'
$runtimeRoot = Join-Path $repository 'target\pre-nas-storage-test'
$projectName = 'lethe-pre-nas-storage-test'
$postgresPort = '55439'
$minioPort = '59009'
$postgresUser = 'lethe_fixture'
$postgresDatabase = 'lethe_fixture'
$postgresPassword = 'lethe-pre-nas-postgres-fixture-only'
$minioUser = 'lethe-pre-nas-minio-fixture'
$minioPassword = 'lethe-pre-nas-minio-fixture-secret'
$dsn = "host=127.0.0.1 port=$postgresPort user=$postgresUser password=$postgresPassword dbname=$postgresDatabase"
$endpoint = "http://127.0.0.1:$minioPort"
$corruptDigest = '032f9b1a83578c9a8ed139173f6318c18d17f5b7b4904617da871bba5450e52b'
$corruptRef = "blob:sha256:$corruptDigest"

$composeArguments = @(
    'compose',
    '--project-name', $projectName,
    '--env-file', $environmentFile,
    '--file', $composeFile
)

function Invoke-Checked {
    param(
        [Parameter(Mandatory)]
        [string]$Command,
        [Parameter(Mandatory)]
        [string[]]$Arguments,
        [Parameter(Mandatory)]
        [string]$Failure
    )
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw $Failure
    }
}

function Invoke-CargoExample {
    param(
        [Parameter(Mandatory)]
        [string]$Package,
        [Parameter(Mandatory)]
        [string]$Example,
        [Parameter(Mandatory)]
        [string[]]$ExampleArguments
    )
    $arguments = @(
        'run', '--locked', '--package', $Package, '--example', $Example, '--'
    ) + $ExampleArguments
    Invoke-Checked -Command 'cargo' -Arguments $arguments -Failure "cargo example failed: $Example"
}

function New-TestSchema {
    param(
        [Parameter(Mandatory)]
        [string]$Schema
    )
    if ($Schema -cnotmatch '^[a-z][a-z0-9_]+$') {
        throw "unsafe fixture schema: $Schema"
    }
    $arguments = $composeArguments + @(
        'exec', '--no-TTY', 'postgres',
        'psql', '--username', $postgresUser, '--dbname', $postgresDatabase,
        '--no-psqlrc', '--set', 'ON_ERROR_STOP=1',
        '--command', "CREATE SCHEMA $Schema AUTHORIZATION $postgresUser"
    )
    Invoke-Checked -Command 'docker' -Arguments $arguments -Failure "failed to create schema $Schema"
}

function New-TestBucket {
    param(
        [Parameter(Mandatory)]
        [string]$Bucket
    )
    if ($Bucket -cnotmatch '^[a-z0-9][a-z0-9-]+[a-z0-9]$') {
        throw "unsafe fixture bucket: $Bucket"
    }
    $arguments = $composeArguments + @(
        'exec', '--no-TTY', 'minio',
        'mc', 'mb', '--ignore-existing', "fixture/$Bucket"
    )
    Invoke-Checked -Command 'docker' -Arguments $arguments -Failure "failed to create bucket $Bucket"
}

Invoke-Checked -Command 'docker' -Arguments ($composeArguments + @('config', '--quiet')) `
    -Failure 'pre-NAS Compose configuration is invalid'
Invoke-Checked -Command 'docker' -Arguments ($composeArguments + @('down', '--volumes', '--remove-orphans')) `
    -Failure 'failed to clear the test-scoped Compose project'

$resolvedRuntimeRoot = [System.IO.Path]::GetFullPath($runtimeRoot)
$resolvedTargetRoot = [System.IO.Path]::GetFullPath((Join-Path $repository 'target'))
if (-not $resolvedRuntimeRoot.StartsWith(
        $resolvedTargetRoot + [System.IO.Path]::DirectorySeparatorChar,
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
    throw "runtime root escapes repository target directory: $resolvedRuntimeRoot"
}
if (Test-Path -LiteralPath $resolvedRuntimeRoot) {
    Remove-Item -LiteralPath $resolvedRuntimeRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $resolvedRuntimeRoot | Out-Null

try {
    Invoke-Checked -Command 'docker' -Arguments ($composeArguments + @('up', '--detach', '--wait')) `
        -Failure 'failed to start disposable PostgreSQL/MinIO services'

    $aliasArguments = $composeArguments + @(
        'exec', '--no-TTY', 'minio',
        'mc', 'alias', 'set', 'fixture', 'http://127.0.0.1:9000', $minioUser, $minioPassword
    )
    Invoke-Checked -Command 'docker' -Arguments $aliasArguments -Failure 'failed to admit MinIO fixture alias'

    $schemas = @(
        'migration_test',
        'observation_test',
        'projection_test',
        'runtime_test',
        'slack_test',
        'cutover_test',
        'postgres_s3_test',
        'orphan_gc_test',
        'parity_test',
        'selfhost_test'
    )
    foreach ($schema in $schemas) {
        New-TestSchema -Schema $schema
    }
    foreach ($bucket in @('s3-conformance', 'postgres-s3-conformance', 'orphan-gc', 'parity', 'selfhost')) {
        New-TestBucket -Bucket $bucket
    }

    $declaredBytes = [System.Text.Encoding]::UTF8.GetBytes('declared-content')
    $declaredHash = [System.Security.Cryptography.SHA256]::HashData($declaredBytes)
    $declaredHex = [System.Convert]::ToHexString($declaredHash).ToLowerInvariant()
    if ($declaredHex -cne $corruptDigest) {
        throw "corrupt fixture digest changed: $declaredHex"
    }
    $corruptFile = Join-Path $resolvedRuntimeRoot 'corrupt-object.bin'
    [System.IO.File]::WriteAllBytes(
        $corruptFile,
        [System.Text.Encoding]::UTF8.GetBytes('corrupt-bytes-v1')
    )
    Invoke-Checked -Command 'docker' -Arguments ($composeArguments + @(
            'cp', $corruptFile, 'minio:/tmp/corrupt-object.bin'
        )) -Failure 'failed to copy corrupt S3 fixture'
    foreach ($bucket in @('s3-conformance', 'postgres-s3-conformance')) {
        Invoke-Checked -Command 'docker' -Arguments ($composeArguments + @(
                'exec', '--no-TTY', 'minio',
                'mc', 'cp', '/tmp/corrupt-object.bin', "fixture/$bucket/sha256/$corruptDigest"
            )) -Failure "failed to install corrupt object in $bucket"
    }

    Invoke-CargoExample 'lethe-storage-postgres' 'migrate_general' @(
        $dsn, 'migration_test', $postgresUser, 'space:migration-test', '2'
    )
    foreach ($entry in @(
            @('observation_conformance', 'observation_test'),
            @('projection_conformance', 'projection_test'),
            @('runtime_conformance', 'runtime_test'),
            @('slack_watermark_conformance', 'slack_test'),
            @('cutover_conformance', 'cutover_test')
        )) {
        Invoke-CargoExample 'lethe-storage-postgres' $entry[0] @(
            $dsn, $entry[1], $postgresUser, "space:$($entry[1])", '2'
        )
    }
    Invoke-CargoExample 'lethe-storage-postgres' 's3_conformance' @(
        $endpoint, 'us-east-1', 's3-conformance', $minioUser, $minioPassword, $corruptRef
    )
    Invoke-CargoExample 'lethe-storage-postgres' 'postgres_s3_conformance' @(
        $dsn, 'postgres_s3_test', $postgresUser, 'space:postgres-s3-test', '2',
        $endpoint, 'us-east-1', 'postgres-s3-conformance', $minioUser, $minioPassword, $corruptRef
    )
    Invoke-CargoExample 'lethe-storage-postgres' 'orphan_gc_conformance' @(
        $dsn, 'orphan_gc_test', $postgresUser, 'space:orphan-gc-test', '2',
        $endpoint, 'us-east-1', 'orphan-gc', $minioUser, $minioPassword
    )
    Invoke-CargoExample 'lethe-selfhost' 'storage_backend_parity' @(
        (Join-Path $resolvedRuntimeRoot 'parity-sqlite'),
        $dsn, 'parity_test', $postgresUser, 'space:parity-test',
        $endpoint, 'parity', $minioUser, $minioPassword
    )
    Invoke-CargoExample 'lethe-selfhost' 'storage_boot_conformance' @(
        'sqlite', (Join-Path $resolvedRuntimeRoot 'sqlite-selfhost')
    )
    Invoke-CargoExample 'lethe-selfhost' 'storage_boot_conformance' @(
        'postgres', (Join-Path $resolvedRuntimeRoot 'postgres-selfhost'),
        $dsn, 'selfhost_test', $postgresUser, 'space:selfhost-test',
        $endpoint, 'selfhost', $minioUser, $minioPassword
    )

    & cargo run --locked --package lethe-selfhost --example storage_boot_conformance -- `
        postgres (Join-Path $resolvedRuntimeRoot 'rejected-selfhost') `
        $dsn selfhost_test $postgresUser 'space:selfhost-test' `
        'http://127.0.0.1:1' selfhost $minioUser $minioPassword
    if ($LASTEXITCODE -eq 0) {
        throw 'selected PostgreSQL/S3 backend unexpectedly booted with unavailable S3'
    }

    Write-Output 'pre_nas_storage_conformance=passed'
}
finally {
    & docker @composeArguments down --volumes --remove-orphans
    if ($LASTEXITCODE -ne 0) {
        Write-Error 'failed to remove the test-scoped Compose project'
    }
}
