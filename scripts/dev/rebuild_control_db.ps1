param(
    [string]$ContainerName = "postgres-warehouse",
    [string]$DatabaseName = "warehouse",
    [string]$DatabaseUser = "warehouse",
    [string]$MigrationsDir = "",
    [switch]$Reset,
    [switch]$IUnderstandThisDeletesData,
    [switch]$PlanOnly
)

$ErrorActionPreference = "Stop"

function Invoke-Checked {
    param(
        [Parameter(Mandatory=$true)]
        [string]$Description,
        [Parameter(Mandatory=$true)]
        [string[]]$Command
    )

    Write-Host $Description
    & $Command[0] @($Command[1..($Command.Length - 1)])

    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $Description"
    }
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..")

if ([string]::IsNullOrWhiteSpace($MigrationsDir)) {
    $MigrationsDir = Join-Path $repoRoot "database\migrations"
}

$migrationsPath = Resolve-Path $MigrationsDir

$files = Get-ChildItem -LiteralPath $migrationsPath -File |
    Where-Object { $_.Name -match "^[0-9]{3}_.+\.sql$" } |
    Sort-Object Name

if (-not $files) {
    throw "No active migration files found in $migrationsPath"
}

$duplicatePrefixes = $files |
    Group-Object { $_.Name.Substring(0, 3) } |
    Where-Object { $_.Count -gt 1 }

if ($duplicatePrefixes) {
    $details = ($duplicatePrefixes | ForEach-Object {
        "$($_.Name): $((($_.Group | Select-Object -ExpandProperty Name) -join ', '))"
    }) -join "; "
    throw "Duplicate active migration number(s) found: $details"
}

$expected = 1
foreach ($file in $files) {
    $actual = [int]$file.Name.Substring(0, 3)
    if ($actual -ne $expected) {
        throw "Missing migration number $($expected.ToString('000')) before $($file.Name)"
    }
    $expected += 1
}

if ($PlanOnly) {
    Write-Host "Migration execution plan:"
    foreach ($file in $files) {
        Write-Host " - $($file.Name)"
    }
    Write-Host "Plan only. No database changes were made."
    return
}

Invoke-Checked `
    -Description "Checking database container: $ContainerName" `
    -Command @("docker", "inspect", $ContainerName)

if ($Reset) {
    if (-not $IUnderstandThisDeletesData) {
        throw "Reset deletes all platform database objects. Re-run with -Reset -IUnderstandThisDeletesData."
    }

    $dropSql = @"
DROP SCHEMA IF EXISTS archive CASCADE;
DROP SCHEMA IF EXISTS ctl CASCADE;
DROP SCHEMA IF EXISTS meta CASCADE;
DROP SCHEMA IF EXISTS runtime CASCADE;
DROP SCHEMA IF EXISTS dq CASCADE;
DROP SCHEMA IF EXISTS lineage CASCADE;
"@

    $tmpDropFile = New-TemporaryFile
    Set-Content -LiteralPath $tmpDropFile -Value $dropSql -Encoding UTF8

    try {
        Invoke-Checked `
            -Description "Copying reset SQL into $ContainerName" `
            -Command @("docker", "cp", $tmpDropFile.FullName, "${ContainerName}:/tmp/rltm_reset_control_db.sql")

        Invoke-Checked `
            -Description "Dropping platform schemas from database $DatabaseName" `
            -Command @(
                "docker", "exec", $ContainerName,
                "psql", "-U", $DatabaseUser, "-d", $DatabaseName,
                "-v", "ON_ERROR_STOP=1",
                "-f", "/tmp/rltm_reset_control_db.sql"
            )
    }
    finally {
        Remove-Item -LiteralPath $tmpDropFile.FullName -Force -ErrorAction SilentlyContinue
    }
}

foreach ($file in $files) {
    $containerPath = "/tmp/$($file.Name)"

    Invoke-Checked `
        -Description "Copying migration $($file.Name)" `
        -Command @("docker", "cp", $file.FullName, "${ContainerName}:$containerPath")

    Invoke-Checked `
        -Description "Running migration $($file.Name)" `
        -Command @(
            "docker", "exec", $ContainerName,
            "psql", "-U", $DatabaseUser, "-d", $DatabaseName,
            "-v", "ON_ERROR_STOP=1",
            "-f", $containerPath
        )
}

Write-Host "Database migration rebuild completed successfully."
