param(
  [string]$EnvFile = ".env"
)

$ErrorActionPreference = "Stop"

Write-Host "== RLTMBI validate ==" -ForegroundColor Cyan

if (!(Test-Path $EnvFile)) {
  Write-Host "ERROR: $EnvFile not found." -ForegroundColor Red
  Write-Host "Create it from .env.example:" -ForegroundColor Yellow
  Write-Host "  Copy-Item .env.example .env"
  exit 1
}

$required = @(
  "PROJECT_ROOT",
  "MINIO_ROOT_USER",
  "MINIO_ROOT_PASSWORD",
  "POSTGRES_USER",
  "POSTGRES_PASSWORD",
  "POSTGRES_DB"
)

$envContent = Get-Content $EnvFile

foreach ($key in $required) {
  $match = $envContent | Where-Object { $_ -match "^\s*$key\s*=" }
  if (!$match) {
    Write-Host "ERROR: Missing $key in $EnvFile" -ForegroundColor Red
    exit 1
  }

  $value = ($match -split "=", 2)[1].Trim()
  if ([string]::IsNullOrWhiteSpace($value)) {
    Write-Host "ERROR: $key is empty in $EnvFile" -ForegroundColor Red
    exit 1
  }
}

$projectRootLine = $envContent | Where-Object { $_ -match "^\s*PROJECT_ROOT\s*=" } | Select-Object -First 1
$projectRoot = ($projectRootLine -split "=", 2)[1].Trim()

if ($projectRoot -match "\\") {
  Write-Host "ERROR: PROJECT_ROOT should use forward slashes on Windows." -ForegroundColor Red
  Write-Host "Example: PROJECT_ROOT=C:/src/rltm_bi_pltfrm" -ForegroundColor Yellow
  exit 1
}

if (!(Test-Path $projectRoot)) {
  Write-Host "ERROR: PROJECT_ROOT does not exist: $projectRoot" -ForegroundColor Red
  exit 1
}

docker network inspect rltm_shared_net *> $null
if ($LASTEXITCODE -ne 0) {
  Write-Host "Docker network rltm_shared_net not found. Creating..." -ForegroundColor Yellow
  docker network create rltm_shared_net | Out-Null
}

Write-Host "Running docker compose config..." -ForegroundColor Cyan

docker compose --project-directory . `
  --env-file $EnvFile `
  -f compose/compose.phase1.yaml `
  -f compose/compose.phase2.yaml `
  -f compose/compose.simulator.yaml `
  config *> $null

if ($LASTEXITCODE -ne 0) {
  Write-Host "ERROR: docker compose config failed." -ForegroundColor Red
  exit 1
}

Write-Host "OK: environment and compose config are valid." -ForegroundColor Green