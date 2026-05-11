param(
  [switch]$Build,
  [string]$EnvFile = ".env"
)

$ErrorActionPreference = "Stop"

& "$PSScriptRoot/validate.ps1" -EnvFile $EnvFile

$buildFlag = ""
if ($Build) {
  $buildFlag = "--build"
}

Write-Host "Starting full platform..." -ForegroundColor Cyan

docker compose --project-directory . `
  --env-file $EnvFile `
  -f compose/compose.phase1.yaml `
  -f compose/compose.phase2.yaml `
  -f compose/compose.simulator.yaml `
  up -d $buildFlag

Write-Host "Platform start requested." -ForegroundColor Green
Write-Host "Dashboard: http://localhost:9090"
Write-Host "Airflow:   http://localhost:8090"
Write-Host "MinIO:     http://localhost:9001"
Write-Host "Web demo:  http://localhost:8091"