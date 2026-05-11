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

Write-Host "Recreating dashboard services..." -ForegroundColor Cyan

docker compose --project-directory . `
  --env-file $EnvFile `
  -f compose/compose.phase1.yaml `
  -f compose/compose.phase2.yaml `
  -f compose/compose.simulator.yaml `
  up -d $buildFlag --force-recreate dashboard-api dashboard

Write-Host "Dashboard recreated." -ForegroundColor Green
Write-Host "Test: http://localhost:9090/api/config"