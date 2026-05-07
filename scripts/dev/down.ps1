param(
  [string]$EnvFile = ".env"
)

$ErrorActionPreference = "Stop"

Write-Host "Stopping platform..." -ForegroundColor Yellow

docker compose --project-directory . `
  --env-file $EnvFile `
  -f compose/compose.phase1.yaml `
  -f compose/compose.phase2.yaml `
  -f compose/compose.simulator.yaml `
  down

Write-Host "Platform stopped." -ForegroundColor Green