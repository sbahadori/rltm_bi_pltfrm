param(
  [string]$EnvFile = ".env"
)

$ErrorActionPreference = "Stop"

& "$PSScriptRoot/validate.ps1" -EnvFile $EnvFile

Write-Host "Recreating Spark stream supervisor and Spark workers..." -ForegroundColor Cyan

docker compose --project-directory . `
  --env-file $EnvFile `
  -f compose/compose.phase1.yaml `
  -f compose/compose.phase2.yaml `
  -f compose/compose.simulator.yaml `
  up -d --force-recreate spark-stream-supervisor spark-master spark-worker spark-worker-2

Write-Host "Streams recreated." -ForegroundColor Green
Write-Host "Check logs:"
Write-Host "  docker logs -f spark-stream-supervisor"