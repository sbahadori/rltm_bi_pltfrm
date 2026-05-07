param(
  [string]$EnvFile = ".env"
)

$ErrorActionPreference = "Continue"

Write-Host "== RLTMBI Doctor ==" -ForegroundColor Cyan

Write-Host "`n[1] Env file" -ForegroundColor Yellow
if (Test-Path $EnvFile) {
  Write-Host "OK: $EnvFile exists" -ForegroundColor Green
} else {
  Write-Host "FAIL: $EnvFile missing" -ForegroundColor Red
}

Write-Host "`n[2] Compose config" -ForegroundColor Yellow
docker compose --project-directory . `
  --env-file $EnvFile `
  -f compose/compose.phase1.yaml `
  -f compose/compose.phase2.yaml `
  -f compose/compose.simulator.yaml `
  config *> $null

if ($LASTEXITCODE -eq 0) {
  Write-Host "OK: compose config valid" -ForegroundColor Green
} else {
  Write-Host "FAIL: compose config invalid" -ForegroundColor Red
}

Write-Host "`n[3] Containers" -ForegroundColor Yellow
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

Write-Host "`n[4] Dashboard API from nginx container" -ForegroundColor Yellow
docker exec dashboard sh -c "wget -qO- http://dashboard-api:8001/health" 2>$null
if ($LASTEXITCODE -eq 0) {
  Write-Host "`nOK: dashboard-api reachable from dashboard" -ForegroundColor Green
} else {
  Write-Host "FAIL: dashboard-api not reachable from dashboard" -ForegroundColor Red
}

Write-Host "`n[5] Stream supervisor S3 env" -ForegroundColor Yellow
docker exec spark-stream-supervisor bash -lc "env | sort | grep -E 'MINIO|AWS|S3'" 2>$null

Write-Host "`n[6] Stream supervisor recent logs" -ForegroundColor Yellow
docker logs spark-stream-supervisor --tail=50 2>$null