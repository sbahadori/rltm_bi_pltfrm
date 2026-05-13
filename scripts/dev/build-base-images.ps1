$ErrorActionPreference = "Stop"

function Read-DotEnv {
  param([string]$Path = ".env")

  $map = @{}

  if (-not (Test-Path $Path)) {
    Write-Host "Missing .env file. Copy .env.example to .env first." -ForegroundColor Red
    exit 1
  }

  Get-Content $Path | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq "" -or $line.StartsWith("#")) {
      return
    }

    $parts = $line.Split("=", 2)
    if ($parts.Count -eq 2) {
      $map[$parts[0].Trim()] = $parts[1].Trim()
    }
  }

  return $map
}

$envMap = Read-DotEnv ".env"

$pythonBaseImage = $envMap["RLTM_PYTHON_BASE_IMAGE"]
$sparkBaseImage = $envMap["RLTM_SPARK_BASE_IMAGE"]
$airflowBaseImage = $envMap["RLTM_AIRFLOW_BASE_IMAGE"]

$requiredFiles = @(
  "shared\spark-dist\spark-3.5.1-bin-hadoop3.tgz",
  "shared\spark-jars\hadoop-aws-3.3.4.jar",
  "shared\spark-jars\aws-java-sdk-bundle-1.12.262.jar",
  "shared\spark-jars\mssql-jdbc-13.4.0.jre11.jar",
  "shared\spark-jars\postgresql-42.7.4.jar",
  "shared\spark-jars\mysql-connector-j-9.0.0.jar",
  "shared\spark-jars\delta-spark_2.12-3.2.0.jar",
  "shared\spark-jars\delta-storage-3.2.0.jar"
)

$missing = @()

foreach ($file in $requiredFiles) {
  if (-not (Test-Path $file)) {
    $missing += $file
  }
}

if ($missing.Count -gt 0) {
  Write-Host "Missing Spark dependency files:" -ForegroundColor Red
  $missing | ForEach-Object { Write-Host " - $_" -ForegroundColor Red }
  Write-Host ""
  Write-Host "Run this first:" -ForegroundColor Yellow
  Write-Host ".\scripts\dev\download-spark-deps.ps1" -ForegroundColor Yellow
  exit 1
}

docker build -t $pythonBaseImage -f docker/base/python/Dockerfile .
docker build -t $sparkBaseImage -f docker/base/spark/Dockerfile .
docker build -t $airflowBaseImage -f docker/base/airflow/Dockerfile .