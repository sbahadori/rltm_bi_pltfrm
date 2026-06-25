param(
    [string]$DashboardContainer = "dashboard-api",
    [string]$RepoRootInContainer = "/workspace/rltm_bi_pltfrm",
    [string]$Username = "",
    [string]$Password = "",
    [string]$Email = "",
    [string]$Role = "admin",
    [string]$ControlDbHost = "postgres-warehouse",
    [string]$ControlDbPort = "5432",
    [string]$ControlDbName = "warehouse",
    [string]$ControlDbUser = "warehouse",
    [string]$ControlDbPassword = "warehouse",
    [string]$AppEnv = "dev"
)

$ErrorActionPreference = "Stop"

function Use-DefaultIfEmpty {
    param([string]$Value, [string]$Default)
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return $Default
    }
    return $Value
}

$Username = Use-DefaultIfEmpty $Username (Use-DefaultIfEmpty $env:DASHBOARD_DEV_ADMIN_USERNAME "admin")
$Password = Use-DefaultIfEmpty $Password $env:DASHBOARD_DEV_ADMIN_PASSWORD
$Email = Use-DefaultIfEmpty $Email (Use-DefaultIfEmpty $env:DASHBOARD_DEV_ADMIN_EMAIL "admin@example.com")
$Role = Use-DefaultIfEmpty $Role (Use-DefaultIfEmpty $env:DASHBOARD_DEV_ADMIN_ROLE "admin")

if ([string]::IsNullOrWhiteSpace($Password)) {
    throw "Dashboard admin password is required. Pass -Password or set DASHBOARD_DEV_ADMIN_PASSWORD."
}

Write-Host "Seeding dashboard user '$Username' into $ControlDbName via container $DashboardContainer"

docker exec `
    -e "APP_ENV=$AppEnv" `
    -e "PIPELINE_REPO_ROOT=$RepoRootInContainer" `
    -e "CONTROL_DB_HOST=$ControlDbHost" `
    -e "CONTROL_DB_PORT=$ControlDbPort" `
    -e "CONTROL_DB_NAME=$ControlDbName" `
    -e "CONTROL_DB_USER=$ControlDbUser" `
    -e "CONTROL_DB_PASSWORD=$ControlDbPassword" `
    -e "DASHBOARD_DEV_ADMIN_USERNAME=$Username" `
    -e "DASHBOARD_DEV_ADMIN_PASSWORD=$Password" `
    -e "DASHBOARD_DEV_ADMIN_EMAIL=$Email" `
    -e "DASHBOARD_DEV_ADMIN_ROLE=$Role" `
    $DashboardContainer `
    python "$RepoRootInContainer/scripts/seed_dashboard_admin_dev.py"

if ($LASTEXITCODE -ne 0) {
    throw "Dashboard user seed failed."
}

Write-Host "Dashboard user seed completed."
