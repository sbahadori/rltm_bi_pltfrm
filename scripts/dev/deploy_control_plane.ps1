param(
    [switch]$ResetDatabase,
    [switch]$IUnderstandThisDeletesData,
    [switch]$SkipMigrations,
    [switch]$SkipDashboardUser,
    [switch]$OnboardFullCatalog,
    [switch]$OnboardingDryRun,
    [switch]$Verify,
    [string]$DashboardUsername = "",
    [string]$DashboardPassword = "",
    [string]$DashboardEmail = "",
    [string]$DashboardRole = "admin",
    [string]$PostgresContainer = "postgres-warehouse",
    [string]$DashboardContainer = "dashboard-api",
    [string]$AirflowContainer = "rltm_bi_pltfrm-airflow-scheduler-1",
    [string]$DatabaseName = "warehouse",
    [string]$DatabaseUser = "warehouse",
    [string]$DatabasePassword = "warehouse"
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

if (-not $SkipMigrations) {
    $migrationArgs = @(
        "-ContainerName", $PostgresContainer,
        "-DatabaseName", $DatabaseName,
        "-DatabaseUser", $DatabaseUser
    )

    if ($ResetDatabase) {
        $migrationArgs += "-Reset"
    }

    if ($IUnderstandThisDeletesData) {
        $migrationArgs += "-IUnderstandThisDeletesData"
    }

    & (Join-Path $scriptDir "rebuild_control_db.ps1") @migrationArgs
}

if (-not $SkipDashboardUser) {
    $seedArgs = @(
        "-DashboardContainer", $DashboardContainer,
        "-Username", $DashboardUsername,
        "-Password", $DashboardPassword,
        "-Email", $DashboardEmail,
        "-Role", $DashboardRole,
        "-ControlDbName", $DatabaseName,
        "-ControlDbUser", $DatabaseUser,
        "-ControlDbPassword", $DatabasePassword
    )

    & (Join-Path $scriptDir "seed_dashboard_admin.ps1") @seedArgs
}

if ($OnboardFullCatalog) {
    $onboardArgs = @("-AirflowContainer", $AirflowContainer)

    if ($OnboardingDryRun) {
        $onboardArgs += "-DryRun"
    }

    & (Join-Path $scriptDir "onboard_catalog.ps1") @onboardArgs
}

if ($Verify) {
    & (Join-Path $scriptDir "verify_control_db.ps1") `
        -PostgresContainer $PostgresContainer `
        -DatabaseName $DatabaseName `
        -DatabaseUser $DatabaseUser
}

Write-Host "Control-plane deployment steps completed."
