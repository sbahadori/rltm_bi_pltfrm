param(
    [string]$PostgresContainer = "postgres-warehouse",
    [string]$DatabaseName = "warehouse",
    [string]$DatabaseUser = "warehouse"
)

$ErrorActionPreference = "Stop"

$sql = @"
SELECT 'dashboard_users' AS check_name, count(*)::text AS value FROM meta.dashboard_user
UNION ALL
SELECT 'active_dashboard_users', count(*)::text FROM meta.dashboard_user WHERE is_active IS TRUE
UNION ALL
SELECT 'pipelines', count(*)::text FROM meta.pipeline
UNION ALL
SELECT 'active_pipelines', count(*)::text FROM meta.pipeline WHERE is_active IS TRUE
UNION ALL
SELECT 'jobs', count(*)::text FROM meta.job
UNION ALL
SELECT 'active_jobs', count(*)::text FROM meta.job WHERE is_active IS TRUE AND active_flag IS TRUE
ORDER BY check_name;

SELECT username, email, role, is_active
FROM meta.dashboard_user
ORDER BY username;
"@

docker exec -i $PostgresContainer psql `
    -U $DatabaseUser `
    -d $DatabaseName `
    -v ON_ERROR_STOP=1 `
    -c $sql

if ($LASTEXITCODE -ne 0) {
    throw "Control DB verification failed."
}
