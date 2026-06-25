# Control DB Deployment Scripts

Run these commands from the repository root.

## 1. Rebuild schemas and run migrations

Preview the migration order without touching the database:

```powershell
.\scripts\dev\rebuild_control_db.ps1 -PlanOnly
```

Run migrations without deleting existing data:

```powershell
.\scripts\dev\rebuild_control_db.ps1
```

For a disposable local database after a full reset:

```powershell
.\scripts\dev\rebuild_control_db.ps1 -Reset -IUnderstandThisDeletesData
```

## 2. Seed the dashboard admin user

This runs the seed inside the `dashboard-api` container so `postgres-warehouse`
resolves correctly on the Docker network.

```powershell
.\scripts\dev\seed_dashboard_admin.ps1 `
  -Username admin `
  -Password "change-me" `
  -Email admin@example.com `
  -Role admin
```

## 3. Onboard catalog jobs

For the full job/pipeline authoring and onboarding workflow, see
`docs/guides/onboard-batch-pipeline-and-jobs.md`.

Dry-run one job:

```powershell
.\scripts\dev\onboard_catalog.ps1 `
  -PipelineName weather_forecast_pipeline `
  -JobName weather_forecast_ingest `
  -DryRun
```

Commit one job:

```powershell
.\scripts\dev\onboard_catalog.ps1 `
  -PipelineName weather_forecast_pipeline `
  -JobName weather_forecast_ingest
```

Onboard the full catalog:

```powershell
.\scripts\dev\onboard_catalog.ps1
```

## One-command local deployment

Run migrations, seed the dashboard user, and stop before onboarding:

```powershell
.\scripts\dev\deploy_control_plane.ps1 `
  -DashboardUsername admin `
  -DashboardPassword "change-me" `
  -DashboardEmail admin@example.com `
  -Verify
```

Run migrations, seed the dashboard user, and dry-run full onboarding:

```powershell
.\scripts\dev\deploy_control_plane.ps1 `
  -DashboardUsername admin `
  -DashboardPassword "change-me" `
  -DashboardEmail admin@example.com `
  -OnboardFullCatalog `
  -OnboardingDryRun
```

## Validate

Quick check:

```powershell
.\scripts\dev\verify_control_db.ps1
```

Manual SQL:

```sql
SELECT username, email, role, is_active
FROM meta.dashboard_user
ORDER BY username;

SELECT pipeline_name, is_active
FROM meta.pipeline
ORDER BY pipeline_name;

SELECT job_code, job_name, pipeline_name, layer, is_active, active_flag
FROM meta.job
ORDER BY pipeline_name, job_code;
```
