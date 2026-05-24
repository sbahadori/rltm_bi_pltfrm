# Database Migrations

This document explains how to define, execute, validate, and troubleshoot the PostgreSQL control-plane migrations for the Realtime BI Platform.

## Purpose

The database migration layer creates the schemas, metadata tables, runtime tables, data-quality tables, lineage tables, stream observability tables, views, and stored procedures/functions required by the platform.

The platform separates the data plane from the control/observability plane:

```text
Kafka / APIs / JDBC -> Spark -> Delta Lake / MinIO
```

PostgreSQL is used for:

```text
meta      = pipeline, job, dataset, and source-system metadata
runtime   = job runs, job events, watermarks, stream runtime state, and stream batch metrics
dq        = data-quality rules and results
lineage   = dataset lineage records
ctl       = stored procedures/functions used by runners, onboarding, dashboard, and Airflow
```

## Expected migration directory

Place migration files under:

```text
database/migrations/
```

Recommended final order:

```text
database/migrations/
  001_schemas.sql
  002_meta_tables.sql
  003_runtime_batch_tables.sql
  004_dq_lineage_tables.sql
  005_ctl_core_usps.sql
  006_ctl_onboarding_usps.sql
  007_ctl_watermark_usps.sql
  010_runtime_stream_tables.sql
  011_runtime_stream_usps.sql
  012_runtime_dashboard_views.sql
```

Run the files in ascending order.

## What each migration does

### `001_schemas.sql`

Creates:

```text
ctl
meta
runtime
dq
lineage
```

### `002_meta_tables.sql`

Creates:

```text
meta.source_system
meta.pipeline
meta.job
meta.job_dependency
meta.dataset
```

These tables hold pipeline, job, dataset, and source-system definitions.

### `003_runtime_batch_tables.sql`

Creates:

```text
runtime.job_run
runtime.job_event
runtime.watermark_state
```

These tables store job execution history, job events, and incremental-load watermarks.

### `004_dq_lineage_tables.sql`

Creates:

```text
dq.quality_rule
dq.quality_result
lineage.dataset_lineage
```

### `005_ctl_core_usps.sql`

Creates core stored procedures/functions used by runners and dashboard:

```text
ctl.usp_get_active_job_metadata
ctl.usp_get_active_job_identity
ctl.usp_upsert_job_run
ctl.usp_insert_job_event
ctl.usp_get_latest_runtime_job_run
ctl.usp_list_control_jobs
ctl.usp_list_runtime_job_runs
ctl.usp_list_runtime_watermarks
ctl.usp_list_quality_results
ctl.usp_list_dataset_lineage
```

### `006_ctl_onboarding_usps.sql`

Creates onboarding procedures/functions:

```text
ctl.usp_list_active_pipeline_specs
ctl.usp_onboard_pipeline
ctl.usp_onboard_source_system
ctl.usp_onboard_job
ctl.usp_onboard_dataset
ctl.usp_onboard_job_dependency
```

These are used by the onboarding workflow to populate `meta.pipeline`, `meta.job`, `meta.dataset`, `meta.source_system`, and `meta.job_dependency`.

### `007_ctl_watermark_usps.sql`

Creates watermark read/write functions:

```text
ctl.usp_get_watermark_state
ctl.usp_upsert_watermark_state
ctl.usp_mark_watermark_success
ctl.usp_list_runtime_watermarks
```

These are required by incremental jobs that track high-watermarks.

### `010_runtime_stream_tables.sql`

Creates stream runtime observability tables:

```text
runtime.stream_unit_current
runtime.stream_batch_metric
```

`runtime.stream_unit_current` stores the latest state per stream unit.

`runtime.stream_batch_metric` stores one metric row per Spark micro-batch.

### `011_runtime_stream_usps.sql`

Creates stream runtime procedures/functions:

```text
ctl.usp_upsert_stream_unit_current
ctl.usp_insert_stream_batch_metric
ctl.usp_get_stream_unit_current
ctl.usp_list_stream_batch_metrics
```

These are called by streaming engines through `shared.runtime.stream_runtime_db`.

### `012_runtime_dashboard_views.sql`

Creates dashboard-oriented views:

```text
runtime.v_stream_unit_current
runtime.v_stream_dashboard_summary
runtime.v_job_run_latest
runtime.v_job_run_dashboard
```

## Execution prerequisites

Make sure the database container is running:

```powershell
docker ps | findstr postgres-warehouse
```

Make sure `.env` contains valid control DB settings:

```text
CONTROL_DB_ENABLED=true
CONTROL_DB_HOST=postgres-warehouse
CONTROL_DB_PORT=5432
CONTROL_DB_NAME=warehouse
CONTROL_DB_USER=warehouse
CONTROL_DB_PASSWORD=warehouse
CONTROL_DB_SSLMODE=disable
```

Adjust database, user, and password names to your local environment.

## Run all migrations

From the repository root:

```powershell
$files = @(
  "001_schemas.sql",
  "002_meta_tables.sql",
  "003_runtime_batch_tables.sql",
  "004_dq_lineage_tables.sql",
  "005_ctl_core_usps.sql",
  "006_ctl_onboarding_usps.sql",
  "007_ctl_watermark_usps.sql",
  "010_runtime_stream_tables.sql",
  "011_runtime_stream_usps.sql",
  "012_runtime_dashboard_views.sql"
)

foreach ($f in $files) {
  docker cp ".\database\migrations\$f" "postgres-warehouse:/tmp/$f"

  docker exec postgres-warehouse psql `
    -U warehouse `
    -d warehouse `
    -v ON_ERROR_STOP=1 `
    -f "/tmp/$f"
}
```

## Run one migration

Example:

```powershell
docker cp .\database\migrations\006_ctl_onboarding_usps.sql postgres-warehouse:/tmp/006_ctl_onboarding_usps.sql

docker exec postgres-warehouse psql `
  -U warehouse `
  -d warehouse `
  -v ON_ERROR_STOP=1 `
  -f /tmp/006_ctl_onboarding_usps.sql
```

## Validate schemas

```sql
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name IN ('ctl', 'meta', 'runtime', 'dq', 'lineage')
ORDER BY schema_name;
```

Expected:

```text
ctl
dq
lineage
meta
runtime
```

## Validate metadata tables

```sql
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'meta'
ORDER BY table_name;
```

Expected core tables:

```text
meta.dataset
meta.job
meta.job_dependency
meta.pipeline
meta.source_system
```

## Validate runtime tables

```sql
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'runtime'
ORDER BY table_name;
```

Expected core tables:

```text
runtime.job_event
runtime.job_run
runtime.stream_batch_metric
runtime.stream_unit_current
runtime.watermark_state
```

## Validate DQ and lineage tables

```sql
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('dq', 'lineage')
ORDER BY table_schema, table_name;
```

Expected:

```text
dq.quality_result
dq.quality_rule
lineage.dataset_lineage
```

## Validate control functions and procedures

```sql
SELECT routine_schema, routine_name, routine_type
FROM information_schema.routines
WHERE routine_schema = 'ctl'
ORDER BY routine_name;
```

Important routines:

```text
usp_get_active_job_metadata
usp_get_active_job_identity
usp_insert_job_event
usp_list_active_pipeline_specs
usp_onboard_pipeline
usp_onboard_job
usp_onboard_dataset
usp_onboard_job_dependency
usp_onboard_source_system
usp_upsert_job_run
usp_get_watermark_state
usp_upsert_watermark_state
usp_upsert_stream_unit_current
usp_insert_stream_batch_metric
usp_get_stream_unit_current
```

## Validate dashboard views

```sql
SELECT table_schema, table_name
FROM information_schema.views
WHERE table_schema = 'runtime'
ORDER BY table_name;
```

Expected views:

```text
runtime.v_job_run_dashboard
runtime.v_job_run_latest
runtime.v_stream_dashboard_summary
runtime.v_stream_unit_current
```

## Smoke tests

### Pipeline-spec reader

```sql
SELECT *
FROM ctl.usp_list_active_pipeline_specs();
```

Before onboarding, this may return zero rows. After onboarding, it should return active pipeline specs from `meta.pipeline.raw_config`.

### Job metadata lookup

```sql
SELECT *
FROM ctl.usp_get_active_job_metadata(
  NULL,
  'bronze.weather_forecast_pipeline.weather_forecast_ingest',
  'bronze.weather_forecast_pipeline.weather_forecast_ingest'
);
```

After onboarding, this should return one active job.

### Watermark function

```sql
SELECT *
FROM ctl.usp_get_watermark_state(
  'bronze.ecommerce_customers.customers',
  'ecommerce_customers',
  'customers',
  'updated_at'
);
```

If no watermark exists yet, the result may be empty. It should not fail with `function does not exist`.

### Stream current state

```sql
SELECT *
FROM ctl.usp_get_stream_unit_current('clickstream_user_events_silver');
```

If the stream has not run yet, this may return no rows. It should not fail.

## Important design rules

### Migrations create structure, not job metadata

Migration files create schemas, tables, views, and stored procedures. They should not hard-code pipeline or job records.

Pipeline/job records are inserted by the onboarding workflow.

### Onboarding must be idempotent

Onboarding procedures use `INSERT ... ON CONFLICT DO UPDATE`, so running onboarding multiple times should update metadata safely.

### Raw data does not go to PostgreSQL

Do not store Kafka events, large API payloads, or full Delta data in PostgreSQL runtime tables. Store only metadata, runtime metrics, watermarks, DQ results, and lineage.

## Common errors

### `ctl.usp_list_active_pipeline_specs()` does not exist

Run:

```text
database/migrations/006_ctl_onboarding_usps.sql
```

### `ctl.usp_get_watermark_state(...)` does not exist

Run:

```text
database/migrations/007_ctl_watermark_usps.sql
```

### Runner cannot resolve active job

Check that `meta.job.job_code` and `meta.job.job_key` match the `CONTROL_JOB_CODE` created by Airflow.

Regular jobs:

```text
{layer}.{pipeline_name}.{job_name}
```

Manifest table jobs:

```text
bronze.{source_id}.{table_id}
```

### Airflow does not load pipeline specs from DB

Check:

```sql
SELECT * FROM ctl.usp_list_active_pipeline_specs();
```

If it returns no rows, run the onboarding DAG.

## Recommended lifecycle

```text
1. Run database migrations.
2. Validate schemas, tables, views, and stored procedures.
3. Run the control_plane_onboarding DAG with dry_run=true.
4. Run the control_plane_onboarding DAG with dry_run=false.
5. Validate meta.pipeline and meta.job.
6. Restart/reparse Airflow if needed.
7. Run batch/stream jobs.
8. Inspect runtime.job_run, runtime.job_event, runtime.watermark_state, and stream runtime tables.
```
