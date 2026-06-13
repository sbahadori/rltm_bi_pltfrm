# Runtime Event Contract

Status: Accepted

## Purpose

Every runtime job must emit a consistent execution event so that the dashboard, control DB, job registry, and future analytical stores can observe jobs without relying on Airflow-only metadata.

## Required fields

Every batch job event must include:

| Field | Required | Description |
|---|---:|---|
| run_id | yes | Job-level runtime run identifier |
| pipeline_name | yes | Logical pipeline name |
| job_name | yes | Logical job name |
| base_job_name | yes | Base task/job name |
| job_code | yes | Stable job code from metadata/catalog |
| job_key | yes | Stable job key |
| runner | yes | Runtime runner name |
| layer | yes | bronze, silver, gold, or stream |
| status | yes | running, success, failed |
| started_at_epoch | yes | Unix start time |
| ended_at_epoch | required on terminal event | Unix end time |
| duration_seconds | required on terminal event | Runtime duration |
| records_read | required on success | Number of source records read |
| records_written | required on success | Number of records written/emitted |
| records_inserted | required on success | Number of inserted records; 0 if none |
| records_updated | required on success | Number of updated records; 0 if none |
| records_deleted | required on success | Number of deleted records; 0 if none |
| target_path | required on success | Main output path |
| airflow_dag_id | optional | Airflow DAG identifier |
| airflow_dag_run_id | optional | Airflow DAG run identifier |
| airflow_task_id | optional | Airflow task identifier |
| error | required on failure | Failure message |
| traceback | optional on failure | Failure traceback |

## Rule

A successful job must never emit null record counters.

If a counter is truly zero, it must be emitted as `0`, not omitted and not `null`.

## Source priority for dashboard observability

The dashboard must resolve runtime state in this order:

1. Control DB runtime rows
2. Local job run registry
3. Stream runtime heartbeat/status
4. Airflow fallback

Airflow is an orchestration fallback, not the source of truth for job-level record counters.