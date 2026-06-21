# Final Platform Source-of-Truth Architecture

Status: Accepted

This document is the canonical source-of-truth contract for the platform. When
code, database migrations, runbooks, or dashboard behavior disagree with this
document, this document wins and the conflicting implementation or document must
be fixed.

## Scope

The contract covers:

- pipeline and job definition ownership
- materialized runtime configuration
- batch runtime state
- stream runtime state
- orchestration and trigger ownership
- dashboard and UI ownership
- analytics and monitoring ownership
- deprecated registry behavior

## Source-of-Truth Matrix

| Concern | Authoritative source | Materialized/derived from | Non-authoritative consumers or sinks |
|---|---|---|---|
| Batch pipeline and job definitions | `configs/batch/pipeline_catalog.json` | Edited through catalog tooling; onboarded by `shared.onboarding.control_plane` | Dashboard UI, Airflow DAG builder, runners |
| JDBC table manifests | `configs/jdbc/*.json` | Expanded by catalog/onboarding logic | Batch runners, dashboard metadata |
| Stream definitions | stream registry/config files | Loaded by stream supervisor and stream runtime DB writers | Dashboard UI |
| Batch executable job config | `meta.job.config` in the Control DB | Materialized from the catalog by onboarding procedures | Runners, Airflow DAG builder, dashboard metadata |
| Pipeline metadata | `meta.pipeline` in the Control DB | Materialized from the catalog by onboarding procedures | Airflow DAG builder, dashboard metadata |
| Batch runtime state | Control DB runtime tables, especially `runtime.job_run` and `runtime.job_event` | Written by runners/runtime event writers | Dashboard API/UI, ClickHouse analytics sink |
| Stream runtime state | Control DB stream runtime tables, especially `runtime.stream_unit_current` and `runtime.stream_batch_metric` | Written by stream supervisor and stream runtime DB writers | Dashboard API/UI, ClickHouse analytics sink |
| Manual trigger requests | Dashboard API action endpoints plus action audit tables | User/API request | Airflow executor |
| Batch orchestration | Airflow DAGs and task runs | Built from onboarded Control DB metadata | Dashboard API/UI |
| Observability analytics | ClickHouse analytical tables | Replicated/emitted from runtime events | BI, historical reporting |
| Service metrics | Prometheus | Exporters and services | Grafana |
| Operational visualization | Grafana and dashboard UI | Control DB, ClickHouse, Prometheus | Users |

## Canonical Flow

```text
Catalog / manifests
        |
        v
shared.onboarding.control_plane
        |
        v
Control DB metadata
  - meta.pipeline
  - meta.job.config
        |
        +--> Airflow DAG builder and executor
        |
        +--> Batch and stream runners
                  |
                  v
            Control DB runtime state
              - runtime.job_run
              - runtime.job_event
              - runtime.stream_unit_current
              - runtime.stream_batch_metric
                  |
                  +--> Dashboard API/UI
                  +--> ClickHouse analytics sink
                  +--> Prometheus/Grafana metrics and dashboards
```

## Rules

1. Catalog files are the design-time source of truth for batch job definitions.
   The Control DB stores the runtime projection of those definitions.
2. `meta.job.config` is not edited directly by dashboard code, ad hoc SQL, or
   runner code. It is produced by catalog onboarding/sync.
3. Batch runtime status, counters, output paths, errors, and run history are
   authoritative only in the Control DB runtime tables and the approved `ctl`
   runtime functions/views.
4. Stream runtime status is authoritative in the Control DB stream runtime
   tables. A stream supervisor status file may be used only as a local/debug
   fallback and must be clearly labelled as fallback data.
5. Airflow is an executor and orchestrator. Airflow metadata can explain the
   executor state of a DAG/task, but it is not the source of truth for platform
   job definitions, runtime counters, output paths, or final job-level status.
6. Dashboard UI is a presentation, trigger-request, and catalog-editing surface.
   It is not a runtime executor and is not the source of truth for definitions
   or runtime state.
7. ClickHouse, Prometheus, Grafana, and Metabase are analytical, monitoring, or
   visualization consumers. They must not be treated as operational control
   sources.
8. The legacy UI dynamic job registry and any local job-run registry are
   deprecated/archived. They must not appear in active source precedence,
   dashboard runtime resolution, or new platform documentation except as
   explicitly labelled migration history.

## Approved Dashboard Runtime Precedence

Batch dashboard runtime data:

1. Control DB runtime rows and `ctl` runtime functions/views.
2. Explicitly labelled executor/debug metadata from Airflow, only when shown as
   non-authoritative fallback while a Control DB runtime gap is being fixed.
3. Catalog/onboarded metadata only to explain that a job is defined but has no
   runtime rows yet.

Stream dashboard runtime data:

1. Control DB stream runtime tables.
2. Stream supervisor status file, only as a local/debug fallback.

Forbidden active runtime sources:

- `job_run_registry`
- UI dynamic job registry tables/procedures
- local JSONL runtime registries for batch jobs
- unlabelled Airflow task/DAG state as platform runtime truth

## Ownership by Component

| Component | Owns | Must not own |
|---|---|---|
| Catalog editor | Editing and validating catalog JSON | Runtime execution state |
| Onboarding layer | Publishing catalog/manifest definitions into Control DB metadata | Runtime run history |
| Control DB | Metadata projection, runtime state, watermarks, auth/action metadata | High-volume historical analytics |
| Airflow | Scheduling, orchestration, task execution | Platform source-of-truth state |
| Batch/stream runners | Runtime event emission and data movement | Catalog approval or UI state |
| Dashboard API/UI | Presentation, trigger requests, catalog draft/apply workflows | Direct execution or direct runtime truth |
| ClickHouse | Analytical copy of events/metrics | Operational control-plane decisions |
| Prometheus/Grafana | Metrics and visualization | Job definition or runtime truth |

## Related Files

- `docs/adr/0001-storage-analytics-observability-architecture.md`
- `docs/contracts/runtime_event_contract.md`
- `database/migrations/007_ctl_onboarding_usps.sql`
- `database/migrations/016_drop_ui_job_registry.sql`
- `shared/onboarding/control_plane.py`
- `shared/control/job_spec_store.py`
- `apps/dashboard/runtime_resolver.py`
- `apps/dashboard/catalog_editor.py`
