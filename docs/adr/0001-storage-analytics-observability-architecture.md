# ADR-0001: Storage, Analytics, and Observability Architecture

## Status

Accepted

## Context

The platform requires a clear separation between operational metadata, runtime control, analytical workloads, BI exploration, service metrics, and monitoring.

The current platform already uses PostgreSQL as the control database for pipeline metadata, job metadata, runtime job runs, watermarks, quality results, and dashboard authentication. However, as the platform grows, high-volume runtime events, stream metrics, quality checks, and historical analytical queries should not overload the operational metadata store.

## Decision

Use the following architectural separation:

| Layer | Technology | Responsibility |
|---|---|---|
| Operational registry and metadata | PostgreSQL | Pipelines, jobs, datasets, lineage, runtime state, watermarks, users, action logs |
| High-volume runtime analytics | ClickHouse | Job events, stream batch metrics, quality history, historical observability analytics |
| Control plane UI | React | Runtime-aware operational dashboard and catalog editor |
| BI exploration | Metabase | Optional analytical exploration over PostgreSQL and ClickHouse |
| Metrics collection | Prometheus | Service metrics, API health, runner metrics, exporter metrics |
| Monitoring and visualization | Grafana | Operational dashboards, alerts, infrastructure monitoring |

## Consequences

### Positive

- Clear separation between operational metadata and analytical workloads.
- PostgreSQL remains small, consistent, and transactional.
- ClickHouse can handle high-volume append-only runtime analytics.
- Prometheus and Grafana provide standard observability for infrastructure and services.
- React allows the dashboard to evolve beyond a single large HTML file.
- Metabase can provide optional self-service BI without coupling it to the control plane.

### Negative

- More infrastructure components increase deployment complexity.
- Data synchronization is required between PostgreSQL runtime tables and ClickHouse analytical tables.
- Additional monitoring configuration is required.
- The platform needs clear data ownership rules to avoid duplicated truth across systems.

## Implementation Strategy

This decision will be implemented incrementally.

Phase 1 keeps PostgreSQL as the source of truth for operational metadata and runtime state.

Phase 2 introduces a standardized runtime event contract and ensures all batch and streaming runners emit consistent metrics.

Phase 3 adds ClickHouse as an analytical sink for high-volume runtime and quality events.

Phase 4 adds Prometheus and Grafana for infrastructure and service monitoring.

Phase 5 migrates the dashboard from a single-file HTML implementation to a React-based control plane.

Phase 6 optionally adds Metabase for BI exploration.



                  ┌────────────────────┐
                  │   Catalog JSON      │
                  │ pipeline_catalog    │
                  │ stream_registry     │
                  └─────────┬──────────┘
                            │
                            ▼
                  ┌────────────────────┐
                  │  Onboarding DAG     │
                  │  Airflow            │
                  └─────────┬──────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────┐
│ PostgreSQL Control DB                                  │
│ meta.pipeline / meta.job / runtime.job_run / watermark │
└───────────────────────┬───────────────────────────────┘
                        │
                        ▼
              ┌───────────────────┐
              │ Dashboard API      │
              │ FastAPI            │
              └───────┬───────────┘
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
  React Control   ClickHouse     Prometheus
  Plane UI        Analytics      Metrics
        │             │             │
        ▼             ▼             ▼
   Operations      Metabase       Grafana