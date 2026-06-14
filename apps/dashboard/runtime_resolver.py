from __future__ import annotations

"""
Runtime resolver.

Single responsibility:
- Resolve job runtime state from the approved source precedence.
- Normalize run rows into one stable API schema.
- Collapse event-level rows into job-run-level rows.

Source precedence:
1. Control DB
2. job_run_registry JSONL
3. Airflow
4. Catalog/onboarded metadata
5. Unknown/unavailable
"""

import json
from typing import Any

try:
    from apps.dashboard.airflow_client import latest_airflow_task
    from apps.dashboard.config_loader import JOB_RUN_REGISTRY_FILE
    from apps.dashboard.db import call_usp_rows, catalog_metadata_for_job, list_runtime_events
    from apps.dashboard.runtime_models import (
        TERMINAL_STATES,
        as_int_or_none,
        duration_seconds,
        epoch_to_iso,
        first_present,
        has_value,
        is_earlier,
        is_later,
        metric_value,
        normalize_state,
        run_group_key,
        state_rank,
    )
    from apps.dashboard.stream_runtime import find_stream_unit, load_stream_status, stream_current_from_db
except ImportError:  # pragma: no cover
    from .airflow_client import latest_airflow_task
    from .config_loader import JOB_RUN_REGISTRY_FILE
    from .db import call_usp_rows, catalog_metadata_for_job, list_runtime_events
    from .runtime_models import (
        TERMINAL_STATES,
        as_int_or_none,
        duration_seconds,
        epoch_to_iso,
        first_present,
        has_value,
        is_earlier,
        is_later,
        metric_value,
        normalize_state,
        run_group_key,
        state_rank,
    )
    from .stream_runtime import find_stream_unit, load_stream_status, stream_current_from_db

try:
    from apps.dashboard.domain_contracts import (
        canonical_job,
        job_id_of,
        job_name_of,
        job_mode_of,
        pipeline_id_of,
    )
except ImportError:  # pragma: no cover
    from .domain_contracts import (
        canonical_job,
        job_id_of,
        job_name_of,
        job_mode_of,
        pipeline_id_of,
    )

def read_registry(limit: int = 2000) -> list[dict[str, Any]]:
    if not JOB_RUN_REGISTRY_FILE.exists():
        return []

    records: list[dict[str, Any]] = []
    try:
        with JOB_RUN_REGISTRY_FILE.open("r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    except Exception:
        return []

    return sorted(records, key=lambda row: int(row.get("ts_epoch") or 0))[-limit:]


def control_row_matches_job(row: dict[str, Any], job: dict[str, Any]) -> bool:
    job = canonical_job(job)
    pipeline = pipeline_id_of(job)
    job_name = job_name_of(job)
    job_id = job_id_of(job)
    job_code = str(job.get("job_code") or job.get("metadata_job_code") or "")

    row_pipeline = str(first_present(row, "pipeline_name", "pipeline", "airflow_dag_id", default="") or "")
    if row_pipeline and pipeline and row_pipeline != pipeline:
        return False

    row_names = {
        str(first_present(row, "job_name", default="") or ""),
        str(first_present(row, "base_job_name", default="") or ""),
        str(first_present(row, "airflow_task_id", default="") or ""),
        str(first_present(row, "task_id", default="") or ""),
        str(first_present(row, "entity_name", default="") or ""),
    }
    row_codes = {
        str(first_present(row, "job_code", default="") or ""),
        str(first_present(row, "job_key", default="") or ""),
        str(first_present(row, "resolved_job_code", default="") or ""),
        str(first_present(row, "resolved_job_key", default="") or ""),
    }

    expected_entity = f"{pipeline}.{job_name}"
    expected_code_suffix = f".{pipeline}.{job_name}"

    return (
        job_name in row_names
        or job_id in row_names
        or expected_entity in row_names
        or bool(job_code and job_code in row_codes)
        or any(code.endswith(expected_code_suffix) for code in row_codes if code)
        or any(code.endswith(f".{job_name}") for code in row_codes if code)
    )


def registry_row_matches_job(row: dict[str, Any], job: dict[str, Any]) -> bool:
    job = canonical_job(job)
    pipeline = pipeline_id_of(job)
    job_name = job_name_of(job)
    job_id = job_id_of(job)
    job_code = str(job.get("job_code") or job.get("metadata_job_code") or "")

    row_pipeline = str(first_present(row, "pipeline_name", "pipeline", "airflow_dag_id", default="") or "")
    if row_pipeline and pipeline and row_pipeline != pipeline:
        return False

    row_names = {
        str(first_present(row, "job", default="") or ""),
        str(first_present(row, "job_name", default="") or ""),
        str(first_present(row, "base_job_name", default="") or ""),
        str(first_present(row, "airflow_task_id", default="") or ""),
        str(first_present(row, "task_id", default="") or ""),
        str(first_present(row, "entity_name", default="") or ""),
        str(first_present(row, "job_id", default="") or ""),
    }
    row_codes = {
        str(first_present(row, "job_code", default="") or ""),
        str(first_present(row, "job_key", default="") or ""),
        str(first_present(row, "resolved_job_code", default="") or ""),
        str(first_present(row, "resolved_job_key", default="") or ""),
    }

    expected_entity = f"{pipeline}.{job_name}"
    expected_code_suffix = f".{pipeline}.{job_name}"

    return (
        job_name in row_names
        or job_id in row_names
        or expected_entity in row_names
        or bool(job_code and job_code in row_codes)
        or any(code.endswith(expected_code_suffix) for code in row_codes if code)
        or any(code.endswith(f".{job_name}") for code in row_codes if code)
    )


def normalize_runtime_run_row(row: dict[str, Any]) -> dict[str, Any]:
    started_at = first_present(row, "started_at", "start_time", "start_date", "observed_at")
    ended_at = first_present(row, "ended_at", "end_time", "end_date")

    run_duration = first_present(row, "duration_seconds", "duration_sec")
    if run_duration is None:
        run_duration = duration_seconds(started_at, ended_at)

    records_read = as_int_or_none(metric_value(row, "records_read", "input_rows", "last_input_rows"))
    records_written = as_int_or_none(metric_value(row, "records_written", "output_rows", "last_valid_rows"))
    records_inserted = as_int_or_none(metric_value(row, "records_inserted", "inserted_rows"))
    records_updated = as_int_or_none(metric_value(row, "records_updated", "updated_rows"))
    records_deleted = as_int_or_none(metric_value(row, "records_deleted", "deleted_rows"))

    if records_inserted is None:
        records_inserted = 0
    if records_updated is None:
        records_updated = 0
    if records_deleted is None:
        records_deleted = 0

    run_id = first_present(row, "run_id", "job_run_id", "runtime_run_id", "event_id", default="-")
    dag_run_id = first_present(row, "dag_run_id", "airflow_dag_run_id", "airflow_run_id", default=None)

    return {
        "run_id": str(run_id),
        "dag_run_id": str(dag_run_id) if dag_run_id else None,
        "state": normalize_state(first_present(row, "status", "state", default="unknown")),
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": run_duration,
        "records_read": records_read,
        "records_written": records_written,
        "records_inserted": records_inserted,
        "records_updated": records_updated,
        "records_deleted": records_deleted,
        "target_path": first_present(row, "target_path", "output_path"),
        "status_reason": first_present(row, "status_reason", "error_message", "last_error"),
        "runtime_source": "control_db",
        "runtime_source_rank": 1,
        "is_fallback": False,
        "fallback_reason": None,
    }


def normalize_registry_run_row(row: dict[str, Any]) -> dict[str, Any]:
    records_read = as_int_or_none(metric_value(row, "records_read", "input_rows", "last_input_rows"))
    records_written = as_int_or_none(metric_value(row, "records_written", "output_rows", "last_valid_rows"))
    records_inserted = as_int_or_none(metric_value(row, "records_inserted", "inserted_rows"))
    records_updated = as_int_or_none(metric_value(row, "records_updated", "updated_rows"))
    records_deleted = as_int_or_none(metric_value(row, "records_deleted", "deleted_rows"))

    if records_inserted is None and records_written is not None:
        records_inserted = records_written
    if records_updated is None:
        records_updated = 0
    if records_deleted is None:
        records_deleted = 0

    started_at = first_present(row, "started_at", "start_time", "start_date")
    if not started_at:
        started_at = epoch_to_iso(first_present(row, "started_at_epoch", "ts_epoch"))

    ended_at = first_present(row, "ended_at", "end_time", "end_date")
    if not ended_at:
        ended_at = epoch_to_iso(first_present(row, "ended_at_epoch", "ts_epoch"))

    dag_run_id = first_present(row, "dag_run_id", "airflow_dag_run_id", "airflow_run_id", default=None)

    return {
        "run_id": str(first_present(row, "run_id", "event_id", default="-")),
        "dag_run_id": str(dag_run_id) if dag_run_id else None,
        "state": normalize_state(first_present(row, "state", "status", default="unknown")),
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": first_present(row, "duration_seconds", "duration_sec"),
        "records_read": records_read,
        "records_written": records_written,
        "records_inserted": records_inserted,
        "records_updated": records_updated,
        "records_deleted": records_deleted,
        "target_path": first_present(row, "target_path", "output_path"),
        "status_reason": first_present(row, "status_reason", "error", "error_message", "last_error"),
        "runtime_source": "job_run_registry",
        "runtime_source_rank": 2,
        "is_fallback": True,
        "fallback_reason": (
            "Control DB had no matching runtime rows; "
            "using local job_run_registry JSONL as debug fallback."
),
    }


def collapse_run_events(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse event-level rows into one row per job run while preserving zero counters."""
    groups: dict[str, dict[str, Any]] = {}
    metric_keys = {
        "records_read",
        "records_written",
        "records_inserted",
        "records_updated",
        "records_deleted",
    }

    for row in rows:
        key = run_group_key(row)
        current = groups.get(key, {}).copy()

        if not current:
            groups[key] = row.copy()
            continue

        incoming_state = normalize_state(row.get("state"))
        current_state = normalize_state(current.get("state"))

        if state_rank(incoming_state) >= state_rank(current_state):
            current["state"] = incoming_state

        if has_value(row.get("started_at")):
            if not has_value(current.get("started_at")) or is_earlier(row.get("started_at"), current.get("started_at")):
                current["started_at"] = row.get("started_at")

        if has_value(row.get("ended_at")):
            if not has_value(current.get("ended_at")) or is_later(row.get("ended_at"), current.get("ended_at")):
                current["ended_at"] = row.get("ended_at")

        if has_value(row.get("duration_seconds")):
            if incoming_state in TERMINAL_STATES or not has_value(current.get("duration_seconds")):
                current["duration_seconds"] = row.get("duration_seconds")

        for key_name in metric_keys:
            if row.get(key_name) is not None:
                current[key_name] = row.get(key_name)

        for key_name in ["run_id", "dag_run_id", "target_path", "status_reason", "runtime_source"]:
            if has_value(row.get(key_name)) and not has_value(current.get(key_name)):
                current[key_name] = row.get(key_name)

        groups[key] = current

    return sorted(
        groups.values(),
        key=lambda row: str(row.get("ended_at") or row.get("started_at") or ""),
        reverse=True,
    )


def control_run_rows_for_job(job: dict[str, Any], limit: int = 10) -> list[dict[str, Any]]:
    job = canonical_job(job)
    try:
        rows = call_usp_rows("usp_list_runtime_job_runs", (max(200, limit * 30),))
    except Exception as exc:
        print(f"[WARN] Could not load runtime job runs from control DB: {exc}", flush=True)
        return []

    matched = [normalize_runtime_run_row(row) for row in rows if control_row_matches_job(row, job)]
    return collapse_run_events(matched)[:limit]


def registry_run_rows_for_job(job: dict[str, Any], limit: int = 10) -> list[dict[str, Any]]:
    job = canonical_job(job)
    rows = read_registry(limit=max(2000, limit * 100))
    matched = [normalize_registry_run_row(row) for row in rows if registry_row_matches_job(row, job)]
    return collapse_run_events(matched)[:limit]

def runtime_db_run_rows_for_job(job: dict[str, Any], limit: int = 10) -> list[dict[str, Any]]:
    job = canonical_job(job)
    rows = list_runtime_events(limit=max(2000, limit * 100))
    matched = [normalize_registry_run_row(row) for row in rows if registry_row_matches_job(row, job)]

    collapsed = collapse_run_events(matched)[:limit]
    for row in collapsed:
        row["runtime_source"] = "runtime_db"
        row["runtime_source_rank"] = 1
        row["is_fallback"] = False
        row["fallback_reason"] = None

    return collapsed

def enrich_jobs(config_jobs: list[dict[str, Any]]) -> dict[str, Any]:
    stream_status = load_stream_status()
    enriched: list[dict[str, Any]] = []

    for job in config_jobs:
        job = canonical_job(job)

        runtime_job = {
            **job,
            "current_status": "defined",
            "latest_run_id": None,
            "started_at": None,
            "ended_at": None,
            "duration_seconds": None,
            "status_reason": None,
            "runtime_source": "catalog",
            "runtime_available": False,
        }

        control_runs = control_run_rows_for_job(job, limit=1)
        if control_runs:
            control = control_runs[0]
            runtime_job.update(
                {
                    "current_status": control.get("state", "unknown"),
                    "latest_run_id": control.get("run_id"),
                    "latest_dag_run_id": control.get("dag_run_id"),
                    "started_at": control.get("started_at"),
                    "ended_at": control.get("ended_at"),
                    "duration_seconds": control.get("duration_seconds"),
                    "target_path": control.get("target_path") or job.get("target_path"),
                    "status_reason": control.get("status_reason"),
                    "runtime_source": "control_db",
                    "runtime_source_rank": 1,
                    "is_fallback": False,
                    "fallback_reason": None,
                    "runtime_available": True,
                    "records_read": control.get("records_read"),
                    "records_written": control.get("records_written"),
                    "records_inserted": control.get("records_inserted"),
                    "records_updated": control.get("records_updated"),
                    "records_deleted": control.get("records_deleted"),
                }
            )
            if runtime_job.get("records_inserted") is None and runtime_job.get("records_written") is not None:
                runtime_job["records_inserted"] = runtime_job["records_written"]
            if runtime_job.get("records_updated") is None:
                runtime_job["records_updated"] = 0
            if runtime_job.get("records_deleted") is None:
                runtime_job["records_deleted"] = 0

        else:
            runtime_db_runs = runtime_db_run_rows_for_job(job, limit=1)
            if runtime_db_runs:
                runtime_db = runtime_db_runs[0]
                runtime_job.update(
                    {
                        "current_status": runtime_db.get("state", "unknown"),
                        "latest_run_id": runtime_db.get("run_id"),
                        "latest_dag_run_id": runtime_db.get("dag_run_id"),
                        "started_at": runtime_db.get("started_at"),
                        "ended_at": runtime_db.get("ended_at"),
                        "duration_seconds": runtime_db.get("duration_seconds"),
                        "target_path": runtime_db.get("target_path") or runtime_job.get("target_path"),
                        "status_reason": runtime_db.get("status_reason"),
                        "runtime_source": "runtime_db",
                        "runtime_source_rank": 1,
                        "is_fallback": False,
                        "fallback_reason": None,
                        "runtime_available": True,
                        "records_read": runtime_db.get("records_read"),
                        "records_written": runtime_db.get("records_written"),
                        "records_inserted": runtime_db.get("records_inserted"),
                        "records_updated": runtime_db.get("records_updated"),
                        "records_deleted": runtime_db.get("records_deleted"),
                    }
                )

            else:
                registry_runs = registry_run_rows_for_job(job, limit=1)
                if registry_runs:
                    registry = registry_runs[0]
                    runtime_job.update(
                        {
                            "current_status": registry.get("state", "unknown"),
                            "latest_run_id": registry.get("run_id"),
                            "started_at": registry.get("started_at"),
                            "ended_at": registry.get("ended_at"),
                            "duration_seconds": registry.get("duration_seconds"),
                            "target_path": registry.get("target_path") or runtime_job.get("target_path"),
                            "status_reason": registry.get("status_reason"),
                            "runtime_source": "job_run_registry",
                            "runtime_source_rank": 2,
                            "is_fallback": True,
                            "fallback_reason": (
                                "Runtime DB had no matching rows; "
                                "using local job_run_registry JSONL as debug fallback."
                            ),
                            "runtime_available": True,
                            "latest_dag_run_id": registry.get("dag_run_id"),
                            "records_read": registry.get("records_read"),
                            "records_written": registry.get("records_written"),
                            "records_inserted": registry.get("records_inserted"),
                            "records_updated": registry.get("records_updated"),
                            "records_deleted": registry.get("records_deleted"),
                        }
                    )

                elif job_mode_of(job) == "batch":
                    airflow = latest_airflow_task(pipeline_id_of(job), job_name_of(job))
                    runtime_source = "catalog" if airflow.get("missing_airflow_dag") else "airflow"
                    runtime_job.update(
                        {
                            **airflow,
                            "runtime_source": runtime_source,
                            "runtime_available": airflow.get("airflow_available", False),
                        }
                    )

                    if airflow.get("missing_airflow_dag"):
                        metadata = catalog_metadata_for_job(job)
                        if metadata:
                            runtime_job.update(
                                {
                                    "current_status": "onboarded",
                                    "runtime_source": "catalog_metadata",
                                    "runtime_available": False,
                                    "metadata_available": True,
                                    "metadata_job_id": metadata.get("job_id"),
                                    "metadata_job_code": metadata.get("job_code"),
                                    "metadata_pipeline_id": metadata.get("pipeline_id"),
                                    "metadata_airflow_dag_id": metadata.get("airflow_dag_id"),
                                    "source_type": metadata.get("source_type") or runtime_job.get("source_type"),
                                    "target_path": metadata.get("target_path") or runtime_job.get("target_path"),
                                    "status_reason": (
                                        "Job is onboarded in the control DB, but no executable Airflow DAG exists yet. "
                                        "Create a dispatcher DAG or bind this pipeline to an existing DAG to execute it."
                                    ),
                                }
                            )

                elif job_mode_of(job) == "dynamic":
                    runtime_job.update(
                        {
                            "current_status": "defined",
                            "latest_run_id": None,
                            "started_at": None,
                            "ended_at": None,
                            "duration_seconds": None,
                            "status_reason": (
                                "Defined in catalog/config layer; no Airflow executor DAG configured yet. "
                                "Executable jobs must be materialized through pipeline_catalog.json and onboarding."
                            ),
                            "runtime_source": "catalog_defined",
                            "runtime_available": False,
                        }
                    )

        if job_mode_of(runtime_job) == "stream":
            db_unit = stream_current_from_db(runtime_job)
            if db_unit:
                runtime_job.update(
                    {
                        "runtime_available": True,
                        "runtime_source": "stream_runtime_db",
                        "runtime_source_rank": 1,
                        "is_fallback": False,
                        "fallback_reason": None,
                        "runtime_unit_name": db_unit.get("unit_name"),
                        "current_status": db_unit.get("computed_status") or runtime_job.get("current_status"),
                        "heartbeat_age_seconds": db_unit.get("heartbeat_age_seconds"),
                        "last_batch_id": db_unit.get("last_batch_id"),
                        "last_input_rows": db_unit.get("last_input_rows"),
                        "last_valid_rows": db_unit.get("last_valid_rows"),
                        "last_invalid_rows": db_unit.get("last_invalid_rows"),
                        "last_write_ok": db_unit.get("last_write_ok"),
                        "last_error": db_unit.get("last_error"),
                        "stream_target_path": db_unit.get("target_path") or runtime_job.get("target_path"),
                        "records_read": as_int_or_none(first_present(db_unit, "records_read", "last_input_rows", "input_rows")),
                        "records_written": as_int_or_none(first_present(db_unit, "records_written", "last_valid_rows", "valid_rows")),
                        "records_inserted": as_int_or_none(first_present(db_unit, "records_inserted", "last_valid_rows", "valid_rows")),
                        "records_updated": as_int_or_none(first_present(db_unit, "records_updated", "updated_rows", default=0)),
                        "records_deleted": as_int_or_none(first_present(db_unit, "records_deleted", "deleted_rows", default=0)),
                    }
                )
            else:
                unit = find_stream_unit(runtime_job, stream_status)
                if unit:
                    heartbeat = unit.get("heartbeat") or {}
                    runtime_job.update(
                        {
                            "runtime_available": True,
                            "runtime_source": "stream_supervisor_file",
                            "runtime_source_rank": 2,
                            "is_fallback": True,
                            "fallback_reason": "Stream runtime DB had no current row; using supervisor status file fallback.",
                            "runtime_unit_name": unit.get("unit_name"),
                            "current_status": unit.get("computed_status") or runtime_job.get("current_status"),
                            "heartbeat_age_seconds": unit.get("heartbeat_age_seconds"),
                            "last_batch_id": heartbeat.get("last_batch_id"),
                            "last_input_rows": heartbeat.get("last_input_rows"),
                            "last_valid_rows": heartbeat.get("last_valid_rows"),
                            "last_invalid_rows": heartbeat.get("last_invalid_rows"),
                            "last_write_ok": heartbeat.get("last_write_ok"),
                            "last_error": heartbeat.get("last_error"),
                            "stream_target_path": heartbeat.get("silver_path") or heartbeat.get("bronze_path") or runtime_job.get("target_path"),
                            "records_read": as_int_or_none(first_present(heartbeat, "records_read", "input_rows")),
                            "records_written": as_int_or_none(first_present(heartbeat, "records_written", "output_rows")),
                            "records_inserted": as_int_or_none(first_present(heartbeat, "records_inserted", "inserted_rows")),
                            "records_updated": as_int_or_none(first_present(heartbeat, "records_updated", "updated_rows", default=0)),
                            "records_deleted": as_int_or_none(first_present(heartbeat, "records_deleted", "deleted_rows", default=0)),
                        }
                    )

        enriched.append(runtime_job)

    return {"jobs": enriched, "stream_status": stream_status}


