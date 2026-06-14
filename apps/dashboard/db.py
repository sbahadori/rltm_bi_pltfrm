from __future__ import annotations

"""
Database gateway for the dashboard service.

Single responsibility:
- Own Control DB connection settings.
- Own safe USP invocation helpers.
- Own JSON-safe conversion of DB values.
- Own small Control DB metadata lookup used by the runtime resolver.

No FastAPI routes, no Airflow logic, no stream-supervisor logic belongs here.
"""

import json
import os
try:
    from apps.dashboard.settings import get_settings
except ImportError:  # pragma: no cover
    from .settings import get_settings
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import psycopg2
import psycopg2.extras


def _db_config() -> dict[str, Any]:
    settings = get_settings()
    return {
        "host": settings.control_db_host,
        "port": settings.control_db_port,
        "dbname": settings.control_db_name,
        "user": settings.control_db_user,
        "password": settings.control_db_password,
        "sslmode": settings.control_db_sslmode,
    }

def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    return value


def _validate_usp(name: str) -> str:
    if not name.startswith("usp_") or not all(ch.isalnum() or ch == "_" for ch in name):
        raise ValueError(f"Invalid USP name: {name}")
    return name


def call_usp_rows(usp: str, params: tuple = ()) -> list[dict[str, Any]]:
    usp = _validate_usp(usp)
    placeholders = ", ".join(["%s"] * len(params))

    conn = psycopg2.connect(**_db_config())
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM ctl.{usp}({placeholders})", params)
            return _json_safe([dict(row) for row in cur.fetchall()])
    finally:
        conn.close()


def call_usp_one(usp: str, params: tuple = ()) -> dict[str, Any] | None:
    rows = call_usp_rows(usp, params)
    return rows[0] if rows else None


def call_usp_void(usp: str, params: tuple = ()) -> None:
    usp = _validate_usp(usp)
    placeholders = ", ".join(["%s"] * len(params))

    conn = psycopg2.connect(**_db_config())
    try:
        with conn.cursor() as cur:
            cur.execute(f"CALL ctl.{usp}({placeholders})", params)
        conn.commit()
    finally:
        conn.close()


def insert_action_log(
    *,
    user: dict[str, Any],
    action_type: str,
    target_type: str | None,
    target_id: str | None,
    request_payload: dict[str, Any],
    result_status: str,
    result_payload: dict[str, Any] | None = None,
    error_message: str | None = None,
    duration_ms: int | None = None,
) -> None:
    """Best-effort action audit logging. Runtime actions must not fail only because logging fails."""
    try:
        call_usp_void(
            "usp_insert_action_log",
            (
                user.get("sub", "unknown"),
                user.get("user_id"),
                action_type,
                target_type,
                target_id,
                json.dumps(request_payload),
                result_status,
                json.dumps(result_payload or {}),
                error_message,
                duration_ms,
            ),
        )
    except Exception as exc:
        print(f"[WARN] Failed to write action log: {exc}", flush=True)


def catalog_metadata_for_job(job: dict[str, Any]) -> dict[str, Any] | None:
    """
    Return Control DB metadata for a catalog-defined job.

    This is metadata, not runtime state. It tells the dashboard whether onboarding
    loaded the job into meta.pipeline/meta.job.
    """
    pipeline_name = job.get("pipeline")
    job_name = job.get("name")

    if not pipeline_name or not job_name:
        return None

    sql = """
        SELECT
            p.pipeline_id,
            p.pipeline_name,
            p.airflow_dag_id,
            p.is_active AS pipeline_is_active,
            j.job_id,
            j.job_code,
            j.job_name,
            j.job_type,
            j.runner,
            j.layer,
            j.source_type,
            j.target_path,
            j.is_active AS job_is_active,
            j.updated_at AS job_updated_at
        FROM meta.job j
        JOIN meta.pipeline p
          ON p.pipeline_name = j.pipeline_name
        WHERE j.pipeline_name = %s
          AND j.job_name = %s
          AND j.is_active = TRUE
        LIMIT 1
    """

    conn = psycopg2.connect(**_db_config())
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (pipeline_name, job_name))
            row = cur.fetchone()
            return _json_safe(dict(row)) if row else None
    except Exception as exc:
        print(f"[WARN] Failed to read catalog metadata for {pipeline_name}.{job_name}: {exc}", flush=True)
        return None
    finally:
        conn.close()


def _as_bigint_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        text = str(value).strip()
        return int(text) if text.isdigit() else None
    except Exception:
        return None


def insert_runtime_event(event: dict[str, Any]) -> bool:
    """
    Best-effort insert into the platform's real runtime event log.

    Uses existing database contract:
    ctl.usp_insert_job_event(
        p_run_id,
        p_job_id,
        p_job_key,
        p_event_type,
        p_event_message,
        p_event_payload
    )
    """
    run_id = event.get("run_id") or event.get("executor_run_id")
    job_id = _as_bigint_or_none(event.get("job_id"))
    job_key = (
        event.get("job_key")
        or event.get("job_code")
        or event.get("entity_name")
        or event.get("job_name")
        or event.get("job")
    )
    event_type = event.get("event_type") or "runtime_event"
    event_message = (
        event.get("status")
        or event.get("state")
        or event.get("status_reason")
        or event.get("error_message")
    )

    try:
        call_usp_void(
            "usp_insert_job_event",
            (
                run_id,
                job_id,
                job_key,
                event_type,
                event_message,
                json.dumps(_json_safe(event)),
            ),
        )
        return True
    except Exception as exc:
        print(f"[WARN] Failed to write runtime job_event: {exc}", flush=True)
        return False

def upsert_runtime_job_run(event: dict[str, Any]) -> bool:
    """
    Best-effort upsert into runtime.job_run using ctl.usp_upsert_job_run.
    """
    run_id = event.get("run_id") or event.get("executor_run_id")
    if not run_id:
        return False

    status = event.get("status") or event.get("state") or "unknown"

    try:
        call_usp_void(
            "usp_upsert_job_run",
            (
                run_id,
                _as_bigint_or_none(event.get("job_id")),
                event.get("job_code") or event.get("job_key"),
                event.get("job_name") or event.get("job"),
                event.get("pipeline_name") or event.get("pipeline") or event.get("pipeline_id"),
                event.get("base_job_name") or event.get("job_name") or event.get("job"),
                event.get("source_id"),
                event.get("table_id"),
                event.get("entity_name"),
                event.get("layer"),
                event.get("runner") or event.get("runner_id"),
                event.get("airflow_dag_id") or event.get("executor_id"),
                event.get("airflow_dag_run_id") or event.get("executor_run_id"),
                event.get("airflow_task_id") or event.get("job_name") or event.get("job"),
                _as_bigint_or_none(event.get("airflow_try_number")),
                status,
                event.get("status_reason"),
                event.get("error_message"),
                event.get("effective_start_date"),
                event.get("effective_end_date"),
                event.get("started_at") or event.get("ts"),
                event.get("ended_at"),
                event.get("duration_seconds"),
                event.get("records_read"),
                event.get("records_written"),
                event.get("source_path"),
                event.get("target_path") or event.get("output_path"),
                json.dumps(_json_safe(event)),
            ),
        )
        return True
    except Exception as exc:
        print(f"[WARN] Failed to upsert runtime job_run: {exc}", flush=True)
        return False
    
def upsert_runtime_job_run_state(event: dict[str, Any]) -> bool:
    run_id = event.get("run_id") or event.get("executor_run_id")
    if not run_id:
        return False

    sql = """
        INSERT INTO runtime.job_run_state (
            run_id,
            latest_event_id,
            latest_event_type,
            state,
            status,
            executor_run_id,
            executor_type,
            executor_id,
            executor_state,
            job_id,
            job,
            job_name,
            pipeline,
            pipeline_id,
            job_code,
            runner_id,
            started_at,
            ended_at,
            duration_seconds,
            records_read,
            records_written,
            records_inserted,
            records_updated,
            records_deleted,
            target_path,
            runtime_source,
            status_reason,
            first_observed_at,
            last_observed_at,
            last_ts_epoch,
            payload
        )
        VALUES (
            %(run_id)s,
            %(latest_event_id)s,
            %(latest_event_type)s,
            %(state)s,
            %(status)s,
            %(executor_run_id)s,
            %(executor_type)s,
            %(executor_id)s,
            %(executor_state)s,
            %(job_id)s,
            %(job)s,
            %(job_name)s,
            %(pipeline)s,
            %(pipeline_id)s,
            %(job_code)s,
            %(runner_id)s,
            %(started_at)s,
            %(ended_at)s,
            %(duration_seconds)s,
            %(records_read)s,
            %(records_written)s,
            %(records_inserted)s,
            %(records_updated)s,
            %(records_deleted)s,
            %(target_path)s,
            %(runtime_source)s,
            %(status_reason)s,
            %(observed_at)s,
            %(observed_at)s,
            %(ts_epoch)s,
            %(payload)s
        )
        ON CONFLICT (run_id) DO UPDATE SET
            latest_event_id = EXCLUDED.latest_event_id,
            latest_event_type = EXCLUDED.latest_event_type,
            state = EXCLUDED.state,
            status = EXCLUDED.status,
            executor_run_id = COALESCE(EXCLUDED.executor_run_id, runtime.job_run_state.executor_run_id),
            executor_type = COALESCE(EXCLUDED.executor_type, runtime.job_run_state.executor_type),
            executor_id = COALESCE(EXCLUDED.executor_id, runtime.job_run_state.executor_id),
            executor_state = EXCLUDED.executor_state,
            job_id = COALESCE(EXCLUDED.job_id, runtime.job_run_state.job_id),
            job = COALESCE(EXCLUDED.job, runtime.job_run_state.job),
            job_name = COALESCE(EXCLUDED.job_name, runtime.job_run_state.job_name),
            pipeline = COALESCE(EXCLUDED.pipeline, runtime.job_run_state.pipeline),
            pipeline_id = COALESCE(EXCLUDED.pipeline_id, runtime.job_run_state.pipeline_id),
            job_code = COALESCE(EXCLUDED.job_code, runtime.job_run_state.job_code),
            runner_id = COALESCE(EXCLUDED.runner_id, runtime.job_run_state.runner_id),
            started_at = COALESCE(runtime.job_run_state.started_at, EXCLUDED.started_at),
            ended_at = COALESCE(EXCLUDED.ended_at, runtime.job_run_state.ended_at),
            duration_seconds = COALESCE(EXCLUDED.duration_seconds, runtime.job_run_state.duration_seconds),
            records_read = COALESCE(EXCLUDED.records_read, runtime.job_run_state.records_read),
            records_written = COALESCE(EXCLUDED.records_written, runtime.job_run_state.records_written),
            records_inserted = COALESCE(EXCLUDED.records_inserted, runtime.job_run_state.records_inserted),
            records_updated = COALESCE(EXCLUDED.records_updated, runtime.job_run_state.records_updated),
            records_deleted = COALESCE(EXCLUDED.records_deleted, runtime.job_run_state.records_deleted),
            target_path = COALESCE(EXCLUDED.target_path, runtime.job_run_state.target_path),
            runtime_source = EXCLUDED.runtime_source,
            status_reason = COALESCE(EXCLUDED.status_reason, runtime.job_run_state.status_reason),
            last_observed_at = EXCLUDED.last_observed_at,
            last_ts_epoch = EXCLUDED.last_ts_epoch,
            payload = EXCLUDED.payload,
            updated_at = now()
        WHERE
            runtime.job_run_state.last_ts_epoch IS NULL
            OR EXCLUDED.last_ts_epoch >= runtime.job_run_state.last_ts_epoch
    """

    params = {
        "run_id": run_id,
        "latest_event_id": event.get("event_id"),
        "latest_event_type": event.get("event_type"),
        "state": event.get("state") or event.get("status"),
        "status": event.get("status") or event.get("state"),
        "executor_run_id": event.get("executor_run_id") or event.get("airflow_dag_run_id"),
        "executor_type": event.get("executor_type") or "airflow",
        "executor_id": event.get("executor_id") or event.get("airflow_dag_id"),
        "executor_state": event.get("executor_state"),
        "job_id": str(event.get("job_id")) if event.get("job_id") is not None else None,
        "job": event.get("job") or event.get("job_name"),
        "job_name": event.get("job_name") or event.get("job"),
        "pipeline": event.get("pipeline") or event.get("pipeline_name"),
        "pipeline_id": event.get("pipeline_id") or event.get("pipeline") or event.get("pipeline_name"),
        "job_code": event.get("job_code") or event.get("job_key"),
        "runner_id": event.get("runner_id") or event.get("runner"),
        "started_at": event.get("started_at") or event.get("ts"),
        "ended_at": event.get("ended_at"),
        "duration_seconds": event.get("duration_seconds"),
        "records_read": event.get("records_read"),
        "records_written": event.get("records_written"),
        "records_inserted": event.get("records_inserted"),
        "records_updated": event.get("records_updated"),
        "records_deleted": event.get("records_deleted"),
        "target_path": event.get("target_path") or event.get("output_path"),
        "runtime_source": event.get("runtime_source") or "runtime_event_writer",
        "status_reason": event.get("status_reason") or event.get("error_message"),
        "observed_at": event.get("observed_at") or event.get("ts"),
        "ts_epoch": event.get("ts_epoch"),
        "payload": psycopg2.extras.Json(_json_safe(event)),
    }

    try:
        conn = psycopg2.connect(**_db_config())
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception as exc:
        print(f"[WARN] Failed to upsert runtime job_run_state: {exc}", flush=True)
        return False
        

def list_runtime_events(limit: int = 2000) -> list[dict[str, Any]]:
    """
    Read runtime events from PostgreSQL.

    If the runtime schema/table does not exist yet, return [] so JSONL fallback
    can continue to work during migration.
    """
    sql = """
        SELECT
            event_id,
            event_type,
            observed_at,
            ts_epoch,
            run_id,
            executor_run_id,
            job_id,
            job,
            job_name,
            pipeline,
            pipeline_id,
            job_code,
            runner_id,
            state,
            status,
            executor_type,
            executor_id,
            executor_state,
            started_at,
            ended_at,
            duration_seconds,
            records_read,
            records_written,
            records_inserted,
            records_updated,
            records_deleted,
            runtime_source,
            status_reason,
            payload
        FROM runtime.job_run_event
        ORDER BY observed_at DESC
        LIMIT %s
    """

    try:
        conn = psycopg2.connect(**_db_config())
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (limit,))
                rows = [dict(row) for row in cur.fetchall()]
        finally:
            conn.close()

        flattened: list[dict[str, Any]] = []
        for row in rows:
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            merged = {
                **payload,
                **{key: value for key, value in row.items() if key != "payload"},
            }
            flattened.append(merged)

        return sorted(
            _json_safe(flattened),
            key=lambda item: int(item.get("ts_epoch") or 0),
        )

    except Exception as exc:
        print(f"[WARN] Could not read runtime events from DB: {exc}", flush=True)
        return []


def list_runtime_job_run_states(limit: int = 2000) -> list[dict[str, Any]]:
    """
    Read latest runtime state rows from PostgreSQL.

    If the runtime state table does not exist yet, return [] so older fallbacks
    can continue to work.
    """
    sql = """
        SELECT
            run_id,
            latest_event_id AS event_id,
            latest_event_type AS event_type,
            state,
            status,
            executor_run_id,
            executor_type,
            executor_id,
            executor_state,
            job_id,
            job,
            job_name,
            pipeline,
            pipeline_id,
            job_code,
            runner_id,
            started_at,
            ended_at,
            duration_seconds,
            records_read,
            records_written,
            records_inserted,
            records_updated,
            records_deleted,
            target_path,
            runtime_source,
            status_reason,
            first_observed_at,
            last_observed_at AS observed_at,
            last_ts_epoch AS ts_epoch,
            payload
        FROM runtime.job_run_state
        ORDER BY last_observed_at DESC
        LIMIT %s
    """

    try:
        conn = psycopg2.connect(**_db_config())
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (limit,))
                rows = [dict(row) for row in cur.fetchall()]
        finally:
            conn.close()

        flattened: list[dict[str, Any]] = []
        for row in rows:
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            merged = {
                **payload,
                **{key: value for key, value in row.items() if key != "payload"},
            }
            flattened.append(merged)

        return _json_safe(flattened)

    except Exception as exc:
        print(f"[WARN] Could not read runtime job_run_state from DB: {exc}", flush=True)
        return []