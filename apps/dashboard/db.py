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


def insert_runtime_event(event: dict[str, Any]) -> bool:
    """
    Best-effort durable runtime event persistence.

    JSONL remains the primary local fallback in Phase 1.
    A DB write failure must not break job execution or reconciliation.
    """
    sql = """
        INSERT INTO runtime.job_run_event (
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
        )
        VALUES (
            %(event_id)s,
            %(event_type)s,
            %(observed_at)s,
            %(ts_epoch)s,
            %(run_id)s,
            %(executor_run_id)s,
            %(job_id)s,
            %(job)s,
            %(job_name)s,
            %(pipeline)s,
            %(pipeline_id)s,
            %(job_code)s,
            %(runner_id)s,
            %(state)s,
            %(status)s,
            %(executor_type)s,
            %(executor_id)s,
            %(executor_state)s,
            %(started_at)s,
            %(ended_at)s,
            %(duration_seconds)s,
            %(records_read)s,
            %(records_written)s,
            %(records_inserted)s,
            %(records_updated)s,
            %(records_deleted)s,
            %(runtime_source)s,
            %(status_reason)s,
            %(payload)s
        )
        ON CONFLICT (event_id) DO NOTHING
    """

    params = {
        "event_id": event.get("event_id"),
        "event_type": event.get("event_type"),
        "observed_at": event.get("observed_at"),
        "ts_epoch": event.get("ts_epoch"),
        "run_id": event.get("run_id"),
        "executor_run_id": event.get("executor_run_id"),
        "job_id": event.get("job_id"),
        "job": event.get("job"),
        "job_name": event.get("job_name"),
        "pipeline": event.get("pipeline"),
        "pipeline_id": event.get("pipeline_id"),
        "job_code": event.get("job_code"),
        "runner_id": event.get("runner_id"),
        "state": event.get("state"),
        "status": event.get("status"),
        "executor_type": event.get("executor_type"),
        "executor_id": event.get("executor_id"),
        "executor_state": event.get("executor_state"),
        "started_at": event.get("started_at"),
        "ended_at": event.get("ended_at"),
        "duration_seconds": event.get("duration_seconds"),
        "records_read": event.get("records_read"),
        "records_written": event.get("records_written"),
        "records_inserted": event.get("records_inserted"),
        "records_updated": event.get("records_updated"),
        "records_deleted": event.get("records_deleted"),
        "runtime_source": event.get("runtime_source"),
        "status_reason": event.get("status_reason"),
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
        print(f"[WARN] Failed to write runtime event to DB: {exc}", flush=True)
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
