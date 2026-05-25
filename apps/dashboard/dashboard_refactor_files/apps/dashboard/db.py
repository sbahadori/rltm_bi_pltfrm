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
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import psycopg2
import psycopg2.extras


def _db_config() -> dict[str, Any]:
    return {
        "host": os.getenv("CONTROL_DB_HOST", "postgres-warehouse"),
        "port": int(os.getenv("CONTROL_DB_PORT", "5432")),
        "dbname": os.getenv("CONTROL_DB_NAME", os.getenv("POSTGRES_DB", "warehouse")),
        "user": os.getenv("CONTROL_DB_USER", os.getenv("POSTGRES_USER", "warehouse")),
        "password": os.getenv("CONTROL_DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "warehouse")),
        "sslmode": os.getenv("CONTROL_DB_SSLMODE", "disable"),
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
