from __future__ import annotations

import json
import os
import socket
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from shared.control.postgres import control_db_enabled, execute
from shared.control.metadata_store import load_job_identity


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def registry_path() -> Path:
    return Path(
        os.getenv(
            "JOB_RUN_REGISTRY_FILE",
            "/workspace/rltm_bi_pltfrm/runtime/job_runs/job_runs.jsonl",
        )
    )


def new_run_id(prefix: str | None = None) -> str:
    return str(uuid.uuid4())


def _write_jsonl(payload: dict[str, Any]) -> None:
    path = registry_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _infer_job_key(payload: dict[str, Any]) -> str | None:
    value = payload.get("job_key")
    if value:
        return str(value)

    value = payload.get("job_code")
    if value:
        return str(value)

    pipeline = payload.get("pipeline") or payload.get("pipeline_name")
    job = payload.get("job") or payload.get("job_name")

    if pipeline and job:
        return f"{pipeline}::{job}"

    return None


def _infer_job_code(payload: dict[str, Any]) -> str | None:
    value = payload.get("job_code")
    if value:
        return str(value)

    value = payload.get("job_key")
    if value:
        return str(value)

    return None


def _resolve_job_identity(payload: dict[str, Any]) -> dict[str, Any]:
    return load_job_identity(
        job_id=_safe_int(payload.get("job_id")),
        job_key=_infer_job_key(payload),
        job_code=_infer_job_code(payload),
    )


def _upsert_job_run(payload: dict[str, Any]) -> None:
    job_identity = _resolve_job_identity(payload)

    job_id = int(job_identity["job_id"])
    job_key = job_identity.get("job_key")
    job_code = (
        payload.get("job_code")
        or job_identity.get("job_code")
        or job_key
    )

    if not job_code:
        raise ValueError(f"Cannot resolve job_code for payload={payload}")

    run_id = str(payload["run_id"])
    status = str(payload.get("status", "unknown"))

    pipeline_name = str(
        payload.get("pipeline_name")
        or payload.get("pipeline")
        or job_identity.get("pipeline_name")
    )

    job_name = str(
        payload.get("job_name")
        or payload.get("job")
        or job_identity.get("job_name")
    )

    base_job_name = (
        payload.get("base_job_name")
        or job_identity.get("base_job_name")
    )

    started_at_epoch = payload.get("started_at_epoch")
    ended_at_epoch = payload.get("ended_at_epoch")

    started_at_expr = "to_timestamp(%s)" if started_at_epoch is not None else "NULL"
    ended_at_expr = "to_timestamp(%s)" if ended_at_epoch is not None else "NULL"

    params: list[Any] = [
        run_id,
        job_id,
        job_code,
        job_name,
        pipeline_name,
        base_job_name,
        payload.get("source_id") or job_identity.get("source_id"),
        payload.get("table_id") or job_identity.get("table_id"),
        payload.get("entity_name") or job_identity.get("entity_name"),
        payload.get("layer") or job_identity.get("layer"),
        payload.get("runner") or job_identity.get("runner"),
        payload.get("airflow_dag_id"),
        payload.get("airflow_dag_run_id"),
        payload.get("airflow_task_id"),
        _safe_int(payload.get("airflow_try_number")),
        status,
        payload.get("status_reason"),
        payload.get("error"),
        payload.get("effective_start_date"),
        payload.get("effective_end_date"),
    ]

    if started_at_epoch is not None:
        params.append(started_at_epoch)

    if ended_at_epoch is not None:
        params.append(ended_at_epoch)

    params.extend(
        [
            payload.get("duration_seconds"),
            payload.get("records_read"),
            payload.get("records_written"),
            payload.get("source_path"),
            payload.get("target_path") or job_identity.get("target_path"),
            json.dumps(
                {
                    **payload,
                    "resolved_job_id": job_id,
                    "resolved_job_key": job_key,
                    "resolved_job_code": job_code,
                },
                default=str,
            ),
        ]
    )

    sql = f"""
        INSERT INTO runtime.job_run (
            run_id,
            job_id,
            job_code,
            job_name,
            pipeline_name,
            base_job_name,
            source_id,
            table_id,
            entity_name,
            layer,
            runner,
            airflow_dag_id,
            airflow_dag_run_id,
            airflow_task_id,
            airflow_try_number,
            status,
            status_reason,
            error_message,
            effective_start_date,
            effective_end_date,
            started_at,
            ended_at,
            duration_seconds,
            records_read,
            records_written,
            source_path,
            target_path,
            payload
        )
        VALUES (
            %s::uuid,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s::timestamp,
            %s::timestamp,
            {started_at_expr},
            {ended_at_expr},
            %s,
            %s,
            %s,
            %s,
            %s,
            %s::jsonb
        )
        ON CONFLICT (run_id)
        DO UPDATE SET
            status = EXCLUDED.status,
            ended_at = COALESCE(EXCLUDED.ended_at, runtime.job_run.ended_at),
            duration_seconds = COALESCE(EXCLUDED.duration_seconds, runtime.job_run.duration_seconds),
            records_read = COALESCE(EXCLUDED.records_read, runtime.job_run.records_read),
            records_written = COALESCE(EXCLUDED.records_written, runtime.job_run.records_written),
            error_message = COALESCE(EXCLUDED.error_message, runtime.job_run.error_message),
            status_reason = COALESCE(EXCLUDED.status_reason, runtime.job_run.status_reason),
            effective_start_date = COALESCE(EXCLUDED.effective_start_date, runtime.job_run.effective_start_date),
            effective_end_date = COALESCE(EXCLUDED.effective_end_date, runtime.job_run.effective_end_date),
            source_path = COALESCE(EXCLUDED.source_path, runtime.job_run.source_path),
            target_path = COALESCE(EXCLUDED.target_path, runtime.job_run.target_path),
            payload = EXCLUDED.payload
    """

    execute(sql, tuple(params))


def _insert_job_event(payload: dict[str, Any]) -> None:
    job_identity = _resolve_job_identity(payload)

    job_id = int(job_identity["job_id"])
    job_key = (
        payload.get("job_key")
        or job_identity.get("job_key")
        or payload.get("job_code")
        or job_identity.get("job_code")
    )

    execute(
        """
        INSERT INTO runtime.job_event (
            run_id,
            job_id,
            job_key,
            event_type,
            event_message,
            event_payload
        )
        VALUES (%s::uuid, %s, %s, %s, %s, %s::jsonb)
        """,
        (
            payload.get("run_id"),
            job_id,
            job_key,
            payload.get("event_type", "unknown_event"),
            payload.get("event_message") or payload.get("status"),
            json.dumps(payload, default=str),
        ),
    )


def append_job_event(**event: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "event_id": uuid.uuid4().hex,
        "ts": utc_now_iso(),
        "ts_epoch": int(time.time()),
        "host": socket.gethostname(),
        **event,
    }

    _write_jsonl(payload)

    if control_db_enabled() and payload.get("run_id"):
        try:
            if payload.get("event_type") in {
                "batch_started",
                "batch_succeeded",
                "batch_failed",
                "stream_started",
                "stream_succeeded",
                "stream_failed",
            }:
                _upsert_job_run(payload)

            _insert_job_event(payload)

        except Exception as exc:
            print(f"[WARN] Failed to write runtime event to control DB: {exc}", flush=True)

    return payload


def exception_to_text(exc: BaseException) -> str:
    return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))