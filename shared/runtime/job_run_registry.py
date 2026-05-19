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

from shared.control.metadata_store import load_job_identity
from shared.control.postgres import call_usp_void, control_db_enabled


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


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None

    try:
        return float(value)
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


def _epoch_to_naive_utc(value: Any) -> datetime | None:
    if value in (None, ""):
        return None

    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


def _iso_to_naive_utc(value: Any) -> datetime | None:
    if value in (None, ""):
        return None

    try:
        text = str(value)

        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        dt = datetime.fromisoformat(text)

        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

        return dt
    except Exception:
        return None


def _upsert_job_run(payload: dict[str, Any]) -> None:
    job_identity = _resolve_job_identity(payload)

    job_id = int(job_identity["job_id"])

    job_key = (
        payload.get("job_key")
        or job_identity.get("job_key")
        or payload.get("job_code")
        or job_identity.get("job_code")
    )

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

    source_id = payload.get("source_id") or job_identity.get("source_id")
    table_id = payload.get("table_id") or job_identity.get("table_id")
    entity_name = payload.get("entity_name") or job_identity.get("entity_name")
    layer = payload.get("layer") or job_identity.get("layer")
    runner = payload.get("runner") or job_identity.get("runner")
    target_path = payload.get("target_path") or job_identity.get("target_path")

    resolved_payload = {
        **payload,
        "resolved_job_id": job_id,
        "resolved_job_key": job_key,
        "resolved_job_code": job_code,
    }

    call_usp_void(
        "usp_upsert_job_run",
        (
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
            payload.get("airflow_dag_id"),
            payload.get("airflow_dag_run_id"),
            payload.get("airflow_task_id"),
            _safe_int(payload.get("airflow_try_number")),
            status,
            payload.get("status_reason"),
            payload.get("error"),
            _iso_to_naive_utc(payload.get("effective_start_date")),
            _iso_to_naive_utc(payload.get("effective_end_date")),
            _epoch_to_naive_utc(payload.get("started_at_epoch")),
            _epoch_to_naive_utc(payload.get("ended_at_epoch")),
            _safe_float(payload.get("duration_seconds")),
            _safe_int(payload.get("records_read")),
            _safe_int(payload.get("records_written")),
            payload.get("source_path"),
            target_path,
            json.dumps(resolved_payload, default=str),
        ),
    )


def _insert_job_event(payload: dict[str, Any]) -> None:
    job_identity = _resolve_job_identity(payload)

    job_id = int(job_identity["job_id"])
    job_key = (
        payload.get("job_key")
        or job_identity.get("job_key")
        or payload.get("job_code")
        or job_identity.get("job_code")
    )

    call_usp_void(
        "usp_insert_job_event",
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

            if os.getenv("CONTROL_DB_STRICT", "false").lower() in {"1", "true", "yes"}:
                raise

    return payload


def exception_to_text(exc: BaseException) -> str:
    return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))