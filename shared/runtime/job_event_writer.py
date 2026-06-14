from __future__ import annotations

import json
import os
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any


JOB_RUN_EVENT_TYPES = {
    "batch_started",
    "batch_succeeded",
    "batch_failed",
    "stream_started",
    "stream_exited",
}


def new_run_id(prefix: str | None = None) -> str:
    # Keep the optional prefix argument for backward-compatible callers, but do
    # not include it in the persisted run_id. The runtime contract is GUID-only.
    _ = prefix
    return str(uuid.uuid4())


def exception_to_text(exc: BaseException) -> str:
    return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))


def append_job_event(**payload: Any) -> None:
    event = dict(payload)
    event_type = str(event.get("event_type") or "runtime_event")
    event["run_id"] = _coerce_run_id(event.get("run_id"))
    event["event_type"] = event_type

    if not _control_db_enabled():
        return

    try:
        _insert_job_event(event)

        if event_type in JOB_RUN_EVENT_TYPES:
            _upsert_job_run(event)

    except Exception as exc:
        print(f"[WARN] Failed to write runtime job event: {exc}", flush=True)

        if _control_db_strict():
            raise


def _control_db_enabled() -> bool:
    enabled = os.getenv("CONTROL_DB_ENABLED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }

    if not enabled:
        return False

    try:
        from shared.control.postgres import control_db_enabled

        return control_db_enabled()
    except Exception as exc:
        print(f"[WARN] Could not check CONTROL_DB_ENABLED: {exc}", flush=True)
        return False


def _call_usp_void(usp_name: str, params: tuple[Any, ...]) -> None:
    from shared.control.postgres import call_usp_void

    call_usp_void(usp_name, params)


def _control_db_strict() -> bool:
    return os.getenv("CONTROL_DB_STRICT", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }


def _json_payload(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)


def _coerce_run_id(value: Any) -> str:
    if value in (None, ""):
        return new_run_id()

    try:
        return str(uuid.UUID(str(value)))
    except Exception as exc:
        raise ValueError(f"run_id must be a GUID/UUID value, got {value!r}") from exc


def _to_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _first_present(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return None


def _epoch_to_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except Exception:
        return None


def _timestamp(payload: dict[str, Any], text_key: str, epoch_key: str) -> Any:
    value = payload.get(text_key)
    if value not in (None, ""):
        return value
    return _epoch_to_datetime(payload.get(epoch_key))


def _status_for_event(event: dict[str, Any]) -> str:
    explicit = event.get("status") or event.get("state")
    if explicit not in (None, ""):
        return str(explicit)

    event_type = str(event.get("event_type") or "")
    if event_type in {"batch_started", "stream_started"}:
        return "running"
    if event_type == "batch_succeeded":
        return "success"
    if event_type == "batch_failed":
        return "failed"
    if event_type == "stream_exited":
        return "stopped"
    return "unknown"


def _event_message(event: dict[str, Any]) -> str | None:
    value = _first_present(
        event,
        "event_message",
        "status_reason",
        "error_message",
        "error",
        "status",
        "state",
    )
    return _to_text(value)


def _insert_job_event(event: dict[str, Any]) -> None:
    _call_usp_void(
        "usp_insert_job_event",
        (
            event["run_id"],
            _safe_int(event.get("job_id")),
            _to_text(_first_present(event, "job_key", "job_code")),
            str(event.get("event_type") or "runtime_event"),
            _event_message(event),
            _json_payload(event),
        ),
    )


def _upsert_job_run(event: dict[str, Any]) -> None:
    job_code = _to_text(_first_present(event, "job_code", "job_key"))
    job_name = _to_text(_first_present(event, "job_name", "job"))
    pipeline_name = _to_text(_first_present(event, "pipeline_name", "pipeline"))

    status = _status_for_event(event)
    error_message = _to_text(_first_present(event, "error_message", "error"))

    _call_usp_void(
        "usp_upsert_job_run",
        (
            event["run_id"],
            _safe_int(event.get("job_id")),
            job_code,
            job_name,
            pipeline_name,
            _to_text(event.get("base_job_name")),
            _to_text(event.get("source_id")),
            _to_text(event.get("table_id")),
            _to_text(event.get("entity_name")),
            _to_text(event.get("layer")),
            _to_text(event.get("runner")),
            _to_text(event.get("airflow_dag_id")),
            _to_text(event.get("airflow_dag_run_id")),
            _to_text(event.get("airflow_task_id")),
            _safe_int(event.get("airflow_try_number")),
            status,
            _to_text(event.get("status_reason") or _event_message(event)),
            error_message,
            _timestamp(event, "effective_start_date", "effective_start_epoch"),
            _timestamp(event, "effective_end_date", "effective_end_epoch"),
            _timestamp(event, "started_at", "started_at_epoch"),
            _timestamp(event, "ended_at", "ended_at_epoch"),
            event.get("duration_seconds"),
            _safe_int(_first_present(event, "records_read", "rows_read", "input_rows")),
            _safe_int(
                _first_present(
                    event,
                    "records_written",
                    "rows_written",
                    "written_rows",
                    "last_written_rows",
                )
            ),
            _to_text(_first_present(event, "source_path", "bronze_path")),
            _to_text(_first_present(event, "target_path", "silver_path", "bronze_path")),
            _json_payload(event),
        ),
    )
