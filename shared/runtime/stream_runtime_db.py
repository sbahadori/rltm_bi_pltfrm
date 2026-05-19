from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from shared.control.postgres import call_usp_void, control_db_enabled

logger = logging.getLogger("stream_runtime_db")


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _safe_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _epoch_to_utc_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except Exception:
        return None


def _json_payload(payload: dict[str, Any] | None) -> str:
    return json.dumps(payload or {}, ensure_ascii=False, default=str)


def _db_strict() -> bool:
    return os.getenv("CONTROL_DB_STRICT", "false").lower() in {"1", "true", "yes"}


def _call_usp_noncritical(usp_name: str, params: tuple[Any, ...]) -> None:
    if not control_db_enabled():
        return

    try:
        call_usp_void(usp_name, params)
    except Exception as exc:
        logger.warning(
            "Failed to write stream runtime data through %s: %s",
            usp_name,
            exc,
            exc_info=True,
        )
        if _db_strict():
            raise


def upsert_stream_unit_current(
    *,
    unit_name: str,
    stream_name: str,
    layer: str,
    computed_status: str | None,
    status_reason: str | None = None,
    pid: int | None = None,
    returncode: int | None = None,
    retries: int | None = None,
    max_retries: int | None = None,
    heartbeat_status: str | None = None,
    heartbeat_ts_epoch: int | None = None,
    heartbeat_age_seconds: int | None = 0,
    last_batch_id: int | None = None,
    last_input_rows: int | None = None,
    last_batch_rows: int | None = None,
    last_valid_rows: int | None = None,
    last_invalid_rows: int | None = None,
    last_written_rows: int | None = None,
    last_written_valid_rows: int | None = None,
    last_written_invalid_rows: int | None = None,
    last_write_ok: bool | None = None,
    last_message: str | None = None,
    last_error: str | None = None,
    target_path: str | None = None,
    checkpoint_path: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    _call_usp_noncritical(
        "usp_upsert_stream_unit_current",
        (
            unit_name,
            stream_name,
            layer,
            computed_status,
            status_reason,
            _safe_int(pid),
            _safe_int(returncode),
            _safe_int(retries),
            _safe_int(max_retries),
            heartbeat_status,
            _epoch_to_utc_dt(heartbeat_ts_epoch),
            _safe_int(heartbeat_age_seconds),
            _safe_int(last_batch_id),
            _safe_int(last_input_rows),
            _safe_int(last_batch_rows),
            _safe_int(last_valid_rows),
            _safe_int(last_invalid_rows),
            _safe_int(last_written_rows),
            _safe_int(last_written_valid_rows),
            _safe_int(last_written_invalid_rows),
            _safe_bool(last_write_ok),
            last_message,
            last_error,
            target_path,
            checkpoint_path,
            _json_payload(payload),
        ),
    )


def upsert_stream_unit_current_from_heartbeat(
    *,
    unit_name: str,
    stream_name: str,
    layer: str,
    heartbeat: dict[str, Any],
    status_reason: str | None = None,
) -> None:
    target_path = (
        heartbeat.get("silver_path")
        or heartbeat.get("bronze_path")
        or heartbeat.get("target_path")
    )

    upsert_stream_unit_current(
        unit_name=unit_name,
        stream_name=stream_name,
        layer=layer,
        computed_status=heartbeat.get("status"),
        status_reason=status_reason or heartbeat.get("last_message"),
        heartbeat_status=heartbeat.get("status"),
        heartbeat_ts_epoch=_safe_int(heartbeat.get("ts_epoch")),
        heartbeat_age_seconds=0,
        last_batch_id=_safe_int(heartbeat.get("last_batch_id")),
        last_input_rows=_safe_int(heartbeat.get("last_input_rows")),
        last_batch_rows=_safe_int(heartbeat.get("last_batch_rows")),
        last_valid_rows=_safe_int(heartbeat.get("last_valid_rows")),
        last_invalid_rows=_safe_int(heartbeat.get("last_invalid_rows")),
        last_written_rows=_safe_int(heartbeat.get("last_written_rows")),
        last_written_valid_rows=_safe_int(heartbeat.get("last_written_valid_rows")),
        last_written_invalid_rows=_safe_int(heartbeat.get("last_written_invalid_rows")),
        last_write_ok=_safe_bool(heartbeat.get("last_write_ok")),
        last_message=heartbeat.get("last_message"),
        last_error=heartbeat.get("last_error"),
        target_path=target_path,
        checkpoint_path=heartbeat.get("checkpoint_path"),
        payload=heartbeat,
    )


def insert_stream_batch_metric(
    *,
    unit_name: str,
    stream_name: str,
    layer: str,
    batch_id: int,
    input_rows: int | None = None,
    batch_rows: int | None = None,
    valid_rows: int | None = None,
    invalid_rows: int | None = None,
    written_rows: int | None = None,
    written_valid_rows: int | None = None,
    written_invalid_rows: int | None = None,
    write_ok: bool,
    message: str | None = None,
    error_message: str | None = None,
    target_path: str | None = None,
    checkpoint_path: str | None = None,
    batch_ts_epoch: int | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    _call_usp_noncritical(
        "usp_insert_stream_batch_metric",
        (
            unit_name,
            stream_name,
            layer,
            _safe_int(batch_id),
            _safe_int(input_rows),
            _safe_int(batch_rows),
            _safe_int(valid_rows),
            _safe_int(invalid_rows),
            _safe_int(written_rows),
            _safe_int(written_valid_rows),
            _safe_int(written_invalid_rows),
            _safe_bool(write_ok),
            message,
            error_message,
            target_path,
            checkpoint_path,
            _epoch_to_utc_dt(batch_ts_epoch) or datetime.now(timezone.utc),
            _json_payload(payload),
        ),
    )