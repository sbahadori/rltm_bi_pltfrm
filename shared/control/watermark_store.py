from __future__ import annotations

from typing import Any

from shared.control.postgres import control_db_enabled, call_usp_one, call_usp_void


def read_watermark_from_control_db(
    *,
    job_key: str,
    source_id: str,
    table_id: str,
    watermark_column: str,
    default_value: Any,
) -> str:
    if not control_db_enabled():
        return str(default_value)

    row = call_usp_one(
        "usp_get_watermark_state",
        (
            job_key,
            source_id,
            table_id,
            watermark_column,
        ),
    )

    if not row or row.get("last_successful_value") is None:
        return str(default_value)

    return str(row["last_successful_value"])


def write_watermark_to_control_db(
    *,
    job_key: str,
    source_id: str,
    table_id: str,
    watermark_column: str,
    value: Any,
    run_id: str | None = None,
    job_id: int | None = None,
) -> None:
    if not control_db_enabled():
        return

    call_usp_void(
        "usp_upsert_watermark_state",
        (
            job_id,
            job_key,
            source_id,
            table_id,
            watermark_column,
            str(value),
            run_id,
        ),
    )