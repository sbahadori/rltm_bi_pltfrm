from __future__ import annotations

from typing import Any

from shared.control.postgres import control_db_enabled, execute, fetch_one


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

    row = fetch_one(
        """
        SELECT last_successful_value
        FROM runtime.watermark_state
        WHERE job_key = %s
          AND source_id = %s
          AND table_id = %s
          AND watermark_column = %s
        ORDER BY updated_at DESC
        LIMIT 1
        """,
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

    execute(
        """
        INSERT INTO runtime.watermark_state (
            job_id,
            job_key,
            source_id,
            table_id,
            watermark_column,
            last_successful_value,
            current_value,
            last_run_id,
            updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
        )
        ON CONFLICT (job_key, source_id, table_id, watermark_column)
        DO UPDATE SET
            job_id = EXCLUDED.job_id,
            last_successful_value = EXCLUDED.last_successful_value,
            current_value = EXCLUDED.current_value,
            last_run_id = EXCLUDED.last_run_id,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            job_id,
            job_key,
            source_id,
            table_id,
            watermark_column,
            str(value),
            str(value),
            run_id,
        ),
    )