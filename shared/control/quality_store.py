from __future__ import annotations

import json
from typing import Any

from shared.control.postgres import control_db_enabled, execute


def write_quality_result(
    *,
    run_id: str,
    job_key: str,
    dataset_key: str,
    status: str,
    observed_value: Any = None,
    expected_value: Any = None,
    details: dict[str, Any] | None = None,
    job_id: int | None = None,
) -> None:
    if not control_db_enabled():
        return

    execute(
        """
        INSERT INTO dq.quality_result (
            run_id,
            job_id,
            job_key,
            dataset_key,
            status,
            observed_value,
            expected_value,
            details
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s::jsonb
        )
        """,
        (
            run_id,
            job_id,
            job_key,
            dataset_key,
            status,
            None if observed_value is None else str(observed_value),
            None if expected_value is None else str(expected_value),
            json.dumps(details or {}),
        ),
    )