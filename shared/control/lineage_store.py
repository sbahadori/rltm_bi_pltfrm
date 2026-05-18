from __future__ import annotations

import json
from typing import Any

from shared.control.postgres import control_db_enabled, execute


def write_dataset_lineage(
    *,
    run_id: str,
    job_key: str,
    source_dataset_key: str,
    target_dataset_key: str,
    transformation_type: str,
    transformation_ref: str | None = None,
    details: dict[str, Any] | None = None,
    job_id: int | None = None,
) -> None:
    if not control_db_enabled():
        return

    try:
        execute(
            """
            INSERT INTO lineage.dataset_lineage (
                run_id,
                job_id,
                job_key,
                source_dataset_key,
                target_dataset_key,
                transformation_type,
                transformation_ref,
                details
            )
            VALUES (
                %s::uuid, %s, %s, %s, %s, %s, %s, %s::jsonb
            )
            """,
            (
                run_id,
                job_id,
                job_key,
                source_dataset_key,
                target_dataset_key,
                transformation_type,
                transformation_ref,
                json.dumps(details or {}),
            ),
        )
    except Exception as exc:
        print(f"[WARN] Failed to write dataset lineage to control DB: {exc}", flush=True)