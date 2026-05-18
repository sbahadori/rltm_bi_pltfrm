from __future__ import annotations

from typing import Any

from shared.control.postgres import fetch_one


def load_job_identity(
    *,
    job_id: int | None = None,
    job_key: str | None = None,
    job_code: str | None = None,
) -> dict[str, Any]:
    if job_id is not None:
        row = fetch_one(
            """
            SELECT
                job_id,
                job_key,
                job_code,
                pipeline_name,
                job_name,
                base_job_name,
                source_id,
                table_id,
                entity_name,
                layer,
                runner,
                target_path
            FROM meta.job
            WHERE job_id = %s
              AND COALESCE(is_active, TRUE) = TRUE
            """,
            (job_id,),
        )
        if row:
            return row

    if job_key:
        row = fetch_one(
            """
            SELECT
                job_id,
                job_key,
                job_code,
                pipeline_name,
                job_name,
                base_job_name,
                source_id,
                table_id,
                entity_name,
                layer,
                runner,
                target_path
            FROM meta.job
            WHERE job_key = %s
              AND COALESCE(is_active, TRUE) = TRUE
            """,
            (job_key,),
        )
        if row:
            return row

    if job_code:
        row = fetch_one(
            """
            SELECT
                job_id,
                job_key,
                job_code,
                pipeline_name,
                job_name,
                base_job_name,
                source_id,
                table_id,
                entity_name,
                layer,
                runner,
                target_path
            FROM meta.job
            WHERE job_code = %s
              AND COALESCE(is_active, TRUE) = TRUE
            """,
            (job_code,),
        )
        if row:
            return row

    raise ValueError(
        f"Active job not found in meta.job. "
        f"job_id={job_id}, job_key={job_key}, job_code={job_code}"
    )


def load_job_identity_by_key(job_key: str) -> dict[str, Any]:
    return load_job_identity(job_key=job_key)