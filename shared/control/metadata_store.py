from __future__ import annotations

from typing import Any

from shared.control.postgres import call_usp_one


def load_job_identity(
    *,
    job_id: int | None = None,
    job_key: str | None = None,
    job_code: str | None = None,
) -> dict[str, Any]:
    row = call_usp_one(
        "usp_get_active_job_identity",
        (
            job_id,
            job_key,
            job_code,
        ),
    )

    if row:
        return row

    raise ValueError(
        f"Active job not found in meta.job. "
        f"job_id={job_id}, job_key={job_key}, job_code={job_code}"
    )


def load_job_identity_by_key(job_key: str) -> dict[str, Any]:
    return load_job_identity(job_key=job_key)