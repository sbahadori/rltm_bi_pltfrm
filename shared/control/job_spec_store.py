from __future__ import annotations

import os
from typing import Any

from shared.control.postgres import call_usp_one


def _normalize_json_object(
    value: Any,
    *,
    field_name: str,
    job_code: str | None = None,
) -> dict[str, Any]:
    if value is None:
        return {}

    if isinstance(value, dict):
        return value

    raise ValueError(
        f"Expected {field_name} to be a JSON object/dict; "
        f"got {type(value).__name__}. job_code={job_code}"
    )


def load_job_metadata(
    *,
    job_id: int | None = None,
    job_key: str | None = None,
    job_code: str | None = None,
) -> dict[str, Any]:
    row = call_usp_one(
        "usp_get_active_job_metadata",
        (
            job_id,
            job_key,
            job_code,
        ),
    )

    if not row:
        raise ValueError(
            f"Active job not found in meta.job. "
            f"job_id={job_id}, job_key={job_key}, job_code={job_code}"
        )

    return row


def load_job_metadata_by_id(job_id: int) -> dict[str, Any]:
    return load_job_metadata(job_id=job_id)


def load_job_metadata_by_code(job_code: str) -> dict[str, Any]:
    return load_job_metadata(job_code=job_code)


def load_job_metadata_by_key(job_key: str) -> dict[str, Any]:
    return load_job_metadata(job_key=job_key)


def load_current_job_metadata() -> dict[str, Any]:
    raw_job_id = os.getenv("CONTROL_JOB_ID")
    job_code = os.getenv("CONTROL_JOB_CODE")
    job_key = os.getenv("CONTROL_JOB_KEY")

    if raw_job_id:
        return load_job_metadata_by_id(int(raw_job_id))

    if job_code:
        return load_job_metadata_by_code(job_code)

    if job_key:
        return load_job_metadata_by_key(job_key)

    raise ValueError(
        "Cannot resolve current job metadata. "
        "None of CONTROL_JOB_ID, CONTROL_JOB_CODE, CONTROL_JOB_KEY is set."
    )


def load_current_job_spec() -> dict[str, Any]:
    meta = load_current_job_metadata()
    job_code = meta.get("job_code") or meta.get("job_key")

    config = _normalize_json_object(
        meta.get("config"),
        field_name="meta.job.config",
        job_code=job_code,
    )

    if not config:
        raise ValueError(f"Empty meta.job.config for job_code={job_code}")

    return config


def load_current_runtime_policy() -> dict[str, Any]:
    meta = load_current_job_metadata()
    job_code = meta.get("job_code") or meta.get("job_key")

    return _normalize_json_object(
        meta.get("runtime_policy"),
        field_name="meta.job.runtime_policy",
        job_code=job_code,
    )

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
