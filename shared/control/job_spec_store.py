from __future__ import annotations

import os
from typing import Any

from shared.control.postgres import fetch_one


def _normalize_json_object(value: Any, *, field_name: str, job_code: str | None = None) -> dict[str, Any]:
    if value is None:
        return {}

    if isinstance(value, dict):
        return value

    raise ValueError(
        f"Expected {field_name} to be a JSON object/dict; "
        f"got {type(value).__name__}. job_code={job_code}"
    )


def load_job_metadata_by_id(job_id: int) -> dict[str, Any]:
    row = fetch_one(
        """
        SELECT
            job_id,
            job_key,
            job_code,
            pipeline_name,
            job_name,
            base_job_name,
            job_type,
            source_type,
            runner,
            layer,
            source_id,
            table_id,
            entity_name,
            manifest_ref,
            target_path,
            config,
            runtime_policy,
            is_active
        FROM meta.job
        WHERE job_id = %s
          AND COALESCE(is_active, TRUE) = TRUE
        """,
        (job_id,),
    )

    if not row:
        raise ValueError(f"Active job_id not found in meta.job: {job_id}")

    return dict(row)


def load_job_metadata_by_code(job_code: str) -> dict[str, Any]:
    row = fetch_one(
        """
        SELECT
            job_id,
            job_key,
            job_code,
            pipeline_name,
            job_name,
            base_job_name,
            job_type,
            source_type,
            runner,
            layer,
            source_id,
            table_id,
            entity_name,
            manifest_ref,
            target_path,
            config,
            runtime_policy,
            is_active
        FROM meta.job
        WHERE job_code = %s
          AND COALESCE(is_active, TRUE) = TRUE
        """,
        (job_code,),
    )

    if not row:
        raise ValueError(f"Active job_code not found in meta.job: {job_code}")

    return dict(row)


def load_job_metadata_by_key(job_key: str) -> dict[str, Any]:
    row = fetch_one(
        """
        SELECT
            job_id,
            job_key,
            job_code,
            pipeline_name,
            job_name,
            base_job_name,
            job_type,
            source_type,
            runner,
            layer,
            source_id,
            table_id,
            entity_name,
            manifest_ref,
            target_path,
            config,
            runtime_policy,
            is_active
        FROM meta.job
        WHERE job_key = %s
          AND COALESCE(is_active, TRUE) = TRUE
        """,
        (job_key,),
    )

    if not row:
        raise ValueError(f"Active job_key not found in meta.job: {job_key}")

    return dict(row)


def load_current_job_metadata() -> dict[str, Any]:
    """
    Resolve the currently running job from Airflow-provided CONTROL_* variables.

    Priority:
      1. CONTROL_JOB_ID
      2. CONTROL_JOB_CODE
      3. CONTROL_JOB_KEY

    This makes runners metadata-driven while keeping JSON catalog as fallback
    only in the runner layer.
    """
    raw_job_id = os.getenv("CONTROL_JOB_ID")
    job_code = os.getenv("CONTROL_JOB_CODE")
    job_key = os.getenv("CONTROL_JOB_KEY")

    if raw_job_id:
        try:
            return load_job_metadata_by_id(int(raw_job_id))
        except ValueError:
            raise
        except Exception as exc:
            raise ValueError(f"Invalid CONTROL_JOB_ID={raw_job_id}: {exc}") from exc

    if job_code:
        return load_job_metadata_by_code(job_code)

    if job_key:
        return load_job_metadata_by_key(job_key)

    raise ValueError(
        "Cannot resolve current job metadata. "
        "None of CONTROL_JOB_ID, CONTROL_JOB_CODE, CONTROL_JOB_KEY is set."
    )


def load_current_job_spec() -> dict[str, Any]:
    """
    Return meta.job.config for the current job.

    meta.job.config is the runtime source of truth for the job specification.
    """
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