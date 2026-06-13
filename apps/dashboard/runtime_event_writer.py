from __future__ import annotations

import json
import time
import uuid
from typing import Any

try:
    from apps.dashboard.config_loader import JOB_RUN_REGISTRY_FILE
    from apps.dashboard.domain_contracts import canonical_job, job_id_of, job_name_of, pipeline_id_of
    from apps.dashboard.runtime_models import now_iso
except ImportError:  # pragma: no cover
    from .config_loader import JOB_RUN_REGISTRY_FILE
    from .domain_contracts import canonical_job, job_id_of, job_name_of, pipeline_id_of
    from .runtime_models import now_iso


def write_job_run_event(
    *,
    job: dict[str, Any],
    execution_result: dict[str, Any],
    submitted_by: str | None = None,
    request_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    job = canonical_job(job)

    ts_epoch = int(time.time())
    observed_at = now_iso()

    executor_run_id = execution_result.get("executor_run_id")
    run_id = executor_run_id or f"run_{ts_epoch}_{job_name_of(job)}"

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "job_submitted",

        # Runtime state
        "state": "submitted",
        "status": "submitted",
        "run_id": run_id,
        "executor_run_id": executor_run_id,

        # Canonical job identity
        "job_id": job_id_of(job),
        "job": job_name_of(job),
        "job_name": job_name_of(job),
        "pipeline": pipeline_id_of(job),
        "pipeline_id": pipeline_id_of(job),
        "job_code": job.get("job_code"),
        "runner_id": job.get("runner_id"),

        # Executor identity
        "executor_type": execution_result.get("executor_type"),
        "executor_id": execution_result.get("executor_id"),

        # Time
        "observed_at": observed_at,
        "started_at": observed_at,
        "ts_epoch": ts_epoch,

        # Actor / request
        "submitted_by": submitted_by,
        "request_payload": request_payload or {},

        # Metrics placeholders
        "records_read": None,
        "records_written": None,
        "records_inserted": None,
        "records_updated": 0,
        "records_deleted": 0,

        # Source
        "runtime_source": "runtime_event_writer",
    }

    JOB_RUN_REGISTRY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with JOB_RUN_REGISTRY_FILE.open("a", encoding="utf-8") as file:
        file.write(json.dumps(event, ensure_ascii=False) + "\n")

    return event