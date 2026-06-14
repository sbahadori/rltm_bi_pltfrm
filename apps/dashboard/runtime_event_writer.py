from __future__ import annotations

import time
import uuid
from typing import Any

try:
    from shared.runtime.job_event_writer import append_job_event
    from apps.dashboard.domain_contracts import canonical_job, job_id_of, job_name_of, pipeline_id_of
    from apps.dashboard.runtime_models import now_iso
except ImportError:  # pragma: no cover
    from shared.runtime.job_event_writer import append_job_event
    from .domain_contracts import canonical_job, job_id_of, job_name_of, pipeline_id_of
    from .runtime_models import now_iso


def append_runtime_event(event: dict[str, Any]) -> dict[str, Any]:
    event = {
        **event,
        "event_id": event.get("event_id") or str(uuid.uuid4()),
        "observed_at": event.get("observed_at") or now_iso(),
        "ts_epoch": event.get("ts_epoch") or int(time.time()),
    }
    append_job_event(**event)
    return event


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
    run_id = str(uuid.uuid4())

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "job_submitted",

        "state": "submitted",
        "status": "submitted",
        "run_id": run_id,
        "executor_run_id": executor_run_id,

        "job_id": job_id_of(job),
        "job": job_name_of(job),
        "job_name": job_name_of(job),
        "pipeline": pipeline_id_of(job),
        "pipeline_id": pipeline_id_of(job),
        "job_code": job.get("job_code"),
        "runner_id": job.get("runner_id"),

        "executor_type": execution_result.get("executor_type"),
        "executor_id": execution_result.get("executor_id"),

        "observed_at": observed_at,
        "started_at": observed_at,
        "ts_epoch": ts_epoch,

        "submitted_by": submitted_by,
        "request_payload": request_payload or {},

        "records_read": None,
        "records_written": None,
        "records_inserted": None,
        "records_updated": 0,
        "records_deleted": 0,

        "runtime_source": "runtime_event_writer",
    }

    return append_runtime_event(event)
