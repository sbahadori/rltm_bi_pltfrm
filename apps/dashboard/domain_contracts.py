from __future__ import annotations

from typing import Any


BATCH = "batch"
STREAM = "stream"

PIPELINE_TYPES = {BATCH, STREAM, "hybrid"}
JOB_MODES = {BATCH, STREAM}

RUN_STATES = {
    "queued",
    "scheduled",
    "starting",
    "running",
    "success",
    "failed",
    "error",
    "cancelled",
    "canceled",
    "skipped",
    "upstream_failed",
    "timeout",
    "unknown",
}

STREAM_STATES = {
    "starting",
    "running",
    "healthy",
    "degraded",
    "stale",
    "failed",
    "stopped",
    "unknown",
}


def pipeline_id_of(obj: dict[str, Any]) -> str:
    return str(
        obj.get("pipeline_id")
        or obj.get("pipeline")
        or obj.get("name")
        or ""
    )


def job_id_of(job: dict[str, Any]) -> str:
    return str(job.get("job_id") or job.get("id") or "")


def job_name_of(job: dict[str, Any]) -> str:
    return str(job.get("job_name") or job.get("name") or "")


def job_mode_of(job: dict[str, Any]) -> str:
    return str(job.get("job_mode") or job.get("type") or "batch")


def runner_id_of(job: dict[str, Any]) -> str:
    return str(job.get("runner_id") or job.get("runner") or job.get("job_type") or "")


def canonical_job(job: dict[str, Any]) -> dict[str, Any]:
    pipeline_id = pipeline_id_of(job)
    job_name = job_name_of(job)
    job_id = job_id_of(job) or f"{pipeline_id}__{job_name}"
    job_mode = job_mode_of(job)
    runner_id = runner_id_of(job)

    return {
        **job,

        # canonical domain fields
        "job_id": job_id,
        "pipeline_id": pipeline_id,
        "job_name": job_name,
        "job_mode": job_mode,
        "runner_id": runner_id,

        # compatibility fields
        "id": job.get("id") or job_id,
        "pipeline": job.get("pipeline") or pipeline_id,
        "name": job.get("name") or job_name,
        "type": job.get("type") or job_mode,
        "runner": job.get("runner") or runner_id,
    }


def canonical_pipeline(pipeline: dict[str, Any]) -> dict[str, Any]:
    pipeline_id = pipeline_id_of(pipeline)
    pipeline_type = pipeline.get("pipeline_type") or pipeline.get("type") or "batch"

    return {
        **pipeline,

        # canonical domain fields
        "pipeline_id": pipeline_id,
        "pipeline_type": pipeline_type,

        # compatibility fields
        "name": pipeline.get("name") or pipeline_id,
        "type": pipeline.get("type") or pipeline_type,
    }