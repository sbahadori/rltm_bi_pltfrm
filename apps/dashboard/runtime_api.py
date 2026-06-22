from __future__ import annotations

"""
Runtime read API router.

Owns only read-only runtime/config endpoints:
- /api/config
- /api/runtime/jobs
- /api/runtime/runs/{job_id}
- /api/runtime/mounts
- /api/runtime/streams/status
"""

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

try:
    from apps.dashboard.config_loader import (
        AIRFLOW_LOG_DIR,
        
        STREAM_LOG_DIR,
        STREAM_STATUS_FILE,
        config_bundle,
        config_payload,
        job_from_config,
        path_state,
    )
    from apps.dashboard.db import catalog_metadata_for_job
    from apps.dashboard.runtime_models import now_iso
    from apps.dashboard.runtime_resolver import control_run_rows_for_job, enrich_jobs
    from apps.dashboard.stream_runtime import load_stream_status
except ImportError:  # pragma: no cover
    from .config_loader import (
        AIRFLOW_LOG_DIR,
        
        STREAM_LOG_DIR,
        STREAM_STATUS_FILE,
        config_bundle,
        config_payload,
        job_from_config,
        path_state,
    )
    from .db import catalog_metadata_for_job
    from .runtime_models import now_iso
    from .runtime_resolver import control_run_rows_for_job, enrich_jobs
    from .stream_runtime import load_stream_status

router = APIRouter()


@router.get("/api/config")
async def get_config() -> JSONResponse:
    return JSONResponse(config_payload())


@router.get("/api/runtime/jobs")
async def get_runtime_jobs() -> JSONResponse:
    bundle = config_bundle()
    runtime = enrich_jobs(bundle["jobs"])
    return JSONResponse(
        {
            "jobs": runtime["jobs"],
            "stream_status": runtime["stream_status"],
            "meta": {"loaded_at": now_iso()},
        }
    )


@router.get("/api/runtime/mounts")
async def get_mounts() -> JSONResponse:
    return JSONResponse(
        {
            "stream_status_file": path_state(STREAM_STATUS_FILE),
            "stream_log_dir": path_state(STREAM_LOG_DIR),
            "airflow_log_dir": path_state(AIRFLOW_LOG_DIR),
            "checked_at": now_iso(),
        }
    )


@router.get("/api/runtime/streams/status")
async def get_stream_status(raw: bool = Query(default=False)) -> JSONResponse:
    return JSONResponse(load_stream_status(raw=raw))


@router.get("/api/runtime/runs/{job_id}")
async def get_runs(job_id: str, limit: int = Query(default=10, ge=1, le=50)) -> JSONResponse:
    job = job_from_config(job_id)

    if not job:
        return JSONResponse(
            {
                "available": False,
                "source": "config",
                "job_id": job_id,
                "runs": [],
                "error": "Job not found in /api/config",
            },
            status_code=404,
        )

    try:
        control_runs = control_run_rows_for_job(job, limit=limit, raise_on_error=True)
    except Exception as exc:
        return JSONResponse(
            {
                "available": False,
                "source": "control_db",
                "source_rank": 1,
                "is_fallback": False,
                "fallback_reason": None,
                "runtime_error": True,
                "job_id": job_id,
                "error": str(exc),
                "runs": [],
                "count": 0,
            },
            status_code=503,
        )

    if control_runs:
        return JSONResponse(
            {
                "available": True,
                "source": "control_db",
                "source_rank": 1,
                "is_fallback": False,
                "fallback_reason": None,
                "job_id": job_id,
                "runs": control_runs,
                "count": len(control_runs),
            }
        )

    if job.get("type") == "batch":
        metadata = catalog_metadata_for_job(job)
        return JSONResponse(
            {
                "available": False,
                "source": "control_db_metadata" if metadata else "control_db_missing",
                "source_rank": 1,
                "is_fallback": False,
                "fallback_reason": None,
                "metadata_available": bool(metadata),
                "runtime_gap": True,
                "job_id": job_id,
                "metadata_job_id": metadata.get("job_id") if metadata else None,
                "metadata_job_code": metadata.get("job_code") if metadata else None,
                "runs": [],
                "count": 0,
                "status_reason": (
                    "No batch runtime rows were found in Control DB for this job. "
                    "Airflow DAG runs are executor metadata and are not used as "
                    "platform runtime history fallback."
                ),
            }
        )

    return JSONResponse(
        {
            "available": False,
            "source": "control_db",
            "source_rank": 1,
            "is_fallback": False,
            "fallback_reason": None,
            "job_id": job_id,
            "runs": [],
            "count": 0,
        }
    )
