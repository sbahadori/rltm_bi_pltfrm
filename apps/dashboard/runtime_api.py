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
    from apps.dashboard.airflow_client import dag_run_rows_for_job
    from apps.dashboard.config_loader import (
        AIRFLOW_LOG_DIR,
        
        STREAM_LOG_DIR,
        STREAM_STATUS_FILE,
        config_bundle,
        config_payload,
        job_from_config,
        path_state,
    )
    from apps.dashboard.runtime_models import now_iso
    from apps.dashboard.runtime_resolver import control_run_rows_for_job, enrich_jobs
    from apps.dashboard.stream_runtime import load_stream_status
except ImportError:  # pragma: no cover
    from .airflow_client import dag_run_rows_for_job
    from .config_loader import (
        AIRFLOW_LOG_DIR,
        
        STREAM_LOG_DIR,
        STREAM_STATUS_FILE,
        config_bundle,
        config_payload,
        job_from_config,
        path_state,
    )
    from .runtime_models import now_iso
    from .runtime_resolver import control_run_rows_for_job, enrich_jobs
    from .stream_runtime import load_stream_status

from apps.dashboard.execution_resolver import executor_id_for_job



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

    control_runs = control_run_rows_for_job(job, limit=limit)
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
        try:
            dag_id = executor_id_for_job(job)
            if not dag_id:
                return JSONResponse(
                    {
                        "available": False,
                        "source": "airflow",
                        "source_rank": 3,
                        "is_fallback": True,
                        "fallback_reason": "No Airflow DAG ID could be resolved for this batch job.",
                        "job_id": job_id,
                        "runs": [],
                        "count": 0,
                    }
                )

            runs = dag_run_rows_for_job(dag_id, limit=limit)
            return JSONResponse(
                {
                    "available": True,
                    "source": "airflow",
                    "source_rank": 3,
                    "is_fallback": True,
                    "fallback_reason": "Control DB had no matching runtime rows; using Airflow DAG runs as executor fallback.; using Airflow DAG runs as executor fallback.",
                    "job_id": job_id,
                    "dag_id": dag_id,
                    "runs": runs,
                    "count": len(runs),
                }
            )
        except Exception as exc:
            return JSONResponse(
                {
                    "available": False,
                    "source": "airflow",
                    "job_id": job_id,
                    "error": str(exc),
                    "runs": [],
                    "count": 0,
                }
            )

    return JSONResponse(
        {
            "available": False,
            "source": "unknown",
            "job_id": job_id,
            "runs": [],
            "count": 0,
        }
    )
