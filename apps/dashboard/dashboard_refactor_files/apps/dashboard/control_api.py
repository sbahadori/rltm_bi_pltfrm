from __future__ import annotations

"""Control DB read-only API router."""

from fastapi import APIRouter

try:
    from db import call_usp_rows
except ImportError:  # pragma: no cover
    from .db import call_usp_rows

router = APIRouter()


@router.get("/api/control/jobs")
def control_jobs() -> dict:
    return {"items": call_usp_rows("usp_list_control_jobs")}


@router.get("/api/runtime/job-runs")
def runtime_job_runs(limit: int = 50) -> dict:
    return {"items": call_usp_rows("usp_list_runtime_job_runs", (limit,))}


@router.get("/api/runtime/watermarks")
def runtime_watermarks() -> dict:
    return {"items": call_usp_rows("usp_list_runtime_watermarks")}


@router.get("/api/quality/results")
def quality_results(limit: int = 100) -> dict:
    return {"items": call_usp_rows("usp_list_quality_results", (limit,))}


@router.get("/api/lineage/datasets")
def dataset_lineage(limit: int = 100) -> dict:
    return {"items": call_usp_rows("usp_list_dataset_lineage", (limit,))}
