from __future__ import annotations

"""
Execution/action API router.

Single responsibility:
- Expose authenticated dashboard actions.
- Delegate Airflow operations to airflow_client.
- Delegate stream-control operations to stream_runtime.
- Write action audit logs through db.insert_action_log.
"""

import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

try:
    from airflow_client import cancel_dag_run, get_dag_runs, list_dags, pause_dag, trigger_dag
    from auth import get_current_user, require_role
    from db import call_usp_rows, insert_action_log
    from stream_runtime import restart_stream, stop_stream
except ImportError:  # pragma: no cover
    from .airflow_client import cancel_dag_run, get_dag_runs, list_dags, pause_dag, trigger_dag
    from .auth import get_current_user, require_role
    from .db import call_usp_rows, insert_action_log
    from .stream_runtime import restart_stream, stop_stream

router = APIRouter()


class DagTriggerRequest(BaseModel):
    dag_id: str
    conf: dict[str, Any] | None = None
    logical_date: str | None = None


class DagPauseRequest(BaseModel):
    dag_id: str
    paused: bool


class DagCancelRequest(BaseModel):
    dag_id: str
    run_id: str


class StreamActionRequest(BaseModel):
    unit_name: str


class OnboardRequest(BaseModel):
    catalog_path: str = "configs/batch/pipeline_catalog.json"
    pipeline_name: str | None = None
    job_name: str | None = None
    table_id: str | None = None
    dry_run: bool = False


def _model_dump(model: BaseModel) -> dict[str, Any]:
    # Pydantic v2 compatibility, with v1 fallback.
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


@router.get("/api/catalog/change-log")
async def catalog_change_log(
    limit: int = Query(default=100, ge=1, le=500),
    user: dict = Depends(get_current_user),
) -> dict:
    try:
        return {"items": call_usp_rows("usp_list_catalog_change_logs", (limit,))}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/actions/dag/trigger")
async def action_dag_trigger(
    req: DagTriggerRequest,
    user: dict = Depends(require_role("admin", "operator")),
) -> dict:
    started = time.time()
    payload = _model_dump(req)
    try:
        result = trigger_dag(req.dag_id, conf=req.conf, logical_date=req.logical_date)
        insert_action_log(
            user=user,
            action_type="dag_trigger",
            target_type="dag",
            target_id=req.dag_id,
            request_payload=payload,
            result_status="success",
            result_payload=result,
            duration_ms=int((time.time() - started) * 1000),
        )
        return {"ok": True, **result}
    except Exception as exc:
        insert_action_log(
            user=user,
            action_type="dag_trigger",
            target_type="dag",
            target_id=req.dag_id,
            request_payload=payload,
            result_status="failed",
            error_message=str(exc),
            duration_ms=int((time.time() - started) * 1000),
        )
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/actions/dag/pause")
async def action_dag_pause(
    req: DagPauseRequest,
    user: dict = Depends(require_role("admin", "operator")),
) -> dict:
    started = time.time()
    payload = _model_dump(req)
    action_type = "dag_pause" if req.paused else "dag_unpause"
    try:
        result = pause_dag(req.dag_id, req.paused)
        insert_action_log(
            user=user,
            action_type=action_type,
            target_type="dag",
            target_id=req.dag_id,
            request_payload=payload,
            result_status="success",
            result_payload=result,
            duration_ms=int((time.time() - started) * 1000),
        )
        return {"ok": True, **result}
    except Exception as exc:
        insert_action_log(
            user=user,
            action_type=action_type,
            target_type="dag",
            target_id=req.dag_id,
            request_payload=payload,
            result_status="failed",
            error_message=str(exc),
            duration_ms=int((time.time() - started) * 1000),
        )
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/actions/dag/cancel")
async def action_dag_cancel(
    req: DagCancelRequest,
    user: dict = Depends(require_role("admin", "operator")),
) -> dict:
    started = time.time()
    payload = _model_dump(req)
    try:
        result = cancel_dag_run(req.dag_id, req.run_id)
        insert_action_log(
            user=user,
            action_type="dag_cancel",
            target_type="dag",
            target_id=req.dag_id,
            request_payload=payload,
            result_status="success",
            result_payload=result,
            duration_ms=int((time.time() - started) * 1000),
        )
        return {"ok": True, **result}
    except Exception as exc:
        insert_action_log(
            user=user,
            action_type="dag_cancel",
            target_type="dag",
            target_id=req.dag_id,
            request_payload=payload,
            result_status="failed",
            error_message=str(exc),
            duration_ms=int((time.time() - started) * 1000),
        )
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/api/actions/dag/{dag_id}/runs")
async def action_dag_runs(
    dag_id: str,
    limit: int = Query(default=10, ge=1, le=50),
    user: dict = Depends(get_current_user),
) -> dict:
    try:
        return {"dag_id": dag_id, "runs": get_dag_runs(dag_id, limit=limit)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/api/actions/dags")
async def action_list_dags(user: dict = Depends(get_current_user)) -> dict:
    try:
        return {"dags": list_dags()}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/actions/stream/restart")
async def action_stream_restart(
    req: StreamActionRequest,
    user: dict = Depends(require_role("admin", "operator")),
) -> dict:
    started = time.time()
    payload = _model_dump(req)
    try:
        result = restart_stream(req.unit_name)
        insert_action_log(
            user=user,
            action_type="stream_restart",
            target_type="stream",
            target_id=req.unit_name,
            request_payload=payload,
            result_status="success",
            result_payload=result,
            duration_ms=int((time.time() - started) * 1000),
        )
        return {"ok": True, **result}
    except Exception as exc:
        insert_action_log(
            user=user,
            action_type="stream_restart",
            target_type="stream",
            target_id=req.unit_name,
            request_payload=payload,
            result_status="failed",
            error_message=str(exc),
            duration_ms=int((time.time() - started) * 1000),
        )
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/actions/stream/stop")
async def action_stream_stop(
    req: StreamActionRequest,
    user: dict = Depends(require_role("admin", "operator")),
) -> dict:
    started = time.time()
    payload = _model_dump(req)
    try:
        result = stop_stream(req.unit_name)
        insert_action_log(
            user=user,
            action_type="stream_stop",
            target_type="stream",
            target_id=req.unit_name,
            request_payload=payload,
            result_status="success",
            result_payload=result,
            duration_ms=int((time.time() - started) * 1000),
        )
        return {"ok": True, **result}
    except Exception as exc:
        insert_action_log(
            user=user,
            action_type="stream_stop",
            target_type="stream",
            target_id=req.unit_name,
            request_payload=payload,
            result_status="failed",
            error_message=str(exc),
            duration_ms=int((time.time() - started) * 1000),
        )
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/api/actions/onboard")
async def action_onboard(req: OnboardRequest, user: dict = Depends(require_role("admin"))) -> dict:
    started = time.time()
    request_payload = _model_dump(req)
    conf: dict[str, Any] = {
        "catalog_path": req.catalog_path,
        "dry_run": req.dry_run,
    }
    if req.pipeline_name:
        conf["pipeline_name"] = req.pipeline_name
    if req.job_name:
        conf["job_name"] = req.job_name
    if req.table_id:
        conf["table_id"] = req.table_id

    try:
        result = trigger_dag("control_plane_onboarding", conf=conf)
        insert_action_log(
            user=user,
            action_type="onboard",
            target_type="pipeline",
            target_id=req.pipeline_name or "all",
            request_payload=request_payload,
            result_status="success",
            result_payload=result,
            duration_ms=int((time.time() - started) * 1000),
        )
        return {"ok": True, **result}
    except Exception as exc:
        insert_action_log(
            user=user,
            action_type="onboard",
            target_type="pipeline",
            target_id=req.pipeline_name or "all",
            request_payload=request_payload,
            result_status="failed",
            error_message=str(exc),
            duration_ms=int((time.time() - started) * 1000),
        )
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/api/actions/history")
async def action_history(
    limit: int = Query(default=50, ge=1, le=500),
    user: dict = Depends(get_current_user),
) -> dict:
    try:
        return {"items": call_usp_rows("usp_list_action_logs", (limit,))}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
