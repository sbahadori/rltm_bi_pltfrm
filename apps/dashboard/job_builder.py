from __future__ import annotations

"""
job_builder.py — Dynamic job & pipeline definition CRUD.

هر جابی که از UI تعریف می‌شود:
  1. در meta.ui_job_definition ذخیره می‌شود
  2. از طریق /api/builder/* endpoints مدیریت می‌شود
  3. پس از ذخیره، onboarding را trigger می‌کند تا meta.job هم به‌روز شود
  4. می‌تواند بلافاصله از Airflow trigger شود
"""

import json
import re
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, field_validator

from auth import get_current_user, require_role

# ─────────────────────────────────────────────────────────────
# این router در app.py include می‌شود
# ─────────────────────────────────────────────────────────────
router = APIRouter(prefix="/api/builder", tags=["builder"])


# ─────────────────────────────────────────────────────────────
# Helper — import می‌کند از app.py تا از کد تکراری جلوگیری شود
# ─────────────────────────────────────────────────────────────
def _db():
    """lazy import برای جلوگیری از circular import"""
    import app as _app
    return _app


def call_rows(usp: str, params: tuple = ()) -> list[dict]:
    return _db().call_usp_rows(usp, params)


def call_one(usp: str, params: tuple = ()) -> dict | None:
    return _db().call_usp_one(usp, params)


def call_void(usp: str, params: tuple = ()) -> None:
    return _db().call_usp_void(usp, params)


# ─────────────────────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────────────────────

def _slugify(text: str) -> str:
    """تبدیل به slug مناسب برای job_def_key"""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_-]+", "_", text)
    return text.strip("_")


class PipelineCreateRequest(BaseModel):
    pipeline_name: str
    display_name: str | None = None
    description: str | None = None
    pipeline_type: str = "batch"
    schedule: str | None = None
    enabled: bool = True
    tags: list[str] = []
    dag_config: dict = {}

    @field_validator("pipeline_name")
    @classmethod
    def validate_pipeline_name(cls, v: str) -> str:
        if not re.match(r"^[a-z][a-z0-9_]*$", v):
            raise ValueError("pipeline_name must be lowercase letters, digits, underscores")
        return v

    @field_validator("pipeline_type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        if v not in ("batch", "stream"):
            raise ValueError("pipeline_type must be 'batch' or 'stream'")
        return v


class JobCreateRequest(BaseModel):
    job_name: str
    display_name: str | None = None
    pipeline_name: str
    layer: str
    job_type: str
    description: str | None = None
    enabled: bool = True
    spec: dict = {}
    runtime_policy: dict = {}
    tags: list[str] = []

    @field_validator("layer")
    @classmethod
    def validate_layer(cls, v: str) -> str:
        if v not in ("bronze", "silver", "gold", "stream"):
            raise ValueError("layer must be bronze/silver/gold/stream")
        return v

    @field_validator("job_name")
    @classmethod
    def validate_job_name(cls, v: str) -> str:
        if not re.match(r"^[a-z][a-z0-9_]*$", v):
            raise ValueError("job_name must be lowercase letters, digits, underscores")
        return v


class JobUpdateRequest(BaseModel):
    display_name: str | None = None
    description: str | None = None
    enabled: bool | None = None
    spec: dict | None = None
    runtime_policy: dict | None = None
    tags: list[str] | None = None


class TriggerJobRequest(BaseModel):
    job_def_key: str
    conf: dict | None = None


# ─────────────────────────────────────────────────────────────
# Pipeline CRUD
# ─────────────────────────────────────────────────────────────

@router.get("/pipelines")
async def list_pipelines(
    include_disabled: bool = False,
    user: dict = Depends(get_current_user),
) -> dict:
    """همه pipeline definition های UI را برمی‌گرداند."""
    items = call_rows("usp_list_ui_pipelines", (include_disabled,))
    return {"items": items, "count": len(items)}


@router.get("/pipelines/{pipeline_name}")
async def get_pipeline(
    pipeline_name: str,
    user: dict = Depends(get_current_user),
) -> dict:
    rows = call_rows("usp_list_ui_pipelines", (True,))
    pipeline = next((r for r in rows if r["pipeline_name"] == pipeline_name), None)
    if not pipeline:
        raise HTTPException(status_code=404, detail=f"Pipeline not found: {pipeline_name}")
    # jobs of this pipeline
    jobs = call_rows("usp_list_ui_jobs", (pipeline_name, True))
    return {**pipeline, "jobs": jobs}


@router.post("/pipelines", dependencies=[Depends(require_role("admin", "operator"))])
async def create_pipeline(
    req: PipelineCreateRequest,
    user: dict = Depends(get_current_user),
) -> dict:
    call_void(
        "usp_upsert_ui_pipeline",
        (
            req.pipeline_name,
            req.display_name or req.pipeline_name,
            req.description,
            req.pipeline_type,
            req.schedule,
            req.enabled,
            json.dumps(req.tags),
            json.dumps(req.dag_config),
            user.get("sub", "unknown"),
        ),
    )
    return {"ok": True, "pipeline_name": req.pipeline_name}


@router.delete("/pipelines/{pipeline_name}", dependencies=[Depends(require_role("admin"))])
async def delete_pipeline(
    pipeline_name: str,
    user: dict = Depends(get_current_user),
) -> dict:
    call_void("usp_delete_ui_pipeline", (pipeline_name,))
    return {"ok": True, "pipeline_name": pipeline_name, "action": "disabled"}


# ─────────────────────────────────────────────────────────────
# Job Templates
# ─────────────────────────────────────────────────────────────

@router.get("/templates")
async def list_templates(user: dict = Depends(get_current_user)) -> dict:
    """قالب‌های آماده job برای UI form."""
    items = call_rows("usp_list_ui_job_templates")
    return {"items": items}


# ─────────────────────────────────────────────────────────────
# Job CRUD
# ─────────────────────────────────────────────────────────────

@router.get("/jobs")
async def list_jobs(
    pipeline_name: str | None = None,
    include_disabled: bool = False,
    user: dict = Depends(get_current_user),
) -> dict:
    items = call_rows("usp_list_ui_jobs", (pipeline_name, include_disabled))
    return {"items": items, "count": len(items)}


@router.get("/jobs/{job_def_key:path}")
async def get_job(
    job_def_key: str,
    user: dict = Depends(get_current_user),
) -> dict:
    row = call_one("usp_get_ui_job", (job_def_key,))
    if not row:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_def_key}")
    return row


@router.post("/jobs", dependencies=[Depends(require_role("admin", "operator"))])
async def create_job(
    req: JobCreateRequest,
    user: dict = Depends(get_current_user),
) -> dict:
    # ساخت job_def_key یکتا
    job_def_key = f"{req.pipeline_name}.{req.layer}.{req.job_name}"

    call_void(
        "usp_upsert_ui_job",
        (
            job_def_key,
            req.job_name,
            req.display_name or req.job_name,
            req.pipeline_name,
            req.layer,
            req.job_type,
            req.description,
            req.enabled,
            json.dumps(req.spec),
            json.dumps(req.runtime_policy),
            json.dumps(req.tags),
            user.get("sub", "unknown"),
        ),
    )

    # onboarding را trigger می‌کند تا meta.job هم به‌روز شود
    _trigger_onboarding_for_job(req.pipeline_name, user)

    return {"ok": True, "job_def_key": job_def_key}


@router.put("/jobs/{job_def_key:path}", dependencies=[Depends(require_role("admin", "operator"))])
async def update_job(
    job_def_key: str,
    req: JobUpdateRequest,
    user: dict = Depends(get_current_user),
) -> dict:
    # جاب موجود را بخوان
    existing = call_one("usp_get_ui_job", (job_def_key,))
    if not existing:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_def_key}")

    # merge کن
    updated_spec   = req.spec           if req.spec           is not None else existing.get("spec", {})
    updated_policy = req.runtime_policy if req.runtime_policy is not None else existing.get("runtime_policy", {})
    updated_tags   = req.tags           if req.tags           is not None else existing.get("tags", [])
    updated_name   = req.display_name   if req.display_name   is not None else existing.get("display_name")
    updated_desc   = req.description    if req.description    is not None else existing.get("description")
    updated_enabled = req.enabled       if req.enabled        is not None else existing.get("enabled", True)

    call_void(
        "usp_upsert_ui_job",
        (
            job_def_key,
            existing["job_name"],
            updated_name,
            existing["pipeline_name"],
            existing["layer"],
            existing["job_type"],
            updated_desc,
            updated_enabled,
            json.dumps(updated_spec),
            json.dumps(updated_policy),
            json.dumps(updated_tags),
            user.get("sub", "unknown"),
        ),
    )

    _trigger_onboarding_for_job(existing["pipeline_name"], user)

    return {"ok": True, "job_def_key": job_def_key}


@router.delete("/jobs/{job_def_key:path}", dependencies=[Depends(require_role("admin"))])
async def delete_job(
    job_def_key: str,
    user: dict = Depends(get_current_user),
) -> dict:
    call_void("usp_delete_ui_job", (job_def_key,))
    return {"ok": True, "job_def_key": job_def_key, "action": "disabled"}


# ─────────────────────────────────────────────────────────────
# Trigger a UI-defined job immediately via Airflow
# ─────────────────────────────────────────────────────────────

@router.post("/jobs/{job_def_key:path}/trigger", dependencies=[Depends(require_role("admin", "operator"))])
async def trigger_ui_job(
    job_def_key: str,
    req: TriggerJobRequest | None = None,
    user: dict = Depends(get_current_user),
) -> dict:
    """
    یک UI job را فوری از طریق Airflow DAG trigger می‌کند.
    pipeline_name را به عنوان dag_id استفاده می‌کند.
    """
    row = call_one("usp_get_ui_job", (job_def_key,))
    if not row:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_def_key}")

    from actions import trigger_dag
    conf = (req.conf if req else None) or {}
    conf["job_name"] = row["job_name"]
    conf["job_def_key"] = job_def_key

    try:
        result = trigger_dag(row["pipeline_name"], conf=conf)
        return {"ok": True, "job_def_key": job_def_key, **result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ─────────────────────────────────────────────────────────────
# Validate spec against template schema
# ─────────────────────────────────────────────────────────────

@router.post("/validate")
async def validate_job_spec(
    payload: dict,
    user: dict = Depends(get_current_user),
) -> dict:
    """
    spec JSON یک job را اعتبارسنجی می‌کند.
    required fields را بررسی می‌کند.
    """
    job_type = payload.get("job_type", "")
    spec = payload.get("spec", {})
    errors: list[str] = []

    if job_type == "generic_api_to_bronze":
        required = [
            ("source.base_url", spec.get("source", {}).get("base_url")),
            ("bronze_write.target_path", spec.get("bronze_write", {}).get("target_path")),
        ]
        for field, val in required:
            if not val:
                errors.append(f"Missing required field: {field}")

    elif job_type in ("generic_bronze_to_silver", "generic_silver_to_gold"):
        required = [
            ("source.path", spec.get("source", {}).get("path")),
            ("target.path", spec.get("target", {}).get("path")),
        ]
        for field, val in required:
            if not val:
                errors.append(f"Missing required field: {field}")

        if spec.get("target", {}).get("mode") == "merge":
            mk = spec.get("target", {}).get("merge_keys", [])
            if not mk:
                errors.append("merge_keys required when mode=merge")

    elif job_type == "generic_jdbc_manifest_to_bronze":
        if not spec.get("manifest_ref"):
            errors.append("Missing required field: manifest_ref")

    return {"valid": len(errors) == 0, "errors": errors}


# ─────────────────────────────────────────────────────────────
# Internal helper
# ─────────────────────────────────────────────────────────────

def _trigger_onboarding_for_job(pipeline_name: str, user: dict) -> None:
    """
    پس از ذخیره یک job جدید، onboarding DAG را trigger می‌کند.
    این کار meta.job را در control DB به‌روز می‌کند.
    اگر trigger شکست خورد، warning می‌دهد اما exception throw نمی‌کند.
    """
    try:
        from actions import trigger_dag
        trigger_dag(
            "control_plane_onboarding",
            conf={
                "catalog_path": "configs/batch/pipeline_catalog.json",
                "pipeline_name": pipeline_name,
                "dry_run": False,
            },
        )
    except Exception as exc:
        print(f"[WARN] Onboarding trigger failed after job save: {exc}", flush=True)