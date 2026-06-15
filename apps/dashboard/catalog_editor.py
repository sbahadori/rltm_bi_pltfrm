from __future__ import annotations

"""
catalog_editor.py

Catalog-first job definition API.

Rule:
    Every real job must exist in configs/batch/pipeline_catalog.json.
    UI/API may help edit that JSON, but DB is not the source of truth for job definitions.

Endpoints:
    GET  /api/catalog/info
    GET  /api/catalog/backups
    POST /api/catalog/jobs/validate
    POST /api/catalog/jobs/preview
    POST /api/catalog/jobs/apply
    POST /api/catalog/jobs/apply-and-onboard
"""

import copy
import json
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import hashlib
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict

try:
    from apps.dashboard.auth import get_current_user, require_role
    from apps.dashboard.settings import get_settings
except ImportError:  # pragma: no cover
    from .auth import get_current_user, require_role
    from .settings import get_settings

router = APIRouter(prefix="/api/catalog", tags=["catalog"])


settings = get_settings()

PIPELINE_REPO_ROOT = settings.pipeline_repo_root
BATCH_CATALOG_PATH = settings.batch_catalog_path
CATALOG_BACKUP_DIR = settings.catalog_backup_dir
ONBOARDING_DAG_ID = settings.control_onboarding_dag_id

KNOWN_JOB_TYPES = {
    "generic_api_to_bronze",
    "generic_bronze_to_silver",
    "generic_silver_to_gold",
    "generic_delta_to_gold",
    "generic_jdbc_to_bronze",
    "generic_jdbc_manifest_to_bronze",
    "generic_sqlserver_to_bronze",
}


NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")


class CatalogPipelineInput(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    description: str | None = None
    enabled: bool = True
    domain: str | None = None
    source_type: str | None = None
    dag: dict[str, Any] | None = None
    tags: list[str] | None = None


class CatalogJobApplyRequest(BaseModel):
    pipeline_name: str
    job: dict[str, Any]

    create_pipeline_if_missing: bool = True
    pipeline: CatalogPipelineInput | None = None
    overwrite_existing_job: bool = True
    dry_run: bool = False

    base_job_hash: str | None = None


class CatalogOnboardRequest(CatalogJobApplyRequest):
    onboarding_dry_run: bool = False


class CatalogPipelineApplyRequest(BaseModel):
    pipeline_name: str
    pipeline: CatalogPipelineInput | None = None
    jobs: list[dict[str, Any]]

    create_pipeline_if_missing: bool = True
    overwrite_existing_jobs: bool = True
    dry_run: bool = False

    base_job_hashes: dict[str, str] | None = None

class CatalogPipelineOnboardRequest(CatalogPipelineApplyRequest):
    onboarding_dry_run: bool = False



def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _catalog_hash(catalog: dict[str, Any]) -> str:
    return _sha256_json(catalog)


def _job_hash(job: dict[str, Any]) -> str:
    return _sha256_json(job)


def _load_catalog() -> dict[str, Any]:
    if not BATCH_CATALOG_PATH.exists():
        return {"pipelines": []}

    try:
        with BATCH_CATALOG_PATH.open("r", encoding="utf-8-sig") as f:
            payload = json.load(f)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read catalog: {BATCH_CATALOG_PATH}: {exc}",
        ) from exc

    if not isinstance(payload, dict):
        raise HTTPException(status_code=500, detail="pipeline_catalog.json must be a JSON object.")

    if "pipelines" not in payload or not isinstance(payload["pipelines"], list):
        payload["pipelines"] = []

    return payload


def _backup_catalog() -> Path:
    CATALOG_BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    if not BATCH_CATALOG_PATH.exists():
        raise HTTPException(status_code=404, detail=f"Catalog file not found: {BATCH_CATALOG_PATH}")

    backup_path = CATALOG_BACKUP_DIR / f"pipeline_catalog_{_utc_stamp()}.json"
    shutil.copy2(BATCH_CATALOG_PATH, backup_path)
    return backup_path


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """
    Atomically write catalog JSON.

    Important:
    The Airflow onboarding task runs as the `airflow` user and must be able
    to read pipeline_catalog.json. NamedTemporaryFile often creates files with
    mode 0600, so after os.replace() Airflow can get PermissionError.

    Therefore we force the target file to be world-readable: 0644.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    text = json.dumps(payload, ensure_ascii=False, indent=2)
    text += "\n"

    target_mode = 0o644

    if path.exists():
        try:
            existing_mode = path.stat().st_mode & 0o777
            target_mode = (existing_mode | 0o644) & 0o777
        except Exception:
            target_mode = 0o644

    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        delete=False,
        dir=str(path.parent),
        prefix=f".{path.name}.",
        suffix=".tmp",
    ) as tmp:
        tmp.write(text)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)

    try:
        tmp_path.chmod(target_mode)
    except Exception:
        pass

    os.replace(tmp_path, path)

    try:
        path.chmod(target_mode)
    except Exception:
        pass

def _validate_name(field: str, value: Any, errors: list[str]) -> None:
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{field} is required.")
        return

    if not NAME_RE.match(value):
        errors.append(f"{field} must be lowercase letters, digits, and underscores; start with a letter.")


def _target_from_job_spec(job: dict[str, Any]) -> str:
    spec = job.get("spec") or {}
    if not isinstance(spec, dict):
        return ""

    return (
        spec.get("bronze_write", {}).get("target_path")
        or spec.get("target", {}).get("path")
        or spec.get("silver_write", {}).get("target_path")
        or spec.get("gold_write", {}).get("target_path")
        or ""
    )

def _views_from_spec(spec: dict[str, Any]) -> list[dict[str, Any]]:
    views = spec.get("views") or spec.get("sources") or []

    if isinstance(views, list):
        return [v for v in views if isinstance(v, dict)]

    return []


def _first_view_path(spec: dict[str, Any]) -> str:
    for view in _views_from_spec(spec):
        path = view.get("path")
        if path:
            return str(path)

    return ""


def _source_path_from_job_spec(job: dict[str, Any]) -> str:
    spec = job.get("spec") or {}

    if not isinstance(spec, dict):
        return ""

    return (
        (spec.get("source") or {}).get("path")
        or _first_view_path(spec)
        or ""
    )


def _has_sql_query(spec: dict[str, Any]) -> bool:
    sql_spec = spec.get("sql") or {}

    return isinstance(sql_spec, dict) and bool(str(sql_spec.get("query") or "").strip())


def validate_catalog_job_payload(pipeline_name: str, job: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []

    _validate_name("pipeline_name", pipeline_name, errors)

    if not isinstance(job, dict):
        return {
            "ok": False,
            "errors": ["job must be a JSON object."],
            "warnings": [],
        }

    _validate_name("job.name", job.get("name"), errors)

    job_type = job.get("job_type")
    if not job_type:
        errors.append("job.job_type is required.")
    elif job_type not in KNOWN_JOB_TYPES:
        warnings.append(f"Unknown job_type: {job_type}")

    spec = job.get("spec")
    if spec is None:
        spec = {}
        job["spec"] = {}

    if not isinstance(spec, dict):
        errors.append("job.spec must be a JSON object.")
        spec = {}

    if job_type == "generic_api_to_bronze":
        auth = spec.get("auth") or {}
        auth.setdefault("type", "none")
        auth.setdefault("secret_env", "")
        spec["auth"] = auth
        if not spec.get("source", {}).get("base_url"):
            errors.append("spec.source.base_url is required for generic_api_to_bronze.")
        if not spec.get("bronze_write", {}).get("target_path"):
            errors.append("spec.bronze_write.target_path is required for generic_api_to_bronze.")

    elif job_type in {"generic_bronze_to_silver", "generic_silver_to_gold", "generic_delta_to_gold"}:
        # SQL-first contract:
        # - Legacy mode may use spec.source.path
        # - New generic mode may use spec.views[] + spec.sql.query
        source_path = _source_path_from_job_spec(job)
        views = _views_from_spec(spec)

        if not source_path:
            errors.append("Either spec.source.path or at least one spec.views[].path is required.")

        if views:
            for idx, view in enumerate(views):
                if not view.get("alias"):
                    errors.append(f"spec.views[{idx}].alias is required.")
                if not view.get("path"):
                    errors.append(f"spec.views[{idx}].path is required.")
                view.setdefault("format", "delta")

        if views and not _has_sql_query(spec):
            errors.append("spec.sql.query is required when spec.views is used.")

        if not spec.get("target", {}).get("path"):
            errors.append("spec.target.path is required.")

        if spec.get("target", {}).get("mode") == "merge":
            merge_keys = spec.get("target", {}).get("merge_keys") or []
            if not merge_keys:
                errors.append("spec.target.merge_keys is required when target.mode = merge.")


    elif job_type == "generic_jdbc_manifest_to_bronze":
        if not job.get("manifest_ref") and not spec.get("manifest_ref"):
            errors.append("manifest_ref or spec.manifest_ref is required for generic_jdbc_manifest_to_bronze.")

    if not _target_from_job_spec(job) and job_type not in {"generic_jdbc_manifest_to_bronze"}:
        warnings.append("No target path detected from job.spec.")

    if "enabled" not in job:
        job["enabled"] = True

    if "tags" in job and not isinstance(job["tags"], list):
        errors.append("job.tags must be a list if provided.")

    return {
        "ok": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "normalized_job": job,
    }


def _validate_catalog_global(catalog: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    seen_pipelines: dict[str, int] = {}

    for pipeline_idx, pipeline in enumerate(catalog.get("pipelines", []) or []):
        if not isinstance(pipeline, dict):
            continue

        pipeline_name = str(pipeline.get("name") or "")
        if pipeline_name:
            previous_idx = seen_pipelines.get(pipeline_name)
            if previous_idx is not None:
                errors.append(
                    f"Duplicate pipeline name: '{pipeline_name}' "
                    f"(pipeline indexes {previous_idx} and {pipeline_idx})"
                )
            else:
                seen_pipelines[pipeline_name] = pipeline_idx

        seen_jobs: dict[str, int] = {}
        for job_idx, job in enumerate(pipeline.get("jobs", []) or []):
            if not isinstance(job, dict):
                continue

            job_name = str(job.get("name") or "")
            if not job_name:
                continue

            previous_job_idx = seen_jobs.get(job_name)
            if previous_job_idx is not None:
                errors.append(
                    f"Duplicate job name '{job_name}' in pipeline '{pipeline_name}' "
                    f"(job indexes {previous_job_idx} and {job_idx})"
                )
            else:
                seen_jobs[job_name] = job_idx

    return errors


def _raise_catalog_global_errors(catalog: dict[str, Any]) -> None:
    errors = _validate_catalog_global(catalog)
    if errors:
        raise HTTPException(
            status_code=422,
            detail={
                "message": "Invalid catalog: duplicate pipeline or job names.",
                "errors": errors,
            },
        )


def _infer_source_type(job: dict[str, Any], pipeline: dict[str, Any] | None = None) -> str:
    job_type = str(job.get("job_type") or "")

    if job.get("source_type"):
        return str(job["source_type"])

    if pipeline and pipeline.get("source_type"):
        return str(pipeline["source_type"])

    spec = job.get("spec") or {}

    if "jdbc" in job_type or job.get("manifest_ref") or spec.get("manifest_ref"):
        return "jdbc"

    if "api" in job_type:
        return "api"

    if "sqlserver" in job_type:
        return "sqlserver"

    return "unknown"


def _normalize_pipeline_for_catalog(
    pipeline: dict[str, Any],
    pipeline_name: str,
    job: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = copy.deepcopy(pipeline or {})

    inferred_source_type = _infer_source_type(job or {}, normalized)

    normalized["name"] = pipeline_name
    normalized["domain"] = normalized.get("domain") or pipeline_name
    normalized["description"] = normalized.get("description") or f"Catalog-managed pipeline: {pipeline_name}"
    normalized["enabled"] = normalized.get("enabled", True)
    normalized["source_type"] = normalized.get("source_type") or inferred_source_type

    dag = normalized.get("dag") or {}
    dag.setdefault("schedule", None)
    dag.setdefault("start_date", "2026-01-01T00:00:00+00:00")
    dag.setdefault("catchup", False)
    dag.setdefault("max_active_runs", 1)

    default_args = dag.get("default_args") or {}
    default_args.setdefault("owner", "admin")
    default_args.setdefault("retries", 0)
    default_args.setdefault("retry_delay_minutes", 1)
    dag["default_args"] = default_args

    tags = dag.get("tags") or normalized.get("tags") or []
    for tag in ["catalog_managed", normalized["source_type"]]:
        if tag not in tags:
            tags.append(tag)
    dag["tags"] = tags

    normalized["dag"] = dag
    normalized.setdefault("jobs", [])

    return normalized

def _normalize_job_for_catalog(job: dict[str, Any], pipeline: dict[str, Any] | None = None) -> dict[str, Any]:
    normalized = copy.deepcopy(job)

    source_type = _infer_source_type(normalized, pipeline)

    normalized["enabled"] = normalized.get("enabled", True)
    normalized["source_type"] = normalized.get("source_type") or source_type
    normalized["dependencies"] = normalized.get("dependencies") or []
    normalized["retries"] = normalized.get("retries", 0)
    normalized["retry_delay_minutes"] = normalized.get("retry_delay_minutes", 1)
    normalized["execution_timeout_minutes"] = normalized.get("execution_timeout_minutes", 10)
    normalized["tags"] = normalized.get("tags") or []

    spec = normalized.get("spec") or {}
    normalized["spec"] = spec

    if normalized.get("job_type") == "generic_api_to_bronze":
        spec.setdefault("load_type", "event")
        spec.setdefault("strategy", "append_event")

        auth = spec.get("auth") or {}
        auth.setdefault("type", "none")
        auth.setdefault("secret_env", "")
        spec["auth"] = auth

        spec.setdefault("request", {"query_params": {}, "headers": {}, "body": None})
        spec.setdefault("response", {"format": "json", "root_path": "$", "record_mode": "single_object"})
        spec.setdefault("validation", {"required_paths": [], "rules": []})
        spec.setdefault("mapping", {})
        spec.setdefault("schema", [])
        spec.setdefault("runtime_policy", {"max_retries": 3, "backoff_seconds": 10})
        spec.setdefault("spark", {"master": None, "packages": [], "conf": {}})
        return normalized

    if normalized.get("job_type") in {
        "generic_bronze_to_silver",
        "generic_silver_to_gold",
        "generic_delta_to_gold",
    }:
        spec.setdefault("filters", spec.get("filters", []))
        spec.setdefault("quality_rules", spec.get("quality_rules", []))
        spec.setdefault("dedupe", spec.get("dedupe", {"key_columns": [], "order_by": []}))
        spec.setdefault("spark", {"master": None, "packages": [], "conf": {}})

        if not (spec.get("source") or {}).get("path"):
            first_path = _first_view_path(spec)
            if first_path:
                spec["source"] = {
                    "path": first_path,
                    "format": "delta",
                }

        return normalized

    return normalized

def _find_pipeline(catalog: dict[str, Any], pipeline_name: str) -> tuple[int | None, dict[str, Any] | None]:
    for idx, pipeline in enumerate(catalog.get("pipelines", [])):
        if pipeline.get("name") == pipeline_name:
            return idx, pipeline

    return None, None


def _build_new_pipeline(req: CatalogJobApplyRequest) -> dict[str, Any]:
    raw_pipeline = req.pipeline.model_dump(exclude_none=True) if req.pipeline else {}
    return _normalize_pipeline_for_catalog(
        raw_pipeline,
        pipeline_name=req.pipeline_name,
        job=req.job,
    )


def _merge_job_into_catalog(req: CatalogJobApplyRequest) -> dict[str, Any]:
    validation = validate_catalog_job_payload(req.pipeline_name, copy.deepcopy(req.job))

    if not validation["ok"]:
        raise HTTPException(
            status_code=422,
            detail={
                "message": "Invalid catalog job payload.",
                "errors": validation["errors"],
                "warnings": validation["warnings"],
            },
        )

    job = validation["normalized_job"]
    catalog = _load_catalog()
    new_catalog = copy.deepcopy(catalog)

    pipeline_idx, pipeline = _find_pipeline(new_catalog, req.pipeline_name)

    change_type = "updated"

    if pipeline is None:
        if not req.create_pipeline_if_missing:
            raise HTTPException(
                status_code=404,
                detail=f"Pipeline not found: {req.pipeline_name}",
            )

        pipeline = _build_new_pipeline(req)
        new_catalog["pipelines"].append(pipeline)
        change_type = "created_pipeline_and_job"
    else:
        pipeline = _normalize_pipeline_for_catalog(
            pipeline,
            pipeline_name=req.pipeline_name,
            job=job,
        )
        if pipeline_idx is not None:
            new_catalog["pipelines"][pipeline_idx] = pipeline

    job = _normalize_job_for_catalog(job, pipeline)
    pipeline.setdefault("jobs", [])

    existing_idx = None
    for idx, existing_job in enumerate(pipeline["jobs"]):
        if existing_job.get("name") == job.get("name"):
            existing_idx = idx
            break

    if existing_idx is None:
        pipeline["jobs"].append(job)
        if change_type != "created_pipeline_and_job":
            change_type = "created_job"
    else:
        if not req.overwrite_existing_job:
            raise HTTPException(
                status_code=409,
                detail=f"Job already exists in catalog: {req.pipeline_name}.{job.get('name')}",
            )

        if req.base_job_hash:
            current_job_hash = _job_hash(pipeline["jobs"][existing_idx])

            if current_job_hash != req.base_job_hash:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": "Stale catalog payload. The job was changed after this payload was loaded.",
                        "pipeline_name": req.pipeline_name,
                        "job_name": job.get("name"),
                        "expected_hash": req.base_job_hash,
                        "current_hash": current_job_hash,
                        "hint": "Reload the current job from catalog, re-apply your changes, then submit again.",
                    },
                )

        pipeline["jobs"][existing_idx] = job
        change_type = "updated_job"

    _raise_catalog_global_errors(new_catalog)

    return {
        "ok": True,
        "change_type": change_type,
        "pipeline_name": req.pipeline_name,
        "job_name": job.get("name"),
        "target_path": _target_from_job_spec(job),
        "validation": validation,
        "catalog": new_catalog,
    }

def _merge_pipeline_jobs_into_catalog(req: CatalogPipelineApplyRequest) -> dict[str, Any]:
    if not req.jobs:
        raise HTTPException(status_code=422, detail="jobs list cannot be empty.")

    catalog = _load_catalog()
    new_catalog = copy.deepcopy(catalog)

    validation_results: list[dict[str, Any]] = []
    normalized_jobs: list[dict[str, Any]] = []

    for job in req.jobs:
        validation = validate_catalog_job_payload(req.pipeline_name, copy.deepcopy(job))
        validation_results.append(validation)

        if not validation["ok"]:
            raise HTTPException(
                status_code=422,
                detail={
                    "message": "Invalid job in pipeline payload.",
                    "job_name": job.get("name"),
                    "errors": validation["errors"],
                    "warnings": validation["warnings"],
                },
            )

        normalized_jobs.append(validation["normalized_job"])

    pipeline_idx, pipeline = _find_pipeline(new_catalog, req.pipeline_name)

    if pipeline is None:
        if not req.create_pipeline_if_missing:
            raise HTTPException(
                status_code=404,
                detail=f"Pipeline not found: {req.pipeline_name}",
            )

        raw_pipeline = req.pipeline.model_dump(exclude_none=True) if req.pipeline else {}
        pipeline = _normalize_pipeline_for_catalog(
            raw_pipeline,
            pipeline_name=req.pipeline_name,
            job=normalized_jobs[0] if normalized_jobs else None,
        )
        new_catalog["pipelines"].append(pipeline)
        change_type = "created_pipeline_and_jobs"
    else:
        pipeline = _normalize_pipeline_for_catalog(
            pipeline,
            pipeline_name=req.pipeline_name,
            job=normalized_jobs[0] if normalized_jobs else None,
        )
        if pipeline_idx is not None:
            new_catalog["pipelines"][pipeline_idx] = pipeline
        change_type = "updated_pipeline_jobs"

    pipeline.setdefault("jobs", [])

    applied_jobs: list[dict[str, Any]] = []

    for job in normalized_jobs:
        job = _normalize_job_for_catalog(job, pipeline)
        job_name = job.get("name")

        existing_idx = None
        for idx, existing_job in enumerate(pipeline["jobs"]):
            if existing_job.get("name") == job_name:
                existing_idx = idx
                break

        if existing_idx is None:
            pipeline["jobs"].append(job)
            job_change_type = "created_job"
        else:
            if not req.overwrite_existing_jobs:
                raise HTTPException(
                    status_code=409,
                    detail=f"Job already exists in catalog: {req.pipeline_name}.{job_name}",
                )
            
            expected_hash = (req.base_job_hashes or {}).get(str(job_name))

            if expected_hash:
                current_job_hash = _job_hash(pipeline["jobs"][existing_idx])

                if current_job_hash != expected_hash:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "message": "Stale catalog payload. One or more jobs changed after this payload was loaded.",
                            "pipeline_name": req.pipeline_name,
                            "job_name": job_name,
                            "expected_hash": expected_hash,
                            "current_hash": current_job_hash,
                            "hint": "Reload the current pipeline from catalog, re-apply your changes, then submit again.",
                        },
                    )
                        
            pipeline["jobs"][existing_idx] = job
            job_change_type = "updated_job"

        applied_jobs.append(
            {
                "job_name": job_name,
                "job_type": job.get("job_type"),
                "target_path": _target_from_job_spec(job),
                "change_type": job_change_type,
            }
        )

    _raise_catalog_global_errors(new_catalog)

    return {
        "ok": True,
        "change_type": change_type,
        "pipeline_name": req.pipeline_name,
        "job_count": len(applied_jobs),
        "jobs": applied_jobs,
        "validation": validation_results,
        "catalog": new_catalog,
    }


def _log_catalog_change(
    *,
    user: dict,
    action_type: str,
    pipeline_name: str,
    job_name: str | None,
    status: str,
    payload: dict[str, Any],
    backup_path: str | None = None,
    error_message: str | None = None,
) -> None:
    """
    Optional audit log. If migration is not applied yet, dashboard must not fail.
    """
    try:
        import app as _app

        _app.call_usp_void(
            "usp_insert_catalog_change_log",
            (
                user.get("sub", "unknown"),
                action_type,
                pipeline_name,
                job_name,
                status,
                json.dumps(payload, ensure_ascii=False),
                backup_path,
                error_message,
            ),
        )
    except Exception as exc:
        print(f"[WARN] Failed to write catalog change log: {exc}", flush=True)


@router.post("/pipelines/validate")
async def validate_catalog_pipeline(
    req: CatalogPipelineApplyRequest,
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    """
    Validate a full pipeline payload with multiple jobs.

    This does not write pipeline_catalog.json.
    It only validates that all jobs can be merged into the catalog contract.
    """
    merged = _merge_pipeline_jobs_into_catalog(req)

    return {
        "ok": True,
        "pipeline_name": merged["pipeline_name"],
        "job_count": merged["job_count"],
        "jobs": merged["jobs"],
        "validation": merged["validation"],
        "change_type": merged["change_type"],
    }



@router.get("/info")
async def catalog_info(user: dict = Depends(get_current_user)) -> dict[str, Any]:
    return {
        "catalog_path": str(BATCH_CATALOG_PATH),
        "catalog_exists": BATCH_CATALOG_PATH.exists(),
        "catalog_writable": os.access(BATCH_CATALOG_PATH, os.W_OK) if BATCH_CATALOG_PATH.exists() else os.access(BATCH_CATALOG_PATH.parent, os.W_OK),
        "backup_dir": str(CATALOG_BACKUP_DIR),
        "backup_dir_exists": CATALOG_BACKUP_DIR.exists(),
        "checked_at": _now_iso(),
    }


@router.get("/backups")
async def catalog_backups(user: dict = Depends(get_current_user)) -> dict[str, Any]:
    if not CATALOG_BACKUP_DIR.exists():
        return {"items": [], "count": 0}

    items = []
    for path in sorted(CATALOG_BACKUP_DIR.glob("pipeline_catalog_*.json"), reverse=True):
        items.append(
            {
                "name": path.name,
                "path": str(path),
                "size_bytes": path.stat().st_size,
                "modified_at": datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat(),
            }
        )

    return {"items": items, "count": len(items)}

@router.get("/pipelines/{pipeline_name}")
async def get_catalog_pipeline(
    pipeline_name: str,
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    catalog = _load_catalog()
    _, pipeline = _find_pipeline(catalog, pipeline_name)

    if pipeline is None:
        raise HTTPException(status_code=404, detail=f"Pipeline not found: {pipeline_name}")

    pipeline_meta = copy.deepcopy(pipeline)
    jobs = pipeline_meta.pop("jobs", [])

    return {
        "pipeline_name": pipeline_name,
        "create_pipeline_if_missing": False,
        "overwrite_existing_jobs": True,
        "pipeline": pipeline_meta,
        "jobs": jobs,
        "base_job_hashes": {
            str(job.get("name")): _job_hash(job)
            for job in jobs
            if job.get("name")
        },
        "catalog_hash": _catalog_hash(catalog),
    }

@router.get("/jobs/{pipeline_name}/{job_name}")
async def get_catalog_job(
    pipeline_name: str,
    job_name: str,
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    catalog = _load_catalog()
    _, pipeline = _find_pipeline(catalog, pipeline_name)

    if pipeline is None:
        raise HTTPException(status_code=404, detail=f"Pipeline not found: {pipeline_name}")

    for job in pipeline.get("jobs", []):
        if job.get("name") == job_name:
            pipeline_meta = copy.deepcopy(pipeline)
            pipeline_meta.pop("jobs", None)

            return {
                "pipeline_name": pipeline_name,
                "create_pipeline_if_missing": False,
                "overwrite_existing_job": True,
                "pipeline": pipeline_meta,
                "job": job,
                "base_job_hash": _job_hash(job),
                "catalog_hash": _catalog_hash(catalog),
            }

    raise HTTPException(
        status_code=404,
        detail=f"Job not found: {pipeline_name}.{job_name}",
    )

@router.post("/jobs/validate")
async def validate_catalog_job(
    req: CatalogJobApplyRequest,
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    validation = validate_catalog_job_payload(req.pipeline_name, copy.deepcopy(req.job))
    return validation


@router.post("/jobs/preview")
async def preview_catalog_job(
    req: CatalogJobApplyRequest,
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    merged = _merge_job_into_catalog(req)
    return {
        "ok": True,
        "dry_run": True,
        "change_type": merged["change_type"],
        "pipeline_name": merged["pipeline_name"],
        "job_name": merged["job_name"],
        "target_path": merged["target_path"],
        "validation": merged["validation"],
        "catalog": merged["catalog"],
    }


@router.post("/jobs/apply", dependencies=[Depends(require_role("admin", "operator"))])
async def apply_catalog_job(
    req: CatalogJobApplyRequest,
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    merged = _merge_job_into_catalog(req)

    if req.dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "change_type": merged["change_type"],
            "pipeline_name": merged["pipeline_name"],
            "job_name": merged["job_name"],
            "target_path": merged["target_path"],
            "validation": merged["validation"],
        }

    backup_path: Path | None = None

    try:
        backup_path = _backup_catalog()
        _atomic_write_json(BATCH_CATALOG_PATH, merged["catalog"])

        result = {
            "ok": True,
            "applied": True,
            "catalog_path": str(BATCH_CATALOG_PATH),
            "backup_path": str(backup_path),
            "change_type": merged["change_type"],
            "pipeline_name": merged["pipeline_name"],
            "job_name": merged["job_name"],
            "target_path": merged["target_path"],
            "validation": merged["validation"],
        }

        _log_catalog_change(
            user=user,
            action_type="catalog_job_apply",
            pipeline_name=merged["pipeline_name"],
            job_name=merged["job_name"],
            status="success",
            payload=result,
            backup_path=str(backup_path),
        )

        return result

    except Exception as exc:
        _log_catalog_change(
            user=user,
            action_type="catalog_job_apply",
            pipeline_name=req.pipeline_name,
            job_name=req.job.get("name"),
            status="failed",
            payload=req.model_dump(),
            backup_path=str(backup_path) if backup_path else None,
            error_message=str(exc),
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/jobs/apply-and-onboard", dependencies=[Depends(require_role("admin", "operator"))])
async def apply_and_onboard_catalog_job(
    req: CatalogOnboardRequest,
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    apply_req = CatalogJobApplyRequest(**req.model_dump(exclude={"onboarding_dry_run"}))
    apply_result = await apply_catalog_job(apply_req, user)

    try:
        from actions import trigger_dag

        conf: dict[str, Any] = {
            "catalog_path": "configs/batch/pipeline_catalog.json",
            "pipeline_name": apply_result["pipeline_name"],
            "job_name": apply_result["job_name"],
            "dry_run": req.onboarding_dry_run,
        }

        onboarding_result = trigger_dag(ONBOARDING_DAG_ID, conf=conf)

        result = {
            "ok": True,
            "applied": True,
            "onboarding_triggered": True,
            "apply": apply_result,
            "onboarding": onboarding_result,
        }

        _log_catalog_change(
            user=user,
            action_type="catalog_job_apply_and_onboard",
            pipeline_name=apply_result["pipeline_name"],
            job_name=apply_result["job_name"],
            status="success",
            payload=result,
            backup_path=apply_result.get("backup_path"),
        )

        return result

    except Exception as exc:
        _log_catalog_change(
            user=user,
            action_type="catalog_job_apply_and_onboard",
            pipeline_name=apply_result["pipeline_name"],
            job_name=apply_result["job_name"],
            status="failed",
            payload=apply_result,
            backup_path=apply_result.get("backup_path"),
            error_message=str(exc),
        )
        raise HTTPException(status_code=500, detail=f"Catalog applied, but onboarding failed: {exc}") from exc
    

@router.post("/pipelines/preview")
async def preview_catalog_pipeline(
    req: CatalogPipelineApplyRequest,
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    merged = _merge_pipeline_jobs_into_catalog(req)
    return {
        "ok": True,
        "dry_run": True,
        "change_type": merged["change_type"],
        "pipeline_name": merged["pipeline_name"],
        "job_count": merged["job_count"],
        "jobs": merged["jobs"],
        "validation": merged["validation"],
        "catalog": merged["catalog"],
    }


@router.post("/pipelines/apply", dependencies=[Depends(require_role("admin", "operator"))])
async def apply_catalog_pipeline(
    req: CatalogPipelineApplyRequest,
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    merged = _merge_pipeline_jobs_into_catalog(req)

    if req.dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "change_type": merged["change_type"],
            "pipeline_name": merged["pipeline_name"],
            "job_count": merged["job_count"],
            "jobs": merged["jobs"],
            "validation": merged["validation"],
        }

    backup_path: Path | None = None

    try:
        backup_path = _backup_catalog()
        _atomic_write_json(BATCH_CATALOG_PATH, merged["catalog"])

        result = {
            "ok": True,
            "applied": True,
            "catalog_path": str(BATCH_CATALOG_PATH),
            "backup_path": str(backup_path),
            "change_type": merged["change_type"],
            "pipeline_name": merged["pipeline_name"],
            "job_count": merged["job_count"],
            "jobs": merged["jobs"],
            "validation": merged["validation"],
        }

        _log_catalog_change(
            user=user,
            action_type="catalog_pipeline_apply",
            pipeline_name=merged["pipeline_name"],
            job_name=None,
            status="success",
            payload=result,
            backup_path=str(backup_path),
        )

        return result

    except Exception as exc:
        _log_catalog_change(
            user=user,
            action_type="catalog_pipeline_apply",
            pipeline_name=req.pipeline_name,
            job_name=None,
            status="failed",
            payload=req.model_dump(),
            backup_path=str(backup_path) if backup_path else None,
            error_message=str(exc),
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/pipelines/apply-and-onboard", dependencies=[Depends(require_role("admin", "operator"))])
async def apply_and_onboard_catalog_pipeline(
    req: CatalogPipelineOnboardRequest,
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    apply_req = CatalogPipelineApplyRequest(**req.model_dump(exclude={"onboarding_dry_run"}))
    apply_result = await apply_catalog_pipeline(apply_req, user)

    try:
        from actions import trigger_dag

        conf: dict[str, Any] = {
            "catalog_path": "configs/batch/pipeline_catalog.json",
            "pipeline_name": apply_result["pipeline_name"],
            "dry_run": req.onboarding_dry_run,
        }

        onboarding_result = trigger_dag(ONBOARDING_DAG_ID, conf=conf)

        result = {
            "ok": True,
            "applied": True,
            "onboarding_triggered": True,
            "apply": apply_result,
            "onboarding": onboarding_result,
        }

        _log_catalog_change(
            user=user,
            action_type="catalog_pipeline_apply_and_onboard",
            pipeline_name=apply_result["pipeline_name"],
            job_name=None,
            status="success",
            payload=result,
            backup_path=apply_result.get("backup_path"),
        )

        return result

    except Exception as exc:
        _log_catalog_change(
            user=user,
            action_type="catalog_pipeline_apply_and_onboard",
            pipeline_name=apply_result["pipeline_name"],
            job_name=None,
            status="failed",
            payload=apply_result,
            backup_path=apply_result.get("backup_path"),
            error_message=str(exc),
        )
        raise HTTPException(status_code=500, detail=f"Catalog applied, but onboarding failed: {exc}") from exc
    
