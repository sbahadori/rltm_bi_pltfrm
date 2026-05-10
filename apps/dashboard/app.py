import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import time

from fastapi import FastAPI ,Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI(title="BI Dashboard Config API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

PIPELINE_REPO_ROOT = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm"))
BATCH_CATALOG_PATH = PIPELINE_REPO_ROOT / "configs" / "batch" / "pipeline_catalog.json"
STREAM_REGISTRY_PATH = PIPELINE_REPO_ROOT / "configs" / "streaming" / "stream_registry.json"

STREAM_STATUS_FILE = Path(os.getenv("STREAM_STATUS_FILE","/runtime/spark_health/stream_supervisor_status.json",))
STREAM_LOG_DIR = Path(os.getenv("STREAM_LOG_DIR","/runtime/spark_health/logs",))
AIRFLOW_LOG_DIR = Path(os.getenv("AIRFLOW_LOG_DIR","/runtime/airflow_logs",))
STREAM_STATUS_STALE_SECONDS = int(os.getenv("STREAM_STATUS_STALE_SECONDS", "120"))
STREAM_HEARTBEAT_STALE_SECONDS = int(os.getenv("STREAM_HEARTBEAT_STALE_SECONDS", "120"))

def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _compute_stream_unit_status(unit: dict[str, Any], now_epoch: int) -> dict[str, Any]:
    pid = unit.get("pid")
    returncode = unit.get("returncode")
    retries = _safe_int(unit.get("retries")) or 0
    max_retries = _safe_int(unit.get("max_retries")) or 0
    heartbeat = unit.get("heartbeat") or {}

    heartbeat_ts = (
        _safe_int(heartbeat.get("ts_epoch"))
        or _safe_int(heartbeat.get("timestamp_epoch"))
        or _safe_int(heartbeat.get("last_update_ts_epoch"))
    )

    heartbeat_age_seconds = None
    if heartbeat_ts is not None:
        heartbeat_age_seconds = max(0, now_epoch - heartbeat_ts)

    heartbeat_status = str(heartbeat.get("status", "")).lower()

    if returncode is not None:
        if retries >= max_retries and max_retries > 0:
            computed_status = "failed"
            status_reason = f"process exited with returncode={returncode} and reached max retries"
        elif int(returncode) == 0:
            computed_status = "stopped"
            status_reason = "process exited successfully"
        else:
            computed_status = "restarting"
            status_reason = f"process exited with returncode={returncode}; retry may be scheduled"
    elif pid is not None:
        if heartbeat_status == "error":
            computed_status = "error"
            status_reason = "heartbeat reports error"
        elif heartbeat_age_seconds is not None and heartbeat_age_seconds > STREAM_HEARTBEAT_STALE_SECONDS:
            computed_status = "stale"
            status_reason = f"heartbeat is stale: {heartbeat_age_seconds}s"
        else:
            computed_status = "running"
            status_reason = "process is running"
    else:
        computed_status = "unknown"
        status_reason = "no pid and no returncode"

    is_healthy = computed_status == "running"

    return {
        "computed_status": computed_status,
        "status_reason": status_reason,
        "is_healthy": is_healthy,
        "heartbeat_age_seconds": heartbeat_age_seconds,
        "heartbeat_status": heartbeat_status or None,
    }

def _path_state(path: Path) -> dict[str, Any]:
    exists = path.exists()
    is_file = path.is_file()
    is_dir = path.is_dir()

    return {
        "path": str(path),
        "exists": exists,
        "is_file": is_file,
        "is_dir": is_dir,
        "readable": os.access(path, os.R_OK) if exists else False,
    }

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _target_path_from_spec(spec: dict[str, Any]) -> str:
    return (
        spec.get("bronze_write", {}).get("target_path")
        or spec.get("target", {}).get("path")
        or spec.get("silver_write", {}).get("target_path")
        or spec.get("gold_write", {}).get("target_path")
        or ""
    )


def _build_batch_jobs(catalog: dict[str, Any]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []

    for pipeline in catalog.get("pipelines", []):
        if not pipeline.get("enabled", True):
            continue

        pipeline_name = pipeline.get("name", "")
        schedule = pipeline.get("dag", {}).get("schedule")

        for job in pipeline.get("jobs", []):
            if not job.get("enabled", True):
                continue

            spec = job.get("spec", {}) or {}
            source = spec.get("source", {}) or {}

            jobs.append(
                {
                    "id": f"{pipeline_name}__{job.get('name', '')}",
                    "name": job.get("name", ""),
                    "pipeline": pipeline_name,
                    "type": "batch",
                    "job_type": job.get("job_type", ""),
                    "source_url": source.get("base_url", ""),
                    "dependencies": job.get("dependencies", []),
                    "timeout_minutes": job.get("execution_timeout_minutes", 30),
                    "tags": job.get("tags", []),
                    "target_path": _target_path_from_spec(spec),
                    "schedule": schedule,
                }
            )

    return jobs


def _build_stream_jobs(registry: dict[str, Any]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []

    for stream in registry.get("streams", []):
        if not stream.get("enabled", True):
            continue

        name = stream.get("name", "")
        source = stream.get("source", {}) or {}
        bronze = stream.get("bronze", {}) or {}
        silver = stream.get("silver", {}) or {}

        if bronze:
            bronze_app_name = bronze.get("app_name", f"{name}_bronze")
            jobs.append(
                {
                    "id": f"{name}__bronze",
                    "name": bronze_app_name,
                    "pipeline": name,
                    "type": "stream",
                    "job_type": bronze.get("engine", "generic_kafka_to_bronze"),
                    "source_url": f"{source.get('bootstrap_servers', '')} / {source.get('topic', '')}",
                    "dependencies": [],
                    "trigger": bronze.get("trigger_interval", "15 seconds"),
                    "target_path": bronze.get("path", ""),
                    "heartbeat_file": bronze.get("heartbeat_file", ""),
                }
            )

        if silver:
            silver_app_name = silver.get("app_name", f"{name}_silver")
            bronze_app_name = bronze.get("app_name", f"{name}_bronze") if bronze else f"{name}_bronze"
            jobs.append(
                {
                    "id": f"{name}__silver",
                    "name": silver_app_name,
                    "pipeline": name,
                    "type": "stream",
                    "job_type": silver.get("engine", "generic_bronze_to_silver"),
                    "source_url": silver.get("bronze_path", ""),
                    "dependencies": [bronze_app_name] if bronze else [],
                    "trigger": silver.get("trigger_interval", "30 seconds"),
                    "target_path": silver.get("path", ""),
                    "quarantine_path": silver.get("quarantine_path", ""),
                    "heartbeat_file": silver.get("heartbeat_file", ""),
                }
            )

    return jobs


def _build_pipelines(catalog: dict[str, Any], registry: dict[str, Any]) -> list[dict[str, Any]]:
    pipelines: list[dict[str, Any]] = []

    for pipeline in catalog.get("pipelines", []):
        if not pipeline.get("enabled", True):
            continue

        dag = pipeline.get("dag", {}) or {}
        pipelines.append(
            {
                "name": pipeline.get("name", ""),
                "type": "batch",
                "description": pipeline.get("description", ""),
                "schedule": dag.get("schedule"),
                "tags": dag.get("tags", []),
                "jobs": [j.get("name", "") for j in pipeline.get("jobs", []) if j.get("enabled", True)],
            }
        )

    for stream in registry.get("streams", []):
        if not stream.get("enabled", True):
            continue

        jobs: list[str] = []
        if stream.get("bronze"):
            jobs.append(stream["bronze"].get("app_name", f"{stream.get('name', '')}_bronze"))
        if stream.get("silver"):
            jobs.append(stream["silver"].get("app_name", f"{stream.get('name', '')}_silver"))

        source = stream.get("source", {}) or {}
        pipelines.append(
            {
                "name": stream.get("name", ""),
                "type": "stream",
                "description": f"Streaming pipeline: {source.get('topic', '')}",
                "schedule": "always-on",
                "tags": ["stream", "kafka"],
                "jobs": jobs,
            }
        )

    return pipelines


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/config")
async def get_config() -> JSONResponse:
    catalog = _load_json(BATCH_CATALOG_PATH) or {"pipelines": []}
    registry = _load_json(STREAM_REGISTRY_PATH) or {"streams": []}

    return JSONResponse(
        {
            "pipelines": _build_pipelines(catalog, registry),
            "jobs": _build_batch_jobs(catalog) + _build_stream_jobs(registry),
            "meta": {
                "batch_catalog_path": str(BATCH_CATALOG_PATH),
                "stream_registry_path": str(STREAM_REGISTRY_PATH),
                "loaded_at": now_iso(),
            },
        }
    )

@app.get("/api/runtime/mounts")
async def get_runtime_mounts() -> JSONResponse:
    return JSONResponse(
        {
            "stream_status_file": _path_state(STREAM_STATUS_FILE),
            "stream_log_dir": _path_state(STREAM_LOG_DIR),
            "airflow_log_dir": _path_state(AIRFLOW_LOG_DIR),
            "env": {
                "STREAM_STATUS_FILE": os.getenv("STREAM_STATUS_FILE", ""),
                "STREAM_LOG_DIR": os.getenv("STREAM_LOG_DIR", ""),
                "AIRFLOW_LOG_DIR": os.getenv("AIRFLOW_LOG_DIR", ""),
            },
            "checked_at": now_iso(),
        }
    )

@app.get("/api/runtime/streams/status")
async def get_stream_runtime_status(raw: bool = Query(default=False)) -> JSONResponse:
    now_epoch = int(time.time())

    if not STREAM_STATUS_FILE.exists():
        return JSONResponse(
            {
                "available": False,
                "status": "unavailable",
                "reason": f"stream status file not found: {STREAM_STATUS_FILE}",
                "stream_status_file": str(STREAM_STATUS_FILE),
                "observed_at_epoch": now_epoch,
                "observed_at": now_iso(),
                "units": {},
            },
            status_code=200,
        )

    try:
        data = _read_json_file(STREAM_STATUS_FILE)
    except Exception as exc:
        return JSONResponse(
            {
                "available": False,
                "status": "error",
                "reason": f"failed to read stream status file: {exc}",
                "stream_status_file": str(STREAM_STATUS_FILE),
                "observed_at_epoch": now_epoch,
                "observed_at": now_iso(),
                "units": {},
            },
            status_code=200,
        )

    supervisor_ts = _safe_int(data.get("ts_epoch"))
    supervisor_age_seconds = None
    if supervisor_ts is not None:
        supervisor_age_seconds = max(0, now_epoch - supervisor_ts)

    supervisor_stale = (
        supervisor_age_seconds is not None
        and supervisor_age_seconds > STREAM_STATUS_STALE_SECONDS
    )

    units = data.get("units", {}) or {}
    enriched_units: dict[str, Any] = {}

    for unit_name, unit in units.items():
        if not isinstance(unit, dict):
            continue

        computed = _compute_stream_unit_status(unit, now_epoch)

        enriched_units[unit_name] = {
            **unit,
            **computed,
            "unit_name": unit_name,
        }

    any_failed = any(
        u.get("computed_status") in {"failed", "error"}
        for u in enriched_units.values()
    )
    any_running = any(
        u.get("computed_status") == "running"
        for u in enriched_units.values()
    )

    if supervisor_stale:
        overall_status = "stale"
    elif any_failed:
        overall_status = "degraded"
    elif any_running:
        overall_status = "running"
    elif enriched_units:
        overall_status = "not_running"
    else:
        overall_status = "empty"

    payload = {
        "available": True,
        "status": overall_status,
        "stream_status_file": str(STREAM_STATUS_FILE),
        "observed_at_epoch": now_epoch,
        "observed_at": now_iso(),
        "supervisor_ts_epoch": supervisor_ts,
        "supervisor_ts_iso": data.get("ts_iso"),
        "supervisor_age_seconds": supervisor_age_seconds,
        "supervisor_stale": supervisor_stale,
        "units": enriched_units,
    }

    if raw:
        payload["raw"] = data

    return JSONResponse(payload)