import base64
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse

app = FastAPI(title="BI Dashboard Runtime API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

PIPELINE_REPO_ROOT = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm"))
BATCH_CATALOG_PATH = PIPELINE_REPO_ROOT / "configs" / "batch" / "pipeline_catalog.json"
STREAM_REGISTRY_PATH = PIPELINE_REPO_ROOT / "configs" / "streaming" / "stream_registry.json"

STREAM_STATUS_FILE = Path(os.getenv("STREAM_STATUS_FILE", "/runtime/spark_health/stream_supervisor_status.json"))
STREAM_LOG_DIR = Path(os.getenv("STREAM_LOG_DIR", "/runtime/spark_health/logs"))
AIRFLOW_LOG_DIR = Path(os.getenv("AIRFLOW_LOG_DIR", "/runtime/airflow_logs"))

STREAM_STATUS_STALE_SECONDS = int(os.getenv("STREAM_STATUS_STALE_SECONDS", "120"))
STREAM_HEARTBEAT_STALE_SECONDS = int(os.getenv("STREAM_HEARTBEAT_STALE_SECONDS", "120"))

AIRFLOW_API_BASE = os.getenv("AIRFLOW_API_BASE", "http://airflow-api-server:8080").rstrip("/")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
AIRFLOW_TIMEOUT_SECONDS = int(os.getenv("AIRFLOW_TIMEOUT_SECONDS", "8"))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _duration_seconds(start: Any, end: Any) -> float | None:
    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    if not start_dt or not end_dt:
        return None
    return round((end_dt - start_dt).total_seconds(), 3)


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _read_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _path_state(path: Path) -> dict[str, Any]:
    exists = path.exists()
    return {
        "path": str(path),
        "exists": exists,
        "is_file": path.is_file(),
        "is_dir": path.is_dir(),
        "readable": os.access(path, os.R_OK) if exists else False,
    }


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
        dag_cfg = pipeline.get("dag", {}) or {}
        schedule = dag_cfg.get("schedule")
        for job in pipeline.get("jobs", []):
            if not job.get("enabled", True):
                continue
            spec = job.get("spec", {}) or {}
            source = spec.get("source", {}) or {}
            job_name = job.get("name", "")
            jobs.append(
                {
                    "id": f"{pipeline_name}__{job_name}",
                    "name": job_name,
                    "pipeline": pipeline_name,
                    "dag_id": pipeline_name,
                    "task_id": job_name,
                    "type": "batch",
                    "job_type": job.get("job_type", ""),
                    "source_url": source.get("base_url", ""),
                    "dependencies": job.get("dependencies", []),
                    "timeout_minutes": job.get("execution_timeout_minutes", 30),
                    "tags": job.get("tags", []),
                    "target_path": _target_path_from_spec(spec),
                    "schedule": schedule,
                    "defined_status": "defined",
                }
            )
    return jobs


def _build_stream_jobs(registry: dict[str, Any]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    for stream in registry.get("streams", []):
        if not stream.get("enabled", True):
            continue
        stream_name = stream.get("name", "")
        source = stream.get("source", {}) or {}
        bronze = stream.get("bronze", {}) or {}
        silver = stream.get("silver", {}) or {}

        if bronze:
            bronze_app_name = bronze.get("app_name", f"bronze_{stream_name}")
            jobs.append(
                {
                    "id": f"{stream_name}__bronze",
                    "name": bronze_app_name,
                    "pipeline": stream_name,
                    "type": "stream",
                    "layer": "bronze",
                    "runtime_unit_name": f"{stream_name}_bronze",
                    "runtime_aliases": [f"{stream_name}_bronze", bronze_app_name],
                    "job_type": bronze.get("engine", "generic_kafka_to_bronze"),
                    "source_url": f"{source.get('bootstrap_servers', '')} / {source.get('topic', '')}",
                    "dependencies": [],
                    "trigger": bronze.get("trigger_interval", "15 seconds"),
                    "target_path": bronze.get("path", ""),
                    "heartbeat_file": bronze.get("heartbeat_file", ""),
                    "defined_status": "defined",
                }
            )
        if silver:
            silver_app_name = silver.get("app_name", f"silver_{stream_name}")
            bronze_app_name = bronze.get("app_name", f"bronze_{stream_name}") if bronze else f"bronze_{stream_name}"
            jobs.append(
                {
                    "id": f"{stream_name}__silver",
                    "name": silver_app_name,
                    "pipeline": stream_name,
                    "type": "stream",
                    "layer": "silver",
                    "runtime_unit_name": f"{stream_name}_silver",
                    "runtime_aliases": [f"{stream_name}_silver", silver_app_name],
                    "job_type": silver.get("engine", "generic_bronze_to_silver"),
                    "source_url": silver.get("bronze_path", ""),
                    "dependencies": [bronze_app_name] if bronze else [],
                    "trigger": silver.get("trigger_interval", "30 seconds"),
                    "target_path": silver.get("path", ""),
                    "quarantine_path": silver.get("quarantine_path", ""),
                    "heartbeat_file": silver.get("heartbeat_file", ""),
                    "defined_status": "defined",
                }
            )
    return jobs


def _build_pipelines(catalog: dict[str, Any], registry: dict[str, Any]) -> list[dict[str, Any]]:
    pipelines: list[dict[str, Any]] = []
    for pipeline in catalog.get("pipelines", []):
        if not pipeline.get("enabled", True):
            continue
        dag = pipeline.get("dag", {}) or {}
        jobs = [j.get("name", "") for j in pipeline.get("jobs", []) if j.get("enabled", True)]
        pipelines.append(
            {
                "name": pipeline.get("name", ""),
                "type": "batch",
                "description": pipeline.get("description", ""),
                "schedule": dag.get("schedule"),
                "tags": dag.get("tags", []),
                "jobs": jobs,
            }
        )
    for stream in registry.get("streams", []):
        if not stream.get("enabled", True):
            continue
        jobs: list[str] = []
        stream_name = stream.get("name", "")
        source = stream.get("source", {}) or {}
        if stream.get("bronze"):
            jobs.append(stream["bronze"].get("app_name", f"bronze_{stream_name}"))
        if stream.get("silver"):
            jobs.append(stream["silver"].get("app_name", f"silver_{stream_name}"))
        pipelines.append(
            {
                "name": stream_name,
                "type": "stream",
                "description": f"Streaming pipeline: {source.get('topic', '')}",
                "schedule": "always-on",
                "tags": ["stream", "kafka"],
                "jobs": jobs,
            }
        )
    return pipelines


def _load_config_bundle() -> dict[str, Any]:
    catalog = _load_json(BATCH_CATALOG_PATH) or {"pipelines": []}
    registry = _load_json(STREAM_REGISTRY_PATH) or {"streams": []}
    return {
        "catalog": catalog,
        "registry": registry,
        "pipelines": _build_pipelines(catalog, registry),
        "jobs": _build_batch_jobs(catalog) + _build_stream_jobs(registry),
    }


def _basic_auth_header() -> str:
    raw = f"{AIRFLOW_USER}:{AIRFLOW_PASSWORD}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def _http_get_json(url: str, timeout: int = AIRFLOW_TIMEOUT_SECONDS) -> dict[str, Any]:
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/json")
    req.add_header("Authorization", _basic_auth_header())
    with urllib.request.urlopen(req, timeout=timeout) as response:
        body = response.read().decode("utf-8")
        return json.loads(body) if body else {}


def _airflow_get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    params = params or {}
    query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    url = f"{AIRFLOW_API_BASE}{path}"
    if query:
        url = f"{url}?{query}"
    return _http_get_json(url)


def _airflow_get_with_fallback(paths: list[tuple[str, dict[str, Any] | None]]) -> tuple[dict[str, Any] | None, str | None]:
    errors: list[str] = []
    for path, params in paths:
        try:
            return _airflow_get(path, params), None
        except urllib.error.HTTPError as exc:
            errors.append(f"{path}: HTTP {exc.code}")
        except urllib.error.URLError as exc:
            errors.append(f"{path}: {exc.reason}")
        except Exception as exc:
            errors.append(f"{path}: {exc}")
    return None, "; ".join(errors)


def _extract_dag_runs(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not payload:
        return []
    for key in ("dag_runs", "dagRuns", "dagruns", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return payload if isinstance(payload, list) else []


def _extract_task_instances(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not payload:
        return []
    for key in ("task_instances", "taskInstances", "taskinstances", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return payload if isinstance(payload, list) else []


def _normalize_dag_run(run: dict[str, Any]) -> dict[str, Any]:
    run_id = run.get("dag_run_id") or run.get("dagRunId") or run.get("run_id") or run.get("id") or ""
    start = run.get("start_date") or run.get("startDate") or run.get("logical_date") or run.get("logicalDate") or run.get("execution_date")
    end = run.get("end_date") or run.get("endDate")
    return {
        "dag_run_id": run_id,
        "run_id": run_id,
        "state": run.get("state") or "unknown",
        "run_type": run.get("run_type") or run.get("runType"),
        "logical_date": run.get("logical_date") or run.get("logicalDate") or run.get("execution_date"),
        "started_at": start,
        "ended_at": end,
        "duration_seconds": _duration_seconds(start, end),
    }


def _normalize_task_instance(ti: dict[str, Any]) -> dict[str, Any]:
    task_id = ti.get("task_id") or ti.get("taskId") or ""
    start = ti.get("start_date") or ti.get("startDate")
    end = ti.get("end_date") or ti.get("endDate")
    return {
        "task_id": task_id,
        "state": ti.get("state") or "unknown",
        "started_at": start,
        "ended_at": end,
        "duration_seconds": _duration_seconds(start, end),
        "try_number": ti.get("try_number") or ti.get("tryNumber"),
        "operator": ti.get("operator"),
    }


def _airflow_latest_for_dag(dag_id: str, limit: int = 5) -> dict[str, Any]:
    quoted = urllib.parse.quote(dag_id, safe="")
    paths = [
        (f"/api/v2/dags/{quoted}/dagRuns", {"limit": limit, "order_by": "-logical_date"}),
        (f"/api/v1/dags/{quoted}/dagRuns", {"limit": limit, "order_by": "-execution_date"}),
    ]
    payload, error = _airflow_get_with_fallback(paths)
    runs = [_normalize_dag_run(r) for r in _extract_dag_runs(payload)]
    return {"available": error is None, "error": error, "runs": runs, "latest": runs[0] if runs else None}


def _airflow_task_instances(dag_id: str, dag_run_id: str) -> dict[str, Any]:
    quoted_dag = urllib.parse.quote(dag_id, safe="")
    quoted_run = urllib.parse.quote(dag_run_id, safe="")
    paths = [
        (f"/api/v2/dags/{quoted_dag}/dagRuns/{quoted_run}/taskInstances", {"limit": 200}),
        (f"/api/v1/dags/{quoted_dag}/dagRuns/{quoted_run}/taskInstances", {"limit": 200}),
    ]
    payload, error = _airflow_get_with_fallback(paths)
    return {"available": error is None, "error": error, "task_instances": [_normalize_task_instance(ti) for ti in _extract_task_instances(payload)]}


def _unit_aliases(unit_name: str) -> list[str]:
    aliases = [unit_name]
    if unit_name.endswith("_bronze"):
        base = unit_name[: -len("_bronze")]
        aliases.append(f"bronze_{base}")
    if unit_name.endswith("_silver"):
        base = unit_name[: -len("_silver")]
        aliases.append(f"silver_{base}")
    return list(dict.fromkeys(aliases))


def _compute_stream_unit_status(unit: dict[str, Any], now_epoch: int) -> dict[str, Any]:
    pid = unit.get("pid")
    returncode = unit.get("returncode")
    retries = _safe_int(unit.get("retries")) or 0
    max_retries = _safe_int(unit.get("max_retries")) or 0
    heartbeat = unit.get("heartbeat") or {}
    heartbeat_ts = _safe_int(heartbeat.get("ts_epoch")) or _safe_int(heartbeat.get("timestamp_epoch")) or _safe_int(heartbeat.get("last_update_ts_epoch"))
    heartbeat_age_seconds = max(0, now_epoch - heartbeat_ts) if heartbeat_ts is not None else None
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

    return {
        "computed_status": computed_status,
        "status_reason": status_reason,
        "is_healthy": computed_status == "running",
        "heartbeat_age_seconds": heartbeat_age_seconds,
        "heartbeat_status": heartbeat_status or None,
    }


def _stream_status_payload(raw: bool = False) -> dict[str, Any]:
    now_epoch = int(time.time())
    if not STREAM_STATUS_FILE.exists():
        return {
            "available": False,
            "status": "unavailable",
            "reason": f"stream status file not found: {STREAM_STATUS_FILE}",
            "stream_status_file": str(STREAM_STATUS_FILE),
            "observed_at_epoch": now_epoch,
            "observed_at": now_iso(),
            "units": {},
        }
    try:
        data = _read_json_file(STREAM_STATUS_FILE)
    except Exception as exc:
        return {
            "available": False,
            "status": "error",
            "reason": f"failed to read stream status file: {exc}",
            "stream_status_file": str(STREAM_STATUS_FILE),
            "observed_at_epoch": now_epoch,
            "observed_at": now_iso(),
            "units": {},
        }

    supervisor_ts = _safe_int(data.get("ts_epoch"))
    supervisor_age_seconds = max(0, now_epoch - supervisor_ts) if supervisor_ts is not None else None
    supervisor_stale = supervisor_age_seconds is not None and supervisor_age_seconds > STREAM_STATUS_STALE_SECONDS
    enriched_units: dict[str, Any] = {}
    for unit_name, unit in (data.get("units", {}) or {}).items():
        if isinstance(unit, dict):
            computed = _compute_stream_unit_status(unit, now_epoch)
            enriched_units[unit_name] = {**unit, **computed, "unit_name": unit_name, "aliases": _unit_aliases(unit_name)}

    any_failed = any(u.get("computed_status") in {"failed", "error"} for u in enriched_units.values())
    any_running = any(u.get("computed_status") == "running" for u in enriched_units.values())
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
    return payload


def _find_stream_unit(job: dict[str, Any], stream_status: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
    expected_names = {job.get("name", ""), job.get("runtime_unit_name", "")}
    expected_names.update(job.get("runtime_aliases") or [])
    for unit_name, unit in (stream_status.get("units", {}) if stream_status else {}).items():
        aliases = set(unit.get("aliases") or [])
        aliases.update({unit_name, unit.get("unit_name", ""), unit.get("app_name", ""), unit.get("name", "")})
        if expected_names.intersection(aliases):
            return unit_name, unit
    return None, None


def _latest_batch_status_by_dag(batch_jobs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {dag_id: _airflow_latest_for_dag(dag_id, limit=5) for dag_id in sorted({job["pipeline"] for job in batch_jobs})}


def _enrich_runtime_jobs(config_jobs: list[dict[str, Any]]) -> dict[str, Any]:
    stream_status = _stream_status_payload(raw=False)
    batch_runtime_by_dag = _latest_batch_status_by_dag([j for j in config_jobs if j.get("type") == "batch"])
    enriched: list[dict[str, Any]] = []
    for job in config_jobs:
        base = {
            **job,
            "current_status": job.get("defined_status", "defined"),
            "runtime_source": "config",
            "runtime_available": False,
            "latest_run_state": None,
            "latest_run_id": None,
            "latest_run_started_at": None,
            "latest_run_ended_at": None,
            "duration_seconds": None,
            "status_reason": None,
        }
        if job.get("type") == "stream":
            unit_name, unit = _find_stream_unit(job, stream_status)
            if unit:
                base.update(
                    {
                        "current_status": unit.get("computed_status", "unknown"),
                        "runtime_source": "stream_supervisor",
                        "runtime_available": True,
                        "runtime_unit_name": unit_name,
                        "status_reason": unit.get("status_reason"),
                        "heartbeat_age_seconds": unit.get("heartbeat_age_seconds"),
                        "latest_run_state": unit.get("computed_status", "unknown"),
                    }
                )
            else:
                base.update(
                    {
                        "current_status": "unknown",
                        "runtime_source": "stream_supervisor",
                        "runtime_available": bool(stream_status.get("available")),
                        "status_reason": "no matching runtime unit found" if stream_status.get("available") else stream_status.get("reason", "stream status unavailable"),
                    }
                )
        elif job.get("type") == "batch":
            batch_runtime = batch_runtime_by_dag.get(job.get("pipeline", ""), {})
            latest = batch_runtime.get("latest")
            if latest:
                base.update(
                    {
                        "current_status": latest.get("state", "unknown"),
                        "runtime_source": "airflow",
                        "runtime_available": bool(batch_runtime.get("available")),
                        "latest_run_state": latest.get("state"),
                        "latest_run_id": latest.get("dag_run_id"),
                        "latest_run_started_at": latest.get("started_at"),
                        "latest_run_ended_at": latest.get("ended_at"),
                        "duration_seconds": latest.get("duration_seconds"),
                        "status_reason": None if batch_runtime.get("available") else batch_runtime.get("error"),
                    }
                )
            else:
                base.update(
                    {
                        "current_status": "no_runs" if batch_runtime.get("available") else "defined",
                        "runtime_source": "airflow",
                        "runtime_available": bool(batch_runtime.get("available")),
                        "status_reason": batch_runtime.get("error"),
                    }
                )
        enriched.append(base)
    return {"jobs": enriched, "stream_status": stream_status, "loaded_at": now_iso()}


def _tail_text(path: Path, lines: int = 300) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Log file not found: {path}")
    return "\n".join(path.read_text(encoding="utf-8", errors="replace").splitlines()[-lines:])


def _candidate_stream_log_paths(job_id: str) -> list[Path]:
    candidates = [STREAM_LOG_DIR / f"{job_id}.log"]
    if job_id.startswith("bronze_"):
        candidates.append(STREAM_LOG_DIR / f"{job_id[len('bronze_'):]}_bronze.log")
    if job_id.startswith("silver_"):
        candidates.append(STREAM_LOG_DIR / f"{job_id[len('silver_'):]}_silver.log")
    if job_id.endswith("__bronze"):
        candidates.append(STREAM_LOG_DIR / f"{job_id[:-len('__bronze')]}_bronze.log")
    if job_id.endswith("__silver"):
        candidates.append(STREAM_LOG_DIR / f"{job_id[:-len('__silver')]}_silver.log")
    return list(dict.fromkeys(candidates))


def _find_latest_airflow_log(dag_id: str, task_id: str) -> Path:
    candidates: list[Path] = []
    dag_dir = AIRFLOW_LOG_DIR / f"dag_id={dag_id}"
    if dag_dir.exists():
        for pattern in [f"**/task_id={task_id}/**/*.log", f"**/{task_id}/**/*.log", f"**/*{task_id}*.log"]:
            candidates.extend(dag_dir.glob(pattern))
    if not candidates and AIRFLOW_LOG_DIR.exists():
        candidates.extend(AIRFLOW_LOG_DIR.glob(f"**/dag_id={dag_id}/**/task_id={task_id}/**/*.log"))
        candidates.extend(AIRFLOW_LOG_DIR.glob(f"**/*{dag_id}*{task_id}*.log"))
    candidates = [p for p in candidates if p.is_file()]
    if not candidates:
        raise FileNotFoundError(f"No Airflow log found for dag_id={dag_id}, task_id={task_id}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/config")
async def get_config() -> JSONResponse:
    bundle = _load_config_bundle()
    return JSONResponse(
        {
            "pipelines": bundle["pipelines"],
            "jobs": bundle["jobs"],
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
                "AIRFLOW_API_BASE": AIRFLOW_API_BASE,
            },
            "checked_at": now_iso(),
        }
    )


@app.get("/api/runtime/streams/status")
async def get_stream_runtime_status(raw: bool = Query(default=False)) -> JSONResponse:
    return JSONResponse(_stream_status_payload(raw=raw))


@app.get("/api/runtime/jobs")
async def get_runtime_jobs() -> JSONResponse:
    bundle = _load_config_bundle()
    runtime = _enrich_runtime_jobs(bundle["jobs"])
    return JSONResponse(
        {
            "jobs": runtime["jobs"],
            "stream_status": runtime["stream_status"],
            "meta": {
                "loaded_at": runtime["loaded_at"],
                "airflow_api_base": AIRFLOW_API_BASE,
                "batch_catalog_path": str(BATCH_CATALOG_PATH),
                "stream_registry_path": str(STREAM_REGISTRY_PATH),
            },
        }
    )


@app.get("/api/runtime/runs/{job_id}")
async def get_runtime_runs(job_id: str, limit: int = Query(default=10, ge=1, le=50)) -> JSONResponse:
    bundle = _load_config_bundle()
    job = next((j for j in bundle["jobs"] if j.get("id") == job_id or j.get("name") == job_id), None)
    if not job:
        return JSONResponse({"job_id": job_id, "available": False, "reason": "job not found", "runs": []}, status_code=404)

    if job.get("type") == "stream":
        stream_status = _stream_status_payload(raw=False)
        unit_name, unit = _find_stream_unit(job, stream_status)
        return JSONResponse(
            {
                "job_id": job.get("id"),
                "job_name": job.get("name"),
                "type": "stream",
                "available": bool(unit),
                "reason": None if unit else "stream history registry is not implemented; current status only",
                "runs": [
                    {
                        "run_id": unit_name,
                        "state": unit.get("computed_status"),
                        "started_at": unit.get("last_start_ts_iso") or unit.get("started_at"),
                        "ended_at": None,
                        "duration_seconds": None,
                        "status_reason": unit.get("status_reason"),
                        "source": "stream_supervisor_current_state",
                    }
                ]
                if unit
                else [],
            }
        )

    dag_id = job.get("pipeline", "")
    task_id = job.get("name", "")
    dag_runtime = _airflow_latest_for_dag(dag_id, limit=limit)
    runs: list[dict[str, Any]] = []
    for run in dag_runtime.get("runs", []):
        dag_run_id = run.get("dag_run_id")
        task_state = None
        task_detail = None
        if dag_run_id:
            ti_payload = _airflow_task_instances(dag_id, dag_run_id)
            for ti in ti_payload.get("task_instances", []):
                if ti.get("task_id") == task_id:
                    task_state = ti.get("state")
                    task_detail = ti
                    break
        runs.append({**run, "job_id": job.get("id"), "job_name": task_id, "task_state": task_state, "task": task_detail, "effective_state": task_state or run.get("state"), "source": "airflow"})
    return JSONResponse({"job_id": job.get("id"), "job_name": task_id, "dag_id": dag_id, "type": "batch", "available": bool(dag_runtime.get("available")), "error": dag_runtime.get("error"), "runs": runs})


@app.get("/api/runtime/logs/{job_id}")
async def get_runtime_logs(
    job_id: str,
    kind: str = Query(default="batch", pattern="^(batch|stream)$"),
    pipeline: str | None = None,
    task: str | None = None,
    lines: int = Query(default=500, ge=10, le=5000),
) -> PlainTextResponse:
    try:
        if kind == "stream":
            for path in _candidate_stream_log_paths(job_id):
                if path.exists():
                    return PlainTextResponse(_tail_text(path, lines))
            raise FileNotFoundError("No stream log found. Tried: " + ", ".join(str(p) for p in _candidate_stream_log_paths(job_id)))

        if not pipeline or not task:
            bundle = _load_config_bundle()
            job = next((j for j in bundle["jobs"] if j.get("id") == job_id or j.get("name") == job_id), None)
            if job:
                pipeline = pipeline or job.get("pipeline")
                task = task or job.get("name")
        if not pipeline or not task:
            return PlainTextResponse("ERROR: pipeline and task are required for batch logs.", status_code=400)
        path = _find_latest_airflow_log(pipeline, task)
        return PlainTextResponse(_tail_text(path, lines))
    except Exception as exc:
        return PlainTextResponse(f"ERROR: {exc}", status_code=404)
