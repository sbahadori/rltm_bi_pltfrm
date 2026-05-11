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


# -----------------------------------------------------------------------------
# Paths / configuration
# -----------------------------------------------------------------------------

PIPELINE_REPO_ROOT = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm"))

BATCH_CATALOG_PATH = (
    PIPELINE_REPO_ROOT / "configs" / "batch" / "pipeline_catalog.json"
)

STREAM_REGISTRY_PATH = (
    PIPELINE_REPO_ROOT / "configs" / "streaming" / "stream_registry.json"
)

STREAM_STATUS_FILE = Path(
    os.getenv(
        "STREAM_STATUS_FILE",
        "/runtime/spark_health/stream_supervisor_status.json",
    )
)

STREAM_LOG_DIR = Path(
    os.getenv(
        "STREAM_LOG_DIR",
        "/runtime/spark_health/logs",
    )
)

AIRFLOW_LOG_DIR = Path(
    os.getenv(
        "AIRFLOW_LOG_DIR",
        "/runtime/airflow_logs",
    )
)

AIRFLOW_API_BASE = os.getenv(
    "AIRFLOW_API_BASE",
    "http://airflow-api-server:8080",
).rstrip("/")

AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")

STREAM_STATUS_STALE_SECONDS = int(os.getenv("STREAM_STATUS_STALE_SECONDS", "120"))
STREAM_HEARTBEAT_STALE_SECONDS = int(os.getenv("STREAM_HEARTBEAT_STALE_SECONDS", "120"))


# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None

    try:
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _duration_seconds(start: Any, end: Any) -> float | None:
    start_dt = _parse_datetime(start)
    end_dt = _parse_datetime(end)

    if not start_dt or not end_dt:
        return None

    return max(0.0, (end_dt - start_dt).total_seconds())


def _path_state(path: Path) -> dict[str, Any]:
    exists = path.exists()

    return {
        "path": str(path),
        "exists": exists,
        "is_file": path.is_file() if exists else False,
        "is_dir": path.is_dir() if exists else False,
        "readable": os.access(path, os.R_OK) if exists else False,
    }


def _tail_file(path: Path, lines: int = 300) -> str:
    if not path.exists():
        raise FileNotFoundError(str(path))

    text = path.read_text(encoding="utf-8", errors="replace")
    return "\n".join(text.splitlines()[-lines:])


def _target_path_from_spec(spec: dict[str, Any]) -> str:
    return (
        spec.get("bronze_write", {}).get("target_path")
        or spec.get("target", {}).get("path")
        or spec.get("silver_write", {}).get("target_path")
        or spec.get("gold_write", {}).get("target_path")
        or ""
    )


def _normalize_state(value: Any) -> str:
    if value is None:
        return "unknown"

    return str(value).strip().lower() or "unknown"


# -----------------------------------------------------------------------------
# Catalog builders
# -----------------------------------------------------------------------------

def _build_batch_jobs(catalog: dict[str, Any]) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []

    for pipeline in catalog.get("pipelines", []):
        if not pipeline.get("enabled", True):
            continue

        pipeline_name = pipeline.get("name", "")
        dag = pipeline.get("dag", {}) or {}
        schedule = dag.get("schedule")
        pipeline_tags = dag.get("tags", [])

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
                    "type": "batch",
                    "job_type": job.get("job_type", ""),
                    "source_url": source.get("base_url", ""),
                    "dependencies": job.get("dependencies", []),
                    "timeout_minutes": job.get("execution_timeout_minutes", 30),
                    "tags": job.get("tags", []) or pipeline_tags,
                    "target_path": _target_path_from_spec(spec),
                    "schedule": schedule,
                    "defined": True,
                    "source": "catalog",
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
            bronze_app_name = bronze.get("app_name", f"bronze_{name}")
            jobs.append(
                {
                    "id": f"{name}__bronze",
                    "name": bronze_app_name,
                    "pipeline": name,
                    "type": "stream",
                    "job_type": bronze.get("engine", "generic_kafka_to_bronze"),
                    "source_url": (
                        f"{source.get('bootstrap_servers', '')} / "
                        f"{source.get('topic', '')}"
                    ),
                    "dependencies": [],
                    "trigger": bronze.get("trigger_interval", "15 seconds"),
                    "target_path": bronze.get("path", ""),
                    "checkpoint_path": bronze.get("checkpoint_path", ""),
                    "heartbeat_file": bronze.get("heartbeat_file", ""),
                    "defined": True,
                    "source": "stream_registry",
                }
            )

        if silver:
            silver_app_name = silver.get("app_name", f"silver_{name}")
            bronze_app_name = bronze.get("app_name", f"bronze_{name}") if bronze else f"bronze_{name}"
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
                    "checkpoint_path": silver.get("checkpoint_path", ""),
                    "heartbeat_file": silver.get("heartbeat_file", ""),
                    "defined": True,
                    "source": "stream_registry",
                }
            )

    return jobs


def _build_pipelines(
    catalog: dict[str, Any],
    registry: dict[str, Any],
) -> list[dict[str, Any]]:
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
                "jobs": [
                    j.get("name", "")
                    for j in pipeline.get("jobs", [])
                    if j.get("enabled", True)
                ],
            }
        )

    for stream in registry.get("streams", []):
        if not stream.get("enabled", True):
            continue

        jobs: list[str] = []
        name = stream.get("name", "")
        source = stream.get("source", {}) or {}

        if stream.get("bronze"):
            jobs.append(stream["bronze"].get("app_name", f"bronze_{name}"))

        if stream.get("silver"):
            jobs.append(stream["silver"].get("app_name", f"silver_{name}"))

        pipelines.append(
            {
                "name": name,
                "type": "stream",
                "description": f"Streaming pipeline: {source.get('topic', '')}",
                "schedule": "always-on",
                "tags": ["stream", "kafka"],
                "jobs": jobs,
            }
        )

    return pipelines


def _load_config_payload() -> dict[str, Any]:
    catalog = _load_json(BATCH_CATALOG_PATH) or {"pipelines": []}
    registry = _load_json(STREAM_REGISTRY_PATH) or {"streams": []}

    return {
        "pipelines": _build_pipelines(catalog, registry),
        "jobs": _build_batch_jobs(catalog) + _build_stream_jobs(registry),
        "meta": {
            "batch_catalog_path": str(BATCH_CATALOG_PATH),
            "stream_registry_path": str(STREAM_REGISTRY_PATH),
            "loaded_at": now_iso(),
        },
    }


# -----------------------------------------------------------------------------
# Stream status
# -----------------------------------------------------------------------------

def _unit_aliases(unit_name: str) -> list[str]:
    aliases = [unit_name]

    if unit_name.endswith("_bronze"):
        base = unit_name[: -len("_bronze")]
        aliases.append(f"bronze_{base}")

    if unit_name.endswith("_silver"):
        base = unit_name[: -len("_silver")]
        aliases.append(f"silver_{base}")

    return list(dict.fromkeys(aliases))


def _supervisor_unit_name_for_job(job: dict[str, Any]) -> str:
    pipeline = job.get("pipeline", "")
    job_id = job.get("id", "")

    if job_id.endswith("__bronze"):
        return f"{pipeline}_bronze"

    if job_id.endswith("__silver"):
        return f"{pipeline}_silver"

    return job.get("name", "")


def _compute_stream_unit_status(
    unit: dict[str, Any],
    now_epoch: int,
) -> dict[str, Any]:
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
            status_reason = (
                f"process exited with returncode={returncode} "
                f"and reached max retries"
            )
        elif int(returncode) == 0:
            computed_status = "stopped"
            status_reason = "process exited successfully"
        else:
            computed_status = "restarting"
            status_reason = (
                f"process exited with returncode={returncode}; retry may be scheduled"
            )
    elif pid is not None:
        if heartbeat_status == "error":
            computed_status = "error"
            status_reason = "heartbeat reports error"
        elif (
            heartbeat_age_seconds is not None
            and heartbeat_age_seconds > STREAM_HEARTBEAT_STALE_SECONDS
        ):
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


def _load_stream_status(raw: bool = False) -> dict[str, Any]:
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
    supervisor_age_seconds = None
    if supervisor_ts is not None:
        supervisor_age_seconds = max(0, now_epoch - supervisor_ts)

    supervisor_stale = (
        supervisor_age_seconds is not None
        and supervisor_age_seconds > STREAM_STATUS_STALE_SECONDS
    )

    enriched_units: dict[str, Any] = {}
    units = data.get("units", {}) or {}

    for unit_name, unit in units.items():
        if not isinstance(unit, dict):
            continue

        computed = _compute_stream_unit_status(unit, now_epoch)
        enriched_units[unit_name] = {
            **unit,
            **computed,
            "unit_name": unit_name,
            "aliases": _unit_aliases(unit_name),
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

    return payload


def _find_stream_unit_for_job(
    job: dict[str, Any],
    stream_status: dict[str, Any],
) -> dict[str, Any] | None:
    units = stream_status.get("units", {}) or {}
    expected_unit = _supervisor_unit_name_for_job(job)
    job_name = job.get("name", "")

    for unit_name, unit in units.items():
        aliases = unit.get("aliases", []) if isinstance(unit, dict) else []

        if (
            unit_name == expected_unit
            or unit_name == job_name
            or job_name in aliases
            or expected_unit in aliases
        ):
            return {
                **unit,
                "unit_name": unit_name,
            }

    return None


# -----------------------------------------------------------------------------
# Airflow API helpers
# -----------------------------------------------------------------------------

_AIRFLOW_TOKEN_CACHE: dict[str, Any] = {
    "access_token": None,
    "expires_at_epoch": 0,
}


def _airflow_auth_token(timeout: int = 10) -> str:
    now_epoch = int(time.time())
    cached_token = _AIRFLOW_TOKEN_CACHE.get("access_token")
    expires_at_epoch = int(_AIRFLOW_TOKEN_CACHE.get("expires_at_epoch") or 0)

    if cached_token and now_epoch < expires_at_epoch:
        return str(cached_token)

    token_url = f"{AIRFLOW_API_BASE}/auth/token"

    payload = json.dumps(
        {
            "username": AIRFLOW_USER,
            "password": AIRFLOW_PASSWORD,
        }
    ).encode("utf-8")

    req = urllib.request.Request(
        token_url,
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )

    with urllib.request.urlopen(req, timeout=timeout) as response:
        body = response.read().decode("utf-8")
        data = json.loads(body) if body else {}

    token = data.get("access_token") or data.get("token")

    if not token:
        raise RuntimeError(f"Airflow token response did not include access_token: {data}")

    # Conservative cache. Airflow token expiry depends on auth manager config;
    # use a short cache to avoid stale-token errors in local dev.
    _AIRFLOW_TOKEN_CACHE["access_token"] = token
    _AIRFLOW_TOKEN_CACHE["expires_at_epoch"] = now_epoch + 240

    return str(token)


def _http_get_json(path: str, timeout: int = 10) -> dict[str, Any]:
    url = f"{AIRFLOW_API_BASE}{path}"

    token = _airflow_auth_token(timeout=timeout)

    req = urllib.request.Request(url)
    req.add_header("Accept", "application/json")
    req.add_header("Authorization", f"Bearer {token}")

    with urllib.request.urlopen(req, timeout=timeout) as response:
        data = response.read().decode("utf-8")
        return json.loads(data) if data else {}

def _airflow_get_with_fallback(paths: list[str]) -> tuple[dict[str, Any] | None, str | None]:
    errors: list[str] = []

    for path in paths:
        try:
            return _http_get_json(path), None
        except Exception as exc:
            errors.append(f"{path}: {exc}")

    return None, " | ".join(errors)


def _extract_list(payload: dict[str, Any], keys: list[str]) -> list[dict[str, Any]]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return value

    return []


def _dag_runs_for_pipeline(
    dag_id: str,
    limit: int = 10,
) -> tuple[list[dict[str, Any]], str | None]:
    quoted = urllib.parse.quote(dag_id, safe="")
    paths = [
        f"/api/v2/dags/{quoted}/dagRuns?order_by=-logical_date&limit={limit}",
        f"/api/v2/dags/{quoted}/dagRuns?order_by=-start_date&limit={limit}",
    ]

    payload, error = _airflow_get_with_fallback(paths)

    if payload is None:
        return [], error

    return _extract_list(payload, ["dag_runs", "dagRuns", "dagruns"]), None


def _task_instances_for_run(
    dag_id: str,
    dag_run_id: str,
) -> tuple[list[dict[str, Any]], str | None]:
    quoted_dag = urllib.parse.quote(dag_id, safe="")
    quoted_run = urllib.parse.quote(dag_run_id, safe="")
    paths = [
        f"/api/v2/dags/{quoted_dag}/dagRuns/{quoted_run}/taskInstances",
    ]

    payload, error = _airflow_get_with_fallback(paths)

    if payload is None:
        return [], error

    return _extract_list(payload, ["task_instances", "taskInstances", "taskinstances"]), None


def _dag_run_id(run: dict[str, Any]) -> str:
    return (
        str(run.get("dag_run_id") or run.get("dagRunId") or run.get("run_id") or "")
    )


def _dag_run_start(run: dict[str, Any]) -> str | None:
    return (
        run.get("start_date")
        or run.get("startDate")
        or run.get("logical_date")
        or run.get("execution_date")
        or run.get("executionDate")
    )


def _dag_run_end(run: dict[str, Any]) -> str | None:
    return run.get("end_date") or run.get("endDate")


def _task_id(task: dict[str, Any]) -> str:
    return str(task.get("task_id") or task.get("taskId") or "")


def _task_start(task: dict[str, Any]) -> str | None:
    return task.get("start_date") or task.get("startDate")


def _task_end(task: dict[str, Any]) -> str | None:
    return task.get("end_date") or task.get("endDate")


def _task_try_number(task: dict[str, Any]) -> int | None:
    return _safe_int(task.get("try_number") or task.get("tryNumber"))


def _latest_task_run_for_job(
    dag_id: str,
    task_id: str,
) -> dict[str, Any]:
    runs, error = _dag_runs_for_pipeline(dag_id, limit=10)

    if error:
        return {
            "airflow_available": False,
            "current_status": "airflow_unavailable",
            "latest_run_state": "airflow_unavailable",
            "status_reason": error,
            "latest_run": None,
        }

    if not runs:
        return {
            "airflow_available": True,
            "current_status": "no_runs",
            "latest_run_state": "no_runs",
            "status_reason": "No Airflow DAG runs found",
            "latest_run": None,
        }

    for run in runs:
        run_id = _dag_run_id(run)
        if not run_id:
            continue

        tasks, task_error = _task_instances_for_run(dag_id, run_id)
        task = next((t for t in tasks if _task_id(t) == task_id), None)

        if task:
            task_state = _normalize_state(task.get("state"))
            start = _task_start(task) or _dag_run_start(run)
            end = _task_end(task) or _dag_run_end(run)

            return {
                "airflow_available": True,
                "current_status": task_state,
                "latest_run_state": task_state,
                "latest_run_id": run_id,
                "latest_dag_run_state": _normalize_state(run.get("state")),
                "started_at": start,
                "ended_at": end,
                "duration_seconds": _duration_seconds(start, end),
                "try_number": _task_try_number(task),
                "status_reason": None,
                "latest_run": {
                    "dag_id": dag_id,
                    "dag_run_id": run_id,
                    "dag_run_state": _normalize_state(run.get("state")),
                    "task_id": task_id,
                    "task_state": task_state,
                    "started_at": start,
                    "ended_at": end,
                    "duration_seconds": _duration_seconds(start, end),
                    "try_number": _task_try_number(task),
                },
            }

        if task_error:
            return {
                "airflow_available": False,
                "current_status": "airflow_task_unavailable",
                "latest_run_state": "airflow_task_unavailable",
                "latest_run_id": run_id,
                "status_reason": task_error,
                "latest_run": None,
            }

    latest = runs[0]
    return {
        "airflow_available": True,
        "current_status": _normalize_state(latest.get("state")),
        "latest_run_state": _normalize_state(latest.get("state")),
        "latest_run_id": _dag_run_id(latest),
        "latest_dag_run_state": _normalize_state(latest.get("state")),
        "started_at": _dag_run_start(latest),
        "ended_at": _dag_run_end(latest),
        "duration_seconds": _duration_seconds(_dag_run_start(latest), _dag_run_end(latest)),
        "status_reason": f"Task '{task_id}' was not found in latest DAG runs",
        "latest_run": None,
    }


def _batch_runs_for_job(
    dag_id: str,
    task_id: str,
    limit: int,
) -> dict[str, Any]:
    runs, error = _dag_runs_for_pipeline(dag_id, limit=limit)

    if error:
        return {
            "available": False,
            "source": "airflow",
            "error": error,
            "runs": [],
        }

    result: list[dict[str, Any]] = []

    for run in runs:
        run_id = _dag_run_id(run)
        tasks: list[dict[str, Any]] = []
        task_error = None

        if run_id:
            tasks, task_error = _task_instances_for_run(dag_id, run_id)

        task = next((t for t in tasks if _task_id(t) == task_id), None)

        start = _dag_run_start(run)
        end = _dag_run_end(run)
        task_state = None
        try_number = None

        if task:
            task_state = _normalize_state(task.get("state"))
            start = _task_start(task) or start
            end = _task_end(task) or end
            try_number = _task_try_number(task)

        result.append(
            {
                "dag_id": dag_id,
                "dag_run_id": run_id,
                "dag_run_state": _normalize_state(run.get("state")),
                "task_id": task_id,
                "task_state": task_state or "task_not_found",
                "state": task_state or _normalize_state(run.get("state")),
                "started_at": start,
                "ended_at": end,
                "duration_seconds": _duration_seconds(start, end),
                "try_number": try_number,
                "task_error": task_error,
            }
        )

    return {
        "available": True,
        "source": "airflow",
        "runs": result,
    }


# -----------------------------------------------------------------------------
# Runtime merge
# -----------------------------------------------------------------------------

def _runtime_jobs_payload() -> dict[str, Any]:
    config = _load_config_payload()
    stream_status = _load_stream_status(raw=False)

    jobs: list[dict[str, Any]] = []

    for job in config.get("jobs", []):
        runtime_job = {
            **job,
            "current_status": "defined",
            "latest_run_state": None,
            "latest_run_id": None,
            "started_at": None,
            "ended_at": None,
            "duration_seconds": None,
            "status_reason": None,
            "runtime_source": "catalog",
        }

        if job.get("type") == "stream":
            unit = _find_stream_unit_for_job(job, stream_status)

            if unit:
                runtime_job.update(
                    {
                        "current_status": unit.get("computed_status", "unknown"),
                        "latest_run_state": unit.get("computed_status", "unknown"),
                        "status_reason": unit.get("status_reason"),
                        "is_healthy": unit.get("is_healthy", False),
                        "heartbeat_age_seconds": unit.get("heartbeat_age_seconds"),
                        "runtime_unit_name": unit.get("unit_name"),
                        "runtime_source": "stream_supervisor",
                        "returncode": unit.get("returncode"),
                        "retries": unit.get("retries"),
                        "max_retries": unit.get("max_retries"),
                    }
                )
            else:
                runtime_job.update(
                    {
                        "current_status": "unknown",
                        "latest_run_state": "unknown",
                        "status_reason": "No matching stream supervisor unit was found",
                        "runtime_source": "stream_supervisor",
                    }
                )

        elif job.get("type") == "batch":
            latest = _latest_task_run_for_job(
                dag_id=job.get("pipeline", ""),
                task_id=job.get("name", ""),
            )

            runtime_job.update(
                {
                    "current_status": latest.get("current_status", "unknown"),
                    "latest_run_state": latest.get("latest_run_state"),
                    "latest_run_id": latest.get("latest_run_id"),
                    "latest_dag_run_state": latest.get("latest_dag_run_state"),
                    "started_at": latest.get("started_at"),
                    "ended_at": latest.get("ended_at"),
                    "duration_seconds": latest.get("duration_seconds"),
                    "try_number": latest.get("try_number"),
                    "status_reason": latest.get("status_reason"),
                    "runtime_source": "airflow",
                    "airflow_available": latest.get("airflow_available", False),
                }
            )

        jobs.append(runtime_job)

    return {
        "jobs": jobs,
        "meta": {
            "loaded_at": now_iso(),
            "config_job_count": len(config.get("jobs", [])),
            "stream_status_available": stream_status.get("available", False),
            "airflow_api_base": AIRFLOW_API_BASE,
        },
    }


def _job_from_config(job_id: str) -> dict[str, Any] | None:
    config = _load_config_payload()
    for job in config.get("jobs", []):
        if job.get("id") == job_id or job.get("name") == job_id:
            return job
    return None


# -----------------------------------------------------------------------------
# Logs
# -----------------------------------------------------------------------------

def _resolve_stream_log_name(name: str) -> str:
    if name.endswith(".log"):
        name = name[:-4]

    # Already supervisor style.
    if name.endswith("_bronze") or name.endswith("_silver"):
        return name

    # Config style: bronze_clickstream_user_events -> clickstream_user_events_bronze
    if name.startswith("bronze_"):
        return f"{name[len('bronze_'):]}_bronze"

    if name.startswith("silver_"):
        return f"{name[len('silver_'):]}_silver"

    return name


def _find_latest_airflow_log(
    dag_id: str,
    task_id: str,
    dag_run_id: str | None = None,
) -> Path | None:
    dag_dir = AIRFLOW_LOG_DIR / f"dag_id={dag_id}"

    candidates: list[Path] = []

    if dag_run_id:
        run_dir = dag_dir / f"run_id={dag_run_id}"
        candidates.extend(run_dir.glob(f"task_id={task_id}/**/*.log"))

    if not candidates:
        candidates.extend(dag_dir.glob(f"**/task_id={task_id}/**/*.log"))

    if not candidates:
        # Airflow log layout fallback.
        candidates.extend(AIRFLOW_LOG_DIR.glob(f"**/{dag_id}/**/{task_id}/**/*.log"))
        candidates.extend(AIRFLOW_LOG_DIR.glob(f"**/{task_id}/**/*.log"))

    if not candidates:
        return None

    return max(candidates, key=lambda p: p.stat().st_mtime)


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------

@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/config")
async def get_config() -> JSONResponse:
    return JSONResponse(_load_config_payload())


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
                "AIRFLOW_API_BASE": os.getenv("AIRFLOW_API_BASE", ""),
            },
            "checked_at": now_iso(),
        }
    )


@app.get("/api/runtime/streams/status")
async def get_stream_runtime_status(raw: bool = Query(default=False)) -> JSONResponse:
    return JSONResponse(_load_stream_status(raw=raw))


@app.get("/api/runtime/jobs")
async def get_runtime_jobs() -> JSONResponse:
    return JSONResponse(_runtime_jobs_payload())


@app.get("/api/runtime/runs/{job_id}")
async def get_runtime_runs(
    job_id: str,
    limit: int = Query(default=10, ge=1, le=50),
) -> JSONResponse:
    job = _job_from_config(job_id)

    if not job:
        return JSONResponse(
            {
                "available": False,
                "job_id": job_id,
                "error": "Job not found in /api/config",
                "runs": [],
            },
            status_code=404,
        )

    if job.get("type") == "batch":
        payload = _batch_runs_for_job(
            dag_id=job.get("pipeline", ""),
            task_id=job.get("name", ""),
            limit=limit,
        )
        payload.update(
            {
                "job_id": job.get("id"),
                "job_name": job.get("name"),
                "pipeline": job.get("pipeline"),
                "type": "batch",
            }
        )
        return JSONResponse(payload)

    if job.get("type") == "stream":
        stream_status = _load_stream_status(raw=False)
        unit = _find_stream_unit_for_job(job, stream_status)
        return JSONResponse(
            {
                "available": True,
                "job_id": job.get("id"),
                "job_name": job.get("name"),
                "pipeline": job.get("pipeline"),
                "type": "stream",
                "source": "stream_supervisor_current_state",
                "runs": [
                    {
                        "state": unit.get("computed_status") if unit else "unknown",
                        "started_at": unit.get("last_start_ts_iso") if unit else None,
                        "ended_at": None,
                        "duration_seconds": None,
                        "unit_name": unit.get("unit_name") if unit else None,
                        "status_reason": unit.get("status_reason") if unit else "No matching stream unit found",
                        "returncode": unit.get("returncode") if unit else None,
                        "retries": unit.get("retries") if unit else None,
                        "max_retries": unit.get("max_retries") if unit else None,
                    }
                ],
                "note": "Stream historical runs require a future Job Run Registry. Current state is returned here.",
            }
        )

    return JSONResponse(
        {
            "available": False,
            "job_id": job_id,
            "error": f"Unsupported job type: {job.get('type')}",
            "runs": [],
        },
        status_code=400,
    )


@app.get("/api/runtime/logs/{job_id}")
async def get_runtime_logs(
    job_id: str,
    kind: str | None = Query(default=None),
    pipeline: str | None = Query(default=None),
    task: str | None = Query(default=None),
    dag_run_id: str | None = Query(default=None),
    lines: int = Query(default=300, ge=10, le=5000),
) -> PlainTextResponse:
    job = _job_from_config(job_id)
    effective_kind = kind or (job.get("type") if job else None)

    if effective_kind == "stream":
        stream_name = _resolve_stream_log_name(
            (job.get("runtime_unit_name") if job else None)
            or (job.get("name") if job else None)
            or job_id
        )
        log_path = STREAM_LOG_DIR / f"{stream_name}.log"

        if not log_path.exists():
            return PlainTextResponse(
                f"Stream log not found: {log_path}\n"
                f"Available log files:\n"
                + "\n".join(sorted(p.name for p in STREAM_LOG_DIR.glob('*.log'))),
                status_code=404,
            )

        return PlainTextResponse(_tail_file(log_path, lines=lines))

    if effective_kind == "batch":
        dag_id = pipeline or (job.get("pipeline") if job else None)
        task_id = task or (job.get("name") if job else None)

        if not dag_id or not task_id:
            return PlainTextResponse(
                "Missing batch log parameters. Required: pipeline and task.",
                status_code=400,
            )

        log_path = _find_latest_airflow_log(dag_id, task_id, dag_run_id=dag_run_id)

        if not log_path:
            return PlainTextResponse(
                f"No Airflow log file found for dag_id={dag_id}, task_id={task_id}.\n"
                f"Airflow log dir: {AIRFLOW_LOG_DIR}",
                status_code=404,
            )

        return PlainTextResponse(_tail_file(log_path, lines=lines))

    return PlainTextResponse(
        f"Unsupported or unknown log kind for job_id={job_id}: {effective_kind}",
        status_code=400,
    )
