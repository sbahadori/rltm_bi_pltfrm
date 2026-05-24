from __future__ import annotations

"""
Dashboard API — نسخه کامل با:
  - JWT auth
  - Execution endpoints (DAG trigger/pause/cancel, stream restart/stop, onboard)
  - WebSocket live log streaming
  - Action audit log
  - User management
"""

import asyncio
import base64
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg2
import psycopg2.extras
from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Query,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel

from auth import (
    create_access_token,
    get_current_user,
    get_current_user_ws,
    hash_password,
    require_role,
    verify_password,
)
from actions import (
    cancel_dag_run,
    get_dag_runs,
    get_stream_control_status,
    list_dags,
    pause_dag,
    restart_stream,
    stop_stream,
    trigger_dag,
)

try:
    from catalog_editor import router as catalog_router
except ImportError:
    from .catalog_editor import router as catalog_router

# ─────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────
app = FastAPI(title="BI Platform Dashboard API")
app.include_router(catalog_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────
# Paths / config
# ─────────────────────────────────────────────────────────────
PIPELINE_REPO_ROOT = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm"))
BATCH_CATALOG_PATH = PIPELINE_REPO_ROOT / "configs" / "batch" / "pipeline_catalog.json"
STREAM_REGISTRY_PATH = PIPELINE_REPO_ROOT / "configs" / "streaming" / "stream_registry.json"
STREAM_STATUS_FILE = Path(os.getenv("STREAM_STATUS_FILE", "/runtime/spark_health/stream_supervisor_status.json"))
STREAM_LOG_DIR = Path(os.getenv("STREAM_LOG_DIR", "/runtime/spark_health/logs"))
AIRFLOW_LOG_DIR = Path(os.getenv("AIRFLOW_LOG_DIR", "/runtime/airflow_logs"))
JOB_RUN_REGISTRY_FILE = Path(os.getenv("JOB_RUN_REGISTRY_FILE", "/workspace/rltm_bi_pltfrm/runtime/job_runs/job_runs.jsonl"))
AIRFLOW_API_BASE = os.getenv("AIRFLOW_API_BASE", "http://airflow-api-server:8080").rstrip("/")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
STREAM_STATUS_STALE_SECONDS = int(os.getenv("STREAM_STATUS_STALE_SECONDS", "120"))
STREAM_HEARTBEAT_STALE_SECONDS = int(os.getenv("STREAM_HEARTBEAT_STALE_SECONDS", "120"))

# ─────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────
def _catalog_metadata_for_job(job: dict) -> dict | None:
    """
    Returns control DB metadata for a catalog-defined job.

    This is different from runtime run state. It only tells us whether
    onboarding has loaded the job into meta.pipeline/meta.job.
    """
    pipeline_name = job.get("pipeline")
    job_name = job.get("name")

    if not pipeline_name or not job_name:
        return None

    sql = """
        SELECT
            p.pipeline_id,
            p.pipeline_name,
            p.airflow_dag_id,
            p.is_active AS pipeline_is_active,
            j.job_id,
            j.job_code,
            j.job_name,
            j.job_type,
            j.runner,
            j.layer,
            j.source_type,
            j.target_path,
            j.is_active AS job_is_active,
            j.updated_at AS job_updated_at
        FROM meta.job j
        JOIN meta.pipeline p
          ON p.pipeline_name = j.pipeline_name
        WHERE j.pipeline_name = %s
          AND j.job_name = %s
          AND j.is_active = TRUE
        LIMIT 1
    """

    conn = psycopg2.connect(**_db_config())
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (pipeline_name, job_name))
            row = cur.fetchone()
            return _json_safe(dict(row)) if row else None
    except Exception as exc:
        print(f"[WARN] Failed to read catalog metadata for {pipeline_name}.{job_name}: {exc}", flush=True)
        return None
    finally:
        conn.close()


def _db_config() -> dict[str, Any]:
    return {
        "host": os.getenv("CONTROL_DB_HOST", "postgres-warehouse"),
        "port": int(os.getenv("CONTROL_DB_PORT", "5432")),
        "dbname": os.getenv("CONTROL_DB_NAME", os.getenv("POSTGRES_DB", "warehouse")),
        "user": os.getenv("CONTROL_DB_USER", os.getenv("POSTGRES_USER", "warehouse")),
        "password": os.getenv("CONTROL_DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "warehouse")),
        "sslmode": os.getenv("CONTROL_DB_SSLMODE", "disable"),
    }


def _json_safe(v: Any) -> Any:
    if isinstance(v, dict):
        return {k: _json_safe(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_json_safe(x) for x in v]
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, UUID):
        return str(v)
    if isinstance(v, Decimal):
        return float(v)
    return v


def _validate_usp(name: str) -> str:
    if not name.startswith("usp_") or not all(c.isalnum() or c == "_" for c in name):
        raise ValueError(f"Invalid USP name: {name}")
    return name


def call_usp_rows(usp: str, params: tuple = ()) -> list[dict]:
    usp = _validate_usp(usp)
    ph = ", ".join(["%s"] * len(params))
    conn = psycopg2.connect(**_db_config())
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM ctl.{usp}({ph})", params)
            return _json_safe([dict(r) for r in cur.fetchall()])
    finally:
        conn.close()


def call_usp_one(usp: str, params: tuple = ()) -> dict | None:
    rows = call_usp_rows(usp, params)
    return rows[0] if rows else None


def call_usp_void(usp: str, params: tuple = ()) -> None:
    usp = _validate_usp(usp)
    ph = ", ".join(["%s"] * len(params))
    conn = psycopg2.connect(**_db_config())
    try:
        with conn.cursor() as cur:
            cur.execute(f"CALL ctl.{usp}({ph})", params)
        conn.commit()
    finally:
        conn.close()


def _log_action(
    *,
    user: dict,
    action_type: str,
    target_type: str | None,
    target_id: str | None,
    request_payload: dict,
    result_status: str,
    result_payload: dict | None = None,
    error_message: str | None = None,
    duration_ms: int | None = None,
) -> None:
    try:
        call_usp_void(
            "usp_insert_action_log",
            (
                user.get("sub", "unknown"),
                user.get("user_id"),
                action_type,
                target_type,
                target_id,
                json.dumps(request_payload),
                result_status,
                json.dumps(result_payload or {}),
                error_message,
                duration_ms,
            ),
        )
    except Exception as exc:
        print(f"[WARN] Failed to write action log: {exc}", flush=True)


# ─────────────────────────────────────────────────────────────
# Utility helpers (catalog, streams, Airflow)
# ─────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)

def _safe_int(v: Any) -> int | None:
    try:
        return None if v is None else int(v)
    except Exception:
        return None


def _safe_float(v: Any) -> float | None:
    try:
        return None if v is None else float(v)
    except Exception:
        return None


def _parse_dt(v: Any) -> datetime | None:
    if not v:
        return None
    try:
        t = str(v)
        if t.endswith("Z"):
            t = t[:-1] + "+00:00"
        return datetime.fromisoformat(t)
    except Exception:
        return None


def _duration(start: Any, end: Any) -> float | None:
    s, e = _parse_dt(start), _parse_dt(end)
    if s and e:
        return max(0.0, (e - s).total_seconds())
    return None


def _tail_file(path: Path, lines: int = 300) -> str:
    if not path.exists():
        raise FileNotFoundError(str(path))
    return "\n".join(path.read_text(encoding="utf-8", errors="replace").splitlines()[-lines:])


def _resolve_stream_log(name: str) -> str:
    if name.endswith(".log"):
        name = name[:-4]
    if name.endswith("_bronze") or name.endswith("_silver"):
        return name
    if name.startswith("bronze_"):
        return f"{name[7:]}_bronze"
    if name.startswith("silver_"):
        return f"{name[7:]}_silver"
    return name


def _path_state(p: Path) -> dict:
    e = p.exists()
    return {"path": str(p), "exists": e, "is_file": p.is_file() if e else False, "readable": os.access(p, os.R_OK) if e else False}


# ─────────────────────────────────────────────────────────────
# Airflow REST helpers
# ─────────────────────────────────────────────────────────────
import threading as _threading
_af_lock = _threading.Lock()
_af_cache: dict[str, Any] = {"token": None, "exp": 0}


def _af_token(timeout: int = 10) -> str:
    with _af_lock:
        if _af_cache["token"] and time.time() < _af_cache["exp"]:
            return str(_af_cache["token"])
        url = f"{AIRFLOW_API_BASE}/auth/token"
        data = json.dumps({"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD}).encode()
        req = urllib.request.Request(url, data=data, method="POST",
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                body = json.loads(r.read().decode())
        except Exception as exc:
            raise RuntimeError(f"Airflow auth: {exc}") from exc
        tok = body.get("access_token") or body.get("token")
        if not tok:
            raise RuntimeError("No token in Airflow response")
        _af_cache.update({"token": tok, "exp": time.time() + 240})
        return str(tok)


def _af_get(path: str, timeout: int = 10) -> dict:
    url = f"{AIRFLOW_API_BASE}{path}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {_af_token()}", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode()) if r.read else {}


def _af_post(path: str, body: Any = None, method: str = "POST", timeout: int = 15) -> dict:
    url = f"{AIRFLOW_API_BASE}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Authorization": f"Bearer {_af_token()}",
                                          "Content-Type": "application/json", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        msg = exc.read().decode(errors="replace")[:500]
        raise RuntimeError(f"Airflow {method} {path} [{exc.code}]: {msg}") from exc


# ─────────────────────────────────────────────────────────────
# Catalog / stream builders  (unchanged from original)
# ─────────────────────────────────────────────────────────────

def _resolve_repo_path(p: str) -> Path:
    path = Path(p)
    return path if path.is_absolute() else (PIPELINE_REPO_ROOT / path).resolve()


def _load_manifest(ref: str) -> dict:
    p = _resolve_repo_path(ref)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _enabled_manifest_tables(m: dict) -> list[dict]:
    return [t for t in m.get("tables", []) if t.get("enabled", True)]


def _manifest_target(m: dict, t: dict) -> str:
    if t.get("target_path"):
        return t["target_path"]
    tpl = (m.get("defaults") or {}).get("target_path_template", "")
    try:
        return tpl.format(source_id=m.get("source_id", ""), table_id=t.get("table_id", ""))
    except Exception:
        return ""


def _target_from_spec(spec: dict) -> str:
    return (
        spec.get("bronze_write", {}).get("target_path")
        or spec.get("target", {}).get("path")
        or spec.get("silver_write", {}).get("target_path")
        or spec.get("gold_write", {}).get("target_path")
        or ""
    )


def _pipeline_job_names(pipeline: dict) -> list[str]:
    names = []
    for job in pipeline.get("jobs", []):
        if not job.get("enabled", True):
            continue
        jt = job.get("job_type", "")
        es = job.get("execution_strategy", "")
        if jt == "generic_jdbc_manifest_to_bronze" and es == "one_task_per_table":
            manifest = _load_manifest(job.get("manifest_ref", ""))
            for t in _enabled_manifest_tables(manifest):
                if t.get("table_id"):
                    names.append(f"{job['name']}__{t['table_id']}")
        else:
            names.append(job["name"])
    return names


def _build_batch_jobs(catalog: dict) -> list[dict]:
    jobs = []
    for p in catalog.get("pipelines", []):
        if not p.get("enabled", True):
            continue
        pname = p.get("name", "")
        dag = p.get("dag", {}) or {}
        schedule = dag.get("schedule")
        tags = dag.get("tags", [])
        for job in p.get("jobs", []):
            if not job.get("enabled", True):
                continue
            jname = job.get("name", "")
            jtype = job.get("job_type", "")
            es = job.get("execution_strategy", "")
            if jtype == "generic_jdbc_manifest_to_bronze" and es == "one_task_per_table":
                manifest = _load_manifest(job.get("manifest_ref", ""))
                sid = manifest.get("source_id", "")
                for t in _enabled_manifest_tables(manifest):
                    tid = t.get("table_id", "")
                    if not tid:
                        continue
                    task = f"{jname}__{tid}"
                    jobs.append({
                        "id": f"{pname}__{task}", "name": task, "pipeline": pname,
                        "type": "batch", "job_type": jtype, "runner": jtype,
                        "job_code": f"bronze.{sid}.{tid}", "source_id": sid, "table_id": tid,
                        "layer": "bronze", "target_path": _manifest_target(manifest, t),
                        "schedule": schedule, "tags": job.get("tags", []) or tags,
                    })
                continue
            spec = job.get("spec", {}) or {}
            jobs.append({
                "id": f"{pname}__{jname}", "name": jname, "pipeline": pname,
                "type": "batch", "job_type": jtype, "runner": jtype,
                "target_path": _target_from_spec(spec), "schedule": schedule,
                "tags": job.get("tags", []) or tags,
            })
    return jobs


def _build_stream_jobs(registry: dict) -> list[dict]:
    jobs = []
    for s in registry.get("streams", []):
        if not s.get("enabled", True):
            continue
        name = s.get("name", "")
        source = s.get("source", {}) or {}
        bronze = s.get("bronze", {}) or {}
        silver = s.get("silver", {}) or {}
        if bronze:
            jobs.append({
                "id": f"{name}__bronze", "name": bronze.get("app_name", f"bronze_{name}"),
                "pipeline": name, "type": "stream",
                "job_type": bronze.get("engine", "generic_kafka_to_bronze"),
                "target_path": bronze.get("path", ""),
                "checkpoint_path": bronze.get("checkpoint_dir", ""),
                "heartbeat_file": bronze.get("heartbeat_file", ""),
                "source_url": f"{source.get('bootstrap_servers','')} / {source.get('topic','')}",
            })
        if silver:
            jobs.append({
                "id": f"{name}__silver", "name": silver.get("app_name", f"silver_{name}"),
                "pipeline": name, "type": "stream",
                "job_type": silver.get("engine", "generic_bronze_to_silver"),
                "target_path": silver.get("path", ""),
                "quarantine_path": silver.get("quarantine_path", ""),
                "checkpoint_path": silver.get("checkpoint_dir", ""),
                "heartbeat_file": silver.get("heartbeat_file", ""),
            })
    return jobs


def _build_pipelines(catalog: dict, registry: dict) -> list[dict]:
    pipelines = []
    for p in catalog.get("pipelines", []):
        if not p.get("enabled", True):
            continue
        dag = p.get("dag", {}) or {}
        pipelines.append({
            "name": p.get("name", ""), "type": "batch",
            "description": p.get("description", ""),
            "schedule": dag.get("schedule"),
            "tags": dag.get("tags", []),
            "jobs": _pipeline_job_names(p),
        })
    for s in registry.get("streams", []):
        if not s.get("enabled", True):
            continue
        name = s.get("name", "")
        jobs = []
        if s.get("bronze"):
            jobs.append(s["bronze"].get("app_name", f"bronze_{name}"))
        if s.get("silver"):
            jobs.append(s["silver"].get("app_name", f"silver_{name}"))
        pipelines.append({
            "name": name, "type": "stream",
            "description": f"Streaming: {s.get('source', {}).get('topic', '')}",
            "schedule": "always-on", "tags": ["stream", "kafka"], "jobs": jobs,
        })
    return pipelines


def _config_bundle() -> dict:
    catalog = _load_json(BATCH_CATALOG_PATH) or {"pipelines": []}
    registry = _load_json(STREAM_REGISTRY_PATH) or {"streams": []}
    return {
        "catalog": catalog,
        "registry": registry,
        "pipelines": _build_pipelines(catalog, registry),
        "jobs": _build_batch_jobs(catalog) + _build_stream_jobs(registry),
    }

def _config_payload() -> dict:
    b = _config_bundle()
    return {"pipelines": b["pipelines"], "jobs": b["jobs"], "meta": {"loaded_at": now_iso()}}


# ─────────────────────────────────────────────────────────────
# Stream status
# ─────────────────────────────────────────────────────────────

def _unit_aliases(name: str) -> list[str]:
    aliases = [name]
    if name.endswith("_bronze"):
        aliases.append(f"bronze_{name[:-7]}")
    if name.endswith("_silver"):
        aliases.append(f"silver_{name[:-7]}")
    return list(dict.fromkeys(aliases))


def _supervisor_unit_for_job(job: dict) -> str:
    pid = job.get("pipeline", "")
    jid = job.get("id", "")
    if jid.endswith("__bronze"):
        return f"{pid}_bronze"
    if jid.endswith("__silver"):
        return f"{pid}_silver"
    return job.get("name", "")


def _compute_unit_status(unit: dict, now: int) -> dict:
    pid = unit.get("pid")
    rc = unit.get("returncode")
    retries = _safe_int(unit.get("retries")) or 0
    max_retries = _safe_int(unit.get("max_retries")) or 0
    hb = unit.get("heartbeat") or {}
    hb_ts = _safe_int(hb.get("ts_epoch"))
    hb_age = max(0, now - hb_ts) if hb_ts else None
    hb_status = str(hb.get("status", "")).lower()

    if rc is not None:
        status = "failed" if (retries >= max_retries and max_retries > 0) else ("stopped" if rc == 0 else "restarting")
        reason = f"returncode={rc}"
    elif pid is not None:
        if hb_status == "error":
            status, reason = "error", "heartbeat reports error"
        elif hb_age and hb_age > STREAM_HEARTBEAT_STALE_SECONDS:
            status, reason = "stale", f"heartbeat stale {hb_age}s"
        else:
            status, reason = "running", "running"
    else:
        status, reason = "unknown", "no pid"

    return {"computed_status": status, "status_reason": reason,
            "heartbeat_age_seconds": hb_age, "heartbeat_status": hb_status or None}


def _load_stream_status(raw: bool = False) -> dict:
    now = int(time.time())
    if not STREAM_STATUS_FILE.exists():
        return {"available": False, "status": "unavailable", "units": {}}
    try:
        data = json.loads(STREAM_STATUS_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"available": False, "status": "error", "reason": str(exc), "units": {}}
    sup_ts = _safe_int(data.get("ts_epoch"))
    sup_age = max(0, now - sup_ts) if sup_ts else None
    sup_stale = sup_age is not None and sup_age > STREAM_STATUS_STALE_SECONDS
    units = {}
    for uname, u in (data.get("units", {}) or {}).items():
        if not isinstance(u, dict):
            continue
        computed = _compute_unit_status(u, now)
        units[uname] = {**u, **computed, "unit_name": uname, "aliases": _unit_aliases(uname)}
    any_failed = any(u.get("computed_status") in {"failed", "error"} for u in units.values())
    any_running = any(u.get("computed_status") == "running" for u in units.values())
    overall = "stale" if sup_stale else ("degraded" if any_failed else ("running" if any_running else "not_running"))
    result = {"available": True, "status": overall, "supervisor_stale": sup_stale,
               "supervisor_age_seconds": sup_age, "units": units, "observed_at": now_iso()}
    if raw:
        result["raw"] = data
    return result


def _find_stream_unit(job: dict, ss: dict) -> dict | None:
    units = ss.get("units", {}) or {}
    expected = _supervisor_unit_for_job(job)
    jname = job.get("name", "")
    for uname, u in units.items():
        aliases = u.get("aliases", []) if isinstance(u, dict) else []
        if uname in (expected, jname) or jname in aliases or expected in aliases:
            return {**u, "unit_name": uname}
    return None


def _stream_current_from_db(job: dict) -> dict | None:
    if job.get("type") != "stream":
        return None
    unit = _supervisor_unit_for_job(job)
    try:
        return call_usp_one("usp_get_stream_unit_current", (unit,))
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
# Runtime job enrichment
# ─────────────────────────────────────────────────────────────

def _normalize_state(v: Any) -> str:
    return str(v or "unknown").strip().lower() or "unknown"


def _read_registry(limit: int = 2000) -> list[dict]:
    if not JOB_RUN_REGISTRY_FILE.exists():
        return []
    records = []
    try:
        with JOB_RUN_REGISTRY_FILE.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                    records.append(r)
                except json.JSONDecodeError:
                    continue
    except Exception:
        return []
    return sorted(records, key=lambda r: int(r.get("ts_epoch") or 0))[-limit:]


def _match_keys(job: dict) -> set[str]:
    keys: set[str] = set()
    jid = str(job.get("id") or "")
    jname = str(job.get("name") or "")
    pipeline = str(job.get("pipeline") or "")
    jtype = str(job.get("type") or "")
    for v in [jid, jname]:
        if v:
            keys.add(v)
    if jtype == "stream":
        if jid.endswith("__bronze"):
            keys.update([f"{pipeline}_bronze", f"bronze_{pipeline}"])
        if jid.endswith("__silver"):
            keys.update([f"{pipeline}_silver", f"silver_{pipeline}"])
        unit = job.get("runtime_unit_name")
        if unit:
            keys.add(str(unit))
    return keys


def _latest_registry_run(job: dict) -> dict | None:
    keys = _match_keys(job)
    matched = []
    for r in _read_registry():
        candidates = {str(r.get(k) or "") for k in ("job_id", "job", "unit_name", "run_id")}
        if keys & candidates:
            matched.append(r)
    if not matched:
        return None
    # merge events by run_id
    by_run: dict[str, dict] = {}
    for r in matched:
        rid = str(r.get("run_id") or r.get("event_id") or "")
        if not rid:
            continue
        merged = {**by_run.get(rid, {}), **r, "state": r.get("status") or "unknown"}
        by_run[rid] = merged
    runs = sorted(by_run.values(), key=lambda x: int(x.get("ts_epoch") or 0), reverse=True)
    return runs[0] if runs else None

def _registry_row_matches_job(row: dict[str, Any], job: dict[str, Any]) -> bool:
    pipeline = str(job.get("pipeline") or "")
    job_name = str(job.get("name") or "")
    job_id = str(job.get("id") or "")
    job_code = str(job.get("job_code") or job.get("metadata_job_code") or "")

    row_pipeline = str(
        _first_present(row, "pipeline_name", "pipeline", "airflow_dag_id", default="") or ""
    )

    if row_pipeline and pipeline and row_pipeline != pipeline:
        return False

    row_names = {
        str(_first_present(row, "job", default="") or ""),
        str(_first_present(row, "job_name", default="") or ""),
        str(_first_present(row, "base_job_name", default="") or ""),
        str(_first_present(row, "airflow_task_id", default="") or ""),
        str(_first_present(row, "task_id", default="") or ""),
        str(_first_present(row, "entity_name", default="") or ""),
        str(_first_present(row, "job_id", default="") or ""),
    }

    row_codes = {
        str(_first_present(row, "job_code", default="") or ""),
        str(_first_present(row, "job_key", default="") or ""),
        str(_first_present(row, "resolved_job_code", default="") or ""),
        str(_first_present(row, "resolved_job_key", default="") or ""),
    }

    expected_entity = f"{pipeline}.{job_name}"
    expected_code_suffix = f".{pipeline}.{job_name}"

    return (
        job_name in row_names
        or job_id in row_names
        or expected_entity in row_names
        or bool(job_code and job_code in row_codes)
        or any(code.endswith(expected_code_suffix) for code in row_codes if code)
        or any(code.endswith(f".{job_name}") for code in row_codes if code)
    )


def _normalize_registry_run_row(row: dict[str, Any]) -> dict[str, Any]:
    records_read = _as_int_or_none(
        _metric_value(row, "records_read", "input_rows", "last_input_rows")
    )

    records_written = _as_int_or_none(
        _metric_value(row, "records_written", "output_rows", "last_valid_rows")
    )

    records_inserted = _as_int_or_none(
        _metric_value(row, "records_inserted", "inserted_rows")
    )

    records_updated = _as_int_or_none(
        _metric_value(row, "records_updated", "updated_rows")
    )

    records_deleted = _as_int_or_none(
        _metric_value(row, "records_deleted", "deleted_rows")
    )

    if records_inserted is None and records_written is not None:
        records_inserted = records_written

    if records_updated is None:
        records_updated = 0

    if records_deleted is None:
        records_deleted = 0

    started_at = _first_present(row, "started_at", "start_time", "start_date")
    if not started_at:
        started_at = _epoch_to_iso(_first_present(row, "started_at_epoch", "ts_epoch"))

    ended_at = _first_present(row, "ended_at", "end_time", "end_date")
    if not ended_at:
        ended_at = _epoch_to_iso(_first_present(row, "ended_at_epoch", "ts_epoch"))

    dag_run_id = _first_present(
        row,
        "dag_run_id",
        "airflow_dag_run_id",
        "airflow_run_id",
        default=None,
    )

    return {
        "run_id": str(_first_present(row, "run_id", "event_id", default="-")),
        "dag_run_id": str(dag_run_id) if dag_run_id else None,
        "state": _normalize_state(_first_present(row, "state", "status", default="unknown")),
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": _first_present(row, "duration_seconds", "duration_sec"),
        "records_read": records_read,
        "records_written": records_written,
        "records_inserted": records_inserted,
        "records_updated": records_updated,
        "records_deleted": records_deleted,
        "target_path": _first_present(row, "target_path", "output_path"),
        "status_reason": _first_present(row, "status_reason", "error", "error_message", "last_error"),
        "runtime_source": "job_run_registry",
    }


def _registry_run_rows_for_job(job: dict[str, Any], limit: int = 10) -> list[dict[str, Any]]:
    rows = _read_registry(limit=max(2000, limit * 100))

    matched = [
        _normalize_registry_run_row(row)
        for row in rows
        if _registry_row_matches_job(row, job)
    ]

    matched = sorted(
        matched,
        key=lambda r: str(r.get("started_at") or r.get("ended_at") or ""),
        reverse=True,
    )

    return matched[:limit]

def _epoch_to_iso(v: Any) -> str | None:
    try:
        return datetime.fromtimestamp(int(v), tz=timezone.utc).isoformat() if v else None
    except Exception:
        return None


def _latest_airflow_task(dag_id: str, task_id: str) -> dict:
    """
    Load latest Airflow task state.

    Important:
    - A catalog-defined job may not have an Airflow DAG yet.
    - Missing DAG should not be treated as platform failure.
    - It means the job is defined in catalog but not executable yet.
    """
    try:
        quoted = urllib.parse.quote(dag_id, safe="")
        data = _af_get(f"/api/v2/dags/{quoted}/dagRuns?order_by=-logical_date&limit=10")
        runs = data.get("dag_runs") or []

        for run in runs:
            rid = str(run.get("dag_run_id") or "")
            if not rid:
                continue

            quoted_run = urllib.parse.quote(rid, safe="")
            ti_data = _af_get(f"/api/v2/dags/{quoted}/dagRuns/{quoted_run}/taskInstances")
            tasks = ti_data.get("task_instances") or []

            task = next(
                (t for t in tasks if str(t.get("task_id") or "") == task_id),
                None,
            )

            if task:
                ts = task.get("start_date") or run.get("start_date")
                te = task.get("end_date") or run.get("end_date")

                return {
                    "airflow_available": True,
                    "current_status": _normalize_state(task.get("state")),
                    "latest_run_id": rid,
                    "latest_dag_run_state": _normalize_state(run.get("state")),
                    "started_at": ts,
                    "ended_at": te,
                    "duration_seconds": _duration(ts, te),
                }

        return {
            "airflow_available": True,
            "current_status": "no_runs",
            "status_reason": "Airflow DAG exists, but no matching task run was found.",
        }

    except urllib.error.HTTPError as exc:
        try:
            body = exc.read().decode(errors="replace")[:500]
        except Exception:
            body = ""

        if exc.code == 404:
            return {
                "airflow_available": False,
                "missing_airflow_dag": True,
                "current_status": "defined",
                "latest_run_id": None,
                "started_at": None,
                "ended_at": None,
                "duration_seconds": None,
                "status_reason": (
                    f"Catalog-defined job; no Airflow DAG was found for dag_id=`{dag_id}`. "
                    "Run onboarding to load metadata into DB. "
                    "A real executor DAG is still required to execute this job."
                ),
            }

        return {
            "airflow_available": False,
            "current_status": "airflow_unavailable",
            "status_reason": f"Airflow HTTP {exc.code}: {body}",
        }

    except Exception as exc:
        return {
            "airflow_available": False,
            "current_status": "airflow_unavailable",
            "status_reason": str(exc),
        }


def _control_row_matches_job(row: dict[str, Any], job: dict[str, Any]) -> bool:
    pipeline = str(job.get("pipeline") or "")
    job_name = str(job.get("name") or "")
    job_id = str(job.get("id") or "")
    job_code = str(job.get("job_code") or job.get("metadata_job_code") or "")

    row_pipeline = str(
        _first_present(row, "pipeline_name", "pipeline", "airflow_dag_id", default="") or ""
    )

    if row_pipeline and pipeline and row_pipeline != pipeline:
        return False

    row_names = {
        str(_first_present(row, "job_name", default="") or ""),
        str(_first_present(row, "base_job_name", default="") or ""),
        str(_first_present(row, "airflow_task_id", default="") or ""),
        str(_first_present(row, "task_id", default="") or ""),
        str(_first_present(row, "entity_name", default="") or ""),
    }

    row_codes = {
        str(_first_present(row, "job_code", default="") or ""),
        str(_first_present(row, "job_key", default="") or ""),
        str(_first_present(row, "resolved_job_code", default="") or ""),
        str(_first_present(row, "resolved_job_key", default="") or ""),
    }

    expected_entity = f"{pipeline}.{job_name}"
    expected_code_suffix = f".{pipeline}.{job_name}"

    return (
        job_name in row_names
        or job_id in row_names
        or expected_entity in row_names
        or bool(job_code and job_code in row_codes)
        or any(code.endswith(expected_code_suffix) for code in row_codes if code)
        or any(code.endswith(f".{job_name}") for code in row_codes if code)
    )


def _latest_control_run(job: dict) -> dict | None:
    """
    Robust latest job-level runtime run lookup.

    Prefer ctl.usp_list_runtime_job_runs because it exposes real runtime rows
    consistently across catalog-generated DAGs, including dynamic jobs.
    """
    try:
        rows = call_usp_rows("usp_list_runtime_job_runs", (200,))
    except Exception as exc:
        print(f"[WARN] Could not load control runtime runs: {exc}", flush=True)
        return None

    matched = [r for r in rows if _control_row_matches_job(r, job)]

    if not matched:
        return None

    matched = sorted(
        matched,
        key=lambda r: str(
            _first_present(r, "started_at", "ended_at", "created_at", default="")
        ),
        reverse=True,
    )

    return matched[0]


def _first_present(row: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in row and row.get(key) is not None:
            return row.get(key)
    return default


def _as_int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def _value_from_embedded_json(row: dict[str, Any], *keys: str) -> Any:
    """
    Some runtime metrics are top-level columns, while others may be inside
    an embedded event JSON column. This helper scans dict/json-string fields.
    """
    for value in row.values():
        payload = None

        if isinstance(value, dict):
            payload = value

        elif isinstance(value, str):
            text = value.strip()
            if text.startswith("{") and any(k in text for k in keys):
                try:
                    payload = json.loads(text)
                except Exception:
                    payload = None

        if isinstance(payload, dict):
            for key in keys:
                if key in payload and payload.get(key) is not None:
                    return payload.get(key)

    return None


def _metric_value(row: dict[str, Any], *keys: str, default: Any = None) -> Any:
    direct = _first_present(row, *keys, default=None)
    if direct is not None:
        return direct

    embedded = _value_from_embedded_json(row, *keys)
    if embedded is not None:
        return embedded

    return default


def _normalize_runtime_run_row(row: dict[str, Any]) -> dict[str, Any]:
    started_at = _first_present(row, "started_at", "start_time", "start_date", "observed_at")
    ended_at = _first_present(row, "ended_at", "end_time", "end_date")

    duration_seconds = _first_present(row, "duration_seconds", "duration_sec")
    if duration_seconds is None:
        duration_seconds = _duration(started_at, ended_at)

    records_read = _as_int_or_none(
        _metric_value(row, "records_read", "input_rows", "last_input_rows")
    )

    records_written = _as_int_or_none(
        _metric_value(row, "records_written", "output_rows", "last_valid_rows")
    )

    records_inserted = _as_int_or_none(
        _metric_value(row, "records_inserted", "inserted_rows")
    )

    records_updated = _as_int_or_none(
        _metric_value(row, "records_updated", "updated_rows")
    )

    records_deleted = _as_int_or_none(
        _metric_value(row, "records_deleted", "deleted_rows")
    )

    if records_inserted is None and records_written is not None:
        records_inserted = records_written

    if records_updated is None:
        records_updated = 0

    if records_deleted is None:
        records_deleted = 0
        

    run_id = _first_present(row, "run_id", "job_run_id", "runtime_run_id", "event_id", default="-")

    dag_run_id = _first_present(
        row,
        "dag_run_id",
        "airflow_dag_run_id",
        "airflow_run_id",
        default=None,
    )

    return {
        "run_id": str(run_id),
        "dag_run_id": str(dag_run_id) if dag_run_id else None,
        "state": _normalize_state(_first_present(row, "status", "state", default="unknown")),
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": duration_seconds,
        "records_read": records_read,
        "records_written": records_written,
        "records_inserted": records_inserted,
        "records_updated": records_updated,
        "records_deleted": records_deleted,
        "target_path": _first_present(row, "target_path", "output_path"),
        "status_reason": _first_present(row, "status_reason", "error_message", "last_error"),
        "runtime_source": "control_db",
    }


def _control_run_rows_for_job(job: dict[str, Any], limit: int = 10) -> list[dict[str, Any]]:
    try:
        rows = call_usp_rows("usp_list_runtime_job_runs", (max(200, limit * 30),))
    except Exception as exc:
        print(f"[WARN] Could not load runtime job runs from control DB: {exc}", flush=True)
        return []

    matched = [
        _normalize_runtime_run_row(row)
        for row in rows
        if _control_row_matches_job(row, job)
    ]

    matched = sorted(
        matched,
        key=lambda r: str(r.get("started_at") or r.get("ended_at") or ""),
        reverse=True,
    )

    return matched[:limit]


def _stream_pseudo_runs_for_job(job: dict[str, Any], limit: int = 10) -> list[dict[str, Any]]:
    """
    Streams are long-running. They may not have discrete Airflow runs.
    Build a latest pseudo-run from stream runtime status / heartbeat.
    """
    ss = _load_stream_status()
    unit = _find_stream_unit(job, ss)

    if not unit:
        db_unit = _stream_current_from_db(job)
        if not db_unit:
            return []
        unit = db_unit

    hb = unit.get("heartbeat") or unit

    state = _normalize_state(
        unit.get("computed_status")
        or unit.get("status")
        or hb.get("status")
        or "unknown"
    )

    last_batch_id = _first_present(hb, "last_batch_id", "batch_id", default=None)

    records_read = _as_int_or_none(_first_present(hb, "records_read", "last_input_rows", "input_rows"))
    records_written = _as_int_or_none(_first_present(hb, "records_written", "last_valid_rows", "valid_rows"))

    records_inserted = _as_int_or_none(_first_present(hb, "records_inserted", "inserted_rows"))
    if records_inserted is None:
        records_inserted = records_written

    records_updated = _as_int_or_none(_first_present(hb, "records_updated", "updated_rows", default=0))
    records_deleted = _as_int_or_none(_first_present(hb, "records_deleted", "deleted_rows", default=0))

    return [
        {
            "run_id": str(last_batch_id if last_batch_id is not None else unit.get("unit_name") or job.get("name")),
            "dag_run_id": None,
            "state": state,
            "started_at": _first_present(hb, "started_at", "start_time"),
            "ended_at": _first_present(hb, "observed_at", "updated_at"),
            "duration_seconds": None,
            "records_read": records_read,
            "records_written": records_written,
            "records_inserted": records_inserted,
            "records_updated": records_updated,
            "records_deleted": records_deleted,
            "target_path": _first_present(hb, "target_path", "bronze_path", "silver_path", default=job.get("target_path")),
            "status_reason": _first_present(hb, "last_error", "status_reason"),
            "runtime_source": "stream_runtime",
        }
    ][:limit]


def _has_explicit_airflow_executor(job: dict) -> bool:
    """
    Static catalog jobs are expected to map to Airflow DAGs by pipeline name.
    UI-defined dynamic jobs should NOT be probed against Airflow unless
    an explicit Airflow executor is configured.

    This keeps dynamic job definition infrastructure-free.
    """
    if not job.get("dynamic"):
        return True

    return bool(job.get("airflow_dag_id"))

def _enrich_jobs(config_jobs: list[dict]) -> dict:
    ss = _load_stream_status()
    enriched = []
    for job in config_jobs:
        rj = {**job, "current_status": "defined", "latest_run_id": None,
               "started_at": None, "ended_at": None, "duration_seconds": None,
               "status_reason": None, "runtime_source": "catalog", "runtime_available": False}
        # 1. Try control DB
        ctrl = _latest_control_run(job)
        if ctrl:
            rj.update({
                "current_status": ctrl.get("status", "unknown"),
                "latest_run_id": str(ctrl.get("run_id") or ""),
                "started_at": ctrl.get("started_at"), "ended_at": ctrl.get("ended_at"),
                "duration_seconds": ctrl.get("duration_seconds"),
                "target_path": ctrl.get("target_path") or job.get("target_path"),
                "status_reason": ctrl.get("status_reason") or ctrl.get("error_message"),
                "runtime_source": "control_db", "runtime_available": True,
                "records_read": _as_int_or_none(
                    _metric_value(ctrl, "records_read", "input_rows", "last_input_rows")
                ),
                "records_written": _as_int_or_none(
                    _metric_value(ctrl, "records_written", "output_rows", "last_valid_rows")
                ),
                "records_inserted": _as_int_or_none(
                    _metric_value(ctrl, "records_inserted", "inserted_rows", "records_written")
                ),
                "records_updated": _as_int_or_none(
                    _metric_value(ctrl, "records_updated", "updated_rows", default=0)
                ),
                "records_deleted": _as_int_or_none(
                    _metric_value(ctrl, "records_deleted", "deleted_rows", default=0)
                ),

            })
            if rj.get("records_inserted") is None and rj.get("records_written") is not None:
                rj["records_inserted"] = rj["records_written"]

            if rj.get("records_updated") is None:
                rj["records_updated"] = 0

            if rj.get("records_deleted") is None:
                rj["records_deleted"] = 0

        else:
            # 2. Try local registry
            reg = _latest_registry_run(job)
            if reg:
                reg_norm = _normalize_registry_run_row(reg)

                rj.update({
                    "current_status": reg_norm.get("state", "unknown"),
                    "latest_run_id": reg_norm.get("run_id"),
                    "started_at": reg_norm.get("started_at"),
                    "ended_at": reg_norm.get("ended_at"),
                    "duration_seconds": reg_norm.get("duration_seconds"),
                    "target_path": reg_norm.get("target_path") or rj.get("target_path"),
                    "status_reason": reg_norm.get("status_reason"),
                    "runtime_source": "job_run_registry",
                    "runtime_available": True,
                    "records_read": reg_norm.get("records_read"),
                    "records_written": reg_norm.get("records_written"),
                    "records_inserted": reg_norm.get("records_inserted"),
                    "records_updated": reg_norm.get("records_updated"),
                    "records_deleted": reg_norm.get("records_deleted"),
                })
            elif job.get("type") == "batch":
                af = _latest_airflow_task(job.get("pipeline", ""), job.get("name", ""))

                runtime_source = "airflow"
                if af.get("missing_airflow_dag"):
                    runtime_source = "catalog"

                rj.update(
                    {
                        **af,
                        "runtime_source": runtime_source,
                        "runtime_available": af.get("airflow_available", False),
                    }
                )

                # If Airflow DAG is missing, check whether onboarding already loaded
                # this catalog job into the control DB metadata tables.
                if af.get("missing_airflow_dag"):
                    meta_job = _catalog_metadata_for_job(job)

                    if meta_job:
                        rj.update(
                            {
                                "current_status": "onboarded",
                                "runtime_source": "catalog_metadata",
                                "runtime_available": False,
                                "metadata_available": True,
                                "metadata_job_id": meta_job.get("job_id"),
                                "metadata_job_code": meta_job.get("job_code"),
                                "metadata_pipeline_id": meta_job.get("pipeline_id"),
                                "metadata_airflow_dag_id": meta_job.get("airflow_dag_id"),
                                "source_type": meta_job.get("source_type") or rj.get("source_type"),
                                "target_path": meta_job.get("target_path") or rj.get("target_path"),
                                "status_reason": (
                                    "Job is onboarded in the control DB, but no executable Airflow DAG exists yet. "
                                    "Create a dispatcher DAG or bind this pipeline to an existing DAG to execute it."
                                ),
                            }
                        )
            elif job.get("dynamic"):
                    rj.update(
                        {
                            "current_status": "defined",
                            "latest_run_id": None,
                            "started_at": None,
                            "ended_at": None,
                            "duration_seconds": None,
                            "status_reason": "Defined in UI registry; no Airflow executor DAG configured yet.",
                            "runtime_source": "ui_job_registry",
                            "runtime_available": False,
                        }
                    )
        # 4. Stream runtime
        if rj.get("type") == "stream":
            db_unit = _stream_current_from_db(rj)
            if db_unit:
                rj.update({
                    "runtime_available": True, "runtime_source": "stream_runtime_db",
                    "runtime_unit_name": db_unit.get("unit_name"),
                    "current_status": db_unit.get("computed_status") or rj.get("current_status"),
                    "heartbeat_age_seconds": db_unit.get("heartbeat_age_seconds"),
                    "last_batch_id": db_unit.get("last_batch_id"),
                    "last_input_rows": db_unit.get("last_input_rows"),
                    "last_valid_rows": db_unit.get("last_valid_rows"),
                    "last_invalid_rows": db_unit.get("last_invalid_rows"),
                    "last_write_ok": db_unit.get("last_write_ok"),
                    "last_error": db_unit.get("last_error"),
                    "stream_target_path": db_unit.get("target_path") or rj.get("target_path"),
                    "records_read": _as_int_or_none(_first_present(db_unit, "records_read", "last_input_rows", "input_rows")),
                    "records_written": _as_int_or_none(_first_present(db_unit, "records_written", "last_valid_rows", "valid_rows")),
                    "records_inserted": _as_int_or_none(_first_present(db_unit, "records_inserted", "last_valid_rows", "valid_rows")),
                    "records_updated": _as_int_or_none(_first_present(db_unit, "records_updated", "updated_rows", default=0)),
                    "records_deleted": _as_int_or_none(_first_present(db_unit, "records_deleted", "deleted_rows", default=0)),
                })
            else:
                unit = _find_stream_unit(rj, ss)
                if unit:
                    hb = unit.get("heartbeat") or {}
                    rj.update({
                        "runtime_available": True, "runtime_source": "stream_supervisor_file",
                        "runtime_unit_name": unit.get("unit_name"),
                        "current_status": unit.get("computed_status") or rj.get("current_status"),
                        "heartbeat_age_seconds": unit.get("heartbeat_age_seconds"),
                        "last_batch_id": hb.get("last_batch_id"),
                        "last_input_rows": hb.get("last_input_rows"),
                        "last_valid_rows": hb.get("last_valid_rows"),
                        "last_invalid_rows": hb.get("last_invalid_rows"),
                        "last_write_ok": hb.get("last_write_ok"),
                        "last_error": hb.get("last_error"),
                        "stream_target_path": hb.get("silver_path") or hb.get("bronze_path") or rj.get("target_path"),
                        "records_read": _as_int_or_none(_first_present(hb, "records_read", "input_rows")),
                        "records_written": _as_int_or_none(_first_present(hb, "records_written", "output_rows")),
                        "records_inserted": _as_int_or_none(_first_present(hb, "records_inserted", "inserted_rows")),
                        "records_updated": _as_int_or_none(_first_present(hb, "records_updated", "updated_rows", default=0)),
                        "records_deleted": _as_int_or_none(_first_present(hb, "records_deleted", "deleted_rows", default=0)),
                    })
        enriched.append(rj)
    return {"jobs": enriched, "stream_status": ss, "loaded_at": now_iso()}


def _job_from_config(job_id: str) -> dict | None:
    for job in _config_payload().get("jobs", []):
        if job.get("id") == job_id or job.get("name") == job_id:
            return job
    return None


def _find_airflow_log(dag_id: str, task_id: str, dag_run_id: str | None = None) -> Path:
    candidates: list[Path] = []
    dag_dir = AIRFLOW_LOG_DIR / f"dag_id={dag_id}"
    if dag_run_id:
        candidates.extend((dag_dir / f"run_id={dag_run_id}").glob(f"task_id={task_id}/**/*.log"))
    if not candidates and dag_dir.exists():
        candidates.extend(dag_dir.glob(f"**/task_id={task_id}/**/*.log"))
    if not candidates and AIRFLOW_LOG_DIR.exists():
        candidates.extend(AIRFLOW_LOG_DIR.glob(f"**/dag_id={dag_id}/**/task_id={task_id}/**/*.log"))
    candidates = [p for p in candidates if p.is_file()]
    if not candidates:
        raise FileNotFoundError(f"No Airflow log: dag={dag_id} task={task_id}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


# ─────────────────────────────────────────────────────────────
# Pydantic request models
# ─────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    username: str
    password: str


class CreateUserRequest(BaseModel):
    username: str
    password: str
    email: str | None = None
    role: str = "viewer"


class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str


class DagTriggerRequest(BaseModel):
    dag_id: str
    conf: dict | None = None
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


# ─────────────────────────────────────────────────────────────
# ─── ROUTES ──────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "ts": now_iso()}


# ─────────────────────────────────────────────────────────────
# Auth routes
# ─────────────────────────────────────────────────────────────

@app.post("/auth/login")
async def login(req: LoginRequest) -> dict:
    row = call_usp_one("usp_get_dashboard_user", (req.username,))
    if not row or not verify_password(req.password, row["password_hash"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    call_usp_void("usp_update_last_login", (req.username,))
    token = create_access_token({
        "sub": row["username"],
        "user_id": row["user_id"],
        "role": row["role"],
    })
    return {"access_token": token, "token_type": "bearer",
            "username": row["username"], "role": row["role"]}


@app.get("/auth/me")
async def me(user: dict = Depends(get_current_user)) -> dict:
    return {"username": user.get("sub"), "role": user.get("role")}


@app.post("/auth/users", dependencies=[Depends(require_role("admin"))])
async def create_user(req: CreateUserRequest, user: dict = Depends(get_current_user)) -> dict:
    call_usp_void("usp_upsert_dashboard_user",
                  (req.username, req.email, hash_password(req.password), req.role))
    return {"created": req.username, "role": req.role}


@app.post("/auth/change-password")
async def change_password(req: ChangePasswordRequest, user: dict = Depends(get_current_user)) -> dict:
    row = call_usp_one("usp_get_dashboard_user", (user["sub"],))
    if not row or not verify_password(req.old_password, row["password_hash"]):
        raise HTTPException(status_code=400, detail="Old password incorrect")
    call_usp_void("usp_upsert_dashboard_user",
                  (user["sub"], row.get("email"), hash_password(req.new_password), row.get("role", "viewer")))
    return {"ok": True}


# ─────────────────────────────────────────────────────────────
# Read-only config routes (no auth required for dashboard view)
# ─────────────────────────────────────────────────────────────

@app.get("/api/config")
async def get_config() -> JSONResponse:
    return JSONResponse(_config_payload())


@app.get("/api/runtime/jobs")
async def get_runtime_jobs() -> JSONResponse:
    bundle = _config_bundle()
    runtime = _enrich_jobs(bundle["jobs"])
    return JSONResponse({"jobs": runtime["jobs"], "stream_status": runtime["stream_status"],
                         "meta": {"loaded_at": runtime["loaded_at"]}})


@app.get("/api/runtime/mounts")
async def get_mounts() -> JSONResponse:
    return JSONResponse({
        "stream_status_file": _path_state(STREAM_STATUS_FILE),
        "stream_log_dir": _path_state(STREAM_LOG_DIR),
        "airflow_log_dir": _path_state(AIRFLOW_LOG_DIR),
        "job_run_registry_file": _path_state(JOB_RUN_REGISTRY_FILE),
        "checked_at": now_iso(),
    })


@app.get("/api/runtime/streams/status")
async def get_stream_status(raw: bool = Query(default=False)) -> JSONResponse:
    return JSONResponse(_load_stream_status(raw=raw))


@app.get("/api/runtime/runs/{job_id}")
async def get_runs(job_id: str, limit: int = Query(default=10, ge=1, le=50)) -> JSONResponse:
    job = _job_from_config(job_id)

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

    control_runs = _control_run_rows_for_job(job, limit=limit)

    if control_runs:
        return JSONResponse(
            {
                "available": True,
                "source": "control_db",
                "job_id": job_id,
                "runs": control_runs,
                "count": len(control_runs),
            }
        )
    
    registry_runs = _registry_run_rows_for_job(job, limit=limit)

    if registry_runs:
        return JSONResponse(
            {
                "available": True,
                "source": "job_run_registry",
                "job_id": job_id,
                "runs": registry_runs,
                "count": len(registry_runs),
            }
        )

    if job.get("type") == "batch":
        try:
            quoted = urllib.parse.quote(job.get("pipeline", ""), safe="")
            data = _af_get(f"/api/v2/dags/{quoted}/dagRuns?order_by=-logical_date&limit={limit}")
            runs = data.get("dag_runs") or []

            result = []

            for run in runs:
                rid = str(run.get("dag_run_id") or "")
                ts = run.get("start_date")
                te = run.get("end_date")

                result.append(
                    {
                        "run_id": None,
                        "dag_run_id": rid,
                        "state": _normalize_state(run.get("state")),
                        "started_at": ts,
                        "ended_at": te,
                        "duration_seconds": _duration(ts, te),
                        "records_read": None,
                        "records_written": None,
                        "records_inserted": None,
                        "records_updated": None,
                        "records_deleted": None,
                        "runtime_source": "airflow",
                    }
                )

            return JSONResponse(
                {
                    "available": True,
                    "source": "airflow",
                    "job_id": job_id,
                    "runs": result,
                    "count": len(result),
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

@app.get("/api/runtime/logs/{job_id}")
async def get_logs(
    job_id: str,
    kind: str = Query(default="batch", pattern="^(batch|stream)$"),
    pipeline: str | None = None,
    task: str | None = None,
    lines: int = Query(default=500, ge=10, le=5000),
) -> PlainTextResponse:
    job = _job_from_config(job_id)
    eff_kind = kind or (job.get("type") if job else None)
    if eff_kind == "stream":
        sname = _resolve_stream_log(
            (job.get("runtime_unit_name") if job else None) or (job.get("name") if job else None) or job_id
        )
        log_path = STREAM_LOG_DIR / f"{sname}.log"
        if not log_path.exists():
            available = sorted(p.name for p in STREAM_LOG_DIR.glob("*.log")) if STREAM_LOG_DIR.exists() else []
            return PlainTextResponse(f"Log not found: {log_path}\nAvailable: {available}", status_code=404)
        return PlainTextResponse(_tail_file(log_path, lines))
    dag_id = pipeline or (job.get("pipeline") if job else None)
    task_id = task or (job.get("name") if job else None)
    if not dag_id or not task_id:
        return PlainTextResponse("Missing pipeline/task", status_code=400)
    try:
        return PlainTextResponse(_tail_file(_find_airflow_log(dag_id, task_id), lines))
    except FileNotFoundError as exc:
        return PlainTextResponse(str(exc), status_code=404)


# ─────────────────────────────────────────────────────────────
# Control DB read routes
# ─────────────────────────────────────────────────────────────

@app.get("/api/control/jobs")
def control_jobs() -> dict:
    return {"items": call_usp_rows("usp_list_control_jobs")}


@app.get("/api/runtime/job-runs")
def runtime_job_runs(limit: int = 50) -> dict:
    return {"items": call_usp_rows("usp_list_runtime_job_runs", (limit,))}


@app.get("/api/runtime/watermarks")
def runtime_watermarks() -> dict:
    return {"items": call_usp_rows("usp_list_runtime_watermarks")}


@app.get("/api/quality/results")
def quality_results(limit: int = 100) -> dict:
    return {"items": call_usp_rows("usp_list_quality_results", (limit,))}


@app.get("/api/lineage/datasets")
def dataset_lineage(limit: int = 100) -> dict:
    return {"items": call_usp_rows("usp_list_dataset_lineage", (limit,))}


# ─────────────────────────────────────────────────────────────
# ─── EXECUTION ENDPOINTS ─────────────────────────────────────
# ─────────────────────────────────────────────────────────────

@app.get("/api/catalog/change-log")
async def catalog_change_log(
    limit: int = Query(default=100, ge=1, le=500),
    user: dict = Depends(get_current_user),
) -> dict:
    try:
        return {"items": call_usp_rows("usp_list_catalog_change_logs", (limit,))}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/actions/dag/trigger")
async def action_dag_trigger(
    req: DagTriggerRequest,
    user: dict = Depends(require_role("admin", "operator")),
) -> dict:
    t0 = time.time()
    try:
        result = trigger_dag(req.dag_id, conf=req.conf, logical_date=req.logical_date)
        _log_action(user=user, action_type="dag_trigger", target_type="dag",
                    target_id=req.dag_id, request_payload=req.model_dump(),
                    result_status="success", result_payload=result,
                    duration_ms=int((time.time() - t0) * 1000))
        return {"ok": True, **result}
    except Exception as exc:
        _log_action(user=user, action_type="dag_trigger", target_type="dag",
                    target_id=req.dag_id, request_payload=req.model_dump(),
                    result_status="failed", error_message=str(exc),
                    duration_ms=int((time.time() - t0) * 1000))
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/actions/dag/pause")
async def action_dag_pause(
    req: DagPauseRequest,
    user: dict = Depends(require_role("admin", "operator")),
) -> dict:
    t0 = time.time()
    try:
        result = pause_dag(req.dag_id, req.paused)
        _log_action(user=user, action_type="dag_pause" if req.paused else "dag_unpause",
                    target_type="dag", target_id=req.dag_id,
                    request_payload=req.model_dump(), result_status="success", result_payload=result,
                    duration_ms=int((time.time() - t0) * 1000))
        return {"ok": True, **result}
    except Exception as exc:
        _log_action(user=user, action_type="dag_pause", target_type="dag", target_id=req.dag_id,
                    request_payload=req.model_dump(), result_status="failed", error_message=str(exc),
                    duration_ms=int((time.time() - t0) * 1000))
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/actions/dag/cancel")
async def action_dag_cancel(
    req: DagCancelRequest,
    user: dict = Depends(require_role("admin", "operator")),
) -> dict:
    t0 = time.time()
    try:
        result = cancel_dag_run(req.dag_id, req.run_id)
        _log_action(user=user, action_type="dag_cancel", target_type="dag", target_id=req.dag_id,
                    request_payload=req.model_dump(), result_status="success", result_payload=result,
                    duration_ms=int((time.time() - t0) * 1000))
        return {"ok": True, **result}
    except Exception as exc:
        _log_action(user=user, action_type="dag_cancel", target_type="dag", target_id=req.dag_id,
                    request_payload=req.model_dump(), result_status="failed", error_message=str(exc),
                    duration_ms=int((time.time() - t0) * 1000))
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/actions/dag/{dag_id}/runs")
async def action_dag_runs(
    dag_id: str,
    limit: int = Query(default=10, ge=1, le=50),
    user: dict = Depends(get_current_user),
) -> dict:
    try:
        runs = get_dag_runs(dag_id, limit=limit)
        return {"dag_id": dag_id, "runs": runs}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/actions/dags")
async def action_list_dags(user: dict = Depends(get_current_user)) -> dict:
    try:
        return {"dags": list_dags()}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/actions/stream/restart")
async def action_stream_restart(
    req: StreamActionRequest,
    user: dict = Depends(require_role("admin", "operator")),
) -> dict:
    t0 = time.time()
    try:
        result = restart_stream(req.unit_name)
        _log_action(user=user, action_type="stream_restart", target_type="stream",
                    target_id=req.unit_name, request_payload=req.model_dump(),
                    result_status="success", result_payload=result,
                    duration_ms=int((time.time() - t0) * 1000))
        return {"ok": True, **result}
    except Exception as exc:
        _log_action(user=user, action_type="stream_restart", target_type="stream",
                    target_id=req.unit_name, request_payload=req.model_dump(),
                    result_status="failed", error_message=str(exc),
                    duration_ms=int((time.time() - t0) * 1000))
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/actions/stream/stop")
async def action_stream_stop(
    req: StreamActionRequest,
    user: dict = Depends(require_role("admin", "operator")),
) -> dict:
    t0 = time.time()
    try:
        result = stop_stream(req.unit_name)
        _log_action(user=user, action_type="stream_stop", target_type="stream",
                    target_id=req.unit_name, request_payload=req.model_dump(),
                    result_status="success", result_payload=result,
                    duration_ms=int((time.time() - t0) * 1000))
        return {"ok": True, **result}
    except Exception as exc:
        _log_action(user=user, action_type="stream_stop", target_type="stream",
                    target_id=req.unit_name, request_payload=req.model_dump(),
                    result_status="failed", error_message=str(exc),
                    duration_ms=int((time.time() - t0) * 1000))
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/actions/onboard")
async def action_onboard(
    req: OnboardRequest,
    user: dict = Depends(require_role("admin")),
) -> dict:
    """Onboarding را از طریق Airflow DAG trigger می‌کند."""
    t0 = time.time()
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
        _log_action(user=user, action_type="onboard", target_type="pipeline",
                    target_id=req.pipeline_name or "all",
                    request_payload=req.model_dump(), result_status="success", result_payload=result,
                    duration_ms=int((time.time() - t0) * 1000))
        return {"ok": True, **result}
    except Exception as exc:
        _log_action(user=user, action_type="onboard", target_type="pipeline",
                    target_id=req.pipeline_name or "all",
                    request_payload=req.model_dump(), result_status="failed", error_message=str(exc),
                    duration_ms=int((time.time() - t0) * 1000))
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/actions/history")
async def action_history(
    limit: int = Query(default=50, ge=1, le=500),
    user: dict = Depends(get_current_user),
) -> dict:
    try:
        items = call_usp_rows("usp_list_action_logs", (limit,))
        return {"items": items}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ─────────────────────────────────────────────────────────────
# WebSocket — live log streaming
# ─────────────────────────────────────────────────────────────

@app.websocket("/api/ws/logs/{unit_name}")
async def ws_logs(
    websocket: WebSocket,
    unit_name: str,
    token: str | None = Query(default=None),
    kind: str = Query(default="stream"),
    pipeline: str | None = Query(default=None),
    task: str | None = Query(default=None),
    lines: int = Query(default=100),
):
    # Auth check
    try:
        get_current_user_ws(token)
    except HTTPException:
        await websocket.close(code=4001)
        return

    await websocket.accept()

    # Resolve log path
    try:
        if kind == "stream":
            resolved = _resolve_stream_log(unit_name)
            log_path = STREAM_LOG_DIR / f"{resolved}.log"
        else:
            dag_id = pipeline or unit_name
            task_id = task or unit_name
            log_path = _find_airflow_log(dag_id, task_id)
    except Exception as exc:
        await websocket.send_json({"type": "error", "message": str(exc)})
        await websocket.close()
        return

    # Send historical tail first
    if log_path.exists():
        historical = _tail_file(log_path, lines)
        await websocket.send_json({"type": "history", "content": historical})

    # Then stream new lines
    last_size = log_path.stat().st_size if log_path.exists() else 0
    try:
        while True:
            await asyncio.sleep(1.0)
            if not log_path.exists():
                await websocket.send_json({"type": "ping"})
                continue
            current_size = log_path.stat().st_size
            if current_size > last_size:
                with log_path.open("r", encoding="utf-8", errors="replace") as f:
                    f.seek(last_size)
                    new_content = f.read()
                if new_content:
                    await websocket.send_json({"type": "append", "content": new_content})
                last_size = current_size
            else:
                await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        pass
    except Exception:
        pass


