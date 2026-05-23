from __future__ import annotations

# actions.py — همه execution logic برای dashboard.
# هر action:
#   1. به Airflow یا supervisor می‌رود
#   2. نتیجه را log می‌کند
#   3. وضعیت ساختاریافته برمی‌گرداند

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any
from datetime import datetime, timezone
# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────
AIRFLOW_API_BASE = os.getenv("AIRFLOW_API_BASE", "http://airflow-api-server:8080").rstrip("/")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")

STREAM_CONTROL_DIR = Path(os.getenv("STREAM_CONTROL_DIR", "/tmp/health/control"))
STREAM_STATUS_FILE = Path(os.getenv("STREAM_STATUS_FILE", "/tmp/health/stream_supervisor_status.json"))

# ─────────────────────────────────────────────────────────────
# Airflow token cache (thread-safe via dict + lock)
# ─────────────────────────────────────────────────────────────
import threading

_token_lock = threading.Lock()
_token_cache: dict[str, Any] = {"access_token": None, "expires_at": 0}


def _get_airflow_token(timeout: int = 10) -> str:
    with _token_lock:
        now = time.time()
        if _token_cache["access_token"] and now < _token_cache["expires_at"]:
            return str(_token_cache["access_token"])

        url = f"{AIRFLOW_API_BASE}/auth/token"
        data = json.dumps({"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD}).encode()
        req = urllib.request.Request(
            url, data=data, method="POST",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = json.loads(resp.read().decode())
        except Exception as exc:
            raise RuntimeError(f"Airflow auth failed: {exc}") from exc

        token = body.get("access_token") or body.get("token")
        if not token:
            raise RuntimeError(f"No token in Airflow response: {body}")

        _token_cache["access_token"] = token
        _token_cache["expires_at"] = now + 240
        return str(token)


def _airflow_request(method: str, path: str, body: Any = None, timeout: int = 15) -> dict[str, Any]:
    """Generic Airflow API call with auto token refresh."""
    url = f"{AIRFLOW_API_BASE}{path}"
    token = _get_airflow_token()
    data = json.dumps(body).encode() if body is not None else None
    headers: dict[str, str] = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    if data:
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        body_txt = exc.read().decode(errors="replace")[:500]
        raise RuntimeError(f"Airflow {method} {path} failed [{exc.code}]: {body_txt}") from exc


# ─────────────────────────────────────────────────────────────
# DAG actions
# ─────────────────────────────────────────────────────────────

def trigger_dag(
    dag_id: str,
    conf: dict | None = None,
    logical_date: str | None = None,
) -> dict[str, Any]:
    """
    Trigger a DAG run.

    Airflow API v2 in this stack requires logical_date in the request body.
    If the dashboard does not provide it, generate a UTC logical_date.
    """
    effective_logical_date = logical_date or datetime.now(timezone.utc).isoformat()

    payload: dict[str, Any] = {
        "conf": conf or {},
        "logical_date": effective_logical_date,
    }

    quoted = urllib.parse.quote(dag_id, safe="")
    result = _airflow_request(
        "POST",
        f"/api/v2/dags/{quoted}/dagRuns",
        body=payload,
    )

    return {
        "dag_id": dag_id,
        "dag_run_id": result.get("dag_run_id") or result.get("run_id"),
        "state": result.get("state"),
        "logical_date": result.get("logical_date") or effective_logical_date,
    }

def pause_dag(dag_id: str, paused: bool) -> dict[str, Any]:
    """Pause or unpause a DAG."""
    quoted = urllib.parse.quote(dag_id, safe="")
    result = _airflow_request("PATCH", f"/api/v2/dags/{quoted}", body={"is_paused": paused})
    return {"dag_id": dag_id, "is_paused": result.get("is_paused", paused)}


def cancel_dag_run(dag_id: str, run_id: str) -> dict[str, Any]:
    """Cancel (mark failed) a running DAG run."""
    quoted_dag = urllib.parse.quote(dag_id, safe="")
    quoted_run = urllib.parse.quote(run_id, safe="")
    result = _airflow_request(
        "PATCH",
        f"/api/v2/dags/{quoted_dag}/dagRuns/{quoted_run}",
        body={"state": "failed"},
    )
    return {"dag_id": dag_id, "run_id": run_id, "state": result.get("state")}


def get_dag_runs(dag_id: str, limit: int = 10) -> list[dict[str, Any]]:
    """List recent runs of a DAG."""
    quoted = urllib.parse.quote(dag_id, safe="")
    result = _airflow_request("GET", f"/api/v2/dags/{quoted}/dagRuns?order_by=-logical_date&limit={limit}")
    items = result.get("dag_runs") or result.get("dagRuns") or []
    return [
        {
            "dag_run_id": r.get("dag_run_id"),
            "state": r.get("state"),
            "logical_date": r.get("logical_date"),
            "start_date": r.get("start_date"),
            "end_date": r.get("end_date"),
        }
        for r in items
    ]


def list_dags() -> list[dict[str, Any]]:
    """List all DAGs."""
    result = _airflow_request("GET", "/api/v2/dags?limit=100")
    items = result.get("dags") or []
    return [
        {
            "dag_id": d.get("dag_id"),
            "is_paused": d.get("is_paused"),
            "is_active": d.get("is_active"),
            "schedule_interval": d.get("schedule_interval") or d.get("timetable_summary"),
            "tags": [t.get("name") for t in (d.get("tags") or [])],
            "last_parsed_time": d.get("last_parsed_time"),
        }
        for d in items
    ]


# ─────────────────────────────────────────────────────────────
# Stream actions — file-based control
# ─────────────────────────────────────────────────────────────

def _write_stream_control(unit_name: str, action: str, extra: dict | None = None) -> None:
    """
    Supervisor در poll loop خود /tmp/health/control/{unit_name}.json را می‌خواند.
    این تابع آن فایل را می‌نویسد.
    """
    STREAM_CONTROL_DIR.mkdir(parents=True, exist_ok=True)
    control_file = STREAM_CONTROL_DIR / f"{unit_name}.json"
    payload = {
        "action": action,
        "ts": time.time(),
        "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        **(extra or {}),
    }
    control_file.write_text(json.dumps(payload), encoding="utf-8")


def restart_stream(unit_name: str) -> dict[str, Any]:
    """درخواست restart یک stream unit."""
    _write_stream_control(unit_name, "restart")
    return {"unit_name": unit_name, "action": "restart", "status": "requested"}


def stop_stream(unit_name: str) -> dict[str, Any]:
    """درخواست stop یک stream unit."""
    _write_stream_control(unit_name, "stop")
    return {"unit_name": unit_name, "action": "stop", "status": "requested"}


def get_stream_control_status(unit_name: str) -> dict[str, Any] | None:
    """وضعیت آخرین control command برای یک unit."""
    f = STREAM_CONTROL_DIR / f"{unit_name}.json"
    if not f.exists():
        return None
    try:
        return json.loads(f.read_text(encoding="utf-8"))
    except Exception:
        return None