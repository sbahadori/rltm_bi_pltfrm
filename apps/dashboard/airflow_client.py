from __future__ import annotations

"""
Airflow API adapter.

Single responsibility:
- Own Airflow authentication and HTTP interaction.
- Expose small dashboard-oriented Airflow operations.

No FastAPI routes, no Control DB calls, and no catalog parsing belongs here.
"""

import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

try:
    from apps.dashboard.config_loader import AIRFLOW_API_BASE, AIRFLOW_PASSWORD, AIRFLOW_USER
    from apps.dashboard.runtime_models import duration_seconds, normalize_state
except ImportError:  # pragma: no cover
    from .config_loader import AIRFLOW_API_BASE, AIRFLOW_PASSWORD, AIRFLOW_USER
    from .runtime_models import duration_seconds, normalize_state

_token_lock = threading.Lock()
_token_cache: dict[str, Any] = {"access_token": None, "expires_at": 0.0}


def get_airflow_token(timeout: int = 10) -> str:
    with _token_lock:
        now = time.time()
        if _token_cache["access_token"] and now < float(_token_cache["expires_at"]):
            return str(_token_cache["access_token"])

        url = f"{AIRFLOW_API_BASE}/auth/token"
        data = json.dumps({"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD}).encode()
        req = urllib.request.Request(
            url,
            data=data,
            method="POST",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )

        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                body = json.loads(response.read().decode())
        except Exception as exc:
            raise RuntimeError(f"Airflow auth failed: {exc}") from exc

        token = body.get("access_token") or body.get("token")
        if not token:
            raise RuntimeError(f"No token in Airflow response: {body}")

        _token_cache["access_token"] = token
        _token_cache["expires_at"] = now + 240
        return str(token)


def airflow_request(method: str, path: str, body: Any = None, timeout: int = 15) -> dict[str, Any]:
    url = f"{AIRFLOW_API_BASE}{path}"
    data = json.dumps(body).encode() if body is not None else None

    headers: dict[str, str] = {
        "Accept": "application/json",
        "Authorization": f"Bearer {get_airflow_token()}",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(url, data=data, method=method, headers=headers)

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode(errors="replace")[:500]
        raise RuntimeError(f"Airflow {method} {path} failed [{exc.code}]: {body_text}") from exc


def airflow_get(path: str, timeout: int = 10) -> dict[str, Any]:
    return airflow_request("GET", path, timeout=timeout)


def airflow_post(path: str, body: Any = None, timeout: int = 15) -> dict[str, Any]:
    return airflow_request("POST", path, body=body, timeout=timeout)


def airflow_patch(path: str, body: Any = None, timeout: int = 15) -> dict[str, Any]:
    return airflow_request("PATCH", path, body=body, timeout=timeout)


def trigger_dag(dag_id: str, conf: dict[str, Any] | None = None, logical_date: str | None = None) -> dict[str, Any]:
    effective_logical_date = logical_date or datetime.now(timezone.utc).isoformat()
    payload = {"conf": conf or {}, "logical_date": effective_logical_date}

    quoted_dag = urllib.parse.quote(dag_id, safe="")
    result = airflow_post(f"/api/v2/dags/{quoted_dag}/dagRuns", body=payload)

    return {
        "dag_id": dag_id,
        "dag_run_id": result.get("dag_run_id") or result.get("run_id"),
        "state": result.get("state"),
        "logical_date": result.get("logical_date") or effective_logical_date,
    }


def pause_dag(dag_id: str, paused: bool) -> dict[str, Any]:
    quoted_dag = urllib.parse.quote(dag_id, safe="")
    result = airflow_patch(f"/api/v2/dags/{quoted_dag}", body={"is_paused": paused})
    return {"dag_id": dag_id, "is_paused": result.get("is_paused", paused)}


def cancel_dag_run(dag_id: str, run_id: str) -> dict[str, Any]:
    quoted_dag = urllib.parse.quote(dag_id, safe="")
    quoted_run = urllib.parse.quote(run_id, safe="")
    result = airflow_patch(
        f"/api/v2/dags/{quoted_dag}/dagRuns/{quoted_run}",
        body={"state": "failed"},
    )
    return {"dag_id": dag_id, "run_id": run_id, "state": result.get("state")}


def get_dag_runs(dag_id: str, limit: int = 10) -> list[dict[str, Any]]:
    quoted_dag = urllib.parse.quote(dag_id, safe="")
    result = airflow_get(f"/api/v2/dags/{quoted_dag}/dagRuns?order_by=-logical_date&limit={limit}")
    items = result.get("dag_runs") or result.get("dagRuns") or []

    return [
        {
            "dag_run_id": run.get("dag_run_id"),
            "state": run.get("state"),
            "logical_date": run.get("logical_date"),
            "start_date": run.get("start_date"),
            "end_date": run.get("end_date"),
        }
        for run in items
    ]


def list_dags() -> list[dict[str, Any]]:
    result = airflow_get("/api/v2/dags?limit=100")
    items = result.get("dags") or []

    return [
        {
            "dag_id": dag.get("dag_id"),
            "is_paused": dag.get("is_paused"),
            "is_active": dag.get("is_active"),
            "schedule_interval": dag.get("schedule_interval") or dag.get("timetable_summary"),
            "tags": [tag.get("name") for tag in (dag.get("tags") or [])],
            "last_parsed_time": dag.get("last_parsed_time"),
        }
        for dag in items
    ]


def latest_airflow_task(dag_id: str, task_id: str) -> dict[str, Any]:
    """
    Load latest matching Airflow task state.

    Missing DAG is treated as a catalog/onboarding state, not as platform failure.
    """
    try:
        quoted_dag = urllib.parse.quote(dag_id, safe="")
        data = airflow_get(f"/api/v2/dags/{quoted_dag}/dagRuns?order_by=-logical_date&limit=10")
        runs = data.get("dag_runs") or []

        for run in runs:
            dag_run_id = str(run.get("dag_run_id") or "")
            if not dag_run_id:
                continue

            quoted_run = urllib.parse.quote(dag_run_id, safe="")
            task_data = airflow_get(f"/api/v2/dags/{quoted_dag}/dagRuns/{quoted_run}/taskInstances")
            tasks = task_data.get("task_instances") or []
            task = next((item for item in tasks if str(item.get("task_id") or "") == task_id), None)

            if task:
                started_at = task.get("start_date") or run.get("start_date")
                ended_at = task.get("end_date") or run.get("end_date")
                return {
                    "airflow_available": True,
                    "current_status": normalize_state(task.get("state")),
                    "latest_run_id": dag_run_id,
                    "latest_dag_run_state": normalize_state(run.get("state")),
                    "started_at": started_at,
                    "ended_at": ended_at,
                    "duration_seconds": duration_seconds(started_at, ended_at),
                }

        return {
            "airflow_available": True,
            "current_status": "no_runs",
            "status_reason": "Airflow DAG exists, but no matching task run was found.",
        }

    except RuntimeError as exc:
        message = str(exc)
        if "[404]" in message:
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
            "status_reason": message,
        }

    except Exception as exc:
        return {
            "airflow_available": False,
            "current_status": "airflow_unavailable",
            "status_reason": str(exc),
        }


def dag_run_rows_for_job(dag_id: str, limit: int = 10) -> list[dict[str, Any]]:
    """Fallback run list used only when Control DB and JSONL registry have no matching rows."""
    quoted_dag = urllib.parse.quote(dag_id, safe="")
    data = airflow_get(f"/api/v2/dags/{quoted_dag}/dagRuns?order_by=-logical_date&limit={limit}")
    runs = data.get("dag_runs") or []
    result: list[dict[str, Any]] = []

    for run in runs:
        dag_run_id = str(run.get("dag_run_id") or "")
        started_at = run.get("start_date")
        ended_at = run.get("end_date")
        result.append(
            {
                "run_id": None,
                "dag_run_id": dag_run_id,
                "state": normalize_state(run.get("state")),
                "started_at": started_at,
                "ended_at": ended_at,
                "duration_seconds": duration_seconds(started_at, ended_at),
                "records_read": None,
                "records_written": None,
                "records_inserted": None,
                "records_updated": None,
                "records_deleted": None,
                "runtime_source": "airflow",
            }
        )

    return result
