from __future__ import annotations

"""
actions.py

Execution action facade for dashboard.

Responsibility:
    - Keep stream supervisor control commands here.
    - Re-export Airflow actions from airflow_client for backward compatibility.

Important:
    Airflow HTTP/auth logic must live only in airflow_client.py.
"""

import json
import os
import time
from pathlib import Path
from typing import Any

try:
    from airflow_client import (
        cancel_dag_run,
        get_dag_runs,
        list_dags,
        pause_dag,
        trigger_dag,
    )
except ImportError:
    from .airflow_client import (
        cancel_dag_run,
        get_dag_runs,
        list_dags,
        pause_dag,
        trigger_dag,
    )


# ─────────────────────────────────────────────────────────────
# Stream actions — file-based supervisor control
# ─────────────────────────────────────────────────────────────

STREAM_CONTROL_DIR = Path(
    os.getenv(
        "STREAM_CONTROL_DIR",
        "/runtime/spark_health/control",
    )
)

STREAM_STATUS_FILE = Path(
    os.getenv(
        "STREAM_STATUS_FILE",
        "/runtime/spark_health/stream_supervisor_status.json",
    )
)


def _write_stream_control(
    unit_name: str,
    action: str,
    extra: dict[str, Any] | None = None,
) -> None:
    """
    Write a control command for stream supervisor.

    The stream supervisor is expected to poll:
        /runtime/spark_health/control/{unit_name}.json

    This module does not execute Spark jobs directly.
    It only writes intent files for the supervisor.
    """
    if not unit_name or not unit_name.strip():
        raise ValueError("unit_name is required.")

    if action not in {"restart", "stop"}:
        raise ValueError(f"Unsupported stream action: {action}")

    STREAM_CONTROL_DIR.mkdir(parents=True, exist_ok=True)

    control_file = STREAM_CONTROL_DIR / f"{unit_name}.json"

    payload: dict[str, Any] = {
        "unit_name": unit_name,
        "action": action,
        "ts": time.time(),
        "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        **(extra or {}),
    }

    control_file.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def restart_stream(unit_name: str) -> dict[str, Any]:
    """
    Request restart for one stream unit.
    """
    _write_stream_control(unit_name, "restart")

    return {
        "unit_name": unit_name,
        "action": "restart",
        "status": "requested",
    }


def stop_stream(unit_name: str) -> dict[str, Any]:
    """
    Request stop for one stream unit.
    """
    _write_stream_control(unit_name, "stop")

    return {
        "unit_name": unit_name,
        "action": "stop",
        "status": "requested",
    }


def get_stream_control_status(unit_name: str) -> dict[str, Any] | None:
    """
    Return the latest stream control command for a unit, if present.
    """
    if not unit_name or not unit_name.strip():
        return None

    control_file = STREAM_CONTROL_DIR / f"{unit_name}.json"

    if not control_file.exists():
        return None

    try:
        return json.loads(control_file.read_text(encoding="utf-8"))
    except Exception:
        return None