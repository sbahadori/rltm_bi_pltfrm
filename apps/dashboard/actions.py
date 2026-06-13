from __future__ import annotations

"""
Backward-compatibility facade for legacy imports.

Do not add implementation logic here.

Source of truth:
- Airflow HTTP/auth/action logic: apps.dashboard.airflow_client
- Stream control/status logic: apps.dashboard.stream_runtime
"""

try:
    from apps.dashboard.airflow_client import (
        cancel_dag_run,
        get_dag_runs,
        list_dags,
        pause_dag,
        trigger_dag,
    )
    from apps.dashboard.stream_runtime import (
        get_stream_control_status,
        restart_stream,
        stop_stream,
        write_stream_control,
    )
except ImportError:  # pragma: no cover
    from .airflow_client import (
        cancel_dag_run,
        get_dag_runs,
        list_dags,
        pause_dag,
        trigger_dag,
    )
    from .stream_runtime import (
        get_stream_control_status,
        restart_stream,
        stop_stream,
        write_stream_control,
    )

__all__ = [
    "cancel_dag_run",
    "get_dag_runs",
    "list_dags",
    "pause_dag",
    "trigger_dag",
    "get_stream_control_status",
    "restart_stream",
    "stop_stream",
    "write_stream_control",
]