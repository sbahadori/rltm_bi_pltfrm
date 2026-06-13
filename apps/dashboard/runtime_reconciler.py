from __future__ import annotations

from typing import Any

try:
    from apps.dashboard.airflow_client import get_dag_run
    from apps.dashboard.runtime_event_writer import append_runtime_event
    from apps.dashboard.runtime_models import normalize_state, now_iso
    from apps.dashboard.runtime_resolver import read_registry
except ImportError:  # pragma: no cover
    from .airflow_client import get_dag_run
    from .runtime_event_writer import append_runtime_event
    from .runtime_models import normalize_state, now_iso
    from .runtime_resolver import read_registry


OPEN_STATES = {"submitted", "queued", "scheduled", "running", "restarting", "unknown"}
TERMINAL_STATES = {"success", "failed", "error", "skipped", "upstream_failed", "cancelled", "canceled"}


def platform_state_from_airflow(airflow_state: str | None) -> str:
    state = normalize_state(airflow_state)

    if state in {"success"}:
        return "success"

    if state in {"failed", "error", "upstream_failed"}:
        return "failed"

    if state in {"cancelled", "canceled"}:
        return "cancelled"

    if state in {"running"}:
        return "running"

    if state in {"queued", "scheduled", "deferred"}:
        return "queued"

    return state or "unknown"


def _run_key(row: dict[str, Any]) -> str | None:
    executor_type = str(row.get("executor_type") or "")
    executor_id = str(row.get("executor_id") or "")
    executor_run_id = str(row.get("executor_run_id") or "")

    if executor_type != "airflow":
        return None

    if not executor_id or not executor_run_id:
        return None

    return f"{executor_type}:{executor_id}:{executor_run_id}"


def _latest_by_executor_run(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}

    for row in rows:
        key = _run_key(row)
        if not key:
            continue

        current = latest.get(key)
        if not current or int(row.get("ts_epoch") or 0) >= int(current.get("ts_epoch") or 0):
            latest[key] = row

    return latest


def reconcile_runtime_once(limit: int = 2000) -> dict[str, Any]:
    """
    Reconcile submitted/running platform runs with their real Airflow DAG run state.

    This function is intentionally one-shot:
    - safe to call from an API endpoint
    - safe to call from a loop later
    - safe to run manually during development
    """
    rows = read_registry(limit=limit)
    latest = _latest_by_executor_run(rows)

    checked = 0
    updated = 0
    skipped = 0
    failed = 0
    events: list[dict[str, Any]] = []

    for row in latest.values():
        current_state = normalize_state(row.get("state") or row.get("status"))

        if current_state in TERMINAL_STATES:
            skipped += 1
            continue

        if current_state not in OPEN_STATES:
            skipped += 1
            continue

        executor_id = str(row.get("executor_id") or "")
        executor_run_id = str(row.get("executor_run_id") or "")

        checked += 1

        try:
            airflow_run = get_dag_run(executor_id, executor_run_id)
            new_state = platform_state_from_airflow(airflow_run.get("state"))

            if new_state == current_state:
                skipped += 1
                continue

            observed_at = now_iso()

            event = append_runtime_event(
                {
                    "event_type": "job_state_reconciled",

                    # Runtime state
                    "state": new_state,
                    "status": new_state,
                    "run_id": row.get("run_id") or executor_run_id,
                    "executor_run_id": executor_run_id,

                    # Canonical job identity copied from original submitted event
                    "job_id": row.get("job_id"),
                    "job": row.get("job"),
                    "job_name": row.get("job_name"),
                    "pipeline": row.get("pipeline"),
                    "pipeline_id": row.get("pipeline_id"),
                    "job_code": row.get("job_code"),
                    "runner_id": row.get("runner_id"),

                    # Executor identity
                    "executor_type": "airflow",
                    "executor_id": executor_id,
                    "executor_state": airflow_run.get("state"),

                    # Time
                    "observed_at": observed_at,
                    "started_at": airflow_run.get("started_at") or row.get("started_at"),
                    "ended_at": airflow_run.get("ended_at"),
                    "duration_seconds": airflow_run.get("duration_seconds"),

                    # Metrics placeholders
                    "records_read": row.get("records_read"),
                    "records_written": row.get("records_written"),
                    "records_inserted": row.get("records_inserted"),
                    "records_updated": row.get("records_updated"),
                    "records_deleted": row.get("records_deleted"),

                    # Source
                    "runtime_source": "runtime_reconciler",
                    "status_reason": (
                        f"Reconciled from Airflow dag_run state `{airflow_run.get('state')}`."
                    ),
                }
            )

            updated += 1
            events.append(event)

        except Exception as exc:
            failed += 1
            event = append_runtime_event(
                {
                    "event_type": "job_reconciliation_failed",
                    "state": current_state,
                    "status": current_state,
                    "run_id": row.get("run_id") or executor_run_id,
                    "executor_run_id": executor_run_id,
                    "job_id": row.get("job_id"),
                    "job": row.get("job"),
                    "job_name": row.get("job_name"),
                    "pipeline": row.get("pipeline"),
                    "pipeline_id": row.get("pipeline_id"),
                    "executor_type": "airflow",
                    "executor_id": executor_id,
                    "runtime_source": "runtime_reconciler",
                    "status_reason": str(exc),
                }
            )
            events.append(event)

    return {
        "ok": True,
        "checked": checked,
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
        "events": events,
    }