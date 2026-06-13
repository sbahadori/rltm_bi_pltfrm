from __future__ import annotations

"""
Small runtime primitives shared by resolver, stream runtime, Airflow adapter, and APIs.

This file intentionally contains pure helpers only:
- no DB calls
- no HTTP calls
- no FastAPI routes
"""

import json
from datetime import datetime, timezone
from typing import Any

TERMINAL_STATES = {
    "success",
    "failed",
    "error",
    "skipped",
    "upstream_failed",
    "cancelled",
    "canceled",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_int(value: Any) -> int | None:
    try:
        return None if value is None else int(value)
    except Exception:
        return None


def safe_float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except Exception:
        return None


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


def duration_seconds(start: Any, end: Any) -> float | None:
    start_dt = parse_dt(start)
    end_dt = parse_dt(end)
    if start_dt and end_dt:
        return max(0.0, (end_dt - start_dt).total_seconds())
    return None


def normalize_state(value: Any) -> str:
    return str(value or "unknown").strip().lower() or "unknown"


def first_present(row: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in row and row.get(key) is not None:
            return row.get(key)
    return default


def as_int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def epoch_to_iso(value: Any) -> str | None:
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat() if value else None
    except Exception:
        return None


def value_from_embedded_json(row: dict[str, Any], *keys: str) -> Any:
    """Scan dict/json-string columns for runtime metrics emitted inside event payloads."""
    for value in row.values():
        payload: dict[str, Any] | None = None

        if isinstance(value, dict):
            payload = value
        elif isinstance(value, str):
            text = value.strip()
            if text.startswith("{") and any(key in text for key in keys):
                try:
                    decoded = json.loads(text)
                    payload = decoded if isinstance(decoded, dict) else None
                except Exception:
                    payload = None

        if isinstance(payload, dict):
            for key in keys:
                if key in payload and payload.get(key) is not None:
                    return payload.get(key)

    return None


def metric_value(row: dict[str, Any], *keys: str, default: Any = None) -> Any:
    direct = first_present(row, *keys, default=None)
    if direct is not None:
        return direct

    embedded = value_from_embedded_json(row, *keys)
    if embedded is not None:
        return embedded

    return default


def state_rank(state: Any) -> int:
    normalized = normalize_state(state)
    if normalized in TERMINAL_STATES:
        return 3
    if normalized in {"running", "restarting"}:
        return 2
    if normalized in {"submitted", "queued", "scheduled"}:
        return 1
    return 0


def has_value(value: Any) -> bool:
    return value is not None and value != "" and value != "-"


def is_later(left: Any, right: Any) -> bool:
    left_dt = parse_dt(left)
    right_dt = parse_dt(right)
    if left_dt and right_dt:
        return left_dt > right_dt
    if left_dt and not right_dt:
        return True
    return False


def is_earlier(left: Any, right: Any) -> bool:
    left_dt = parse_dt(left)
    right_dt = parse_dt(right)
    if left_dt and right_dt:
        return left_dt < right_dt
    if left_dt and not right_dt:
        return True
    return False


def run_group_key(row: dict[str, Any]) -> str:
    run_id = str(row.get("run_id") or "").strip()
    if run_id and run_id != "-":
        return f"run:{run_id}"

    dag_run_id = str(row.get("dag_run_id") or "").strip()
    if dag_run_id:
        return f"dag:{dag_run_id}"

    return f"fallback:{row.get('started_at') or ''}:{row.get('state') or ''}"
