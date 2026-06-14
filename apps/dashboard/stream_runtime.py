from __future__ import annotations

"""
Stream runtime adapter.

Single responsibility:
- Read stream-supervisor state.
- Compute heartbeat/staleness status.
- Write stream control commands.
- Read stream unit current status from Control DB.

No FastAPI routes and no Airflow logic belongs here.
"""

import json
import time
from typing import Any

try:
    from apps.dashboard.config_loader import (
        STREAM_CONTROL_DIR,
        STREAM_HEARTBEAT_STALE_SECONDS,
        STREAM_STATUS_FILE,
        STREAM_STATUS_STALE_SECONDS,
    )
    from apps.dashboard.db import call_usp_one
    from apps.dashboard.runtime_models import as_int_or_none, first_present, normalize_state, now_iso, safe_int
except ImportError:  # pragma: no cover
    from .config_loader import (
        STREAM_CONTROL_DIR,
        STREAM_HEARTBEAT_STALE_SECONDS,
        STREAM_STATUS_FILE,
        STREAM_STATUS_STALE_SECONDS,
    )
    from .db import call_usp_one
    from .runtime_models import as_int_or_none, first_present, normalize_state, now_iso, safe_int


_LAST_GOOD_STATUS_DATA: dict[str, Any] | None = None
_LAST_GOOD_STATUS_READ_AT: str | None = None
_STATUS_FILE_ERROR: dict[str, str] | None = None


def unit_aliases(name: str) -> list[str]:
    aliases = [name]
    if name.endswith("_bronze"):
        aliases.append(f"bronze_{name[:-7]}")
    if name.endswith("_silver"):
        aliases.append(f"silver_{name[:-7]}")
    return list(dict.fromkeys(aliases))


def supervisor_unit_for_job(job: dict[str, Any]) -> str:
    pipeline = job.get("pipeline", "")
    job_id = job.get("id", "")
    if job_id.endswith("__bronze"):
        return f"{pipeline}_bronze"
    if job_id.endswith("__silver"):
        return f"{pipeline}_silver"
    return job.get("name", "")


def compute_unit_status(unit: dict[str, Any], now_epoch: int) -> dict[str, Any]:
    pid = unit.get("pid")
    return_code = unit.get("returncode")
    retries = safe_int(unit.get("retries")) or 0
    max_retries = safe_int(unit.get("max_retries")) or 0
    heartbeat = unit.get("heartbeat") or {}
    heartbeat_ts = safe_int(heartbeat.get("ts_epoch"))
    heartbeat_age = max(0, now_epoch - heartbeat_ts) if heartbeat_ts else None
    heartbeat_status = str(heartbeat.get("status", "")).lower()

    if return_code is not None:
        status = "failed" if (retries >= max_retries and max_retries > 0) else ("stopped" if return_code == 0 else "restarting")
        reason = f"returncode={return_code}"
    elif pid is not None:
        if heartbeat_status == "error":
            status, reason = "error", "heartbeat reports error"
        elif heartbeat_age and heartbeat_age > STREAM_HEARTBEAT_STALE_SECONDS:
            status, reason = "stale", f"heartbeat stale {heartbeat_age}s"
        else:
            status, reason = "running", "running"
    else:
        status, reason = "unknown", "no pid"

    return {
        "computed_status": status,
        "status_reason": reason,
        "heartbeat_age_seconds": heartbeat_age,
        "heartbeat_status": heartbeat_status or None,
    }


def _remember_status_success(data: dict[str, Any], observed_at: str) -> None:
    global _LAST_GOOD_STATUS_DATA, _LAST_GOOD_STATUS_READ_AT, _STATUS_FILE_ERROR

    _LAST_GOOD_STATUS_DATA = data
    _LAST_GOOD_STATUS_READ_AT = observed_at
    _STATUS_FILE_ERROR = None


def _remember_status_error(exc: Exception, observed_at: str) -> dict[str, str]:
    global _STATUS_FILE_ERROR

    reason = str(exc)
    if not _STATUS_FILE_ERROR or _STATUS_FILE_ERROR.get("reason") != reason:
        _STATUS_FILE_ERROR = {
            "reason": reason,
            "first_seen_at": observed_at,
        }

    _STATUS_FILE_ERROR["last_seen_at"] = observed_at
    return dict(_STATUS_FILE_ERROR)


def _build_stream_status(
    data: dict[str, Any],
    *,
    now_epoch: int,
    observed_at: str,
    last_successful_read_at: str | None,
    raw: bool,
) -> dict[str, Any]:
    supervisor_ts = safe_int(data.get("ts_epoch"))
    supervisor_age = max(0, now_epoch - supervisor_ts) if supervisor_ts else None
    supervisor_stale = supervisor_age is not None and supervisor_age > STREAM_STATUS_STALE_SECONDS

    units: dict[str, Any] = {}
    for unit_name, unit in (data.get("units", {}) or {}).items():
        if not isinstance(unit, dict):
            continue
        computed = compute_unit_status(unit, now_epoch)
        units[unit_name] = {
            **unit,
            **computed,
            "unit_name": unit_name,
            "aliases": unit_aliases(unit_name),
        }

    any_failed = any(unit.get("computed_status") in {"failed", "error"} for unit in units.values())
    any_running = any(unit.get("computed_status") == "running" for unit in units.values())
    overall = "stale" if supervisor_stale else ("degraded" if any_failed else ("running" if any_running else "not_running"))

    result = {
        "available": True,
        "status": overall,
        "supervisor_stale": supervisor_stale,
        "supervisor_age_seconds": supervisor_age,
        "units": units,
        "observed_at": observed_at,
        "last_successful_read_at": last_successful_read_at,
    }
    if raw:
        result["raw"] = data

    return result


def load_stream_status(raw: bool = False) -> dict[str, Any]:
    now_epoch = int(time.time())
    observed_at = now_iso()

    if not STREAM_STATUS_FILE.exists():
        return {
            "available": False,
            "status": "unavailable",
            "units": {},
            "observed_at": observed_at,
            "last_successful_read_at": _LAST_GOOD_STATUS_READ_AT,
        }

    try:
        data = json.loads(STREAM_STATUS_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("stream status file must contain a JSON object")
    except Exception as exc:
        error = _remember_status_error(exc, observed_at)

        if _LAST_GOOD_STATUS_DATA is None:
            return {
                "available": False,
                "status": "error",
                "reason": error["reason"],
                "units": {},
                "observed_at": observed_at,
                "last_successful_read_at": None,
                "status_file_error": True,
                "status_file_error_first_seen_at": error["first_seen_at"],
                "status_file_error_last_seen_at": error["last_seen_at"],
            }

        result = _build_stream_status(
            _LAST_GOOD_STATUS_DATA,
            now_epoch=now_epoch,
            observed_at=observed_at,
            last_successful_read_at=_LAST_GOOD_STATUS_READ_AT,
            raw=raw,
        )
        result.update(
            {
                "status": "error",
                "reason": error["reason"],
                "status_file_error": True,
                "status_file_error_first_seen_at": error["first_seen_at"],
                "status_file_error_last_seen_at": error["last_seen_at"],
                "using_cached_status": True,
            }
        )
        if raw:
            result["raw_is_cached"] = True
        return result

    _remember_status_success(data, observed_at)
    result = _build_stream_status(
        data,
        now_epoch=now_epoch,
        observed_at=observed_at,
        last_successful_read_at=observed_at,
        raw=raw,
    )
    result["status_file_error"] = False
    return result


def find_stream_unit(job: dict[str, Any], stream_status: dict[str, Any]) -> dict[str, Any] | None:
    units = stream_status.get("units", {}) or {}
    expected = supervisor_unit_for_job(job)
    job_name = job.get("name", "")

    for unit_name, unit in units.items():
        aliases = unit.get("aliases", []) if isinstance(unit, dict) else []
        if unit_name in (expected, job_name) or job_name in aliases or expected in aliases:
            return {**unit, "unit_name": unit_name}

    return None


def stream_current_from_db(job: dict[str, Any]) -> dict[str, Any] | None:
    if job.get("type") != "stream":
        return None

    unit = supervisor_unit_for_job(job)
    try:
        return call_usp_one("usp_get_stream_unit_current", (unit,))
    except Exception:
        return None


def stream_pseudo_runs_for_job(job: dict[str, Any], limit: int = 10) -> list[dict[str, Any]]:
    stream_status = load_stream_status()
    unit = find_stream_unit(job, stream_status)

    if not unit:
        db_unit = stream_current_from_db(job)
        if not db_unit:
            return []
        unit = db_unit

    heartbeat = unit.get("heartbeat") or unit
    state = normalize_state(unit.get("computed_status") or unit.get("status") or heartbeat.get("status") or "unknown")
    last_batch_id = first_present(heartbeat, "last_batch_id", "batch_id", default=None)
    run_id = first_present(heartbeat, "run_id", unit.get("run_id"), default=None)

    records_read = as_int_or_none(first_present(heartbeat, "records_read", "last_input_rows", "input_rows"))
    records_written = as_int_or_none(first_present(heartbeat, "records_written", "last_valid_rows", "valid_rows"))
    records_inserted = as_int_or_none(first_present(heartbeat, "records_inserted", "inserted_rows"))
    if records_inserted is None:
        records_inserted = records_written

    records_updated = as_int_or_none(first_present(heartbeat, "records_updated", "updated_rows", default=0))
    records_deleted = as_int_or_none(first_present(heartbeat, "records_deleted", "deleted_rows", default=0))

    return [
        {
            "run_id": str(run_id or last_batch_id or unit.get("unit_name") or job.get("name")),
            "dag_run_id": None,
            "state": state,
            "started_at": first_present(heartbeat, "started_at", "start_time"),
            "ended_at": first_present(heartbeat, "observed_at", "updated_at"),
            "duration_seconds": None,
            "records_read": records_read,
            "records_written": records_written,
            "records_inserted": records_inserted,
            "records_updated": records_updated,
            "records_deleted": records_deleted,
            "target_path": first_present(heartbeat, "target_path", "bronze_path", "silver_path", default=job.get("target_path")),
            "status_reason": first_present(heartbeat, "last_error", "status_reason"),
            "runtime_source": "stream_runtime",
        }
    ][:limit]


def resolve_stream_log_name(name: str) -> str:
    if name.endswith(".log"):
        name = name[:-4]
    if name.endswith("_bronze") or name.endswith("_silver"):
        return name
    if name.startswith("bronze_"):
        return f"{name[7:]}_bronze"
    if name.startswith("silver_"):
        return f"{name[7:]}_silver"
    return name


def write_stream_control(unit_name: str, action: str, extra: dict[str, Any] | None = None) -> None:
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
    write_stream_control(unit_name, "restart")
    return {"unit_name": unit_name, "action": "restart", "status": "requested"}


def stop_stream(unit_name: str) -> dict[str, Any]:
    write_stream_control(unit_name, "stop")
    return {"unit_name": unit_name, "action": "stop", "status": "requested"}


def get_stream_control_status(unit_name: str) -> dict[str, Any] | None:
    control_file = STREAM_CONTROL_DIR / f"{unit_name}.json"
    if not control_file.exists():
        return None
    try:
        return json.loads(control_file.read_text(encoding="utf-8"))
    except Exception:
        return None
