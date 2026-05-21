import base64
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import UUID
from pathlib import Path
from typing import Any
import psycopg2
import psycopg2.extras

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

JOB_RUN_REGISTRY_FILE = Path(
    os.getenv(
        "JOB_RUN_REGISTRY_FILE",
        "/workspace/rltm_bi_pltfrm/runtime/job_runs/job_runs.jsonl",
    )
)

# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------

def _latest_control_run_for_job(job: dict[str, Any]) -> dict[str, Any] | None:
    source_id = job.get("source_id")
    table_id = job.get("table_id")
    pipeline_name = job.get("pipeline")
    job_name = job.get("name")

    job_code = (
        job.get("job_code")
        or job.get("job_key")
        or (
            f"bronze.{source_id}.{table_id}"
            if source_id and table_id
            else None
        )
    )

    row = call_usp_one(
        "usp_get_latest_runtime_job_run",
        (
            job_code,
            source_id,
            table_id,
            pipeline_name,
            job_name,
        ),
    )

    return row



def control_db_config():
    return {
        "host": os.getenv("CONTROL_DB_HOST", "postgres-warehouse"),
        "port": int(os.getenv("CONTROL_DB_PORT", "5432")),
        "dbname": os.getenv("CONTROL_DB_NAME", os.getenv("POSTGRES_DB", "warehouse")),
        "user": os.getenv("CONTROL_DB_USER", os.getenv("POSTGRES_USER", "warehouse")),
        "password": os.getenv("CONTROL_DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "warehouse")),
        "sslmode": os.getenv("CONTROL_DB_SSLMODE", "disable"),
    }


def _json_safe(value):
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}

    if isinstance(value, list):
        return [_json_safe(v) for v in value]

    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, UUID):
        return str(value)

    if isinstance(value, Decimal):
        return float(value)

    return value


def _validate_usp_name(usp_name: str) -> str:
    """
    Only allow calls to explicitly named ctl.usp_* functions.

    This keeps the dashboard code USP-friendly while avoiding arbitrary SQL
    object-name injection through the helper.
    """
    if not usp_name.startswith("usp_"):
        raise ValueError(f"Stored function name must start with 'usp_': {usp_name}")

    if not all(ch.isalnum() or ch == "_" for ch in usp_name):
        raise ValueError(f"Invalid stored function name: {usp_name}")

    return usp_name


def call_usp_rows(
    usp_name: str,
    params: tuple[Any, ...] | None = None,
) -> list[dict[str, Any]]:
    """
    Read from a PostgreSQL function under ctl schema.

    Business SQL must live inside ctl.usp_* functions. The only SQL left here is
    the generic invocation wrapper:
        SELECT * FROM ctl.usp_name(...)
    """
    usp_name = _validate_usp_name(usp_name)
    params = params or tuple()
    placeholders = ", ".join(["%s"] * len(params))

    sql = f"SELECT * FROM ctl.{usp_name}({placeholders})"

    conn = psycopg2.connect(**control_db_config())
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = [dict(row) for row in cur.fetchall()]
            return _json_safe(rows)
    finally:
        conn.close()


def call_usp_one(
    usp_name: str,
    params: tuple[Any, ...] | None = None,
) -> dict[str, Any] | None:
    rows = call_usp_rows(usp_name, params)
    return rows[0] if rows else None

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

def _resolve_repo_path(path_str: str) -> Path:
    path = Path(path_str)

    if path.is_absolute():
        return path

    return (PIPELINE_REPO_ROOT / path).resolve()


def _load_manifest(manifest_ref: str) -> dict[str, Any]:
    manifest_path = _resolve_repo_path(manifest_ref)

    if not manifest_path.exists():
        return {}

    try:
        with manifest_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _enabled_manifest_tables(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        table
        for table in manifest.get("tables", [])
        if table.get("enabled", True)
    ]


def _manifest_table_target_path(
    manifest: dict[str, Any],
    table: dict[str, Any],
) -> str:
    if table.get("target_path"):
        return table["target_path"]

    defaults = manifest.get("defaults", {}) or {}
    template = defaults.get("target_path_template", "")

    if not template:
        return ""

    try:
        return template.format(
            source_id=manifest.get("source_id", ""),
            table_id=table.get("table_id", ""),
        )
    except Exception:
        return ""


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

def _epoch_to_iso(value: Any) -> str | None:
    try:
        if value is None:
            return None

        return datetime.fromtimestamp(
            int(value),
            tz=timezone.utc,
        ).isoformat()
    except Exception:
        return None


def _read_job_run_registry(limit: int = 2000) -> list[dict[str, Any]]:
    """
    Read runtime/job_runs/job_runs.jsonl and return normalized records.

    The registry is append-only JSONL. Invalid lines are skipped intentionally,
    because observability must not break the dashboard.
    """
    if not JOB_RUN_REGISTRY_FILE.exists():
        return []

    records: list[dict[str, Any]] = []

    try:
        with JOB_RUN_REGISTRY_FILE.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()

                if not line:
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                records.append(_normalize_registry_record(record))
    except Exception:
        return []

    records = sorted(
        records,
        key=lambda r: int(r.get("ts_epoch") or 0),
    )

    return records[-limit:]


def _normalize_registry_record(record: dict[str, Any]) -> dict[str, Any]:
    started_at = (
        record.get("started_at")
        or _epoch_to_iso(record.get("started_at_epoch"))
        or record.get("ts")
    )

    ended_at = (
        record.get("ended_at")
        or _epoch_to_iso(record.get("ended_at_epoch"))
    )

    return {
        **record,
        "state": record.get("status") or "unknown",
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": record.get("duration_seconds"),
        "source": "job_run_registry",
    }


def _registry_match_keys_for_job(job: dict[str, Any]) -> set[str]:
    """
    Build all possible identifiers that can refer to the same runtime job.

    Batch example:
      gold_price_pipeline__gold_price_ingest
      gold_price_ingest

    Stream example:
      clickstream_user_events__bronze
      clickstream_user_events_bronze
      bronze_clickstream_user_events
    """
    keys: set[str] = set()

    job_id = str(job.get("id") or "")
    job_name = str(job.get("name") or "")
    pipeline = str(job.get("pipeline") or "")
    job_type = str(job.get("type") or "")

    for value in [job_id, job_name]:
        if value:
            keys.add(value)

    if job_type == "stream":
        if job_id.endswith("__bronze"):
            keys.add(f"{pipeline}_bronze")
            keys.add(f"bronze_{pipeline}")

        if job_id.endswith("__silver"):
            keys.add(f"{pipeline}_silver")
            keys.add(f"silver_{pipeline}")

        runtime_unit = job.get("runtime_unit_name")
        if runtime_unit:
            keys.add(str(runtime_unit))

        for alias in job.get("runtime_aliases") or []:
            keys.add(str(alias))

    return keys


def _registry_records_for_job(
    job: dict[str, Any],
    limit: int = 20,
) -> list[dict[str, Any]]:
    keys = _registry_match_keys_for_job(job)

    matched: list[dict[str, Any]] = []

    for record in _read_job_run_registry():
        candidates = {
            str(record.get("job_id") or ""),
            str(record.get("job") or ""),
            str(record.get("unit_name") or ""),
            str(record.get("run_id") or ""),
        }

        if keys.intersection(candidates):
            matched.append(record)

    return matched[-limit:]


def _registry_runs_for_job(
    job: dict[str, Any],
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Convert registry events into run-level records.

    For batch:
      batch_started + batch_succeeded/batch_failed are merged by run_id.

    For stream:
      stream_started is returned as a running run.
      stream_exited updates the same run if run_id matches.
    """
    records = _registry_records_for_job(job, limit=500)

    by_run_id: dict[str, dict[str, Any]] = {}

    for record in records:
        run_id = str(record.get("run_id") or record.get("event_id") or "")

        if not run_id:
            continue

        existing = by_run_id.get(run_id, {})

        merged = {
            **existing,
            **record,
            "run_id": run_id,
            "job_id": record.get("job_id") or job.get("id"),
            "job_name": record.get("job") or job.get("name"),
            "pipeline": record.get("pipeline") or job.get("pipeline"),
            "type": record.get("type") or job.get("type"),
            "state": record.get("status") or existing.get("state") or "unknown",
            "source": "job_run_registry",
        }

        if not merged.get("started_at"):
            merged["started_at"] = (
                record.get("started_at")
                or _epoch_to_iso(record.get("started_at_epoch"))
                or record.get("ts")
            )

        if record.get("ended_at") or record.get("ended_at_epoch"):
            merged["ended_at"] = (
                record.get("ended_at")
                or _epoch_to_iso(record.get("ended_at_epoch"))
            )

        if record.get("duration_seconds") is not None:
            merged["duration_seconds"] = record.get("duration_seconds")

        by_run_id[run_id] = merged

    runs = sorted(
        by_run_id.values(),
        key=lambda r: int(r.get("ts_epoch") or r.get("started_at_epoch") or 0),
        reverse=True,
    )

    return runs[:limit]


def _latest_registry_run_for_job(job: dict[str, Any]) -> dict[str, Any] | None:
    runs = _registry_runs_for_job(job, limit=1)
    return runs[0] if runs else None

def _enrich_runtime_jobs(config_jobs: list[dict[str, Any]]) -> dict[str, Any]:
    stream_status = _load_stream_status(raw=False)

    enriched_jobs: list[dict[str, Any]] = []

    for job in config_jobs:
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
            "runtime_available": False,
        }

        latest_control = _latest_control_run_for_job(job)

        if latest_control:
            runtime_job.update(
                {
                    "current_status": latest_control.get("status", "unknown"),
                    "latest_run_state": latest_control.get("status"),
                    "latest_run_id": str(latest_control.get("run_id")),
                    "platform_run_id": str(latest_control.get("run_id")),
                    "airflow_dag_run_id": latest_control.get("airflow_dag_run_id"),
                    "airflow_task_id": latest_control.get("airflow_task_id"),
                    "started_at": latest_control.get("started_at"),
                    "ended_at": latest_control.get("ended_at"),
                    "duration_seconds": latest_control.get("duration_seconds"),
                    "records_written": latest_control.get("records_written"),
                    "target_path": latest_control.get("target_path") or job.get("target_path"),
                    "status_reason": latest_control.get("status_reason") or latest_control.get("error_message"),
                    "runtime_source": "control_db",
                    "runtime_available": True,
                }
            )
        else:
            latest_registry = _latest_registry_run_for_job(job)

            if latest_registry:
                runtime_job.update(
                    {
                        "current_status": latest_registry.get("state", "unknown"),
                        "latest_run_state": latest_registry.get("state"),
                        "latest_run_id": latest_registry.get("run_id"),
                        "started_at": latest_registry.get("started_at"),
                        "ended_at": latest_registry.get("ended_at"),
                        "duration_seconds": latest_registry.get("duration_seconds"),
                        "status_reason": latest_registry.get("error") or latest_registry.get("message"),
                        "runtime_source": "job_run_registry",
                        "runtime_available": True,
                    }
                )
            elif job.get("type") == "stream":
                runtime_job.update(
                    {
                        "current_status": "defined",
                        "latest_run_state": None,
                        "latest_run_id": None,
                        "runtime_source": "catalog",
                        "runtime_available": False,
                    }
                )
            else:
                latest_airflow = _latest_task_run_for_job(
                    dag_id=job.get("pipeline", ""),
                    task_id=job.get("name", ""),
                )


                runtime_job.update(
                    {
                        "current_status": latest_airflow.get("current_status", "no_runs"),
                        "latest_run_state": latest_airflow.get("latest_run_state"),
                        "latest_run_id": latest_airflow.get("latest_run_id"),
                        "latest_dag_run_state": latest_airflow.get("latest_dag_run_state"),
                        "started_at": latest_airflow.get("started_at"),
                        "ended_at": latest_airflow.get("ended_at"),
                        "duration_seconds": latest_airflow.get("duration_seconds"),
                        "try_number": latest_airflow.get("try_number"),
                        "status_reason": latest_airflow.get("status_reason"),
                        "runtime_source": "airflow",
                        "runtime_available": latest_airflow.get("airflow_available", False),
                    }
                )

# -------------------------------------------------------------
# Merge live stream supervisor state into stream jobs.
# This is what powers Runtime unit, heartbeat, batch progress,
# valid/invalid rows, and write status in the dashboard drawer.
# -------------------------------------------------------------
        if runtime_job.get("type") == "stream":
            db_unit = _stream_current_from_db(runtime_job)

            if db_unit:

                latest_metric = _latest_stream_batch_metric_from_db(
                    db_unit.get("unit_name")
                )

                last_batch_ts_iso = _stream_batch_time(latest_metric)

                last_execution_at = (
                    last_batch_ts_iso
                    or db_unit.get("heartbeat_ts")
                    or db_unit.get("updated_at")
)
                runtime_job.update(
                    {
                        "runtime_available": True,
                        "runtime_source": "stream_runtime_db",
                        "runtime_unit_name": db_unit.get("unit_name"),

                        "current_status": db_unit.get("computed_status")
                        or runtime_job.get("current_status"),
                        "latest_run_state": db_unit.get("computed_status")
                        or runtime_job.get("latest_run_state"),
                        "status_reason": db_unit.get("status_reason")
                        or runtime_job.get("status_reason"),

                        "pid": db_unit.get("pid"),
                        "returncode": db_unit.get("returncode"),
                        "retries": db_unit.get("retries"),
                        "max_retries": db_unit.get("max_retries"),

                        "heartbeat_status": db_unit.get("heartbeat_status"),
                        "heartbeat_age_seconds": db_unit.get("heartbeat_age_seconds"),

                        "last_batch_id": db_unit.get("last_batch_id"),
                        "last_input_rows": db_unit.get("last_input_rows"),
                        "last_batch_rows": db_unit.get("last_batch_rows"),

                        "last_valid_rows": db_unit.get("last_valid_rows"),
                        "last_invalid_rows": db_unit.get("last_invalid_rows"),

                        "last_written_rows": db_unit.get("last_written_rows"),
                        "last_written_valid_rows": db_unit.get("last_written_valid_rows"),
                        "last_written_invalid_rows": db_unit.get("last_written_invalid_rows"),

                        "last_write_ok": db_unit.get("last_write_ok"),
                        "last_message": db_unit.get("last_message"),
                        "last_error": db_unit.get("last_error"),

                        "stream_target_path": db_unit.get("target_path")
                        or runtime_job.get("target_path"),
                        "checkpoint_path": db_unit.get("checkpoint_path")
                        or runtime_job.get("checkpoint_path"),
                        "last_batch_ts_iso": last_batch_ts_iso,
                        "last_execution_at": last_execution_at,
                        "heartbeat_ts": db_unit.get("heartbeat_ts"),
                        "runtime_updated_at": db_unit.get("updated_at"),

                        "latest_run_id": (
                            f"{db_unit.get('unit_name')}::batch-{db_unit.get('last_batch_id')}"
                            if db_unit.get("last_batch_id") is not None
                            else db_unit.get("unit_name")
                        ),

                        "started_at": last_execution_at or runtime_job.get("started_at"),
                    }
                )

            else:
                unit = _find_stream_unit_for_job(runtime_job, stream_status)

                if unit:
                    heartbeat = unit.get("heartbeat") or {}
                    last_execution_at = (
                        heartbeat.get("last_batch_ts_iso")
                        or heartbeat.get("ts_iso")
                        or unit.get("last_start_ts_iso")
                    )
                    runtime_job.update(
                        {
                            "runtime_available": True,
                            "runtime_source": "stream_supervisor_file",
                            "runtime_unit_name": unit.get("unit_name"),

                            "current_status": unit.get("computed_status")
                            or runtime_job.get("current_status"),
                            "latest_run_state": unit.get("computed_status")
                            or runtime_job.get("latest_run_state"),
                            "status_reason": unit.get("status_reason")
                            or runtime_job.get("status_reason"),

                            "heartbeat_age_seconds": unit.get("heartbeat_age_seconds"),
                            "heartbeat_status": unit.get("heartbeat_status"),

                            "last_batch_id": heartbeat.get("last_batch_id"),
                            "last_batch_ts_epoch": heartbeat.get("last_batch_ts_epoch"),
                            "last_batch_ts_iso": heartbeat.get("last_batch_ts_iso"),
                            "last_input_rows": heartbeat.get("last_input_rows"),
                            "last_batch_rows": heartbeat.get("last_batch_rows"),

                            "last_valid_rows": heartbeat.get("last_valid_rows"),
                            "last_invalid_rows": heartbeat.get("last_invalid_rows"),

                            "last_written_rows": heartbeat.get("last_written_rows"),
                            "last_written_valid_rows": heartbeat.get("last_written_valid_rows"),
                            "last_written_invalid_rows": heartbeat.get("last_written_invalid_rows"),

                            "last_write_ok": heartbeat.get("last_write_ok"),
                            "last_message": heartbeat.get("last_message"),
                            "last_error": heartbeat.get("last_error"),

                            "stream_target_path": (
                                heartbeat.get("silver_path")
                                or heartbeat.get("bronze_path")
                                or runtime_job.get("target_path")
                            ),
                            "checkpoint_path": (
                                heartbeat.get("checkpoint_path")
                                or runtime_job.get("checkpoint_path")
                            ),
                            "last_execution_at": last_execution_at,
                            "started_at": last_execution_at or runtime_job.get("started_at"),
                            "heartbeat_ts": heartbeat.get("ts_iso"),
                            "latest_run_id": (
                                f"{unit.get('unit_name')}::batch-{heartbeat.get('last_batch_id')}"
                                if heartbeat.get("last_batch_id") is not None
                                else unit.get("unit_name")
                            ),
                        }
                    )
                    
        enriched_jobs.append(runtime_job)

    return {
        "jobs": enriched_jobs,
        "stream_status": stream_status,
        "loaded_at": now_iso(),
    }



def _stream_current_from_db(job: dict[str, Any]) -> dict[str, Any] | None:
    if job.get("type") != "stream":
        return None

    unit_name = _supervisor_unit_name_for_job(job)

    try:
        row = call_usp_one("usp_get_stream_unit_current", (unit_name,))
        return row
    except Exception:
        return None
    
def _stream_batch_metrics_from_db(
    unit_name: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    try:
        return call_usp_rows(
            "usp_list_stream_batch_metrics",
            (
                unit_name,
                limit,
            ),
        )
    except Exception:
        return []


def _latest_stream_batch_metric_from_db(
    unit_name: str,
) -> dict[str, Any] | None:
    rows = _stream_batch_metrics_from_db(unit_name, limit=1)
    return rows[0] if rows else None


def _stream_batch_time(metric: dict[str, Any] | None) -> Any:
    if not metric:
        return None

    return (
        metric.get("batch_ts")
        or metric.get("updated_at")
        or metric.get("created_at")
    )

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

            job_name = job.get("name", "")
            job_type = job.get("job_type", "")
            execution_strategy = job.get("execution_strategy", "")

            # -----------------------------------------------------------------
            # Manifest-driven JDBC ingestion:
            # one Airflow task per source table.
            # Do NOT expose the base manifest job as a runnable/loggable task.
            # -----------------------------------------------------------------
            if (
                job_type == "generic_jdbc_manifest_to_bronze"
                and execution_strategy == "one_task_per_table"
            ):
                manifest_ref = job.get("manifest_ref", "")
                manifest = _load_manifest(manifest_ref)
                source_id = manifest.get("source_id", "")

                for table in _enabled_manifest_tables(manifest):
                    table_id = table.get("table_id", "")

                    if not table_id:
                        continue

                    task_id = f"{job_name}__{table_id}"
                    target_path = _manifest_table_target_path(manifest, table)
                    job_code = f"bronze.{source_id}.{table_id}"
                    job_key = job_code
                    entity_name = f"{source_id}.{table_id}"

                    jobs.append(
                        {
                            "id": f"{pipeline_name}__{task_id}",
                            "name": task_id,
                            "pipeline": pipeline_name,
                            "type": "batch",
                            "job_type": job_type,
                            "runner": job_type,
                            "job_code": job_code,
                            "job_key": job_key,
                            "entity_name": entity_name,
                            "layer": "bronze",
                            "source_type": job.get("source_type", "jdbc"),
                            "source_url": table.get("source_table", ""),
                            "source_id": source_id,
                            "table_id": table_id,
                            "base_job_name": job_name,
                            "manifest_ref": manifest_ref,
                            "dependencies": job.get("dependencies", []),
                            "timeout_minutes": job.get("execution_timeout_minutes", 30),
                            "tags": job.get("tags", []) or pipeline_tags,
                            "target_path": target_path,
                            "schedule": schedule,
                            "defined": True,
                            "source": "catalog_manifest",
                        }
                    )

                continue

            # -----------------------------------------------------------------
            # Existing non-manifest batch jobs.
            # -----------------------------------------------------------------
            spec = job.get("spec", {}) or {}
            source = spec.get("source", {}) or {}

            jobs.append(
                {
                    "id": f"{pipeline_name}__{job_name}",
                    "name": job_name,
                    "pipeline": pipeline_name,
                    "type": "batch",
                    "job_type": job_type,
                    "runner": job_type,
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
                    "checkpoint_path": bronze.get("checkpoint_dir", ""),
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
                    "checkpoint_path": silver.get("checkpoint_dir", ""),
                    "heartbeat_file": silver.get("heartbeat_file", ""),
                    "defined": True,
                    "source": "stream_registry",
                }
            )

    return jobs

def _pipeline_batch_job_names(pipeline: dict[str, Any]) -> list[str]:
    names: list[str] = []

    for job in pipeline.get("jobs", []):
        if not job.get("enabled", True):
            continue

        job_name = job.get("name", "")
        job_type = job.get("job_type", "")
        execution_strategy = job.get("execution_strategy", "")

        if (
            job_type == "generic_jdbc_manifest_to_bronze"
            and execution_strategy == "one_task_per_table"
        ):
            manifest = _load_manifest(job.get("manifest_ref", ""))

            for table in _enabled_manifest_tables(manifest):
                table_id = table.get("table_id", "")

                if table_id:
                    names.append(f"{job_name}__{table_id}")

            continue

        names.append(job_name)

    return names

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
                "jobs": _pipeline_batch_job_names(pipeline),
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

def _load_config_bundle() -> dict[str, Any]:
    catalog = _load_json(BATCH_CATALOG_PATH) or {"pipelines": []}
    registry = _load_json(STREAM_REGISTRY_PATH) or {"streams": []}

    return {
        "catalog": catalog,
        "registry": registry,
        "pipelines": _build_pipelines(catalog, registry),
        "jobs": _build_batch_jobs(catalog) + _build_stream_jobs(registry),
        "meta": {
            "batch_catalog_path": str(BATCH_CATALOG_PATH),
            "stream_registry_path": str(STREAM_REGISTRY_PATH),
            "loaded_at": now_iso(),
        },
    }

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

def _control_batch_runs_for_job(
    job: dict[str, Any],
    limit: int,
) -> dict[str, Any]:
    job_code = job.get("job_code") or job.get("job_key")
    source_id = job.get("source_id")
    table_id = job.get("table_id")
    pipeline_name = job.get("pipeline")
    job_name = job.get("name")

    try:
        rows = call_usp_rows(
            "usp_list_batch_runs_for_job",
            (
                job_code,
                source_id,
                table_id,
                pipeline_name,
                job_name,
                limit,
            ),
        )

        return {
            "available": True,
            "source": "control_db",
            "runs": [
                {
                    "run_id": row.get("run_id"),
                    "display_run_id": row.get("display_run_id"),
                    "platform_run_id": row.get("run_id"),
                    "airflow_dag_run_id": row.get("airflow_dag_run_id"),
                    "task_id": row.get("airflow_task_id"),
                    "state": row.get("state"),
                    "started_at": row.get("started_at"),
                    "ended_at": row.get("ended_at"),
                    "duration_seconds": row.get("duration_seconds"),
                    "try_number": row.get("airflow_try_number"),
                    "records_read": row.get("records_read"),
                    "records_written": row.get("records_written"),
                    "records_inserted": row.get("records_inserted"),
                    "records_updated": row.get("records_updated"),
                    "records_deleted": row.get("records_deleted"),
                    "target_path": row.get("target_path"),
                    "status_reason": row.get("status_reason"),
                    "error_message": row.get("error_message"),
                    "source": "control_db",
                }
                for row in rows
            ],
        }

    except Exception as exc:
        return {
            "available": False,
            "source": "control_db",
            "error": str(exc),
            "runs": [],
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
) -> Path:
    candidates: list[Path] = []

    dag_dir = AIRFLOW_LOG_DIR / f"dag_id={dag_id}"

    if dag_run_id:
        run_dir = dag_dir / f"run_id={dag_run_id}"
        candidates.extend(run_dir.glob(f"task_id={task_id}/**/*.log"))

    if not candidates and dag_dir.exists():
        candidates.extend(dag_dir.glob(f"**/task_id={task_id}/**/*.log"))
        candidates.extend(dag_dir.glob(f"**/*{task_id}*.log"))

    if not candidates and AIRFLOW_LOG_DIR.exists():
        candidates.extend(
            AIRFLOW_LOG_DIR.glob(f"**/dag_id={dag_id}/**/task_id={task_id}/**/*.log")
        )
        candidates.extend(
            AIRFLOW_LOG_DIR.glob(f"**/*{dag_id}*{task_id}*.log")
        )

    candidates = [p for p in candidates if p.is_file()]

    if not candidates:
        raise FileNotFoundError(
            f"No Airflow log found for dag_id={dag_id}, task_id={task_id}"
        )

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
            "job_run_registry_file": _path_state(JOB_RUN_REGISTRY_FILE),
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
        payload = _control_batch_runs_for_job(job, limit=limit)

        # Fallback to Airflow only if Control DB has no run history.
        if not payload.get("runs"):
            payload = _batch_runs_for_job(
                dag_id=job.get("pipeline", ""),
                task_id=job.get("name", ""),
                limit=limit,
            )
            payload["source"] = "airflow_fallback"

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
        unit_name = _supervisor_unit_name_for_job(job)
        current = _stream_current_from_db(job)
        metrics = _stream_batch_metrics_from_db(unit_name, limit=limit)

        if metrics:
            return JSONResponse(
                {
                    "job_id": job.get("id"),
                    "job_name": job.get("name"),
                    "pipeline": job.get("pipeline"),
                    "type": "stream",
                    "available": True,
                    "source": "stream_batch_metric",
                    "runtime_unit_name": unit_name,
                    "current_state": current.get("computed_status") if current else None,
                    "heartbeat_age_seconds": current.get("heartbeat_age_seconds") if current else None,
                    "runs": [
                        {
                            "run_id": f"{unit_name}::batch-{m.get('batch_id')}",
                            "batch_id": m.get("batch_id"),
                            "state": "success" if m.get("write_ok") is True else "failed",
                            "started_at": m.get("batch_ts"),
                            "ended_at": m.get("batch_ts"),
                            "duration_seconds": None,
                            "try_number": None,
                            "input_rows": m.get("input_rows"),
                            "batch_rows": m.get("batch_rows"),
                            "valid_rows": m.get("valid_rows"),
                            "invalid_rows": m.get("invalid_rows"),
                            "written_rows": m.get("written_rows"),
                            "written_valid_rows": m.get("written_valid_rows"),
                            "written_invalid_rows": m.get("written_invalid_rows"),
                            "write_ok": m.get("write_ok"),
                            "message": m.get("message"),
                            "error_message": m.get("error_message"),
                            "target_path": m.get("target_path"),
                            "checkpoint_path": m.get("checkpoint_path"),
                            "source": "stream_batch_metric",
                        }
                        for m in metrics
                    ],
                }
            )

        # fallback: current supervisor state
        stream_status = _load_stream_status(raw=False)
        unit = _find_stream_unit_for_job(job, stream_status)

        return JSONResponse(
            {
                "job_id": job.get("id"),
                "job_name": job.get("name"),
                "pipeline": job.get("pipeline"),
                "type": "stream",
                "available": bool(unit),
                "source": "stream_supervisor_current_state",
                "runs": [
                    {
                        "run_id": unit.get("unit_name") if unit else None,
                        "state": unit.get("computed_status") if unit else "unknown",
                        "started_at": unit.get("last_start_ts_iso") if unit else None,
                        "ended_at": None,
                        "duration_seconds": None,
                        "try_number": None,
                        "status_reason": unit.get("status_reason") if unit else "No matching stream unit found",
                        "source": "stream_supervisor_current_state",
                    }
                ],
            }
        )

@app.get("/api/runtime/logs/{job_id}")
async def get_runtime_logs(
    job_id: str,
    kind: str = Query(default="batch", pattern="^(batch|stream)$"),
    pipeline: str | None = None,
    task: str | None = None,
    dag_run_id: str | None = None,
    lines: int = Query(default=500, ge=10, le=5000),
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

@app.get("/api/control/jobs")
def control_jobs():
    return {
        "items": call_usp_rows("usp_list_control_jobs")
    }



@app.get("/api/runtime/job-runs")
def runtime_job_runs(limit: int = 50):
    return {
        "items": call_usp_rows(
            "usp_list_runtime_job_runs",
            (limit,),
        )
    }



@app.get("/api/runtime/watermarks")
def runtime_watermarks():
    return {
        "items": call_usp_rows("usp_list_runtime_watermarks")
    }



@app.get("/api/quality/results")
def quality_results(limit: int = 100):
    return {
        "items": call_usp_rows(
            "usp_list_quality_results",
            (limit,),
        )
    }



@app.get("/api/lineage/datasets")
def dataset_lineage(limit: int = 100):
    return {
        "items": call_usp_rows(
            "usp_list_dataset_lineage",
            (limit,),
        )
    }