from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Any, Iterator

from shared.runtime.job_event_writer import (
    append_job_event,
    exception_to_text,
    new_run_id,
)


def _env_int(name: str) -> int | None:
    value = os.getenv(name)
    if value in (None, ""):
        return None
    try:
        return int(value)
    except ValueError:
        return None

def _required_success_metric(context: dict[str, Any], key: str) -> int:
    value = context.get(key)

    if value is None or value == "":
        raise RuntimeError(
            f"Runtime contract violation: successful job has null metric `{key}`. "
            f"job_code={context.get('job_code')} run_id={context.get('run_id')}"
        )

    try:
        return int(value)
    except Exception as exc:
        raise RuntimeError(
            f"Runtime contract violation: metric `{key}` must be integer-like. "
            f"value={value!r} job_code={context.get('job_code')}"
        ) from exc


def _optional_failure_metric(context: dict[str, Any], key: str) -> int | None:
    value = context.get(key)

    if value is None or value == "":
        return None

    try:
        return int(value)
    except Exception:
        return None


def _finalize_success_metrics(context: dict[str, Any]) -> dict[str, int]:
    return {
        "records_read": _required_success_metric(context, "records_read"),
        "records_written": _required_success_metric(context, "records_written"),
        "records_inserted": _required_success_metric(context, "records_inserted"),
        "records_updated": _required_success_metric(context, "records_updated"),
        "records_deleted": _required_success_metric(context, "records_deleted"),
    }


def _finalize_failure_metrics(context: dict[str, Any]) -> dict[str, int | None]:
    return {
        "records_read": _optional_failure_metric(context, "records_read"),
        "records_written": _optional_failure_metric(context, "records_written"),
        "records_inserted": _optional_failure_metric(context, "records_inserted"),
        "records_updated": _optional_failure_metric(context, "records_updated"),
        "records_deleted": _optional_failure_metric(context, "records_deleted"),
    }


def build_runtime_context(
    *,
    default_pipeline_name: str,
    default_job_name: str,
    default_job_code: str,
    default_base_job_name: str | None = None,
    default_layer: str | None = None,
    default_runner: str | None = None,
    default_target_path: str | None = None,
    default_source_id: str | None = None,
    default_table_id: str | None = None,
    default_entity_name: str | None = None,
) -> dict[str, Any]:
    source_id = os.getenv("CONTROL_SOURCE_ID", default_source_id or "")
    table_id = os.getenv("CONTROL_TABLE_ID", default_table_id or "")
    entity_name = os.getenv(
        "CONTROL_ENTITY_NAME",
        default_entity_name or f"{source_id}.{table_id}".strip("."),
    )

    return {
        "run_id": os.getenv("CONTROL_RUN_ID") or new_run_id(),
        "job_id": _env_int("CONTROL_JOB_ID"),
        "job_key": os.getenv("CONTROL_JOB_KEY", default_job_code),
        "job_code": os.getenv("CONTROL_JOB_CODE", default_job_code),
        "pipeline_name": os.getenv("CONTROL_PIPELINE_NAME", default_pipeline_name),
        "job_name": os.getenv("CONTROL_JOB_NAME", default_job_name),
        "base_job_name": os.getenv("CONTROL_BASE_JOB_NAME", default_base_job_name or default_job_name),
        "source_id": source_id or None,
        "table_id": table_id or None,
        "entity_name": entity_name or None,
        "layer": os.getenv("CONTROL_LAYER", default_layer or ""),
        "runner": os.getenv("CONTROL_RUNNER", default_runner or default_job_name),
        "target_path": os.getenv("CONTROL_TARGET_PATH", default_target_path or ""),
        "airflow_dag_id": os.getenv("AIRFLOW_DAG_ID"),
        "airflow_dag_run_id": os.getenv("AIRFLOW_DAG_RUN_ID"),
        "airflow_task_id": os.getenv("AIRFLOW_TASK_ID"),
        "airflow_try_number": _env_int("AIRFLOW_TRY_NUMBER"),
        "effective_start_date": os.getenv("EFFECTIVE_START_DATE"),
        "effective_end_date": os.getenv("EFFECTIVE_END_DATE"),
        "records_read": None,
        "records_written": None,
        "records_inserted": None,
        "records_updated": None,
        "records_deleted": None,
    }


@contextmanager
def control_run(
    *,
    context: dict[str, Any],
    run_type: str = "batch",
) -> Iterator[dict[str, Any]]:
    started = time.time()

    append_job_event(
        event_type="batch_started",
        run_id=context["run_id"],
        type=run_type,
        job_id=context.get("job_id"),
        job_key=context.get("job_key"),
        job_code=context.get("job_code"),
        pipeline=context.get("pipeline_name"),
        pipeline_name=context.get("pipeline_name"),
        job=context.get("job_name"),
        job_name=context.get("job_name"),
        base_job_name=context.get("base_job_name"),
        source_id=context.get("source_id"),
        table_id=context.get("table_id"),
        entity_name=context.get("entity_name"),
        layer=context.get("layer"),
        runner=context.get("runner"),
        target_path=context.get("target_path"),
        airflow_dag_id=context.get("airflow_dag_id"),
        airflow_dag_run_id=context.get("airflow_dag_run_id"),
        airflow_task_id=context.get("airflow_task_id"),
        airflow_try_number=context.get("airflow_try_number"),
        status="running",
        started_at_epoch=int(started),
        effective_start_date=context.get("effective_start_date"),
        effective_end_date=context.get("effective_end_date"),
    )

    try:
        yield context

        ended = time.time()
        metrics=_finalize_success_metrics(context)
        append_job_event(
            event_type="batch_succeeded",
            run_id=context["run_id"],
            type=run_type,
            job_id=context.get("job_id"),
            job_key=context.get("job_key"),
            job_code=context.get("job_code"),
            pipeline=context.get("pipeline_name"),
            pipeline_name=context.get("pipeline_name"),
            job=context.get("job_name"),
            job_name=context.get("job_name"),
            base_job_name=context.get("base_job_name"),
            source_id=context.get("source_id"),
            table_id=context.get("table_id"),
            entity_name=context.get("entity_name"),
            layer=context.get("layer"),
            runner=context.get("runner"),
            target_path=context.get("target_path"),
            airflow_dag_id=context.get("airflow_dag_id"),
            airflow_dag_run_id=context.get("airflow_dag_run_id"),
            airflow_task_id=context.get("airflow_task_id"),
            airflow_try_number=context.get("airflow_try_number"),
            status="success",
            started_at_epoch=int(started),
            ended_at_epoch=int(ended),
            duration_seconds=round(ended - started, 3),
            records_read=metrics["records_read"],
            records_written=metrics["records_written"],
            records_inserted=metrics["records_inserted"],
            records_updated=metrics["records_updated"],
            records_deleted=metrics["records_deleted"],
            effective_start_date=context.get("effective_start_date"),
            effective_end_date=context.get("effective_end_date"),
        )

    except Exception as exc:
        ended = time.time()
        metrics = _finalize_failure_metrics(context)
        append_job_event(
            event_type="batch_failed",
            run_id=context["run_id"],
            type=run_type,
            job_id=context.get("job_id"),
            job_key=context.get("job_key"),
            job_code=context.get("job_code"),
            pipeline=context.get("pipeline_name"),
            pipeline_name=context.get("pipeline_name"),
            job=context.get("job_name"),
            job_name=context.get("job_name"),
            base_job_name=context.get("base_job_name"),
            source_id=context.get("source_id"),
            table_id=context.get("table_id"),
            entity_name=context.get("entity_name"),
            layer=context.get("layer"),
            runner=context.get("runner"),
            target_path=context.get("target_path"),
            airflow_dag_id=context.get("airflow_dag_id"),
            airflow_dag_run_id=context.get("airflow_dag_run_id"),
            airflow_task_id=context.get("airflow_task_id"),
            airflow_try_number=context.get("airflow_try_number"),
            status="failed",
            started_at_epoch=int(started),
            ended_at_epoch=int(ended),
            duration_seconds=round(ended - started, 3),
            error=str(exc),
            traceback=exception_to_text(exc),
            records_read=metrics["records_read"],
            records_written=metrics["records_written"],
            records_inserted=metrics["records_inserted"],
            records_updated=metrics["records_updated"],
            records_deleted=metrics["records_deleted"],
            effective_start_date=context.get("effective_start_date"),
            effective_end_date=context.get("effective_end_date"),
        )

        raise