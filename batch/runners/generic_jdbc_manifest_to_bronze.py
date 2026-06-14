from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------
# Bootstrap repo path
# -----------------------------------------------------------------------------

REPO_ROOT = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from batch.utils.jdbc_connection_registry import build_runtime_connection  # noqa: E402
from batch.utils.jdbc_ingestion_engine import (  # noqa: E402
    add_bronze_metadata,
    build_jdbc_reader,
    read_max_bound_value,
    write_bronze_table,
)
from batch.utils.jdbc_load_state import (  # noqa: E402
    get_state_path,
    read_state,
    write_state,
)
from batch.utils.jdbc_manifest_loader import (  # noqa: E402
    build_effective_table_config,
    get_enabled_tables,
    get_table_by_id,
    load_jdbc_manifest,
)
from shared.control.lineage_store import write_dataset_lineage  # noqa: E402
from shared.control.quality_store import write_quality_result  # noqa: E402
from shared.control.watermark_store import (  # noqa: E402
    read_watermark_from_control_db,
    write_watermark_to_control_db,
)
from shared.core.spark import create_spark  # noqa: E402
from shared.runtime.job_event_writer import (  # noqa: E402
    append_job_event,
    exception_to_text,
    new_run_id,
)


# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------

def env_int(name: str) -> int | None:
    value = os.getenv(name)
    if value in (None, ""):
        return None

    try:
        return int(value)
    except ValueError:
        return None


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)

    if value in (None, ""):
        return default

    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _to_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default

    try:
        return int(value)
    except Exception:
        return default


def zero_metrics(*, target_path: str | None = None, skipped_empty: bool = True) -> dict[str, Any]:
    return {
        "rows_read": 0,
        "records_written": 0,
        "records_inserted": 0,
        "records_updated": 0,
        "records_deleted": 0,
        "target_path": target_path,
        "skipped_empty": skipped_empty,
    }


# -----------------------------------------------------------------------------
# CLI / Spark / Control context
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-ref", required=True)
    parser.add_argument("--table-id", required=False, default=None)
    return parser.parse_args()


def build_spark() -> SparkSession:
    return create_spark("generic_jdbc_manifest_to_bronze")


def build_control_context(*, manifest: dict[str, Any], table_id: str | None) -> dict[str, Any]:
    source_id = os.getenv("CONTROL_SOURCE_ID", manifest.get("source_id", "unknown_source"))
    table_id_value = os.getenv("CONTROL_TABLE_ID", table_id or "manifest")

    default_job_code = f"bronze.{source_id}.{table_id_value}"
    default_job_name = f"jdbc_manifest_to_bronze__{table_id_value}"

    return {
        "job_id": env_int("CONTROL_JOB_ID"),
        "job_key": os.getenv("CONTROL_JOB_KEY", default_job_code),
        "job_code": os.getenv("CONTROL_JOB_CODE", default_job_code),
        "job_name": os.getenv("CONTROL_JOB_NAME", default_job_name),
        "base_job_name": os.getenv("CONTROL_BASE_JOB_NAME", "generic_jdbc_manifest_to_bronze"),
        "pipeline_name": os.getenv("CONTROL_PIPELINE_NAME", f"{source_id}_bronze_pipeline"),
        "source_id": source_id,
        "table_id": table_id_value,
        "entity_name": os.getenv("CONTROL_ENTITY_NAME", f"{source_id}.{table_id_value}"),
        "layer": os.getenv("CONTROL_LAYER", "bronze"),
        "runner": os.getenv("CONTROL_RUNNER", "generic_jdbc_manifest_to_bronze"),
        "target_path": os.getenv("CONTROL_TARGET_PATH"),
        "airflow_dag_id": os.getenv("AIRFLOW_DAG_ID"),
        "airflow_dag_run_id": os.getenv("AIRFLOW_DAG_RUN_ID"),
        "airflow_task_id": os.getenv("AIRFLOW_TASK_ID"),
        "airflow_try_number": env_int("AIRFLOW_TRY_NUMBER"),
        "effective_start_date": os.getenv("EFFECTIVE_START_DATE"),
        "effective_end_date": os.getenv("EFFECTIVE_END_DATE"),
    }


# -----------------------------------------------------------------------------
# Incremental state helpers
# -----------------------------------------------------------------------------

def strategy_uses_upper_bound(strategy: str) -> bool:
    return strategy in {"sequence", "rowversion"}


def build_state_key(*, load_type: str, strategy: str, column: str) -> str:
    return f"{load_type}::{strategy}::{column}"


# -----------------------------------------------------------------------------
# Table ingestion
# -----------------------------------------------------------------------------

def ingest_one_table(
    *,
    spark: SparkSession,
    manifest: dict[str, Any],
    runtime_connection: dict[str, Any],
    table: dict[str, Any],
    batch_run_id: str,
    control: dict[str, Any],
) -> dict[str, Any]:
    table_cfg = build_effective_table_config(manifest, table)

    read_policy = table_cfg.get("read_policy") or {}
    write_policy = table_cfg.get("write_policy") or {}

    control_job_key = control["job_key"]
    control_job_id = control.get("job_id")

    source_id = manifest["source_id"]
    table_id = table_cfg["table_id"]
    target_path = table_cfg["target_path"]

    load_type = table_cfg.get("load_type", "full")
    strategy = table_cfg.get("strategy", "overwrite")

    print(
        f"load_type={load_type} strategy={strategy}"
        f" read_policy={read_policy} write_policy={write_policy}"
        f"[JDBC_READ_START] "
        f"source_id={source_id} "
        f"table_id={table_id} "
        f"source_table={table_cfg['source_table']} "
        f"load_type={load_type} "
        f"strategy={strategy}",
        flush=True,
    )

    lower_bound = None
    upper_bound = None
    state_path = get_state_path(table_cfg, source_id)
    state_key = None

    if load_type == "full":
        if strategy == "overwrite":
            table_cfg["bronze_mode"] = table_cfg.get("bronze_mode", "overwrite")
        elif strategy == "append_snapshot":
            table_cfg["bronze_mode"] = table_cfg.get("bronze_mode", "append")
        else:
            raise ValueError(
                f"Unsupported full strategy='{strategy}' for table_id='{table_id}'"
            )

    elif load_type == "incremental":
        watermark = table_cfg.get("watermark") or {}
        watermark_column = watermark.get("column")

        if not watermark_column:
            raise ValueError(
                f"Table '{table_id}' load_type='incremental' requires watermark.column"
            )

        state_key = build_state_key(
            load_type=load_type,
            strategy=strategy,
            column=watermark_column,
        )

        lower_bound = read_state(
            spark,
            state_path=state_path,
            source_id=source_id,
            table_id=table_id,
            state_key=state_key,
            default_value=watermark.get("initial_value", 0),
        )

        lower_bound = read_watermark_from_control_db(
            job_key=control_job_key,
            source_id=source_id,
            table_id=table_id,
            watermark_column=watermark_column,
            default_value=lower_bound,
        )

        if strategy_uses_upper_bound(strategy):
            upper_bound = read_max_bound_value(
                spark,
                runtime_connection=runtime_connection,
                table_cfg=table_cfg,
                column=watermark_column,
            )

            if upper_bound is None or str(upper_bound) == str(lower_bound):
                print(
                    f"[JDBC_SKIP_NO_NEW_BOUND] "
                    f"table_id={table_id} "
                    f"strategy={strategy} "
                    f"last_value={lower_bound}",
                    flush=True,
                )
                return zero_metrics(target_path=target_path, skipped_empty=True)

        print(
            f"[JDBC_INCREMENTAL_STATE] "
            f"table_id={table_id} "
            f"strategy={strategy} "
            f"watermark_column={watermark_column} "
            f"watermark_type={watermark.get('type')} "
            f"lower_bound={lower_bound} "
            f"upper_bound={upper_bound}",
            flush=True,
        )

        table_cfg["bronze_mode"] = table_cfg.get("bronze_mode", "append")

    elif load_type == "cdc":
        raise NotImplementedError(
            f"CDC is not implemented yet for JDBC runner. table_id={table_id}"
        )

    else:
        raise ValueError(
            f"Unsupported load_type='{load_type}' for table_id='{table_id}'"
        )

    df = build_jdbc_reader(
        spark=spark,
        runtime_connection=runtime_connection,
        table_cfg=table_cfg,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
    )

    # RLBP-030 rule:
    # Every successful job must emit non-null records_* counters.
    # Therefore this runner counts rows by default. For very large production
    # tables this can later be replaced by database-specific estimates or
    # writer-returned metrics, but the dashboard contract must not receive nulls.
    rows_read = int(df.count())

    write_quality_result(
        job_id=control_job_id,
        job_key=control_job_key,
        run_id=batch_run_id,
        dataset_key=f"{source_id}::{table_id}::bronze",
        rule_name="row_count_positive",
        rule_type="row_count",
        status="passed" if rows_read > 0 else "warning",
        observed_value=rows_read,
        expected_value="> 0",
        details={
            "source_id": source_id,
            "table_id": table_id,
            "load_type": load_type,
            "strategy": strategy,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
        },
    )

    if rows_read == 0:
        print(
            f"[JDBC_SKIP_EMPTY] "
            f"table_id={table_id} "
            f"load_type={load_type} "
            f"strategy={strategy} "
            f"lower_bound={lower_bound} "
            f"upper_bound={upper_bound}",
            flush=True,
        )
        return zero_metrics(target_path=target_path, skipped_empty=True)

    bronze_df = add_bronze_metadata(
        df,
        source_id=source_id,
        table_id=table_id,
        source_table=table_cfg["source_table"],
        batch_run_id=batch_run_id,
        load_type=load_type,
        strategy=strategy,
        add_row_hash=table_cfg.get("add_row_hash", True),
    )

    write_bronze_table(bronze_df, table_cfg)

    records_written = rows_read
    records_inserted = rows_read
    records_updated = 0
    records_deleted = 0

    write_dataset_lineage(
        run_id=batch_run_id,
        job_id=control_job_id,
        job_key=control_job_key,
        source_dataset_key=f"{source_id}::{table_cfg['source_table']}",
        target_dataset_key=f"{source_id}::{table_id}::bronze",
        transformation_type="jdbc_to_bronze",
        transformation_ref=table_cfg.get("sql", {}).get("extract_ref"),
        details={
            "target_path": target_path,
            "load_type": load_type,
            "strategy": strategy,
            "read_policy": read_policy,
            "write_policy": write_policy,
        },
    )

    if load_type == "incremental":
        watermark = table_cfg["watermark"]
        watermark_column = watermark["column"]

        if strategy_uses_upper_bound(strategy):
            next_state_value = upper_bound
        else:
            next_state_value = df.agg({watermark_column: "max"}).collect()[0][0]

        if next_state_value is not None:
            write_state(
                spark,
                state_path=state_path,
                source_id=source_id,
                table_id=table_id,
                state_key=state_key,
                state_value=next_state_value,
                batch_run_id=batch_run_id,
            )

            write_watermark_to_control_db(
                job_id=control_job_id,
                job_key=control_job_key,
                source_id=source_id,
                table_id=table_id,
                watermark_column=watermark_column,
                value=next_state_value,
                run_id=batch_run_id,
            )

    print(
        f"[JDBC_BRONZE_WRITE_OK] "
        f"table_id={table_id} "
        f"load_type={load_type} "
        f"strategy={strategy} "
        f"read={rows_read} "
        f"written={records_written} "
        f"inserted={records_inserted} "
        f"updated={records_updated} "
        f"deleted={records_deleted} "
        f"target={target_path}",
        flush=True,
    )

    return {
        "rows_read": rows_read,
        "records_written": records_written,
        "records_inserted": records_inserted,
        "records_updated": records_updated,
        "records_deleted": records_deleted,
        "target_path": target_path,
        "skipped_empty": False,
    }


# -----------------------------------------------------------------------------
# Event emitters
# -----------------------------------------------------------------------------

def append_started_event(*, run_id: str, started: float, control: dict[str, Any]) -> None:
    append_job_event(
        event_type="batch_started",
        run_id=run_id,
        type="batch",

        job_id=control.get("job_id"),
        job_key=control["job_key"],
        job_code=control["job_code"],
        pipeline=control["pipeline_name"],
        pipeline_name=control["pipeline_name"],
        job=control["job_name"],
        job_name=control["job_name"],
        base_job_name=control["base_job_name"],

        source_id=control["source_id"],
        table_id=control["table_id"],
        entity_name=control["entity_name"],
        layer=control["layer"],
        runner=control["runner"],
        target_path=control["target_path"],

        airflow_dag_id=control["airflow_dag_id"],
        airflow_dag_run_id=control["airflow_dag_run_id"],
        airflow_task_id=control["airflow_task_id"],
        airflow_try_number=control["airflow_try_number"],

        status="running",
        started_at_epoch=int(started),
        effective_start_date=control["effective_start_date"],
        effective_end_date=control["effective_end_date"],
    )


def append_terminal_event(
    *,
    event_type: str,
    run_id: str,
    started: float,
    ended: float,
    control: dict[str, Any],
    metrics: dict[str, Any],
    error: Exception | None = None,
) -> None:
    payload = {
        "event_type": event_type,
        "run_id": run_id,
        "type": "batch",

        "job_id": control.get("job_id"),
        "job_key": control["job_key"],
        "job_code": control["job_code"],
        "pipeline": control["pipeline_name"],
        "pipeline_name": control["pipeline_name"],
        "job": control["job_name"],
        "job_name": control["job_name"],
        "base_job_name": control["base_job_name"],

        "source_id": control["source_id"],
        "table_id": control["table_id"],
        "entity_name": control["entity_name"],
        "layer": control["layer"],
        "runner": control["runner"],
        "target_path": metrics.get("target_path") or control.get("target_path"),

        "airflow_dag_id": control["airflow_dag_id"],
        "airflow_dag_run_id": control["airflow_dag_run_id"],
        "airflow_task_id": control["airflow_task_id"],
        "airflow_try_number": control["airflow_try_number"],

        "status": "failed" if error else "success",
        "started_at_epoch": int(started),
        "ended_at_epoch": int(ended),
        "duration_seconds": round(ended - started, 3),

        "records_read": _to_int(metrics.get("records_read", metrics.get("rows_read")), 0),
        "records_written": _to_int(metrics.get("records_written"), 0),
        "records_inserted": _to_int(metrics.get("records_inserted"), 0),
        "records_updated": _to_int(metrics.get("records_updated"), 0),
        "records_deleted": _to_int(metrics.get("records_deleted"), 0),

        "effective_start_date": control["effective_start_date"],
        "effective_end_date": control["effective_end_date"],
    }

    if error is not None:
        payload["error"] = str(error)
        payload["traceback"] = exception_to_text(error)

    append_job_event(**payload)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    manifest = load_jdbc_manifest(args.manifest_ref)
    runtime_connection = build_runtime_connection(manifest)

    if args.table_id:
        tables = [get_table_by_id(manifest, args.table_id)]
    else:
        tables = get_enabled_tables(manifest)

    control = build_control_context(
        manifest=manifest,
        table_id=args.table_id,
    )

    batch_run_id = new_run_id()
    started = time.time()
    spark: SparkSession | None = None

    total_metrics = {
        "rows_read": 0,
        "records_written": 0,
        "records_inserted": 0,
        "records_updated": 0,
        "records_deleted": 0,
        "target_path": control.get("target_path"),
    }
    target_paths: list[str] = []

    append_started_event(
        run_id=batch_run_id,
        started=started,
        control=control,
    )

    try:
        spark = build_spark()

        for table in tables:
            metrics = ingest_one_table(
                spark=spark,
                manifest=manifest,
                runtime_connection=runtime_connection,
                table=table,
                batch_run_id=batch_run_id,
                control=control,
            )

            total_metrics["rows_read"] += _to_int(metrics.get("rows_read"), 0)
            total_metrics["records_written"] += _to_int(metrics.get("records_written"), 0)
            total_metrics["records_inserted"] += _to_int(metrics.get("records_inserted"), 0)
            total_metrics["records_updated"] += _to_int(metrics.get("records_updated"), 0)
            total_metrics["records_deleted"] += _to_int(metrics.get("records_deleted"), 0)

            if metrics.get("target_path"):
                target_paths.append(str(metrics["target_path"]))

        if target_paths:
            unique_targets = list(dict.fromkeys(target_paths))
            total_metrics["target_path"] = unique_targets[0] if len(unique_targets) == 1 else ",".join(unique_targets)

        ended = time.time()

        append_terminal_event(
            event_type="batch_succeeded",
            run_id=batch_run_id,
            started=started,
            ended=ended,
            control=control,
            metrics=total_metrics,
        )

    except Exception as exc:
        ended = time.time()

        append_terminal_event(
            event_type="batch_failed",
            run_id=batch_run_id,
            started=started,
            ended=ended,
            control=control,
            metrics=total_metrics,
            error=exc,
        )

        raise

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
