from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

from shared.core.spark import create_spark

REPO_ROOT = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from shared.control.watermark_store import (
    read_watermark_from_control_db,
    write_watermark_to_control_db,
)

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
from shared.runtime.job_run_registry import (  # noqa: E402
    append_job_event,
    exception_to_text,
    new_run_id,
)

from shared.control.quality_store import write_quality_result

from shared.control.lineage_store import write_dataset_lineage


def env_int(name: str) -> int | None:
    value = os.getenv(name)
    if value in (None, ""):
        return None
    try:
        return int(value)
    except ValueError:
        return None


def build_control_context(*, manifest: dict, table_id: str | None) -> dict:
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

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-ref", required=True)
    parser.add_argument("--table-id", required=False, default=None)
    return parser.parse_args()


def build_spark() -> SparkSession:
    return create_spark("generic_jdbc_manifest_to_bronze")


def strategy_uses_upper_bound(strategy: str) -> bool:
    return strategy in {"sequence", "rowversion"}


def build_state_key(*, load_type: str, strategy: str, column: str) -> str:
    return f"{load_type}::{strategy}::{column}"


def ingest_one_table(
    *,
    spark: SparkSession,
    manifest: dict,
    runtime_connection: dict,
    table: dict,
    batch_run_id: str,
    control: dict,
) -> int:
    table_cfg = build_effective_table_config(manifest, table)
    control_job_key = control["job_key"]
    control_job_id = control.get("job_id")
    load_type = table_cfg.get("load_type", "full")
    strategy = table_cfg.get("strategy", "overwrite")

    print(
        f"[JDBC_READ_START] source_id={manifest['source_id']} "
        f"table_id={table_cfg['table_id']} "
        f"source_table={table_cfg['source_table']} "
        f"load_type={load_type} strategy={strategy}",
        flush=True,
    )

    lower_bound = None
    upper_bound = None
    state_path = get_state_path(table_cfg, manifest["source_id"])
    state_key = None

    if load_type == "full":
        if strategy == "overwrite":
            table_cfg["bronze_mode"] = table_cfg.get("bronze_mode", "overwrite")
        elif strategy == "append_snapshot":
            table_cfg["bronze_mode"] = table_cfg.get("bronze_mode", "append")
        else:
            raise ValueError(
                f"Unsupported full strategy='{strategy}' "
                f"for table_id='{table_cfg['table_id']}'"
            )

    elif load_type == "incremental":
        watermark = table_cfg.get("watermark") or {}
        watermark_column = watermark.get("column")

        if not watermark_column:
            raise ValueError(
                f"Table '{table_cfg['table_id']}' load_type='incremental' "
                f"requires watermark.column"
            )

        state_key = build_state_key(
            load_type=load_type,
            strategy=strategy,
            column=watermark_column,
        )

        lower_bound = read_state(
            spark,
            state_path=state_path,
            source_id=manifest["source_id"],
            table_id=table_cfg["table_id"],
            state_key=state_key,
            default_value=watermark.get("initial_value", 0),
        )

        lower_bound = read_watermark_from_control_db(
            job_key=control_job_key,
            source_id=manifest["source_id"],
            table_id=table_cfg["table_id"],
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
                    f"[JDBC_SKIP_NO_NEW_BOUND] table_id={table_cfg['table_id']} "
                    f"strategy={strategy} last_value={lower_bound}",
                    flush=True,
                )
                return 0

        print(
            f"[JDBC_INCREMENTAL_STATE] table_id={table_cfg['table_id']} "
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
            f"CDC is not implemented yet for JDBC runner. "
            f"table_id={table_cfg['table_id']} strategy={strategy}"
        )

    else:
        raise ValueError(
            f"Unsupported load_type='{load_type}' "
            f"for table_id='{table_cfg['table_id']}'"
        )

    df = build_jdbc_reader(
        spark=spark,
        runtime_connection=runtime_connection,
        table_cfg=table_cfg,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
    )

    count_rows = bool(table_cfg.get("count_rows", False))

    rows_read: int | None = None
    records_written: int | None = None
    records_inserted: int | None = None
    records_updated: int | None = None
    records_deleted: int | None = None

    if count_rows:
        rows_read = df.count()

        write_quality_result(
            job_id=control_job_id,
            job_key=control_job_key,
            run_id=batch_run_id,
            dataset_key=f"{manifest['source_id']}::{table_cfg['table_id']}::bronze",
            rule_name="row_count_positive",
            rule_type="row_count",
            status="passed" if rows_read > 0 else "warning",
            observed_value=rows_read,
            expected_value="> 0",
            details={
                "source_id": manifest["source_id"],
                "table_id": table_cfg["table_id"],
                "load_type": load_type,
                "strategy": strategy,
                "lower_bound": lower_bound,
                "upper_bound": upper_bound,
            },
        )

        if rows_read == 0:
            print(
                f"[JDBC_SKIP_EMPTY] table_id={table_cfg['table_id']} "
                f"load_type={load_type} strategy={strategy} "
                f"lower_bound={lower_bound} upper_bound={upper_bound}",
                flush=True,
            )

            return {
                "rows_read": 0,
                "records_written": 0,
                "records_inserted": 0,
                "records_updated": 0,
                "records_deleted": 0,
                "skipped_empty": True,
            }

    else:
        if load_type == "incremental":
            has_rows = df.limit(1).count() > 0

            if not has_rows:
                print(
                    f"[JDBC_SKIP_EMPTY_INCREMENTAL] "
                    f"table_id={table_cfg['table_id']} "
                    f"load_type={load_type} "
                    f"strategy={strategy} "
                    f"lower_bound={lower_bound} "
                    f"upper_bound={upper_bound}",
                    flush=True,
                )

                return {
                    "rows_read": 0,
                    "records_written": 0,
                    "records_inserted": 0,
                    "records_updated": 0,
                    "records_deleted": 0,
                    "skipped_empty": True,
                }
    
    bronze_df = add_bronze_metadata(
        df,
        source_id=manifest["source_id"],
        table_id=table_cfg["table_id"],
        source_table=table_cfg["source_table"],
        batch_run_id=batch_run_id,
        load_type=load_type,
        strategy=strategy,
        add_row_hash=table_cfg.get("add_row_hash", True),
    )

    write_bronze_table(bronze_df, table_cfg)
    if count_rows:
        records_written = rows_read
        records_inserted = rows_read
        records_updated = 0
        records_deleted = 0


    write_dataset_lineage(
        run_id=batch_run_id,
        job_id=control_job_id,
        job_key=control_job_key,
        source_dataset_key=f"{manifest['source_id']}::{table_cfg['source_table']}",
        target_dataset_key=f"{manifest['source_id']}::{table_cfg['table_id']}::bronze",
        transformation_type="jdbc_to_bronze",
        transformation_ref=table_cfg.get("sql", {}).get("extract_ref"),
        details={
            "target_path": table_cfg["target_path"],
            "load_type": load_type,
            "strategy": strategy,
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
                source_id=manifest["source_id"],
                table_id=table_cfg["table_id"],
                state_key=state_key,
                state_value=next_state_value,
                batch_run_id=batch_run_id,
            )

            write_watermark_to_control_db(
                job_id=control_job_id,
                job_key=control_job_key,
                source_id=manifest["source_id"],
                table_id=table_cfg["table_id"],
                watermark_column=watermark_column,
                value=next_state_value,
                run_id=batch_run_id,
            )


    records_text = rows_read if rows_read is not None else "not_counted"

    print(
        f"[JDBC_BRONZE_WRITE_OK] table_id={table_cfg['table_id']} "
        f"load_type={load_type} strategy={strategy} "
        f"rows={records_text} target={table_cfg['target_path']}",
        flush=True,
    )

    return {
        "rows_read": rows_read,
        "records_written": records_written,
        "records_inserted": records_inserted,
        "records_updated": records_updated,
        "records_deleted": records_deleted,
        "skipped_empty": False,
    }


def main():
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
    spark = None
    total_rows_read = 0
    total_records_written = 0
    total_records_inserted = 0
    total_records_updated = 0
    total_records_deleted = 0
    counted_any_table = False

    append_job_event(
        event_type="batch_started",
        run_id=batch_run_id,
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

            if metrics.get("rows_read") is not None:
                total_rows_read += int(metrics.get("rows_read") or 0)
                counted_any_table = True

            if metrics.get("records_written") is not None:
                total_records_written += int(metrics.get("records_written") or 0)

            if metrics.get("records_inserted") is not None:
                total_records_inserted += int(metrics.get("records_inserted") or 0)

            if metrics.get("records_updated") is not None:
                total_records_updated += int(metrics.get("records_updated") or 0)

            if metrics.get("records_deleted") is not None:
                total_records_deleted += int(metrics.get("records_deleted") or 0)

        ended = time.time()

        append_job_event(
            event_type="batch_succeeded",
            run_id=batch_run_id,
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

            status="success",
            started_at_epoch=int(started),
            ended_at_epoch=int(ended),
            duration_seconds=round(ended - started, 3),
            records_read=total_rows_read if counted_any_table else None,
            records_written=total_records_written if counted_any_table else None,
            records_inserted=total_records_inserted if counted_any_table else None,
            records_updated=total_records_updated if counted_any_table else None,
            records_deleted=total_records_deleted if counted_any_table else None,
            effective_start_date=control["effective_start_date"],
            effective_end_date=control["effective_end_date"],
        )

    except Exception as exc:
        ended = time.time()

        append_job_event(
            event_type="batch_failed",
            run_id=batch_run_id,
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

            status="failed",
            started_at_epoch=int(started),
            ended_at_epoch=int(ended),
            duration_seconds=round(ended - started, 3),
            error=str(exc),
            traceback=exception_to_text(exc),
            effective_start_date=control["effective_start_date"],
            effective_end_date=control["effective_end_date"],
        )

        raise

    finally:
        if spark is not None:
            spark.stop()
            
if __name__ == "__main__":
    main()