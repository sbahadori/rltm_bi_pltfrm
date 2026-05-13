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

from batch.utils.jdbc_connection_registry import build_runtime_connection  # noqa: E402
from batch.utils.jdbc_ingestion_engine import (  # noqa: E402
    add_bronze_metadata,
    build_jdbc_reader,
    read_max_value,
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


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-ref", required=True)
    parser.add_argument("--table-id", required=False, default=None)
    return parser.parse_args()


def build_spark() -> SparkSession:
    return create_spark("generic_jdbc_manifest_to_bronze")


def ingest_one_table(
    *,
    spark: SparkSession,
    manifest: dict,
    runtime_connection: dict,
    table: dict,
    batch_run_id: str,
) -> int:
    table_cfg = build_effective_table_config(manifest, table)
    load_type = table_cfg.get("load_type", "full")

    print(
        f"[JDBC_READ_START] source_id={manifest['source_id']} "
        f"table_id={table_cfg['table_id']} source_table={table_cfg['source_table']} "
        f"load_type={load_type}",
        flush=True,
    )

    lower_bound = None
    upper_bound = None
    state_path = get_state_path(table_cfg, manifest["source_id"])

    if load_type == "full":
        table_cfg["bronze_mode"] = table_cfg.get("bronze_mode", "overwrite")

    elif load_type == "incremental":
        watermark = table_cfg.get("watermark") or {}
        watermark_column = watermark.get("column")

        if not watermark_column:
            raise ValueError(
                f"Table '{table_cfg['table_id']}' load_type='incremental' "
                f"requires watermark.column"
            )

        state_key = f"watermark::{watermark_column}"

        lower_bound = read_state(
            spark,
            state_path=state_path,
            source_id=manifest["source_id"],
            table_id=table_cfg["table_id"],
            state_key=state_key,
            default_value=watermark.get("initial_value", "1970-01-01T00:00:00"),
        )

        print(
            f"[JDBC_INCREMENTAL_STATE] table_id={table_cfg['table_id']} "
            f"watermark_column={watermark_column} "
            f"watermark_type={watermark.get('type', 'timestamp')} "
            f"lower_bound={lower_bound}",
            flush=True,
        )

        table_cfg["bronze_mode"] = table_cfg.get("bronze_mode", "append")

    elif load_type == "transactional":
        transactional = table_cfg.get("transactional") or {}
        tx_column = transactional.get("column")

        if not tx_column:
            raise ValueError(
                f"Table '{table_cfg['table_id']}' load_type='transactional' "
                f"requires transactional.column"
            )

        state_key = f"transactional::{tx_column}"

        lower_bound = read_state(
            spark,
            state_path=state_path,
            source_id=manifest["source_id"],
            table_id=table_cfg["table_id"],
            state_key=state_key,
            default_value=transactional.get("initial_value", 0),
        )

        upper_bound = read_max_value(
            spark,
            runtime_connection=runtime_connection,
            table_cfg=table_cfg,
            column=tx_column,
        )

        if upper_bound is None or str(upper_bound) == str(lower_bound):
            print(
                f"[JDBC_SKIP_NO_NEW_TX] table_id={table_cfg['table_id']} "
                f"last_value={lower_bound}",
                flush=True,
            )
            return 0

        table_cfg["bronze_mode"] = table_cfg.get("bronze_mode", "append")

    else:
        raise ValueError(
            f"Unsupported load_type='{load_type}' for table_id='{table_cfg['table_id']}'"
        )

    df = build_jdbc_reader(
        spark=spark,
        runtime_connection=runtime_connection,
        table_cfg=table_cfg,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
    )

    count_rows = bool(table_cfg.get("count_rows", False))

    if count_rows:
        rows_read = df.count()

        if rows_read == 0:
            print(
                f"[JDBC_SKIP_EMPTY] table_id={table_cfg['table_id']} load_type={load_type}",
                flush=True,
            )
            return 0
    else:
        rows_read = -1

    bronze_df = add_bronze_metadata(
        df,
        source_id=manifest["source_id"],
        table_id=table_cfg["table_id"],
        source_table=table_cfg["source_table"],
        batch_run_id=batch_run_id,
        load_type=load_type,
        add_row_hash=table_cfg.get("add_row_hash", True),
    )

    write_bronze_table(bronze_df, table_cfg)

    if load_type == "incremental":
        watermark = table_cfg["watermark"]
        watermark_column = watermark["column"]
        state_key = f"watermark::{watermark_column}"

        max_value = df.agg({watermark_column: "max"}).collect()[0][0]

        if max_value is not None:
            write_state(
                spark,
                state_path=state_path,
                source_id=manifest["source_id"],
                table_id=table_cfg["table_id"],
                state_key=state_key,
                state_value=max_value,
                batch_run_id=batch_run_id,
            )

    if load_type == "transactional":
        transactional = table_cfg["transactional"]
        tx_column = transactional["column"]
        state_key = f"transactional::{tx_column}"

        write_state(
            spark,
            state_path=state_path,
            source_id=manifest["source_id"],
            table_id=table_cfg["table_id"],
            state_key=state_key,
            state_value=upper_bound,
            batch_run_id=batch_run_id,
        )

    records_text = rows_read if rows_read >= 0 else "not_counted"

    print(
        f"[JDBC_BRONZE_WRITE_OK] table_id={table_cfg['table_id']} "
        f"load_type={load_type} rows={records_text} target={table_cfg['target_path']}",
        flush=True,
    )

    return rows_read

def main():
    args = parse_args()

    manifest = load_jdbc_manifest(args.manifest_ref)
    runtime_connection = build_runtime_connection(manifest)

    if args.table_id:
        tables = [get_table_by_id(manifest, args.table_id)]
    else:
        tables = get_enabled_tables(manifest)

    batch_run_id = new_run_id(f"{manifest['source_id']}__jdbc_manifest_to_bronze")
    started = time.time()

    append_job_event(
        event_type="batch_started",
        run_id=batch_run_id,
        type="batch",
        pipeline=manifest["source_id"],
        job="generic_jdbc_manifest_to_bronze",
        job_id=f"{manifest['source_id']}__jdbc_manifest_to_bronze",
        status="running",
    )

    spark = None
    total_rows = 0

    try:
        spark = build_spark()

        for table in tables:
            rows = ingest_one_table(
                spark=spark,
                manifest=manifest,
                runtime_connection=runtime_connection,
                table=table,
                batch_run_id=batch_run_id,
            )

            if rows >= 0:
                total_rows += rows

        ended = time.time()

        append_job_event(
            event_type="batch_succeeded",
            run_id=batch_run_id,
            type="batch",
            pipeline=manifest["source_id"],
            job="generic_jdbc_manifest_to_bronze",
            job_id=f"{manifest['source_id']}__jdbc_manifest_to_bronze",
            status="success",
            started_at_epoch=int(started),
            ended_at_epoch=int(ended),
            duration_seconds=round(ended - started, 3),
            records_written=total_rows,
        )

        print(
            f"[SUCCESS] source_id={manifest['source_id']} tables={len(tables)} rows={total_rows}",
            flush=True,
        )

    except Exception as exc:
        ended = time.time()

        append_job_event(
            event_type="batch_failed",
            run_id=batch_run_id,
            type="batch",
            pipeline=manifest["source_id"],
            job="generic_jdbc_manifest_to_bronze",
            job_id=f"{manifest['source_id']}__jdbc_manifest_to_bronze",
            status="failed",
            started_at_epoch=int(started),
            ended_at_epoch=int(ended),
            duration_seconds=round(ended - started, 3),
            error=str(exc),
            traceback=exception_to_text(exc),
        )

        raise

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()