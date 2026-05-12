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
    write_bronze_table,
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

    print(
        f"[JDBC_READ_START] source_id={manifest['source_id']} "
        f"table_id={table_cfg['table_id']} source_table={table_cfg['source_table']}",
        flush=True,
    )

    df = build_jdbc_reader(
        spark=spark,
        runtime_connection=runtime_connection,
        table_cfg=table_cfg,
    )

    rows_read = df.count()

    bronze_df = add_bronze_metadata(
        df,
        source_id=manifest["source_id"],
        table_id=table_cfg["table_id"],
        source_table=table_cfg["source_table"],
        batch_run_id=batch_run_id,
        add_row_hash=table_cfg.get("add_row_hash", True),
    )

    write_bronze_table(bronze_df, table_cfg)

    print(
        f"[JDBC_BRONZE_WRITE_OK] table_id={table_cfg['table_id']} "
        f"rows={rows_read} target={table_cfg['target_path']}",
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
            total_rows += ingest_one_table(
                spark=spark,
                manifest=manifest,
                runtime_connection=runtime_connection,
                table=table,
                batch_run_id=batch_run_id,
            )

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