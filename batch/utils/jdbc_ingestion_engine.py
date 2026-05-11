from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def quote_sqlserver_table(table_name: str) -> str:
    parts = table_name.split(".")
    return ".".join(f"[{part}]" for part in parts)


def build_jdbc_reader(
    spark: SparkSession,
    runtime_connection: dict[str, Any],
    table_cfg: dict[str, Any],
) -> DataFrame:
    db_type = runtime_connection["type"]
    source_table = table_cfg["source_table"]

    if db_type == "sqlserver":
        dbtable = quote_sqlserver_table(source_table)
    else:
        dbtable = source_table

    reader = (
        spark.read.format("jdbc")
        .option("url", runtime_connection["jdbc_url"])
        .option("driver", runtime_connection["driver"])
        .option("user", runtime_connection["user"])
        .option("password", runtime_connection["password"])
        .option("dbtable", dbtable)
    )

    jdbc_options = {
        **runtime_connection.get("default_options", {}),
        "fetchsize": str(table_cfg.get("fetch_size", 10000)),
    }

    for key, value in jdbc_options.items():
        reader = reader.option(key, str(value))

    df = reader.load()

    columns = table_cfg.get("columns") or []
    if columns:
        df = df.select(*columns)

    return df


def add_bronze_metadata(
    df: DataFrame,
    *,
    source_id: str,
    table_id: str,
    source_table: str,
    batch_run_id: str,
    add_row_hash: bool = True,
) -> DataFrame:
    enriched = (
        df.withColumn("_source_id", F.lit(source_id))
        .withColumn("_table_id", F.lit(table_id))
        .withColumn("_source_table", F.lit(source_table))
        .withColumn("_batch_run_id", F.lit(batch_run_id))
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("ingest_year", F.year("_ingestion_ts"))
        .withColumn("ingest_month", F.month("_ingestion_ts"))
        .withColumn("ingest_day", F.dayofmonth("_ingestion_ts"))
    )

    if add_row_hash:
        business_cols = [c for c in df.columns]
        enriched = enriched.withColumn(
            "_row_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in business_cols],
                ),
                256,
            ),
        )

    return enriched


def write_bronze_table(df: DataFrame, table_cfg: dict[str, Any]) -> None:
    writer = df.write.format(table_cfg.get("bronze_format", "delta")).mode(
        table_cfg.get("bronze_mode", "append")
    )

    partition_by = table_cfg.get("partition_by", [])
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(table_cfg["target_path"])