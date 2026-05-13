from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from batch.specs.batch_catalog_utils import resolve_repo_path


def quote_identifier(identifier: str, db_type: str) -> str:
    if not identifier:
        raise ValueError("Identifier cannot be empty")

    if db_type == "sqlserver":
        return f"[{identifier}]"

    if db_type == "mysql":
        return f"`{identifier}`"

    return f'"{identifier}"'


def quote_table_name(table_name: str, db_type: str) -> str:
    parts = table_name.split(".")

    if db_type == "sqlserver":
        return ".".join(f"[{part}]" for part in parts)

    if db_type == "mysql":
        return ".".join(f"`{part}`" for part in parts)

    return ".".join(f'"{part}"' for part in parts)


def format_sql_literal(value: Any, value_type: str) -> str:
    if value is None:
        return "NULL"

    normalized_type = str(value_type).lower()

    if normalized_type in {
        "long",
        "int",
        "integer",
        "bigint",
        "float",
        "double",
        "decimal",
        "number",
    }:
        raw = str(value).strip()

        try:
            if normalized_type in {"float", "double", "decimal", "number"}:
                float(raw)
            else:
                int(float(raw))
        except ValueError as exc:
            raise ValueError(
                f"Invalid numeric SQL bound value='{value}' for value_type='{value_type}'. "
                "Check manifest watermark.initial_value or persisted ingestion state."
            ) from exc

        return raw

    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def load_sql_ref(sql_ref: str) -> str:
    path = resolve_repo_path(sql_ref)

    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")

    sql = path.read_text(encoding="utf-8").strip()

    if not sql:
        raise ValueError(f"SQL file is empty: {path}")

    return sql.rstrip(";").strip()


def get_sql_template(table_cfg: dict[str, Any], key: str) -> str | None:
    sql_cfg = table_cfg.get("sql") or {}

    inline_sql = sql_cfg.get(key)
    ref_sql = sql_cfg.get(f"{key}_ref")

    if inline_sql and ref_sql:
        raise ValueError(
            f"Table '{table_cfg['table_id']}' defines both sql.{key} and sql.{key}_ref"
        )

    if ref_sql:
        return load_sql_ref(ref_sql)

    if inline_sql:
        return str(inline_sql).rstrip(";").strip()

    return None


def build_columns_sql(table_cfg: dict[str, Any], db_type: str) -> str:
    columns = table_cfg.get("columns") or []

    if not columns:
        return "*"

    return ", ".join(quote_identifier(column, db_type) for column in columns)


def build_sql_context(
    *,
    runtime_connection: dict[str, Any],
    table_cfg: dict[str, Any],
    lower_bound: Any | None = None,
    upper_bound: Any | None = None,
) -> dict[str, Any]:
    db_type = runtime_connection["type"]

    watermark = table_cfg.get("watermark") or {}
    transactional = table_cfg.get("transactional") or {}

    watermark_column = watermark.get("column")
    watermark_type = watermark.get("type", "timestamp")

    transaction_column = transactional.get("column")
    transaction_type = transactional.get("type", "long")

    load_type = table_cfg.get("load_type", "full")

    bound_type = watermark_type if load_type == "incremental" else transaction_type

    return {
        "source_table": quote_table_name(table_cfg["source_table"], db_type),
        "raw_source_table": table_cfg["source_table"],
        "columns": build_columns_sql(table_cfg, db_type),
        "watermark_column": quote_identifier(watermark_column, db_type)
        if watermark_column
        else "",
        "raw_watermark_column": watermark_column or "",
        "transaction_column": quote_identifier(transaction_column, db_type)
        if transaction_column
        else "",
        "raw_transaction_column": transaction_column or "",
        "lower_bound": format_sql_literal(lower_bound, bound_type),
        "upper_bound": format_sql_literal(upper_bound, transaction_type),
    }


def render_sql_template(template: str, context: dict[str, Any]) -> str:
    try:
        return template.format(**context).rstrip(";").strip()
    except KeyError as exc:
        raise ValueError(f"Unknown SQL template placeholder: {exc}") from exc


def wrap_as_jdbc_subquery(sql: str) -> str:
    return f"({sql.rstrip(';').strip()}) AS src"


def build_default_select_sql(
    *,
    runtime_connection: dict[str, Any],
    table_cfg: dict[str, Any],
    lower_bound: Any | None = None,
    upper_bound: Any | None = None,
) -> str:
    load_type = table_cfg.get("load_type", "full")
    db_type = runtime_connection["type"]

    context = build_sql_context(
        runtime_connection=runtime_connection,
        table_cfg=table_cfg,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
    )

    base_sql = f"SELECT {context['columns']} FROM {context['source_table']}"

    if load_type == "full":
        return base_sql

    if load_type == "incremental":
        watermark = table_cfg.get("watermark") or {}
        watermark_column = watermark["column"]
        watermark_type = watermark.get("type", "timestamp")

        return (
            f"{base_sql} "
            f"WHERE {quote_identifier(watermark_column, db_type)} > "
            f"{format_sql_literal(lower_bound, watermark_type)}"
        )

    if load_type == "transactional":
        transactional = table_cfg.get("transactional") or {}
        tx_column = transactional["column"]
        tx_type = transactional.get("type", "long")

        quoted_tx = quote_identifier(tx_column, db_type)

        return (
            f"{base_sql} "
            f"WHERE {quoted_tx} > {format_sql_literal(lower_bound, tx_type)} "
            f"AND {quoted_tx} <= {format_sql_literal(upper_bound, tx_type)}"
        )

    raise ValueError(f"Unsupported load_type: {load_type}")


def build_source_sql(
    *,
    runtime_connection: dict[str, Any],
    table_cfg: dict[str, Any],
    lower_bound: Any | None = None,
    upper_bound: Any | None = None,
) -> str:
    load_type = table_cfg.get("load_type", "full")

    template = get_sql_template(table_cfg, load_type)

    if template:
        context = build_sql_context(
            runtime_connection=runtime_connection,
            table_cfg=table_cfg,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
        )
        return render_sql_template(template, context)

    return build_default_select_sql(
        runtime_connection=runtime_connection,
        table_cfg=table_cfg,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
    )


def build_max_value_sql(
    *,
    runtime_connection: dict[str, Any],
    table_cfg: dict[str, Any],
    column: str,
) -> str:
    template = get_sql_template(table_cfg, "max_transactional")

    if template:
        context = build_sql_context(
            runtime_connection=runtime_connection,
            table_cfg=table_cfg,
        )
        return render_sql_template(template, context)

    db_type = runtime_connection["type"]
    source_table = quote_table_name(table_cfg["source_table"], db_type)
    quoted_col = quote_identifier(column, db_type)

    return f"SELECT MAX({quoted_col}) AS max_value FROM {source_table}"


def read_max_value(
    spark: SparkSession,
    *,
    runtime_connection: dict[str, Any],
    table_cfg: dict[str, Any],
    column: str,
) -> Any:
    sql = build_max_value_sql(
        runtime_connection=runtime_connection,
        table_cfg=table_cfg,
        column=column,
    )

    df = (
        spark.read.format("jdbc")
        .option("url", runtime_connection["jdbc_url"])
        .option("driver", runtime_connection["driver"])
        .option("user", runtime_connection["user"])
        .option("password", runtime_connection["password"])
        .option("dbtable", wrap_as_jdbc_subquery(sql))
        .load()
    )

    rows = df.collect()
    if not rows:
        return None

    return rows[0]["max_value"]


def build_jdbc_reader(
    spark: SparkSession,
    runtime_connection: dict[str, Any],
    table_cfg: dict[str, Any],
    *,
    lower_bound: Any | None = None,
    upper_bound: Any | None = None,
) -> DataFrame:
    source_sql = build_source_sql(
        runtime_connection=runtime_connection,
        table_cfg=table_cfg,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
    )

    print(
        f"[JDBC_SOURCE_SQL] table_id={table_cfg['table_id']} sql={source_sql}",
        flush=True,
    )

    reader = (
        spark.read.format("jdbc")
        .option("url", runtime_connection["jdbc_url"])
        .option("driver", runtime_connection["driver"])
        .option("user", runtime_connection["user"])
        .option("password", runtime_connection["password"])
        .option("dbtable", wrap_as_jdbc_subquery(source_sql))
    )

    jdbc_options = {
        **runtime_connection.get("default_options", {}),
        "fetchsize": str(table_cfg.get("fetch_size", 10000)),
    }

    for key, value in jdbc_options.items():
        reader = reader.option(key, str(value))

    return reader.load()


def add_bronze_metadata(
    df: DataFrame,
    *,
    source_id: str,
    table_id: str,
    source_table: str,
    batch_run_id: str,
    load_type: str,
    add_row_hash: bool = True,
) -> DataFrame:
    enriched = (
        df.withColumn("_source_id", F.lit(source_id))
        .withColumn("_table_id", F.lit(table_id))
        .withColumn("_source_table", F.lit(source_table))
        .withColumn("_batch_run_id", F.lit(batch_run_id))
        .withColumn("_load_type", F.lit(load_type))
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
                    *[
                        F.coalesce(F.col(c).cast("string"), F.lit(""))
                        for c in business_cols
                    ],
                ),
                256,
            ),
        )

    return enriched


def write_bronze_table(df: DataFrame, table_cfg: dict[str, Any]) -> None:
    bronze_format = table_cfg.get("bronze_format", "delta")
    bronze_mode = table_cfg.get("bronze_mode", "append")

    writer = df.write.format(bronze_format).mode(bronze_mode)

    if bronze_format == "delta":
        if bronze_mode == "overwrite":
            writer = writer.option("overwriteSchema", "true")
        else:
            writer = writer.option("mergeSchema", "true")

    partition_by = table_cfg.get("partition_by", [])
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(table_cfg["target_path"])