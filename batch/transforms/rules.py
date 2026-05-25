import argparse
import os
import sys
from pathlib import Path
from typing import Any
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import Column
from pyspark.sql.functions import col, expr, lit, row_number


def apply_select_map(df: DataFrame, select_map: dict[str, str]) -> DataFrame:
    if not select_map:
        return df

    cols = [
        col(source_name).alias(target_name)
        for target_name, source_name in select_map.items()
    ]

    return df.select(*cols)


def apply_filters(df: DataFrame, filters_spec: list[dict[str, Any]]) -> DataFrame:
    result = df
    for rule in filters_spec or []:
        rule_type = rule["type"]
        field = rule["field"]

        if rule_type == "equals":
            result = result.filter(col(field) == lit(rule["value"]))
        elif rule_type == "not_equals":
            result = result.filter(col(field) != lit(rule["value"]))
        elif rule_type == "greater_than":
            result = result.filter(col(field) > lit(rule["value"]))
        elif rule_type == "greater_or_equal":
            result = result.filter(col(field) >= lit(rule["value"]))
        elif rule_type == "less_than":
            result = result.filter(col(field) < lit(rule["value"]))
        elif rule_type == "less_or_equal":
            result = result.filter(col(field) <= lit(rule["value"]))
        elif rule_type == "is_not_null":
            result = result.filter(col(field).isNotNull())
        else:
            raise ValueError(f"Unsupported filter type: {rule_type}")

    return result


def apply_derived_fields(df: DataFrame, derived_fields: dict[str, dict[str, Any]]) -> DataFrame:
    result = df
    for field_name, spec in (derived_fields or {}).items():
        kind = spec["kind"]

        if kind == "sql":
            result = result.withColumn(field_name, expr(spec["expr"]))
        else:
            raise ValueError(f"Unsupported derived field kind: {kind}")

    return result


def apply_quality_rules(df: DataFrame, quality_rules: list[dict[str, Any]]) -> DataFrame:
    result = df
    for rule in quality_rules or []:
        rule_type = rule["type"]
        field = rule["field"]

        if rule_type == "not_null":
            result = result.filter(col(field).isNotNull())
        elif rule_type == "greater_than":
            result = result.filter(col(field) > lit(rule["value"]))
        elif rule_type == "greater_or_equal":
            result = result.filter(col(field) >= lit(rule["value"]))
        elif rule_type == "equals":
            result = result.filter(col(field) == lit(rule["value"]))
        else:
            raise ValueError(f"Unsupported quality rule type: {rule_type}")

    return result

def _parse_order_item(item: Any) -> Column:
    """
    Parse catalog order specs such as:
      - "ingestion_ts desc"
      - "ingestion_ts asc"
      - {"column": "ingestion_ts", "direction": "desc"}
    """
    if isinstance(item, dict):
        column_name = str(item.get("column") or item.get("field") or "").strip()
        direction = str(item.get("direction") or item.get("order") or "asc").strip().lower()
    else:
        raw = str(item or "").strip()
        parts = raw.split()
        column_name = parts[0] if parts else ""
        direction = parts[1].lower() if len(parts) > 1 else "asc"

    if not column_name:
        raise ValueError(f"Invalid dedupe order item: {item!r}")

    if direction in {"desc", "descending"}:
        return col(column_name).desc_nulls_last()

    if direction in {"asc", "ascending"}:
        return col(column_name).asc_nulls_last()

    raise ValueError(f"Invalid dedupe order direction: {direction!r} in {item!r}")

def apply_dedupe(df: DataFrame, dedupe_spec: dict[str, Any]) -> DataFrame:
    if not dedupe_spec:
        return df

    key_columns = dedupe_spec.get("key_columns", [])
    order_by = dedupe_spec.get("order_by", [])

    if not key_columns:
        return df

    if not order_by:
        return df.dropDuplicates(key_columns)

    order_exprs = [_parse_order_item(item) for item in order_by]
    w = Window.partitionBy(*key_columns).orderBy(*order_exprs)

    return (
        df.withColumn("_rn", row_number().over(w))
          .filter(col("_rn") == 1)
          .drop("_rn")
    )