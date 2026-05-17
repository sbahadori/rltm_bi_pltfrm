from __future__ import annotations

import json
from typing import Any

from batch.specs.batch_catalog_utils import resolve_repo_path


VALID_LOAD_TYPES = {"full", "incremental", "cdc"}

VALID_STRATEGIES_BY_LOAD_TYPE = {
    "full": {"overwrite", "append_snapshot"},
    "incremental": {
        "timestamp",
        "numeric_watermark",
        "sequence",
        "rowversion",
    },
    "cdc": {
        "sqlserver_cdc",
        "sqlserver_change_tracking",
        "postgres_logical_replication",
        "mysql_binlog",
        "debezium",
    },
}

NUMERIC_STRATEGIES = {"numeric_watermark", "sequence", "rowversion"}
BOUNDED_INCREMENTAL_STRATEGIES = {"sequence", "rowversion"}


def load_jdbc_manifest(manifest_ref: str) -> dict[str, Any]:
    path = resolve_repo_path(manifest_ref)

    if not path.exists():
        raise FileNotFoundError(f"JDBC manifest not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        manifest = json.load(f)

    validate_jdbc_manifest(manifest, manifest_ref)
    return manifest


def validate_jdbc_manifest(manifest: dict[str, Any], manifest_ref: str) -> None:
    required = [
        "version",
        "source_id",
        "source_type",
        "connection_ref",
        "enabled",
        "defaults",
        "tables",
    ]

    missing = [key for key in required if key not in manifest]
    if missing:
        raise ValueError(f"JDBC manifest '{manifest_ref}' missing keys: {missing}")

    if manifest["source_type"] != "jdbc":
        raise ValueError(f"JDBC manifest '{manifest_ref}' must have source_type='jdbc'")

    if not isinstance(manifest["tables"], list) or not manifest["tables"]:
        raise ValueError(f"JDBC manifest '{manifest_ref}' must define non-empty tables")

    for table in manifest["tables"]:
        validate_jdbc_table_config(manifest, table, manifest_ref)


def validate_jdbc_table_config(
    manifest: dict[str, Any],
    table: dict[str, Any],
    manifest_ref: str,
) -> None:
    table_id = table.get("table_id", "<missing-table-id>")

    for required_key in ["table_id", "source_table"]:
        if required_key not in table:
            raise ValueError(
                f"JDBC manifest '{manifest_ref}' table '{table_id}' "
                f"missing required key: {required_key}"
            )

    effective = build_effective_table_config(manifest, table)

    load_type = effective["load_type"]
    strategy = effective["strategy"]

    if load_type not in VALID_LOAD_TYPES:
        raise ValueError(
            f"JDBC manifest '{manifest_ref}' table '{table_id}' has invalid "
            f"load_type='{load_type}'. Valid values: {sorted(VALID_LOAD_TYPES)}"
        )

    valid_strategies = VALID_STRATEGIES_BY_LOAD_TYPE[load_type]
    if strategy not in valid_strategies:
        raise ValueError(
            f"JDBC manifest '{manifest_ref}' table '{table_id}' has invalid "
            f"strategy='{strategy}' for load_type='{load_type}'. "
            f"Valid strategies: {sorted(valid_strategies)}"
        )

    if load_type == "incremental":
        watermark = effective.get("watermark") or {}
        if "column" not in watermark:
            raise ValueError(
                f"JDBC manifest '{manifest_ref}' table '{table_id}' "
                f"load_type='incremental' requires watermark.column"
            )

        if "type" not in watermark:
            raise ValueError(
                f"JDBC manifest '{manifest_ref}' table '{table_id}' "
                f"load_type='incremental' requires watermark.type"
            )

        if strategy in NUMERIC_STRATEGIES:
            value_type = str(watermark["type"]).lower()
            if value_type not in {
                "long",
                "int",
                "integer",
                "bigint",
                "float",
                "double",
                "decimal",
                "number",
            }:
                raise ValueError(
                    f"JDBC manifest '{manifest_ref}' table '{table_id}' "
                    f"strategy='{strategy}' requires numeric watermark.type, "
                    f"got '{watermark['type']}'"
                )

    if load_type == "cdc":
        cdc_cfg = effective.get("cdc") or {}
        if not cdc_cfg:
            raise ValueError(
                f"JDBC manifest '{manifest_ref}' table '{table_id}' "
                f"load_type='cdc' requires cdc configuration"
            )


def get_enabled_tables(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    return [table for table in manifest["tables"] if table.get("enabled", True)]


def get_table_by_id(manifest: dict[str, Any], table_id: str) -> dict[str, Any]:
    for table in get_enabled_tables(manifest):
        if table["table_id"] == table_id:
            return table

    raise ValueError(
        f"Enabled table_id='{table_id}' not found in manifest "
        f"source_id='{manifest['source_id']}'"
    )


def default_strategy_for_load_type(load_type: str) -> str:
    if load_type == "full":
        return "overwrite"

    if load_type == "incremental":
        return "timestamp"

    if load_type == "cdc":
        return "debezium"

    raise ValueError(f"Unsupported load_type: {load_type}")


def default_bronze_mode(load_type: str, strategy: str) -> str:
    if load_type == "full" and strategy == "overwrite":
        return "overwrite"

    return "append"


def build_effective_table_config(
    manifest: dict[str, Any],
    table: dict[str, Any],
) -> dict[str, Any]:
    defaults = manifest.get("defaults", {})
    effective = {**defaults, **table}

    # Backward compatibility with older manifests.
    if "load_type" not in effective:
        effective["load_type"] = effective.get("read_mode", "full")

    if "strategy" not in effective:
        effective["strategy"] = default_strategy_for_load_type(effective["load_type"])

    effective.setdefault(
        "bronze_mode",
        default_bronze_mode(effective["load_type"], effective["strategy"]),
    )

    effective.setdefault(
        "state_path_template",
        "s3a://lakehouse/_state/jdbc/{source_id}/{table_id}",
    )

    effective.setdefault("sql", {})

    target_template = effective.get("target_path_template")
    if "target_path" not in effective:
        if not target_template:
            raise ValueError(
                f"Table '{table['table_id']}' requires either target_path "
                f"or target_path_template"
            )

        effective["target_path"] = target_template.format(
            source_id=manifest["source_id"],
            table_id=table["table_id"],
        )

    return effective


def is_bounded_incremental_strategy(strategy: str) -> bool:
    return strategy in BOUNDED_INCREMENTAL_STRATEGIES