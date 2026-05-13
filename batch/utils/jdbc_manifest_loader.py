from __future__ import annotations

import json
from typing import Any

from batch.specs.batch_catalog_utils import resolve_repo_path


VALID_LOAD_TYPES = {"full", "incremental", "transactional"}


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

    missing = [k for k in required if k not in manifest]
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

    if load_type not in VALID_LOAD_TYPES:
        raise ValueError(
            f"JDBC manifest '{manifest_ref}' table '{table_id}' has invalid "
            f"load_type='{load_type}'. Valid values: {sorted(VALID_LOAD_TYPES)}"
        )

    if load_type == "incremental":
        watermark = effective.get("watermark") or {}
        if "column" not in watermark:
            raise ValueError(
                f"JDBC manifest '{manifest_ref}' table '{table_id}' "
                f"load_type='incremental' requires watermark.column"
            )

    if load_type == "transactional":
        transactional = effective.get("transactional") or {}
        if "column" not in transactional:
            raise ValueError(
                f"JDBC manifest '{manifest_ref}' table '{table_id}' "
                f"load_type='transactional' requires transactional.column"
            )


def get_enabled_tables(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    return [t for t in manifest["tables"] if t.get("enabled", True)]


def get_table_by_id(manifest: dict[str, Any], table_id: str) -> dict[str, Any]:
    for table in get_enabled_tables(manifest):
        if table["table_id"] == table_id:
            return table

    raise ValueError(
        f"Enabled table_id='{table_id}' not found in manifest source_id='{manifest['source_id']}'"
    )


def build_effective_table_config(
    manifest: dict[str, Any],
    table: dict[str, Any],
) -> dict[str, Any]:
    defaults = manifest.get("defaults", {})
    effective = {**defaults, **table}

    # Backward compatibility: older manifests may use read_mode.
    if "load_type" not in effective:
        effective["load_type"] = effective.get("read_mode", "full")

    effective.setdefault(
        "bronze_mode",
        "overwrite" if effective["load_type"] == "full" else "append",
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
                f"Table '{table['table_id']}' requires either target_path or target_path_template"
            )

        effective["target_path"] = target_template.format(
            source_id=manifest["source_id"],
            table_id=table["table_id"],
        )

    return effective