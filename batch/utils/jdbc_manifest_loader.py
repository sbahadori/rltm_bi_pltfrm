from __future__ import annotations

import json
from typing import Any

from batch.specs.batch_catalog_utils import resolve_repo_path


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

    target_template = effective.get("target_path_template")
    if "target_path" not in effective:
        effective["target_path"] = target_template.format(
            source_id=manifest["source_id"],
            table_id=table["table_id"],
        )

    return effective