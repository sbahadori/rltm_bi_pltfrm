from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

ALLOWED_SOURCE_TYPES = {"kafka"}
ALLOWED_BRONZE_ENGINES = {"generic_kafka_to_bronze"}
ALLOWED_SILVER_ENGINES = {"generic_bronze_to_silver"}


def _require_non_empty_str(section: dict, key: str, section_name: str, stream_name: str) -> None:
    value = section.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Invalid '{section_name}.{key}' for stream '{stream_name}'")

def validate_registry(registry: dict[str, Any]) -> None:
    streams = registry.get("streams", [])
    names = [s.get("name") for s in streams]
    if len(names) != len(set(names)):
        raise ValueError("Duplicate stream names found in registry")

    for spec in streams:
        validate_stream_spec(spec)


def get_repo_root() -> Path:
    return Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()


def resolve_repo_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (get_repo_root() / path).resolve()


def load_stream_registry(registry_path: str | Path) -> dict[str, Any]:
    path = resolve_repo_path(str(registry_path))
    if not path.exists():
        raise FileNotFoundError(f"Stream registry not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    streams = data.get("streams")
    if not isinstance(streams, list):
        raise ValueError(f"Registry {path} must contain top-level 'streams' list")
    validate_registry(data)
    return data


def get_stream_spec(registry_path: str | Path, stream_name: str) -> dict[str, Any]:
    registry = load_stream_registry(registry_path)
    for spec in registry["streams"]:
        if spec.get("name") == stream_name:
            return spec
    raise ValueError(f"Stream '{stream_name}' not found in registry {registry_path}")


def validate_stream_spec(spec: dict[str, Any]) -> None:
    required_top = ["name", "enabled", "source", "bronze", "silver", "restart_policy"]
    missing_top = [k for k in required_top if k not in spec]
    if missing_top:
        raise ValueError(f"Missing top-level keys for stream '{spec.get('name', '?')}': {missing_top}")

    stream_name = spec["name"]

    source = spec["source"]
    bronze = spec["bronze"]
    silver = spec["silver"]

    if source.get("type") not in ALLOWED_SOURCE_TYPES:
        raise ValueError(f"Unsupported source.type for stream '{stream_name}': {source.get('type')}")

    if bronze.get("engine") not in ALLOWED_BRONZE_ENGINES:
        raise ValueError(f"Unsupported bronze.engine for stream '{stream_name}': {bronze.get('engine')}")

    if silver.get("engine") not in ALLOWED_SILVER_ENGINES:
        raise ValueError(f"Unsupported silver.engine for stream '{stream_name}': {silver.get('engine')}")

    for key in ["bootstrap_servers", "topic"]:
        _require_non_empty_str(source, key, "source", stream_name)

    for key in ["app_name", "path", "checkpoint_dir", "heartbeat_file"]:
        _require_non_empty_str(bronze, key, "bronze", stream_name)

    for key in ["app_name", "bronze_path", "path", "quarantine_path", "checkpoint_dir", "heartbeat_file"]:
        _require_non_empty_str(silver, key, "silver", stream_name)

    if not isinstance(silver.get("field_map", {}), dict):
        raise ValueError(f"silver.field_map must be a dict for stream '{stream_name}'")

    if not isinstance(silver.get("required_fields", []), list):
        raise ValueError(f"silver.required_fields must be a list for stream '{stream_name}'")

    if not isinstance(silver.get("identity_keys", []), list):
        raise ValueError(f"silver.identity_keys must be a list for stream '{stream_name}'")

    plugin = silver.get("transform_plugin")
    if plugin is not None:
        if not isinstance(plugin, str) or ":" not in plugin:
            raise ValueError(f"silver.transform_plugin must be 'module:function' for stream '{stream_name}'")    missing_top = [k for k in required_top if k not in spec]
    if missing_top:
        raise ValueError(f"Missing top-level keys for stream '{spec.get('name', '?')}': {missing_top}")

    source_required = ["type", "bootstrap_servers", "topic"]
    bronze_required = ["engine", "app_name", "path", "checkpoint_dir", "heartbeat_file"]
    silver_required = ["engine", "app_name", "bronze_path", "path", "quarantine_path", "checkpoint_dir", "heartbeat_file"]

    for section_name, required_keys in {
        "source": source_required,
        "bronze": bronze_required,
        "silver": silver_required,
    }.items():
        section = spec.get(section_name, {})
        missing = [k for k in required_keys if k not in section]
        if missing:
            raise ValueError(f"Missing keys in section '{section_name}' for stream '{spec['name']}': {missing}")