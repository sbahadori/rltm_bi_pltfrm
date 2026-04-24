from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


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