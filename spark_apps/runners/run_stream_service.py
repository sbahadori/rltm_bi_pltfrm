from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

import yaml


def get_repo_root() -> Path:
    return Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()


def resolve_repo_path(path_str: str | Path) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (get_repo_root() / path).resolve()


def load_registry(path: str | Path) -> dict:
    registry_path = resolve_repo_path(path)
    if not registry_path.exists():
        raise FileNotFoundError(f"Stream registry not found: {registry_path}")

    with registry_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def find_stream(registry: dict, stream_name: str) -> dict:
    streams = registry.get("streams", [])
    for item in streams:
        if item.get("name") == stream_name:
            return item
    raise ValueError(f"Stream '{stream_name}' not found in registry")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", default="configs/streaming/stream_registry.yaml")
    parser.add_argument("--stream-name", required=True)
    args = parser.parse_args()

    repo_root = get_repo_root()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from shared.lib.spark_submit_utils import build_spark_submit_command, load_pipeline_config

    registry = load_registry(args.registry)
    stream_meta = find_stream(registry, args.stream_name)

    if not stream_meta.get("enabled", True):
        print(f"Stream '{args.stream_name}' is disabled. Exiting.")
        return

    pipeline_config = stream_meta.get("pipeline_config")
    if not pipeline_config:
        raise ValueError(f"Stream '{args.stream_name}' has no pipeline_config in registry")

    cfg = load_pipeline_config(pipeline_config)
    cmd = build_spark_submit_command(cfg)

    print(f"Executing stream '{args.stream_name}' command:")
    print(cmd)
    subprocess.run(cmd, shell=True, check=True)


if __name__ == "__main__":
    main()