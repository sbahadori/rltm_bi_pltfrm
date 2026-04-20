from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml


DEFAULT_SPARK_PACKAGES = [
    "io.delta:delta-spark_2.12:3.2.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
]

DEFAULT_SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.ui.showConsoleProgress": "false",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to pipeline YAML")
    return parser.parse_args()


def get_repo_root() -> Path:
    return Path(os.getenv("PIPELINE_REPO_ROOT", Path(__file__).resolve().parents[2])).resolve()


def resolve_repo_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (get_repo_root() / path).resolve()


def load_config(config_path: str) -> dict[str, Any]:
    path = resolve_repo_path(config_path)
    with path.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    required = ["name", "domain", "job_type", "entrypoint", "args", "dependencies", "schedule"]
    missing = [key for key in required if key not in config]
    if missing:
        raise ValueError(f"Missing required keys in {path}: {missing}")

    return config


def build_cli_args(args: dict[str, Any]) -> list[str]:
    cli_args: list[str] = []

    for key, value in (args or {}).items():
        flag = f"--{key.replace('_', '-')}"

        if value is None:
            continue

        if isinstance(value, bool):
            if value:
                cli_args.append(flag)
            continue

        if isinstance(value, list):
            for item in value:
                cli_args.extend([flag, str(item)])
            continue

        cli_args.extend([flag, str(value)])

    return cli_args


def build_command(config: dict[str, Any]) -> list[str]:
    entrypoint = resolve_repo_path(config["entrypoint"])
    job_type = config.get("job_type", "spark_batch")

    if job_type.startswith("spark"):
        spark_submit = os.getenv("SPARK_SUBMIT", "/opt/spark/bin/spark-submit")
        spark_master = config.get("spark_master") or os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
        packages = config.get("packages", DEFAULT_SPARK_PACKAGES)
        spark_conf = {**DEFAULT_SPARK_CONF, **config.get("spark_conf", {})}

        cmd = [spark_submit, "--master", spark_master]
        if packages:
            cmd.extend(["--packages", ",".join(packages)])
        for key, value in spark_conf.items():
            cmd.extend(["--conf", f"{key}={value}"])

        cmd.append(str(entrypoint))
        cmd.extend(build_cli_args(config.get("args", {})))
        return cmd

    cmd = [sys.executable, str(entrypoint)]
    cmd.extend(build_cli_args(config.get("args", {})))
    return cmd


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    cmd = build_command(config)

    print("Executing command:")
    print(" ".join(shlex.quote(part) for part in cmd))

    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()