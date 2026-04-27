from __future__ import annotations

import os
import shlex
from pathlib import Path
from typing import Any


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


def get_repo_root() -> Path:
    return Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()


def resolve_repo_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (get_repo_root() / path).resolve()


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


def merge_spark_packages(config_packages: list[str] | None) -> list[str]:
    merged: list[str] = []
    for package in DEFAULT_SPARK_PACKAGES + list(config_packages or []):
        if package not in merged:
            merged.append(package)
    return merged


def build_spark_submit_command(
    job_spec: dict[str, Any],
    *,
    repo_root: str | Path | None = None,
    spark_submit: str | None = None,
) -> str:
    repo_root_path = Path(repo_root).resolve() if repo_root else get_repo_root()
    spark_submit_bin = spark_submit or os.getenv("SPARK_SUBMIT", "/opt/spark/bin/spark-submit")

    spark_cfg = job_spec.get("spark", {}) or {}
    spark_master = spark_cfg.get("master") or os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

    entrypoint = Path(job_spec["entrypoint"])
    entrypoint_path = entrypoint if entrypoint.is_absolute() else (repo_root_path / entrypoint).resolve()
    if not entrypoint_path.exists():
        raise FileNotFoundError(f"Spark entrypoint not found: {entrypoint_path}")

    packages = merge_spark_packages(spark_cfg.get("packages"))
    spark_conf = {**DEFAULT_SPARK_CONF, **spark_cfg.get("conf", {})}
    args = build_cli_args(job_spec.get("args", {}))

    cmd: list[str] = [spark_submit_bin, "--master", spark_master]

    if packages:
        cmd.extend(["--packages", ",".join(packages)])

    for key, value in spark_conf.items():
        cmd.extend(["--conf", f"{key}={value}"])

    cmd.append(str(entrypoint_path))
    cmd.extend(args)

    return " ".join(shlex.quote(part) for part in cmd)