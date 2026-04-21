from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path
from typing import Iterable

from airflow.providers.standard.operators.bash import BashOperator


def _bootstrap_repo_path() -> Path:
    repo_root = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    return repo_root


REPO_ROOT = _bootstrap_repo_path()

from shared.lib.spark_submit_utils import build_spark_submit_command, get_repo_root, load_pipeline_config  # noqa: E402


def build_common_env() -> dict[str, str]:
    return {
        "PIPELINE_REPO_ROOT": str(get_repo_root()),
        "S3_ENDPOINT": os.getenv("S3_ENDPOINT", "http://minio:9000"),
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minio"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"),
        "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
        "SPARK_SUBMIT": os.getenv("SPARK_SUBMIT", "/home/airflow/.local/bin/spark-submit"),
        "SPARK_MASTER_URL": os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"),
        "PATH": f"/home/airflow/.local/bin:{os.getenv('PATH', '')}",
    }


def discover_pipeline_configs(domain: str | None = None) -> list[str]:
    config_root = get_repo_root() / "configs" / "pipelines"
    if not config_root.exists():
        return []

    pattern = f"{domain}_*.yaml" if domain else "*.yaml"
    configs = sorted(config_root.glob(pattern))
    return [str(path.relative_to(get_repo_root())) for path in configs]


def build_tasks_from_configs(
    *,
    dag,
    config_paths: Iterable[str],
    common_env: dict[str, str] | None = None,
    execution_timeout_minutes: int = 30,
) -> dict[str, BashOperator]:
    env = build_common_env()
    if common_env:
        env.update(common_env)

    configs_by_name: dict[str, dict] = {}
    for config_path in config_paths:
        cfg = load_pipeline_config(config_path)
        name = cfg["name"]
        if name in configs_by_name:
            raise ValueError(f"Duplicate pipeline name found: {name}")
        configs_by_name[name] = cfg

    tasks: dict[str, BashOperator] = {}
    for name, cfg in configs_by_name.items():
        cmd = build_spark_submit_command(cfg)

        task = BashOperator(
            task_id=name,
            bash_command=cmd,
            env=env,
            append_env=True,
            execution_timeout=timedelta(minutes=cfg.get("execution_timeout_minutes", execution_timeout_minutes)),
            dag=dag,
        )
        tasks[name] = task

    for name, cfg in configs_by_name.items():
        for dep in cfg.get("dependencies", []):
            if dep not in tasks:
                raise ValueError(
                    f"Pipeline '{name}' depends on '{dep}', but '{dep}' is not among DAG tasks. "
                    f"Only pipeline names should be listed in dependencies."
                )
            tasks[dep] >> tasks[name]

    return tasks