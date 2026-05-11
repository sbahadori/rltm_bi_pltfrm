from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from pathlib import Path
import logging

def _bootstrap_repo_path() -> Path:
    repo_root = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    return repo_root


REPO_ROOT = _bootstrap_repo_path()

from streaming.specs.stream_spec_utils import (  # noqa: E402
    get_repo_root,
    get_stream_spec,
    validate_stream_spec,
)


ENGINE_ENTRYPOINTS = {
    "generic_kafka_to_bronze": "streaming/engines/generic_kafka_to_bronze.py",
    "generic_bronze_to_silver": "streaming/engines/generic_bronze_to_silver.py",
}


def build_submit_command(entrypoint: str, registry: str, stream_name: str) -> str:
    spark_submit_bin = os.getenv("SPARK_SUBMIT", "/opt/spark/bin/spark-submit")
    spark_master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    repo_root = get_repo_root()

    # Defensive normalization:
    # Avoid accidentally treating "/streaming/..." as an absolute container path.
    entrypoint = entrypoint.lstrip("/")
    entrypoint_path = (repo_root / entrypoint).resolve()

    if not entrypoint_path.exists():
        raise FileNotFoundError(
            f"Streaming engine entrypoint not found: {entrypoint_path}"
        )

    packages = ["io.delta:delta-spark_2.12:3.2.0"]

    if entrypoint.endswith("generic_kafka_to_bronze.py"):
        packages.append("org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")

    cmd = [
        spark_submit_bin,
        "--master",
        spark_master,
        "--packages",
        ",".join(packages),
        "--conf",
        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf",
        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf",
        "spark.ui.showConsoleProgress=false",
        "--conf",
        "spark.jars.ivy=/tmp/.ivy2",
        "--conf",
        "spark.executor.cores=1",
        "--conf",
        "spark.executor.memory=1G",
        "--conf",
        "spark.cores.max=1",
        str(entrypoint_path),
        "--registry",
        registry,
        "--stream-name",
        stream_name,
    ]

    return " ".join(shlex.quote(x) for x in cmd)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", required=True)
    parser.add_argument("--stream-name", required=True)
    parser.add_argument("--layer", required=True, choices=["bronze", "silver"])
    return parser.parse_args()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)

logger = logging.getLogger("stream_service")


def main() -> None:
    args = parse_args()

    spec = get_stream_spec(args.registry, args.stream_name)
    validate_stream_spec(spec)
    logger.info(
        "Starting stream service stream_name=%s layer=%s registry=%s",
        args.stream_name,
        args.layer,
        args.registry,
    )
    layer_spec = spec[args.layer]
    engine = layer_spec["engine"]

    if engine not in ENGINE_ENTRYPOINTS:
        raise ValueError(
            f"Unsupported streaming engine '{engine}'. "
            f"Supported engines: {sorted(ENGINE_ENTRYPOINTS)}"
        )

    entrypoint = ENGINE_ENTRYPOINTS[engine]
    cmd = build_submit_command(entrypoint, args.registry, spec["name"])

    logger.info(
        "Loaded stream spec stream_name=%s layer=%s engine=%s",
        args.stream_name,
        args.layer,
        layer_spec.get("engine"),
    )
    
    logger.info("Submitting stream engine command: %s", cmd)

    try:
        subprocess.run(cmd, shell=True, check=True)
        logger.info(
            "Stream engine exited successfully stream_name=%s layer=%s",
            args.stream_name,
            args.layer,
        )
    except subprocess.CalledProcessError as exc:
        logger.exception(
            "Stream engine failed stream_name=%s layer=%s returncode=%s",
            args.stream_name,
            args.layer,
            exc.returncode,
        )
        raise

if __name__ == "__main__":
    main()