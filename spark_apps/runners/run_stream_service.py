from __future__ import annotations

import argparse
import os
import shlex
import subprocess

from shared.lib.stream_spec_utils import get_repo_root, get_stream_spec, validate_stream_spec


ENGINE_ENTRYPOINTS = {
    "generic_kafka_to_bronze": "spark_apps/streaming/generic_kafka_to_bronze.py",
    "generic_bronze_to_silver": "spark_apps/streaming/generic_bronze_to_silver.py",
}


def build_submit_command(entrypoint: str, registry: str, stream_name: str) -> str:
    spark_submit_bin = os.getenv("SPARK_SUBMIT", "/opt/spark/bin/spark-submit")
    spark_master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    repo_root = get_repo_root()
    entrypoint_path = (repo_root / entrypoint).resolve()

    packages = ["io.delta:delta-spark_2.12:3.2.0"]
    if entrypoint.endswith("generic_kafka_to_bronze.py"):
        packages.append("org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")

    cmd = [
        spark_submit_bin,
        "--master", spark_master,
        "--packages", ",".join(packages),
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf", "spark.ui.showConsoleProgress=false",
        "--conf", "spark.jars.ivy=/tmp/.ivy2",
        "--conf", "spark.executor.cores=1",
        "--conf", "spark.executor.memory=1G",
        "--conf", "spark.cores.max=1",
        str(entrypoint_path),
        "--registry", registry,
        "--stream-name", stream_name,
    ]
    return " ".join(shlex.quote(x) for x in cmd)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", required=True)
    parser.add_argument("--stream-name", required=True)
    parser.add_argument("--layer", required=True, choices=["bronze", "silver"])
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spec = get_stream_spec(args.registry, args.stream_name)
    validate_stream_spec(spec)

    layer_spec = spec[args.layer]
    engine = layer_spec["engine"]
    entrypoint = ENGINE_ENTRYPOINTS[engine]

    cmd = build_submit_command(entrypoint, args.registry, spec["name"])

    print(f"Executing stream '{spec['name']}' layer '{args.layer}' command:\n{cmd}")
    subprocess.run(cmd, shell=True, check=True)


if __name__ == "__main__":
    main()