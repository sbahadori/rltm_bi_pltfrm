import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

REPO_ROOT = os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")
JOBS_ROOT = f"{REPO_ROOT}/jobs"
SPARK_SUBMIT = os.getenv("SPARK_SUBMIT", "/home/airflow/.local/bin/spark-submit")

SPARK_PACKAGES = ",".join([
    "io.delta:delta-spark_2.12:3.2.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])


COMMON_ENV = {
    "PIPELINE_REPO_ROOT": REPO_ROOT,
    "S3_ENDPOINT": os.getenv("S3_ENDPOINT", "http://minio:9000"),
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minio"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minio123"),
    "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
    "PATH": f"/home/airflow/.local/bin:{os.getenv('PATH', '')}",
}

def spark_cmd(job_path: str, extra_args: str = "") -> str:
    return (
        f'{SPARK_SUBMIT} '
        f'--packages {SPARK_PACKAGES} '
        f'--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" '
        f'--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" '
        f'--conf "spark.ui.showConsoleProgress=false" '
        f'{job_path} {extra_args}'
    ).strip()

with DAG(
    dag_id="gold_price_pipeline",
    description="Phase 2 orchestration for the gold-price Bronze/Silver/Gold pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "admin",
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["gold", "spark", "phase2", "airflow"],
) as dag:

    ingest_to_bronze = BashOperator(
        task_id="ingest_to_bronze",
        bash_command=spark_cmd(f"{JOBS_ROOT}/ingestion/ingest_to_bronze.py", "--run-once"),
        env=COMMON_ENV,
        append_env=True,
        execution_timeout=timedelta(minutes=10),
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=spark_cmd(f"{JOBS_ROOT}/bronze_to_silver/bronze_to_silver.py"),
        env=COMMON_ENV,
        append_env=True,
        execution_timeout=timedelta(minutes=10),
    )

    silver_to_gold_bars_1m = BashOperator(
        task_id="silver_to_gold_bars_1m",
        bash_command=spark_cmd(f"{JOBS_ROOT}/silver_to_gold/gold_bars_1m.py"),
        env=COMMON_ENV,
        append_env=True,
        execution_timeout=timedelta(minutes=10),
    )

    gold_bars_to_metrics = BashOperator(
        task_id="gold_price_metrics",
        bash_command=spark_cmd(f"{JOBS_ROOT}/silver_to_gold/gold_price_metrics.py"),
        env=COMMON_ENV,
        append_env=True,
        execution_timeout=timedelta(minutes=10),
    )

    ingest_to_bronze >> bronze_to_silver >> silver_to_gold_bars_1m >> gold_bars_to_metrics