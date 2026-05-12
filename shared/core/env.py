from __future__ import annotations

import os


def required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    return default if value is None or value.strip() == "" else value


def s3_env() -> dict[str, str]:
    return {
        "endpoint": required_env("S3_ENDPOINT"),
        "access_key": required_env("AWS_ACCESS_KEY_ID"),
        "secret_key": required_env("AWS_SECRET_ACCESS_KEY"),
        "region": env_or_default("AWS_REGION", "us-east-1"),
    }


def airflow_api_env() -> dict[str, str]:
    return {
        "base_url": required_env("AIRFLOW_API_BASE"),
        "username": required_env("AIRFLOW_USER"),
        "password": required_env("AIRFLOW_PASSWORD"),
    }