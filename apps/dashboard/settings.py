from __future__ import annotations

from functools import lru_cache
import os
from dataclasses import dataclass
from pathlib import Path


def env_str(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}

def validate_production_safety(settings: DashboardSettings) -> None:
    app_env = settings.app_env.strip().lower()

    if app_env not in {"prod", "production"}:
        return

    weak_secret_values = {
        "",
        "dev",
        "secret",
        "change-me",
        "change_me",
        "dev-secret",
        "dev-secret-change-me",
        "local-dev-secret",
    }

    if settings.dashboard_secret_key.strip().lower() in weak_secret_values:
        raise RuntimeError(
            "Unsafe DASHBOARD_SECRET_KEY for production. "
            "Set a strong DASHBOARD_SECRET_KEY before starting the dashboard API."
        )

    if settings.airflow_password.strip().lower() in {"admin", "password", "airflow"}:
        raise RuntimeError(
            "Unsafe AIRFLOW_PASSWORD for production. "
            "Set a secure Airflow password/secret before starting the dashboard API."
        )

    if settings.control_db_password.strip().lower() in {"warehouse", "postgres", "password", "admin"}:
        raise RuntimeError(
            "Unsafe CONTROL_DB_PASSWORD for production. "
            "Set a secure database password before starting the dashboard API."
        )
    
@dataclass(frozen=True)
class DashboardSettings:
    pipeline_repo_root: Path

    batch_catalog_path: Path
    stream_registry_path: Path
    job_run_registry_file: Path

    stream_status_file: Path
    stream_log_dir: Path
    airflow_log_dir: Path
    stream_control_dir: Path

    stream_status_stale_seconds: int
    stream_heartbeat_stale_seconds: int

    control_db_host: str
    control_db_port: int
    control_db_name: str
    control_db_user: str
    control_db_password: str
    control_db_sslmode: str

    airflow_api_base: str
    airflow_user: str
    airflow_password: str

    dashboard_secret_key: str
    dashboard_token_expire_minutes: int
    app_env: str
    
    catalog_backup_dir: Path
    control_onboarding_dag_id: str

    @classmethod
    def from_env(cls) -> "DashboardSettings":
        repo_root = Path(env_str("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm"))

        return cls(
            pipeline_repo_root=repo_root,

            batch_catalog_path=Path(
                env_str(
                    "BATCH_CATALOG_PATH",
                    str(repo_root / "configs" / "batch" / "pipeline_catalog.json"),
                )
            ),
            stream_registry_path=Path(
                env_str(
                    "STREAM_REGISTRY_PATH",
                    str(repo_root / "configs" / "streaming" / "stream_registry.json"),
                )
            ),
            job_run_registry_file=Path(
                env_str(
                    "JOB_RUN_REGISTRY_FILE",
                    str(repo_root / "runtime" / "job_runs" / "job_runs.jsonl"),
                )
            ),

            stream_status_file=Path(
                env_str("STREAM_STATUS_FILE", "/runtime/spark_health/stream_supervisor_status.json")
            ),
            stream_log_dir=Path(env_str("STREAM_LOG_DIR", "/runtime/spark_health/logs")),
            airflow_log_dir=Path(env_str("AIRFLOW_LOG_DIR", "/runtime/airflow_logs")),
            stream_control_dir=Path(env_str("STREAM_CONTROL_DIR", "/runtime/spark_health/control")),

            stream_status_stale_seconds=env_int("STREAM_STATUS_STALE_SECONDS", 120),
            stream_heartbeat_stale_seconds=env_int("STREAM_HEARTBEAT_STALE_SECONDS", 120),

            control_db_host=env_str("CONTROL_DB_HOST", "postgres-warehouse"),
            control_db_port=env_int("CONTROL_DB_PORT", 5432),
            control_db_name=env_str("CONTROL_DB_NAME", env_str("POSTGRES_DB", "warehouse")),
            control_db_user=env_str("CONTROL_DB_USER", env_str("POSTGRES_USER", "warehouse")),
            control_db_password=env_str("CONTROL_DB_PASSWORD", env_str("POSTGRES_PASSWORD", "warehouse")),
            control_db_sslmode=env_str("CONTROL_DB_SSLMODE", "disable"),

            airflow_api_base=env_str("AIRFLOW_API_BASE", "http://airflow-api-server:8080").rstrip("/"),
            airflow_user=env_str("AIRFLOW_USER", "admin"),
            airflow_password=env_str("AIRFLOW_PASSWORD", "admin"),
            control_onboarding_dag_id=env_str(
                "CONTROL_ONBOARDING_DAG_ID",
                "control_plane_onboarding",
            ),
            dashboard_secret_key=env_str("DASHBOARD_SECRET_KEY", ""),
            dashboard_token_expire_minutes=env_int("DASHBOARD_TOKEN_EXPIRE_MINUTES", 480),
            app_env=env_str("APP_ENV", env_str("ENV", "dev")).lower(),

            catalog_backup_dir=Path(
                env_str(
                    "CATALOG_BACKUP_DIR",
                    str(repo_root / "runtime" / "catalog_backups"),
                )
            ),
        )


_SETTINGS: DashboardSettings | None = None


@lru_cache(maxsize=1)
def get_settings() -> DashboardSettings:
    settings = DashboardSettings.from_env()
    validate_production_safety(settings)
    return settings
