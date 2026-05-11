from __future__ import annotations

import json
import os
from typing import Any

from batch.specs.batch_catalog_utils import resolve_repo_path
from batch.utils.jdbc_manifest_loader import load_jdbc_manifest


DEFAULT_JDBC_CONNECTIONS_PATH = "configs/connections/jdbc_connections.json"


def load_jdbc_connections(
    connections_ref: str = DEFAULT_JDBC_CONNECTIONS_PATH,
) -> dict[str, Any]:
    path = resolve_repo_path(connections_ref)

    if not path.exists():
        raise FileNotFoundError(f"JDBC connections registry not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    return data["connections"]


def get_jdbc_connection(connection_ref: str) -> dict[str, Any]:
    connections = load_jdbc_connections()

    if connection_ref not in connections:
        raise ValueError(f"Unknown JDBC connection_ref: {connection_ref}")

    return connections[connection_ref]


def get_spark_packages_for_manifest(manifest_ref: str) -> list[str]:
    manifest = load_jdbc_manifest(manifest_ref)
    connection = get_jdbc_connection(manifest["connection_ref"])
    return connection.get("spark_packages", [])


def build_runtime_connection(manifest: dict[str, Any]) -> dict[str, Any]:
    connection = get_jdbc_connection(manifest["connection_ref"])
    env_cfg = connection["env"]

    host = os.getenv(env_cfg["host"])
    port = os.getenv(env_cfg["port"], str(connection.get("default_port")))
    database = os.getenv(env_cfg["database"])
    user = os.getenv(env_cfg["user"])
    password = os.getenv(env_cfg["password"])

    missing = [
        name
        for name, value in {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
        }.items()
        if not value
    ]

    if missing:
        raise RuntimeError(
            f"Missing environment values for connection_ref='{manifest['connection_ref']}': {missing}"
        )

    jdbc_url = connection["jdbc_url_template"].format(
        host=host,
        port=port,
        database=database,
    )

    return {
        "type": connection["type"],
        "driver": connection["driver"],
        "jdbc_url": jdbc_url,
        "user": user,
        "password": password,
        "default_options": connection.get("default_options", {}),
    }