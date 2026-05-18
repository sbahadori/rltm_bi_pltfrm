from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterator

import psycopg2
import psycopg2.extras


def control_db_enabled() -> bool:
    return os.getenv("CONTROL_DB_ENABLED", "false").lower() in {"1", "true", "yes"}


def control_db_config() -> dict[str, Any]:
    return {
        "host": os.getenv("CONTROL_DB_HOST", "postgres-warehouse"),
        "port": int(os.getenv("CONTROL_DB_PORT", "5432")),
        "dbname": os.getenv("CONTROL_DB_NAME", os.getenv("POSTGRES_DB", "warehouse")),
        "user": os.getenv("CONTROL_DB_USER", os.getenv("POSTGRES_USER", "warehouse")),
        "password": os.getenv("CONTROL_DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "warehouse")),
        "sslmode": os.getenv("CONTROL_DB_SSLMODE", "disable"),
    }


@contextmanager
def get_conn() -> Iterator[psycopg2.extensions.connection]:
    conn = psycopg2.connect(**control_db_config())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def fetch_all(sql: str, params: tuple[Any, ...] | dict[str, Any] | None = None) -> list[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]


def fetch_one(sql: str, params: tuple[Any, ...] | dict[str, Any] | None = None) -> dict[str, Any] | None:
    rows = fetch_all(sql, params)
    return rows[0] if rows else None


def execute(sql: str, params: tuple[Any, ...] | dict[str, Any] | None = None) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)