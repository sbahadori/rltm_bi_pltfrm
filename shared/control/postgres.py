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


def call_usp_rows(
    usp_name: str,
    params: tuple[Any, ...] | None = None,
) -> list[dict[str, Any]]:
    """
    For PostgreSQL functions returning rows:
      SELECT * FROM ctl.usp_name(%s, %s, ...)
    """
    params = params or tuple()
    placeholders = ", ".join(["%s"] * len(params))

    sql = f"SELECT * FROM ctl.{usp_name}({placeholders})"

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]


def call_usp_one(
    usp_name: str,
    params: tuple[Any, ...] | None = None,
) -> dict[str, Any] | None:
    rows = call_usp_rows(usp_name, params)
    return rows[0] if rows else None


def call_usp_void(
    usp_name: str,
    params: tuple[Any, ...] | None = None,
) -> None:
    """
    For PostgreSQL procedures:
      CALL ctl.usp_name(%s, %s, ...)
    """
    params = params or tuple()
    placeholders = ", ".join(["%s"] * len(params))

    sql = f"CALL ctl.{usp_name}({placeholders})"

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)