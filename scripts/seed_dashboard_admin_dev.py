from __future__ import annotations

"""
seed_dashboard_admin_dev.py

Development-only dashboard admin seed script.

Do not run this in production.
This script intentionally refuses to run when APP_ENV=prod or APP_ENV=production.
"""

import os
import sys

import psycopg2


def _is_prod() -> bool:
    return os.getenv("APP_ENV", "dev").strip().lower() in {"prod", "production"}


def main() -> None:
    if _is_prod():
        raise RuntimeError(
            "Refusing to seed dashboard admin in production. "
            "Create production users through a secure provisioning process."
        )

    repo_root = os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")
    sys.path.insert(0, repo_root)
    sys.path.insert(0, os.path.join(repo_root, "apps", "dashboard"))

    try:
        from apps.dashboard.auth import hash_password
    except Exception:
        from auth import hash_password

    username = os.getenv("DASHBOARD_DEV_ADMIN_USERNAME", "admin")
    password = os.getenv("DASHBOARD_DEV_ADMIN_PASSWORD", "admin")
    email = os.getenv("DASHBOARD_DEV_ADMIN_EMAIL", "admin@example.com")
    role = os.getenv("DASHBOARD_DEV_ADMIN_ROLE", "admin")

    if not username or not password:
        raise RuntimeError("DASHBOARD_DEV_ADMIN_USERNAME and DASHBOARD_DEV_ADMIN_PASSWORD are required.")

    db_config = {
        "host": os.getenv("CONTROL_DB_HOST", os.getenv("POSTGRES_HOST", "postgres-warehouse")),
        "port": int(os.getenv("CONTROL_DB_PORT", os.getenv("POSTGRES_PORT", "5432"))),
        "dbname": os.getenv("CONTROL_DB_NAME", os.getenv("POSTGRES_DB", "warehouse")),
        "user": os.getenv("CONTROL_DB_USER", os.getenv("POSTGRES_USER", "warehouse")),
        "password": os.getenv("CONTROL_DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "warehouse")),
    }

    conn = psycopg2.connect(**db_config)
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute(
            "CALL ctl.usp_upsert_dashboard_user(%s, %s, %s, %s)",
            (username, email, hash_password(password), role),
        )

        cur.execute("SELECT * FROM ctl.usp_get_dashboard_user(%s)", (username,))
        row = cur.fetchone()

    print(
        {
            "dashboard_dev_seed": "ok",
            "admin_exists": bool(row),
            "username": row[1] if row else None,
            "role": row[4] if row else None,
        }
    )


if __name__ == "__main__":
    main()
