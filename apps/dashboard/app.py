from __future__ import annotations

"""
BI Platform Dashboard API composition root.

This file intentionally does not contain runtime, SQL, Airflow, stream, or log
resolution logic. It only creates the FastAPI app, configures middleware, and
registers routers.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

try:
    from apps.dashboard.action_api import router as action_router
    from apps.dashboard.auth_api import router as auth_router
    from apps.dashboard.catalog_editor import router as catalog_router
    from apps.dashboard.control_api import router as control_router
    from apps.dashboard.log_api import router as log_router
    from apps.dashboard.runtime_api import router as runtime_router
    from apps.dashboard.runtime_models import now_iso
    from apps.dashboard.websocket_api import router as websocket_router

except ImportError:  # pragma: no cover - package import fallback
    from .action_api import router as action_router
    from .auth_api import router as auth_router
    from .catalog_editor import router as catalog_router
    from .control_api import router as control_router
    from .log_api import router as log_router
    from .runtime_api import router as runtime_router
    from .runtime_models import now_iso
    from .websocket_api import router as websocket_router



def create_app() -> FastAPI:
    app = FastAPI(title="BI Platform Dashboard API")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["*"],
    )

    app.include_router(catalog_router)
    app.include_router(auth_router)
    app.include_router(runtime_router)
    app.include_router(control_router)
    app.include_router(action_router)
    app.include_router(log_router)
    app.include_router(websocket_router)

        
    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "ts": now_iso()}

    return app


app = create_app()
