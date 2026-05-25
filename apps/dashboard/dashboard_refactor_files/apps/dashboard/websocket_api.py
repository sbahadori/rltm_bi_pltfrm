from __future__ import annotations

"""WebSocket routes for live log streaming."""

import asyncio

from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect

try:
    from auth import get_current_user_ws
    from config_loader import STREAM_LOG_DIR
    from log_api import find_airflow_log, tail_file
    from stream_runtime import resolve_stream_log_name
except ImportError:  # pragma: no cover
    from .auth import get_current_user_ws
    from .config_loader import STREAM_LOG_DIR
    from .log_api import find_airflow_log, tail_file
    from .stream_runtime import resolve_stream_log_name

router = APIRouter()


@router.websocket("/api/ws/logs/{unit_name}")
async def ws_logs(
    websocket: WebSocket,
    unit_name: str,
    token: str | None = Query(default=None),
    kind: str = Query(default="stream"),
    pipeline: str | None = Query(default=None),
    task: str | None = Query(default=None),
    lines: int = Query(default=100),
):
    try:
        get_current_user_ws(token)
    except HTTPException:
        await websocket.close(code=4001)
        return

    await websocket.accept()

    try:
        if kind == "stream":
            resolved = resolve_stream_log_name(unit_name)
            log_path = STREAM_LOG_DIR / f"{resolved}.log"
        else:
            dag_id = pipeline or unit_name
            task_id = task or unit_name
            log_path = find_airflow_log(dag_id, task_id)
    except Exception as exc:
        await websocket.send_json({"type": "error", "message": str(exc)})
        await websocket.close()
        return

    if log_path.exists():
        await websocket.send_json({"type": "history", "content": tail_file(log_path, lines)})

    last_size = log_path.stat().st_size if log_path.exists() else 0

    try:
        while True:
            await asyncio.sleep(1.0)
            if not log_path.exists():
                await websocket.send_json({"type": "ping"})
                continue

            current_size = log_path.stat().st_size
            if current_size > last_size:
                with log_path.open("r", encoding="utf-8", errors="replace") as file:
                    file.seek(last_size)
                    new_content = file.read()
                if new_content:
                    await websocket.send_json({"type": "append", "content": new_content})
                last_size = current_size
            else:
                await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
