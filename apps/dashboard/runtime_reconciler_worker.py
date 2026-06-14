from __future__ import annotations

import asyncio
from typing import Any

try:
    from apps.dashboard.runtime_reconciler import reconcile_runtime_once
    from apps.dashboard.settings import get_settings
except ImportError:  # pragma: no cover
    from .runtime_reconciler import reconcile_runtime_once
    from .settings import get_settings


_worker_task: asyncio.Task | None = None
_stop_event: asyncio.Event | None = None


async def _sleep_or_stop(seconds: int) -> None:
    global _stop_event

    if _stop_event is None:
        await asyncio.sleep(seconds)
        return

    try:
        await asyncio.wait_for(_stop_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        return


async def _runtime_reconciler_loop() -> None:
    settings = get_settings()

    if not settings.runtime_reconciler_enabled:
        print("[runtime-reconciler] disabled by config", flush=True)
        return

    interval = max(3, int(settings.runtime_reconciler_interval_seconds))
    limit = max(1, int(settings.runtime_reconciler_limit))
    failure_cooldown = max(0, int(settings.runtime_reconciler_failure_cooldown_seconds))

    print(
        "[runtime-reconciler] started "
        f"interval={interval}s limit={limit} failure_cooldown={failure_cooldown}s",
        flush=True,
    )

    while _stop_event is None or not _stop_event.is_set():
        try:
            result: dict[str, Any] = reconcile_runtime_once(
                limit=limit,
                failure_cooldown_seconds=failure_cooldown,
            )

            if result.get("checked") or result.get("updated") or result.get("failed"):
                print(
                    "[runtime-reconciler] "
                    f"checked={result.get('checked')} "
                    f"updated={result.get('updated')} "
                    f"skipped={result.get('skipped')} "
                    f"failed={result.get('failed')} "
                    f"throttled_failures={result.get('throttled_failures')}",
                    flush=True,
                )

        except Exception as exc:
            print(f"[runtime-reconciler] loop error: {exc}", flush=True)

        await _sleep_or_stop(interval)

    print("[runtime-reconciler] stopped", flush=True)


def start_runtime_reconciler_worker() -> None:
    global _worker_task, _stop_event

    if _worker_task and not _worker_task.done():
        return

    _stop_event = asyncio.Event()
    _worker_task = asyncio.create_task(_runtime_reconciler_loop())


async def stop_runtime_reconciler_worker() -> None:
    global _worker_task, _stop_event

    if _stop_event:
        _stop_event.set()

    if _worker_task:
        try:
            await asyncio.wait_for(_worker_task, timeout=10)
        except asyncio.TimeoutError:
            _worker_task.cancel()
        finally:
            _worker_task = None
            _stop_event = None