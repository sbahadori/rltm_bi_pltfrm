from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _bootstrap_repo_path() -> Path:
    repo_root = Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    return repo_root


REPO_ROOT = _bootstrap_repo_path()

from shared.runtime.job_event_writer import append_job_event, new_run_id  # noqa: E402
from shared.runtime.stream_runtime_db import upsert_stream_unit_current  # noqa: E402
from streaming.specs.stream_spec_utils import (  # noqa: E402
    load_stream_registry,
    resolve_repo_path,
)


POLL_INTERVAL_SEC = 5


@dataclass
class UnitState:
    unit_name: str
    stream_name: str
    layer: str  # bronze | silver
    enabled: bool

    retries: int = 0
    max_retries: int = 10
    backoff_seconds: int = 10

    pid: int | None = None
    returncode: int | None = None
    run_id: str | None = None

    heartbeat_file: str | None = None
    checkpoint_dir: str | None = None

    last_start_ts_epoch: int | None = None
    next_retry_ts_epoch: int | None = None

    stop_requested: bool = False
    restart_requested: bool = False


class StreamSupervisor:
    def __init__(self, registry_path: str, status_file: str) -> None:
        self.registry_path = registry_path
        self.status_file = Path(status_file)
        self.control_dir = Path(os.getenv("STREAM_CONTROL_DIR", "/runtime/spark_health/control"))
        self.log_dir = Path(os.getenv("STREAM_LOG_DIR", "/runtime/spark_health/logs"))

        self.stop_requested = False
        self.disabled_units: set[str] = set()

        self.processes: dict[str, subprocess.Popen[str]] = {}
        self.states: dict[str, UnitState] = {}

        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, *_args: Any) -> None:
        self.stop_requested = True
        self._terminate_all()

    def _runtime_identity(self, state: UnitState, unit_name: str | None = None) -> dict[str, Any]:
        unit = unit_name or state.unit_name
        job_code = f"{state.stream_name}__{state.layer}"
        job_name = f"{state.layer}_{state.stream_name}"

        return {
            "type": "stream",
            "unit_name": unit,
            "stream_name": state.stream_name,
            "layer": state.layer,

            # Must match meta.job
            "job_id": None,
            "job_key": job_code,
            "job_code": job_code,
            "job": job_name,
            "job_name": job_name,
            "pipeline": state.stream_name,
            "pipeline_name": state.stream_name,
            "runner": "stream_supervisor",
        }

    def _emit_runtime_event(self, **payload: Any) -> None:
        try:
            append_job_event(**payload)
        except Exception as exc:
            print(f"[supervisor][WARN] runtime event write failed: {exc}", flush=True)

    def _upsert_current_state(
        self,
        state: UnitState,
        *,
        computed_status: str,
        status_reason: str | None = None,
        pid: int | None = None,
        returncode: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        try:
            upsert_stream_unit_current(
                unit_name=state.unit_name,
                stream_name=state.stream_name,
                layer=state.layer,
                run_id=state.run_id,
                computed_status=computed_status,
                status_reason=status_reason,
                pid=pid if pid is not None else state.pid,
                returncode=returncode,
                retries=state.retries,
                max_retries=state.max_retries,
                checkpoint_path=state.checkpoint_dir,
                payload={
                    "source": "stream_supervisor",
                    "unit_name": state.unit_name,
                    "stream_name": state.stream_name,
                    "layer": state.layer,
                    "run_id": state.run_id,
                    "pid": pid if pid is not None else state.pid,
                    "returncode": returncode,
                    "retries": state.retries,
                    "max_retries": state.max_retries,
                    **(payload or {}),
                },
            )
        except Exception as exc:
            print(f"[supervisor][WARN] stream current state write failed: {exc}", flush=True)

    def _load_units(self) -> None:
        registry = load_stream_registry(self.registry_path)
        streams = registry.get("streams", [])

        new_states: dict[str, UnitState] = {}

        for stream in streams:
            stream_name = stream["name"]
            stream_enabled = bool(stream.get("enabled", True))
            restart_policy = stream.get("restart_policy", {})
            max_retries = int(restart_policy.get("max_retries", 10))
            backoff_seconds = int(restart_policy.get("backoff_seconds", 10))

            for layer in ("bronze", "silver"):
                layer_spec = stream.get(layer)
                if not layer_spec:
                    continue

                unit_name = f"{stream_name}_{layer}"
                previous = self.states.get(unit_name)

                enabled = stream_enabled and unit_name not in self.disabled_units

                state = UnitState(
                    unit_name=unit_name,
                    stream_name=stream_name,
                    layer=layer,
                    enabled=enabled,
                    pid=previous.pid if previous else None,
                    returncode=previous.returncode if previous else None,
                    run_id=previous.run_id if previous else None,
                    retries=previous.retries if previous else 0,
                    max_retries=max_retries,
                    backoff_seconds=backoff_seconds,
                    heartbeat_file=layer_spec.get("heartbeat_file"),
                    checkpoint_dir=layer_spec.get("checkpoint_dir"),
                    last_start_ts_epoch=previous.last_start_ts_epoch if previous else None,
                    next_retry_ts_epoch=previous.next_retry_ts_epoch if previous else None,
                    stop_requested=previous.stop_requested if previous else False,
                    restart_requested=previous.restart_requested if previous else False,
                )

                new_states[unit_name] = state

        self.states = new_states

    def _build_command(self, stream_name: str, layer: str) -> list[str]:
        runner_path = REPO_ROOT / "streaming" / "runners" / "run_stream_service.py"

        if not runner_path.exists():
            raise FileNotFoundError(f"Stream runner not found: {runner_path}")

        return [
            "python3",
            str(runner_path),
            "--registry",
            self.registry_path,
            "--stream-name",
            stream_name,
            "--layer",
            layer,
        ]

    def _start_unit(self, state: UnitState) -> None:
        cmd = self._build_command(state.stream_name, state.layer)

        print(
            f"[supervisor] command for '{state.unit_name}': {' '.join(cmd)}",
            flush=True,
        )

        self.log_dir.mkdir(parents=True, exist_ok=True)
        log_path = self.log_dir / f"{state.unit_name}.log"
        log_file = log_path.open("a", encoding="utf-8")

        print(
            f"[supervisor] writing logs for '{state.unit_name}' to {log_path}",
            flush=True,
        )

        started_epoch = int(time.time())
        run_id = new_run_id(state.unit_name)

        child_env = os.environ.copy()
        child_env["STREAM_RUN_ID"] = run_id
        child_env["STREAM_UNIT_NAME"] = state.unit_name

        proc = subprocess.Popen(
            cmd,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
            env=child_env,
        )

        state.run_id = run_id
        state.pid = proc.pid
        state.returncode = None
        state.last_start_ts_epoch = started_epoch
        state.next_retry_ts_epoch = None
        state.stop_requested = False
        state.restart_requested = False

        self.processes[state.unit_name] = proc

        self._emit_runtime_event(
            event_type="stream_started",
            run_id=run_id,
            **self._runtime_identity(state),
            status="running",
            status_reason="Stream process started by supervisor.",
            pid=proc.pid,
            started_at_epoch=started_epoch,

            # Metrics are not produced by the supervisor.
            records_read=None,
            records_written=None,
            records_inserted=None,
            records_updated=None,
            records_deleted=None,
        )

        self._upsert_current_state(
            state,
            computed_status="running",
            status_reason="Stream process started by supervisor.",
            pid=proc.pid,
            payload={
                "event_type": "stream_started",
                "started_at_epoch": started_epoch,
            },
        )

    def _terminate_all(self) -> None:
        for unit_name, proc in list(self.processes.items()):
            try:
                state = self.states.get(unit_name)
                if state:
                    state.stop_requested = True

                if proc.poll() is None:
                    print(f"[supervisor] terminating '{unit_name}'", flush=True)
                    proc.terminate()
            except Exception:
                pass

        deadline = time.time() + 20

        while time.time() < deadline:
            alive = [p for p in self.processes.values() if p.poll() is None]
            if not alive:
                break
            time.sleep(0.5)

        for unit_name, proc in list(self.processes.items()):
            try:
                if proc.poll() is None:
                    print(f"[supervisor] killing '{unit_name}'", flush=True)
                    proc.kill()
            except Exception:
                pass

    def _apply_control_commands(self) -> None:
        if not self.control_dir.exists():
            return

        for control_file in sorted(self.control_dir.glob("*.json")):
            unit_name = control_file.stem

            try:
                payload = json.loads(control_file.read_text(encoding="utf-8"))
            except Exception as exc:
                print(f"[supervisor][WARN] invalid control file {control_file}: {exc}", flush=True)
                try:
                    control_file.unlink()
                except Exception:
                    pass
                continue

            action = str(payload.get("action") or "").strip().lower()
            state = self.states.get(unit_name)
            proc = self.processes.get(unit_name)

            if action == "stop":
                self.disabled_units.add(unit_name)

                if state:
                    state.enabled = False
                    state.stop_requested = True

                    self._emit_runtime_event(
                        event_type="stream_stop_requested",
                        run_id=state.run_id,
                        **self._runtime_identity(state, unit_name),
                        status="stopping",
                        status_reason="Stop requested from dashboard.",
                    )
                    self._upsert_current_state(
                        state,
                        computed_status="stopping",
                        status_reason="Stop requested from dashboard.",
                        payload={"event_type": "stream_stop_requested"},
                    )

                if proc and proc.poll() is None:
                    print(f"[supervisor] stop requested for '{unit_name}'", flush=True)
                    proc.terminate()

            elif action == "restart":
                self.disabled_units.discard(unit_name)

                if state:
                    state.enabled = True
                    state.restart_requested = True
                    state.stop_requested = False
                    state.retries = 0
                    state.next_retry_ts_epoch = None

                    self._emit_runtime_event(
                        event_type="stream_restart_requested",
                        run_id=state.run_id,
                        **self._runtime_identity(state, unit_name),
                        status="restarting",
                        status_reason="Restart requested from dashboard.",
                    )
                    self._upsert_current_state(
                        state,
                        computed_status="restarting",
                        status_reason="Restart requested from dashboard.",
                        payload={"event_type": "stream_restart_requested"},
                    )

                if proc and proc.poll() is None:
                    print(f"[supervisor] restart requested for '{unit_name}'", flush=True)
                    proc.terminate()
                elif state:
                    state.returncode = None
                    state.pid = None

            else:
                print(
                    f"[supervisor][WARN] unknown control action={action!r} for unit={unit_name}",
                    flush=True,
                )

            try:
                control_file.unlink()
            except Exception:
                pass

    def _refresh_process_states(self) -> None:
        for unit_name, proc in list(self.processes.items()):
            rc = proc.poll()
            state = self.states.get(unit_name)

            if rc is None:
                if state:
                    state.pid = proc.pid
                    state.returncode = None
                continue

            if state:
                ended_epoch = int(time.time())
                started_epoch = state.last_start_ts_epoch or ended_epoch

                if state.restart_requested:
                    status = "restarting"
                    reason = f"Stream process exited during restart request with returncode={rc}"
                elif state.stop_requested or unit_name in self.disabled_units:
                    status = "stopped"
                    reason = f"Stream process stopped by request with returncode={rc}"
                elif rc == 0:
                    status = "stopped"
                    reason = "Stream process exited cleanly."
                else:
                    status = "failed"
                    reason = f"Stream process exited with returncode={rc}"

                self._emit_runtime_event(
                    event_type="stream_exited",
                    run_id=state.run_id or new_run_id(unit_name),
                    **self._runtime_identity(state, unit_name),
                    status=status,
                    status_reason=reason,
                    returncode=rc,
                    retries=state.retries,
                    max_retries=state.max_retries,

                    # These are necessary to close the process-level run.
                    started_at_epoch=started_epoch,
                    ended_at_epoch=ended_epoch,
                    duration_seconds=round(ended_epoch - started_epoch, 3),

                    # Metrics are unknown at supervisor level.
                    records_read=None,
                    records_written=None,
                    records_inserted=None,
                    records_updated=None,
                    records_deleted=None,
                )

                self._upsert_current_state(
                    state,
                    computed_status=status,
                    status_reason=reason,
                    pid=proc.pid,
                    returncode=rc,
                    payload={
                        "event_type": "stream_exited",
                        "started_at_epoch": started_epoch,
                        "ended_at_epoch": ended_epoch,
                        "duration_seconds": round(ended_epoch - started_epoch, 3),
                    },
                )

                state.pid = None
                state.returncode = rc

                if state.restart_requested:
                    state.returncode = None
                    state.next_retry_ts_epoch = None
                    state.restart_requested = False
                    state.stop_requested = False
                elif state.stop_requested or unit_name in self.disabled_units:
                    state.next_retry_ts_epoch = None
                    state.stop_requested = False
                else:
                    state.next_retry_ts_epoch = int(time.time()) + state.backoff_seconds

                print(
                    f"[supervisor] '{unit_name}' exited with returncode={rc}",
                    flush=True,
                )

            del self.processes[unit_name]

    def _maybe_start_units(self) -> None:
        now = int(time.time())

        for unit_name, state in self.states.items():
            if not state.enabled:
                continue

            if unit_name in self.processes:
                continue

            if state.returncode is None and state.pid is not None:
                continue

            if state.returncode is not None:
                if state.retries >= state.max_retries:
                    print(
                        f"[supervisor] '{unit_name}' exceeded max_retries; not restarting",
                        flush=True,
                    )
                    continue

                if state.next_retry_ts_epoch is not None and now < state.next_retry_ts_epoch:
                    continue

                state.retries += 1

            print(f"[supervisor] starting '{unit_name}'", flush=True)
            self._start_unit(state)

    def _health_of_unit(self, state: UnitState) -> dict[str, Any]:
        heartbeat_data: dict[str, Any] | None = None

        if state.heartbeat_file:
            path = Path(state.heartbeat_file)
            if path.exists():
                try:
                    heartbeat_data = json.loads(path.read_text(encoding="utf-8"))
                except Exception:
                    heartbeat_data = {"parse_error": True}

        return {
            "enabled": state.enabled,
            "run_id": state.run_id,
            "pid": state.pid,
            "returncode": state.returncode,
            "retries": state.retries,
            "max_retries": state.max_retries,
            "backoff_seconds": state.backoff_seconds,
            "heartbeat_file": state.heartbeat_file,
            "checkpoint_dir": state.checkpoint_dir,
            "last_start_ts_epoch": state.last_start_ts_epoch,
            "next_retry_ts_epoch": state.next_retry_ts_epoch,
            "stop_requested": state.stop_requested,
            "restart_requested": state.restart_requested,
            "heartbeat": heartbeat_data,
        }

    def _write_status(self) -> None:
        self.status_file.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "ts_epoch": int(time.time()),
            "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "units": {
                unit_name: self._health_of_unit(state)
                for unit_name, state in sorted(self.states.items())
            },
        }

        tmp_file = self.status_file.with_name(f".{self.status_file.name}.{os.getpid()}.tmp")
        tmp_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_file.replace(self.status_file)

    def run(self) -> None:
        while not self.stop_requested:
            self._load_units()
            self._apply_control_commands()
            self._refresh_process_states()
            self._maybe_start_units()
            self._write_status()
            time.sleep(POLL_INTERVAL_SEC)

        self._write_status()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", required=True)
    parser.add_argument("--status-file", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    registry_path = str(resolve_repo_path(args.registry))
    supervisor = StreamSupervisor(
        registry_path=registry_path,
        status_file=args.status_file,
    )
    supervisor.run()


if __name__ == "__main__":
    main()
