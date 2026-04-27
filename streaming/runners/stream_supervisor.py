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

from streaming.specs.stream_spec_utils import load_stream_registry, resolve_repo_path

POLL_INTERVAL_SEC = 5


@dataclass
class UnitState:
    unit_name: str
    stream_name: str
    layer: str  # bronze | silver
    enabled: bool
    pid: int | None = None
    returncode: int | None = None
    retries: int = 0
    max_retries: int = 10
    backoff_seconds: int = 10
    heartbeat_file: str | None = None
    checkpoint_dir: str | None = None
    last_start_ts_epoch: int | None = None
    next_retry_ts_epoch: int | None = None


class StreamSupervisor:
    def __init__(self, registry_path: str, status_file: str) -> None:
        self.registry_path = registry_path
        self.status_file = Path(status_file)
        self.stop_requested = False

        self.processes: dict[str, subprocess.Popen[str]] = {}
        self.states: dict[str, UnitState] = {}

        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, *_args: Any) -> None:
        self.stop_requested = True
        self._terminate_all()

    def _load_units(self) -> None:
        registry = load_stream_registry(self.registry_path)
        streams = registry.get("streams", [])

        new_states: dict[str, UnitState] = {}

        for stream in streams:
            stream_name = stream["name"]
            enabled = bool(stream.get("enabled", True))
            restart_policy = stream.get("restart_policy", {})
            max_retries = int(restart_policy.get("max_retries", 10))
            backoff_seconds = int(restart_policy.get("backoff_seconds", 10))

            for layer in ("bronze", "silver"):
                layer_spec = stream.get(layer)
                if not layer_spec:
                    continue

                unit_name = f"{stream_name}_{layer}"

                previous = self.states.get(unit_name)
                state = UnitState(
                    unit_name=unit_name,
                    stream_name=stream_name,
                    layer=layer,
                    enabled=enabled,
                    pid=previous.pid if previous else None,
                    returncode=previous.returncode if previous else None,
                    retries=previous.retries if previous else 0,
                    max_retries=max_retries,
                    backoff_seconds=backoff_seconds,
                    heartbeat_file=layer_spec.get("heartbeat_file"),
                    checkpoint_dir=layer_spec.get("checkpoint_dir"),
                    last_start_ts_epoch=previous.last_start_ts_epoch if previous else None,
                    next_retry_ts_epoch=previous.next_retry_ts_epoch if previous else None,
                )
                new_states[unit_name] = state

        self.states = new_states

    def _build_command(self, stream_name: str, layer: str) -> list[str]:
        return [
            "python3",
            "/opt/spark/streaming/runners/run_stream_service.py",
            "--registry",
            self.registry_path,
            "--stream-name",
            stream_name,
            "--layer",
            layer,
        ]

    def _start_unit(self, state: UnitState) -> None:
        cmd = self._build_command(state.stream_name, state.layer)
        proc = subprocess.Popen(
            cmd,
            stdout=sys.stdout,
            stderr=sys.stderr,
            text=True,
            env=os.environ.copy(),
        )
        self.processes[state.unit_name] = proc
        state.pid = proc.pid
        state.returncode = None
        state.last_start_ts_epoch = int(time.time())
        state.next_retry_ts_epoch = None

    def _terminate_all(self) -> None:
        for unit_name, proc in list(self.processes.items()):
            try:
                if proc.poll() is None:
                    proc.terminate()
            except Exception:
                pass

        deadline = time.time() + 20
        while time.time() < deadline:
            alive = [p for p in self.processes.values() if p.poll() is None]
            if not alive:
                break
            time.sleep(0.5)

        for proc in self.processes.values():
            try:
                if proc.poll() is None:
                    proc.kill()
            except Exception:
                pass

    def _refresh_process_states(self) -> None:
        for unit_name, proc in list(self.processes.items()):
            rc = proc.poll()
            if rc is None:
                state = self.states.get(unit_name)
                if state:
                    state.pid = proc.pid
                    state.returncode = None
                continue

            state = self.states.get(unit_name)
            if state:
                state.pid = None
                state.returncode = rc
                state.next_retry_ts_epoch = int(time.time()) + state.backoff_seconds

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
                    print(f"[supervisor] '{unit_name}' exceeded max_retries; not restarting", flush=True)
                    continue

                if state.next_retry_ts_epoch is not None and now < state.next_retry_ts_epoch:
                    continue

                state.retries += 1

            print(f"[supervisor] starting '{unit_name}'", flush=True)
            self._start_unit(state)

    def _health_of_unit(self, state: UnitState) -> dict[str, Any]:
        heartbeat_data: dict[str, Any] | None = None

        if state.heartbeat_file:
            p = Path(state.heartbeat_file)
            if p.exists():
                try:
                    heartbeat_data = json.loads(p.read_text(encoding="utf-8"))
                except Exception:
                    heartbeat_data = {"parse_error": True}

        return {
            "enabled": state.enabled,
            "pid": state.pid,
            "returncode": state.returncode,
            "retries": state.retries,
            "max_retries": state.max_retries,
            "backoff_seconds": state.backoff_seconds,
            "heartbeat_file": state.heartbeat_file,
            "checkpoint_dir": state.checkpoint_dir,
            "last_start_ts_epoch": state.last_start_ts_epoch,
            "next_retry_ts_epoch": state.next_retry_ts_epoch,
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

        self.status_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def run(self) -> None:
        while not self.stop_requested:
            self._load_units()
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
    supervisor = StreamSupervisor(registry_path=registry_path, status_file=args.status_file)
    supervisor.run()


if __name__ == "__main__":
    main()