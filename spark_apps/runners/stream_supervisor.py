from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import yaml

STOP = False
PROCS: dict[str, subprocess.Popen] = {}
LOG_THREADS: dict[str, threading.Thread] = {}
RETRY_META: dict[str, dict] = {}


def get_repo_root() -> Path:
    return Path(os.getenv("PIPELINE_REPO_ROOT", "/workspace/rltm_bi_pltfrm")).resolve()


def resolve_repo_path(path_str: str | Path) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (get_repo_root() / path).resolve()


def load_registry(path: str | Path) -> dict:
    registry_path = resolve_repo_path(path)
    if not registry_path.exists():
        raise FileNotFoundError(f"Stream registry not found: {registry_path}")

    with registry_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def enabled_streams(registry: dict) -> list[dict]:
    return [s for s in registry.get("streams", []) if s.get("enabled", True)]


def build_cmd(stream_name: str, registry_path: str) -> list[str]:
    return [
        "python3",
        "/opt/spark/apps/runners/run_stream_service.py",
        "--registry",
        registry_path,
        "--stream-name",
        stream_name,
    ]


def stream_output(name: str, pipe) -> None:
    try:
        for line in iter(pipe.readline, ""):
            if not line:
                break
            print(f"[{name}] {line.rstrip()}", flush=True)
    finally:
        try:
            pipe.close()
        except Exception:
            pass


def write_status_file(streams: list[dict], status_path: Path) -> None:
    payload = {
        "ts_epoch": int(time.time()),
        "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "streams": {},
    }

    for s in streams:
        name = s["name"]
        proc = PROCS.get(name)
        meta = RETRY_META.get(name, {})
        payload["streams"][name] = {
            "enabled": s.get("enabled", True),
            "pid": proc.pid if proc and proc.poll() is None else None,
            "returncode": proc.poll() if proc else None,
            "retries": meta.get("retries", 0),
            "max_retries": meta.get("max_retries", 0),
            "backoff_seconds": meta.get("backoff_seconds", 0),
            "heartbeat_file": s.get("heartbeat_file"),
            "checkpoint_dir": s.get("checkpoint_dir"),
        }

    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def launch_stream(stream: dict, registry_path: str) -> subprocess.Popen:
    name = stream["name"]
    cmd = build_cmd(name, registry_path)

    print(f"[supervisor] launching stream '{name}'")
    print(f"[supervisor] cmd={' '.join(cmd)}")

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    PROCS[name] = proc

    t = threading.Thread(target=stream_output, args=(name, proc.stdout), daemon=True)
    t.start()
    LOG_THREADS[name] = t

    return proc


def shutdown_handler(signum, frame) -> None:
    global STOP
    STOP = True
    print(f"[supervisor] shutdown signal received: {signum}", flush=True)

    for name, proc in PROCS.items():
        if proc.poll() is None:
            print(f"[supervisor] terminating '{name}'", flush=True)
            proc.terminate()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", default="configs/streaming/stream_registry.yaml")
    parser.add_argument("--status-file", default="/tmp/health/stream_supervisor_status.json")
    parser.add_argument("--poll-seconds", type=int, default=2)
    args = parser.parse_args()

    registry_path = str(resolve_repo_path(args.registry))
    status_path = Path(args.status_file)

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    registry = load_registry(registry_path)
    streams = enabled_streams(registry)

    for s in streams:
        rp = s.get("restart_policy", {}) or {}
        RETRY_META[s["name"]] = {
            "retries": 0,
            "max_retries": int(rp.get("max_retries", 5)),
            "backoff_seconds": int(rp.get("backoff_seconds", 10)),
        }

    for s in streams:
        launch_stream(s, registry_path)

    try:
        while not STOP:
            for s in streams:
                name = s["name"]
                proc = PROCS.get(name)
                if proc is None:
                    continue

                rc = proc.poll()
                if rc is None:
                    continue

                meta = RETRY_META[name]
                if meta["retries"] >= meta["max_retries"]:
                    print(f"[supervisor] '{name}' exceeded max_retries; not restarting", flush=True)
                    continue

                meta["retries"] += 1
                backoff = meta["backoff_seconds"]
                print(
                    f"[supervisor] '{name}' exited rc={rc}; retry {meta['retries']}/{meta['max_retries']} "
                    f"after {backoff}s",
                    flush=True,
                )
                time.sleep(backoff)
                launch_stream(s, registry_path)

            write_status_file(streams, status_path)
            time.sleep(args.poll_seconds)

    finally:
        for name, proc in PROCS.items():
            if proc.poll() is None:
                try:
                    proc.terminate()
                except Exception:
                    pass

        deadline = time.time() + 20
        for name, proc in PROCS.items():
            if proc.poll() is None:
                remaining = max(1, int(deadline - time.time()))
                try:
                    proc.wait(timeout=remaining)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass

        write_status_file(streams, status_path)
        print("[supervisor] stopped", flush=True)


if __name__ == "__main__":
    main()