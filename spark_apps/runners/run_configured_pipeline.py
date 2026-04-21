from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to pipeline YAML")
    return parser.parse_args()


def get_repo_root() -> Path:
    return Path(os.getenv("PIPELINE_REPO_ROOT", Path(__file__).resolve().parents[2])).resolve()


def bootstrap_import_path() -> None:
    repo_root = get_repo_root()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))


def main() -> None:
    bootstrap_import_path()

    from shared.lib.spark_submit_utils import build_spark_submit_command, load_pipeline_config
    
    args = parse_args()
    config = load_pipeline_config(args.config)
    cmd = build_spark_submit_command(config)

    print("Executing command:")
    print(cmd)

    subprocess.run(cmd, shell=True, check=True)


if __name__ == "__main__":
    main()