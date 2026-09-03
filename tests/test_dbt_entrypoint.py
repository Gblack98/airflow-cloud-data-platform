"""The retry logic lives in a shell script, so we exercise it with fake binaries."""

import os
import subprocess
from pathlib import Path

ENTRYPOINT = Path(__file__).resolve().parent.parent / "docker" / "dbt" / "entrypoint.sh"

GSUTIL = """#!/usr/bin/env bash
[ "$1" = "-q" ] && shift
[ "$1" = "stat" ] && exit ${STATE_FOUND:-1}
exit 0
"""

DBT = """#!/usr/bin/env bash
echo "$@" >> "$DBT_CALLS"
"""


def run_entrypoint(tmp_path, state_found):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in (("gsutil", GSUTIL), ("dbt", DBT)):
        stub = bin_dir / name
        stub.write_text(body)
        stub.chmod(0o755)

    calls = tmp_path / "calls.txt"
    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "DBT_STATE_BUCKET": "test-state",
        "DBT_SELECT": "staging",
        "DAG_ID": "dbt_stateful_retry",
        "RUN_ID": "manual__2026-01-01",
        "STATE_FOUND": "0" if state_found else "1",
        "DBT_CALLS": str(calls),
    }
    subprocess.run(["bash", str(ENTRYPOINT)], cwd=tmp_path, env=env, check=True)
    return calls.read_text().strip()


def test_first_attempt_runs_everything(tmp_path):
    assert run_entrypoint(tmp_path, state_found=False) == "run --select staging"


def test_retry_only_replays_failures_and_their_children(tmp_path):
    call = run_entrypoint(tmp_path, state_found=True)
    assert "--select staging,result:error+" in call
    assert "--state /tmp/last_run" in call
