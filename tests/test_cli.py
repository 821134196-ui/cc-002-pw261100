"""End-to-end CLI test driving `python -m busflow` in a subprocess."""
import os
import subprocess
import sys
from pathlib import Path

from helpers import make_v1, make_v2, write_jsonl

ROOT = Path(__file__).resolve().parents[1]


def run_cli(*args):
    env = dict(os.environ, PYTHONPATH=str(ROOT))
    return subprocess.run(
        [sys.executable, "-m", "busflow", *[str(a) for a in args]],
        capture_output=True, text=True, env=env, cwd=ROOT,
    )


def test_cli_full_workflow(tmp_path):
    data = tmp_path / "data"
    f1 = write_jsonl(tmp_path / "day1.jsonl", [
        make_v1("E1", board="2026-11-01 08:15:00"),
        make_v2("E2", line="7", board="2026-11-01T09:00:00Z"),
        "{broken json",
    ])
    f2 = write_jsonl(tmp_path / "conflict.jsonl", [
        make_v1("E1", station="Other", board="2026-11-01 10:15:00"),
    ])

    r = run_cli("--data-dir", data, "ingest", f1)
    assert r.returncode == 0, r.stderr
    assert "2 accepted" in r.stdout and "1 invalid" in r.stdout

    # idempotent re-import
    r = run_cli("--data-dir", data, "ingest", f1)
    assert r.returncode == 0 and "skipped" in r.stdout

    r = run_cli("--data-dir", data, "ingest", f2)
    assert r.returncode == 0 and "1 conflicts" in r.stdout

    r = run_cli("--data-dir", data, "status")
    assert r.returncode == 0, r.stderr
    assert "Events: 2 total" in r.stdout
    assert "Quarantine: 2 pending" in r.stdout

    r = run_cli("--data-dir", data, "stats")
    assert r.returncode == 0, r.stderr
    assert "unique_passengers" in r.stdout and "2026-11-01" in r.stdout

    r = run_cli("--data-dir", data, "recompute", "--all")
    assert r.returncode == 0 and "recomputed 1 date(s)" in r.stdout

    r = run_cli("--data-dir", data, "reprocess-quarantine",
                "--strategy", "use-quarantined")
    assert r.returncode == 0 and "1 ingested" in r.stdout

    # the conflict was resolved in favor of the quarantined (hour-10) record
    r = run_cli("--data-dir", data, "stats", "--date", "2026-11-01")
    assert r.returncode == 0
    lines = [ln for ln in r.stdout.splitlines() if ln.startswith("2026-11-01")]
    hours = {ln.split()[2] for ln in lines}
    assert "10" in hours and "8" not in hours


def test_cli_ingest_missing_file_fails(tmp_path):
    r = run_cli("--data-dir", tmp_path / "data", "ingest",
                tmp_path / "nope.jsonl")
    assert r.returncode == 1
    assert "no such file" in r.stderr
