from __future__ import annotations

import json
import subprocess
from pathlib import Path

from fastapi.testclient import TestClient

from dispatch.server.app import create_app


def _write_job(runtime_dir: Path, server_job_id: str, scheduler_job_id: str) -> None:
    jobs_dir = runtime_dir / "scheduler-job-ledger" / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    (jobs_dir / f"{server_job_id}.json").write_text(
        json.dumps(
            {
                "server_job_id": server_job_id,
                "created_at": "2026-05-09T00:00:00Z",
                "spec_sha256": "x" * 64,
                "state": "submitted",
                "scheduler_job_id": scheduler_job_id,
                "execution_backend": "slurm",
                "accepted": True,
                "reason": "ok",
                "command": ["sbatch", "--wrap", "true"],
                "spec": {"job_name": "demo"},
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def test_list_jobs_refresh_persists_scheduler_state(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("SCHED_ORCH_ENABLE_BULK_REFRESH", "1")
    monkeypatch.setenv("SCHED_ORCH_BULK_REFRESH_MAX_JOBS", "10")

    _write_job(tmp_path, "job-a", "111")
    _write_job(tmp_path, "job-b", "222")

    def fake_run(cmd, check, capture_output, text, timeout):
        assert cmd[0] == "squeue"
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=0,
            stdout="111|a|RUNNING|0:01|1|1|1G|None\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    client = TestClient(create_app())
    resp = client.get("/v1/jobs?refresh=1", headers={"X-API-Key": "test-key"})
    assert resp.status_code == 200

    rec_a = json.loads((tmp_path / "scheduler-job-ledger" / "jobs" / "job-a.json").read_text(encoding="utf-8"))
    rec_b = json.loads((tmp_path / "scheduler-job-ledger" / "jobs" / "job-b.json").read_text(encoding="utf-8"))

    assert rec_a["scheduler_state"] == "RUNNING"
    assert isinstance(rec_a.get("last_refresh_at"), str) and rec_a["last_refresh_at"].endswith("Z")

    assert rec_b["scheduler_state"] == "not_in_queue"
    assert isinstance(rec_b.get("last_refresh_at"), str) and rec_b["last_refresh_at"].endswith("Z")


def test_list_jobs_refresh_requires_explicit_enable(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    _write_job(tmp_path, "job-a", "111")

    client = TestClient(create_app())
    resp = client.get("/v1/jobs?refresh=1", headers={"X-API-Key": "test-key"})
    assert resp.status_code == 400
    assert resp.json() == {
        "error": "bad_request",
        "message": (
            "refresh requires SCHED_ORCH_ENABLE_BULK_REFRESH=1 for scheduler-backed jobs "
            "or SCHED_ORCH_ENABLE_DIRECT_REFRESH=1 for direct jobs"
        ),
    }
