from __future__ import annotations

import json
from pathlib import Path
from subprocess import CompletedProcess

from fastapi.testclient import TestClient


def test_slurm_refresh_uses_scontrol_when_sacct_unavailable(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    server_job_id = "server-job-1"
    job_path = tmp_path / "scheduler-job-ledger" / "jobs" / f"{server_job_id}.json"
    job_path.parent.mkdir(parents=True, exist_ok=True)
    job_path.write_text(
        json.dumps(
            {
                "server_job_id": server_job_id,
                "created_at": "2026-01-01T00:00:00Z",
                "spec_sha256": "example",
                "state": "running",
                "scheduler_job_id": "9001",
                "accepted": True,
                "reason": "ok",
                "command": ["sbatch"],
                "execution_backend": "slurm",
                "spec": {"job_name": "x"},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    calls: list[list[str]] = []

    def fake_run(args, **kwargs):
        calls.append(list(args))
        if args[0] == "squeue":
            # Job no longer appears in the queue.
            return CompletedProcess(args=args, returncode=0, stdout="", stderr="")
        if args[0] == "sacct":
            # Minimal local Slurm disables accounting.
            raise FileNotFoundError("sacct")
        if args[:3] == ["scontrol", "show", "job"]:
            return CompletedProcess(
                args=args,
                returncode=0,
                stdout="JobId=9001 JobName=x\n   JobState=COMPLETED ExitCode=0:0\n",
                stderr="",
            )
        raise AssertionError(args)

    import dispatch.server.app as app_mod

    monkeypatch.setattr(app_mod.subprocess, "run", fake_run, raising=True)

    from dispatch.server.app import create_app

    client = TestClient(create_app())
    r = client.get(f"/v1/jobs/{server_job_id}?refresh=1", headers={"X-API-Key": "test-key"})
    assert r.status_code == 200
    assert r.json()["state"] == "succeeded"

    updated = json.loads(job_path.read_text(encoding="utf-8"))
    assert updated["scheduler_state"] == "COMPLETED"
    assert updated["state"] == "succeeded"
    assert updated["exit_code"] == 0
    assert any(call[:3] == ["scontrol", "show", "job"] for call in calls)
