from __future__ import annotations

import json
from subprocess import CompletedProcess

from fastapi.testclient import TestClient


def test_cancel_job_executes_scancel_and_updates_ledger(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("SCHED_ORCH_ENABLE_SCHEDULER_EXEC", "1")

    # Seed a submitted job record.
    server_job_id = "server-job-1"
    job_path = tmp_path / "scheduler-job-ledger" / "jobs" / f"{server_job_id}.json"
    job_path.parent.mkdir(parents=True, exist_ok=True)
    job_path.write_text(
        json.dumps(
            {
                "server_job_id": server_job_id,
                "created_at": "2026-01-01T00:00:00Z",
                "spec_sha256": "example",
                "state": "submitted",
                "scheduler_job_id": "9001",
                "execution_backend": "slurm",
                "accepted": True,
                "reason": "ok",
                "command": ["sbatch", "--wrap", "true"],
                "spec": {"job_name": "x"},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    calls = []

    def fake_run(args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    import scheduler_orchestration.server.app as app_mod

    monkeypatch.setattr(app_mod.subprocess, "run", fake_run, raising=True)

    from scheduler_orchestration.server.app import create_app

    client = TestClient(create_app())

    r = client.delete(f"/v1/jobs/{server_job_id}", headers={"X-API-Key": "test-key"})
    assert r.status_code == 200
    assert r.json() == {"server_job_id": server_job_id, "canceled": True}

    assert len(calls) == 1
    assert calls[0]["args"] == ["scancel", "9001"]
    assert calls[0]["kwargs"].get("check") is True

    updated = json.loads(job_path.read_text(encoding="utf-8"))
    assert updated["state"] == "canceled"
    assert isinstance(updated.get("canceled_at"), str)
