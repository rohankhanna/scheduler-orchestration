from __future__ import annotations

import json
from subprocess import CompletedProcess

from fastapi.testclient import TestClient


def test_get_job_refresh_persists_scheduler_state_to_ledger(tmp_path, monkeypatch):
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
                "state": "submitted",
                "scheduler_job_id": "9001",
                "accepted": True,
                "reason": "ok",
                "command": ["sbatch", "--wrap", "true"],
                "execution_backend": "slurm",
                "spec": {"job_name": "x"},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    calls = []

    def fake_run(args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return CompletedProcess(
            args=args,
            returncode=0,
            stdout="9001|job-a|RUNNING|00:01:02|1|4|4G|None\n",
            stderr="",
        )

    import dispatch.server.app as app_mod

    monkeypatch.setattr(app_mod.subprocess, "run", fake_run, raising=True)

    from dispatch.server.app import create_app

    client = TestClient(create_app())

    r = client.get(f"/v1/jobs/{server_job_id}?refresh=1", headers={"X-API-Key": "test-key"})
    assert r.status_code == 200
    assert r.json()["detail"]["scheduler_state"] == "RUNNING"

    assert len(calls) == 1

    updated = json.loads(job_path.read_text(encoding="utf-8"))
    assert updated["scheduler_state"] == "RUNNING"
    assert isinstance(updated.get("last_refresh_at"), str)
