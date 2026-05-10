from __future__ import annotations

import json
import os
from subprocess import CompletedProcess

from fastapi.testclient import TestClient


def test_get_job_with_scheduler_job_id_queries_squeue_and_updates_detail(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    # Seed a ledger record that looks like a submitted job.
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
                "spec": {"job_name": "x", "workflow_preset": "x", "device_preference": "cpu", "resources": {"graphics_processing_units": 0, "central_processing_unit_cores": 1, "memory_gibibytes": 1}, "dependencies": {"parents": [], "policy": "afterok"}, "environment_overrides": {}, "artifacts_root": "EXAMPLE_ONLY", "retry_policy": {"max_attempts": 1}, "drain_behavior": {"honor_drain": True}},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    calls = []

    def fake_run(args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        # jobid|name|state|time|nodes|cpus|min_memory|reason
        return CompletedProcess(
            args=args,
            returncode=0,
            stdout="9001|job-a|RUNNING|00:01:02|1|4|4G|None\n",
            stderr="",
        )

    import scheduler_orchestration.backends as backends_mod

    monkeypatch.setattr(backends_mod.subprocess, "run", fake_run, raising=True)

    from scheduler_orchestration.server.app import create_app

    client = TestClient(create_app())

    r = client.get(f"/v1/jobs/{server_job_id}", headers={"X-API-Key": "test-key"})
    assert r.status_code == 200
    body = r.json()

    # Ensure we queried squeue for that scheduler job id.
    assert len(calls) == 1
    assert calls[0]["args"] == [
        "squeue",
        "--jobs=9001",
        "--noheader",
        "--format=%i|%j|%T|%M|%D|%C|%m|%R",
    ]

    # The detail should reflect the scheduler state.
    assert body["scheduler_job_id"] == "9001"
    assert body["detail"]["scheduler_state"] == "RUNNING"
