from __future__ import annotations

import json
import os
from subprocess import CompletedProcess

from fastapi.testclient import TestClient


def _example_spec_dict():
    return {
        "job_name": "embedding-shard-0001",
        "workflow_preset": "embedding:v1",
        "device_preference": "gpu",
        "resources": {
            "graphics_processing_units": 1,
            "central_processing_unit_cores": 4,
            "memory_gibibytes": 24,
        },
        "dependencies": {"parents": [], "policy": "afterok"},
        "environment_overrides": {"EXAMPLE_ONLY_ENV": "example"},
        "artifacts_root": "EXAMPLE_ONLY_/absolute/path/to/artifacts",
        "retry_policy": {"max_attempts": 3},
        "drain_behavior": {"honor_drain": True},
    }


def test_submit_job_executes_sbatch_and_persists_scheduler_job_id(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("SCHED_ORCH_ENABLE_SCHEDULER_EXEC", "1")

    calls = []

    def fake_run(args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return CompletedProcess(
            args=args,
            returncode=0,
            stdout="Submitted batch job 9001\n",
            stderr="",
        )

    import scheduler_orchestration.server.app as app_mod

    monkeypatch.setattr(app_mod.subprocess, "run", fake_run, raising=True)

    from scheduler_orchestration.server.app import create_app

    client = TestClient(create_app())

    r = client.post(
        "/v1/jobs",
        headers={"X-API-Key": "test-key"},
        json={"spec": _example_spec_dict()},
    )
    assert r.status_code == 202
    server_job_id = r.json()["server_job_id"]

    # Ensure sbatch ran with a safe argv list (no shell) and bounded exec.
    assert len(calls) == 1
    assert calls[0]["args"][0] == "sbatch"
    assert calls[0]["kwargs"].get("check") is True
    assert calls[0]["kwargs"].get("text") is True
    assert calls[0]["kwargs"].get("capture_output") is True
    assert isinstance(calls[0]["kwargs"].get("timeout"), (int, float))

    # Durable job record should now include scheduler_job_id and state=submitted.
    job_path = tmp_path / "scheduler-job-ledger" / "jobs" / f"{server_job_id}.json"
    record = json.loads(job_path.read_text(encoding="utf-8"))
    assert record["scheduler_job_id"] == "9001"
    assert record["state"] == "submitted"
    assert isinstance(record["spec_sha256"], str)
    assert len(record["spec_sha256"]) == 64
