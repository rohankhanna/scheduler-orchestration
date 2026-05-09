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
        "payload": {"argv": ["echo", "hello-from-direct"]},
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


def test_direct_exec_payload_uses_systemd_run_and_persists_exit_code(tmp_path, monkeypatch):
    # Enable the direct-exec backend and allow real payload execution.
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("SCHED_ORCH_EXECUTION_BACKEND", "direct")
    monkeypatch.setenv("SCHED_ORCH_ENABLE_DIRECT_EXEC", "1")
    monkeypatch.setenv("SCHED_ORCH_ENABLE_DIRECT_PAYLOAD_EXEC", "1")

    calls = []

    def fake_run(args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return CompletedProcess(args=args, returncode=0, stdout="", stderr="")

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

    assert len(calls) == 1
    argv = calls[0]["args"]

    # We should be using systemd-run to create a governed scope.
    assert argv[0] == "systemd-run"
    assert "--scope" in argv
    assert f"--unit=sched-orch-job-{server_job_id}.scope" in argv
    assert "--wait" in argv
    assert "--pipe" in argv

    # Direct payload should be passed after the "--" marker.
    assert argv[-3:] == ["--", "echo", "hello-from-direct"]

    # Durable job record should include exit code and a terminal state.
    job_path = tmp_path / "scheduler-job-ledger" / "jobs" / f"{server_job_id}.json"
    record = json.loads(job_path.read_text(encoding="utf-8"))
    assert record["execution_backend"] == "direct"
    assert record["direct_scope_name"] == f"sched-orch-job-{server_job_id}.scope"
    assert record["exit_code"] == 0
    assert record["state"] in {"succeeded", "submitted"}
    assert isinstance(record["spec_sha256"], str)
    assert len(record["spec_sha256"]) == 64
