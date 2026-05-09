from __future__ import annotations

import json
from subprocess import CompletedProcess

from fastapi.testclient import TestClient


def _example_spec_dict():
    return {
        "job_name": "embedding-shard-0001",
        "workflow_preset": "embedding:v1",
        "device_preference": "gpu",
        "payload": {"argv": ["python3", "-c", "print('hello-from-direct')"]},
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


def test_direct_exec_persists_logs_and_exposes_logs_endpoint(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("SCHED_ORCH_EXECUTION_BACKEND", "direct")
    monkeypatch.setenv("SCHED_ORCH_ENABLE_DIRECT_EXEC", "1")
    monkeypatch.setenv("SCHED_ORCH_ENABLE_DIRECT_PAYLOAD_EXEC", "1")

    def fake_run(args, **kwargs):
        return CompletedProcess(args=args, returncode=0, stdout="hello-out\n", stderr="hello-err\n")

    import scheduler_orchestration.server.app as app_mod

    monkeypatch.setattr(app_mod.subprocess, "run", fake_run, raising=True)

    from scheduler_orchestration.server.app import create_app

    client = TestClient(create_app())

    submit = client.post(
        "/v1/jobs",
        headers={"X-API-Key": "test-key"},
        json={"spec": _example_spec_dict()},
    )
    assert submit.status_code == 202
    server_job_id = submit.json()["server_job_id"]

    stdout_path = tmp_path / "scheduler-job-ledger" / "logs" / server_job_id / "stdout.txt"
    stderr_path = tmp_path / "scheduler-job-ledger" / "logs" / server_job_id / "stderr.txt"
    assert stdout_path.read_text(encoding="utf-8") == "hello-out\n"
    assert stderr_path.read_text(encoding="utf-8") == "hello-err\n"

    detail = json.loads(
        (tmp_path / "scheduler-job-ledger" / "jobs" / f"{server_job_id}.json").read_text(encoding="utf-8")
    )
    assert detail["log_capture"]["stdout"] is True
    assert detail["log_capture"]["stderr"] is True

    logs = client.get(
        f"/v1/jobs/{server_job_id}/logs",
        headers={"X-API-Key": "test-key"},
    )
    assert logs.status_code == 200
    assert logs.json() == {
        "server_job_id": server_job_id,
        "execution_backend": "direct",
        "stdout": "hello-out\n",
        "stderr": "hello-err\n",
    }
