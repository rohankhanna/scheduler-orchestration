from __future__ import annotations

import json
import uuid
from pathlib import Path

from fastapi.testclient import TestClient


def _example_spec_dict() -> dict:
    return {
        "job_name": "unit-test-job",
        "workflow_preset": "unit-test",
        "device_preference": "cpu",
        "resources": {"cpus": 1, "memory_mb": 128},
        "dependencies": [],
        "environment_overrides": {},
        "artifacts_root": "/tmp",
        "retry_policy": {"max_retries": 0},
        "drain_behavior": {"on_drain": "block"},
    }


def test_submit_job_builds_plan_via_backend_ops(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    from scheduler_orchestration.server import app as server_app

    calls = {"build_plan": 0}

    class StubOps:
        def __init__(self):
            self.name = "slurm"

        def build_plan(self, spec, drain_state_path):
            calls["build_plan"] += 1
            return {"allowed": True, "reason": "ok", "command": ["sbatch", "--wrap", "true"]}

        def build_cancel_command(self, record):
            raise AssertionError("cancel should not be called during submit")

    def _stub_get_backend_ops(*args, **kwargs):
        return StubOps()

    monkeypatch.setattr(server_app, "get_backend_ops", _stub_get_backend_ops)

    client = TestClient(server_app.create_app())

    r = client.post(
        "/v1/jobs",
        headers={"X-API-Key": "test-key"},
        json={"spec": _example_spec_dict()},
    )
    assert r.status_code == 202
    assert calls["build_plan"] == 1


def test_cancel_job_builds_cancel_command_via_backend_ops(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    # Keep slurm cancellation gate enabled so cancel path proceeds.
    monkeypatch.setenv("SCHED_ORCH_ENABLE_SCHEDULER_EXEC", "1")

    from scheduler_orchestration.server import app as server_app

    server_job_id = str(uuid.uuid4())

    record = {
        "server_job_id": server_job_id,
        "created_at": "2026-01-01T00:00:00Z",
        "spec_sha256": "x" * 64,
        "state": "submitted",
        "scheduler_job_id": "9001",
        "execution_backend": "slurm",
        "accepted": True,
        "reason": "ok",
        "command": ["sbatch", "--wrap", "true"],
        "spec": _example_spec_dict(),
    }

    jobs_dir = tmp_path / "scheduler-job-ledger" / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    (jobs_dir / f"{server_job_id}.json").write_text(json.dumps(record) + "\n", encoding="utf-8")

    calls = {"build_cancel_command": 0}

    class StubOps:
        def __init__(self):
            self.name = "slurm"

        def build_plan(self, spec, drain_state_path):
            raise AssertionError("build_plan should not be called during cancel")

        def build_cancel_command(self, rec):
            calls["build_cancel_command"] += 1
            return ["scancel", "9001"]

    def _stub_get_backend_ops(*args, **kwargs):
        return StubOps()

    monkeypatch.setattr(server_app, "get_backend_ops", _stub_get_backend_ops)

    # Prevent real subprocess execution; returning a success exit code is enough.
    def fake_run(args, **kwargs):
        return type("P", (), {"returncode": 0, "stdout": "", "stderr": ""})()

    monkeypatch.setattr(server_app.subprocess, "run", fake_run, raising=True)

    client = TestClient(server_app.create_app())
    r = client.delete(f"/v1/jobs/{server_job_id}", headers={"X-API-Key": "test-key"})
    assert r.status_code == 200
    assert calls["build_cancel_command"] == 1
