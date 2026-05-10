from __future__ import annotations

import json
from subprocess import CompletedProcess

from fastapi.testclient import TestClient


def _example_slurm_spec_dict() -> dict:
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


def test_submit_executes_sbatch_via_backend_layer_subprocess(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("SCHED_ORCH_ENABLE_SCHEDULER_EXEC", "1")

    import scheduler_orchestration.backends as backends_mod
    import scheduler_orchestration.server.app as server_app

    class ServerSubprocessStub:
        def run(self, *args, **kwargs):
            raise AssertionError("server.app.subprocess.run should not be called; backend layer must execute")

    class BackendSubprocessStub:
        def __init__(self):
            self.calls: list[dict] = []

        def run(self, args, **kwargs):
            self.calls.append({"args": args, "kwargs": kwargs})
            return CompletedProcess(args=args, returncode=0, stdout="Submitted batch job 9001\n", stderr="")

    backend_subprocess = BackendSubprocessStub()

    # Use distinct objects so patching can't be defeated by shared module identity.
    monkeypatch.setattr(server_app, "subprocess", ServerSubprocessStub(), raising=True)
    monkeypatch.setattr(backends_mod, "subprocess", backend_subprocess, raising=True)

    client = TestClient(server_app.create_app())
    r = client.post(
        "/v1/jobs",
        headers={"X-API-Key": "test-key"},
        json={"spec": _example_slurm_spec_dict()},
    )

    assert r.status_code == 202
    server_job_id = r.json()["server_job_id"]

    assert len(backend_subprocess.calls) == 1
    assert backend_subprocess.calls[0]["args"][0] == "sbatch"

    job_path = tmp_path / "scheduler-job-ledger" / "jobs" / f"{server_job_id}.json"
    record = json.loads(job_path.read_text(encoding="utf-8"))
    assert record["scheduler_job_id"] == "9001"
    assert record["state"] == "submitted"


def test_cancel_executes_scancel_via_backend_layer_subprocess(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("SCHED_ORCH_ENABLE_SCHEDULER_EXEC", "1")

    import scheduler_orchestration.backends as backends_mod
    import scheduler_orchestration.server.app as server_app

    server_job_id = "server-job-1"
    job_path = tmp_path / "scheduler-job-ledger" / "jobs" / f"{server_job_id}.json"
    job_path.parent.mkdir(parents=True, exist_ok=True)
    job_path.write_text(
        json.dumps(
            {
                "server_job_id": server_job_id,
                "created_at": "2026-01-01T00:00:00Z",
                "spec_sha256": "x" * 64,
                "state": "submitted",
                "scheduler_job_id": "9001",
                "execution_backend": "slurm",
                "accepted": True,
                "reason": "ok",
                "command": ["sbatch", "--wrap", "true"],
                "spec": _example_slurm_spec_dict(),
            }
        )
        + "\n",
        encoding="utf-8",
    )

    class ServerSubprocessStub:
        def run(self, *args, **kwargs):
            raise AssertionError("server.app.subprocess.run should not be called; backend layer must execute")

    class BackendSubprocessStub:
        def __init__(self):
            self.calls: list[dict] = []

        def run(self, args, **kwargs):
            self.calls.append({"args": args, "kwargs": kwargs})
            return CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    backend_subprocess = BackendSubprocessStub()

    monkeypatch.setattr(server_app, "subprocess", ServerSubprocessStub(), raising=True)
    monkeypatch.setattr(backends_mod, "subprocess", backend_subprocess, raising=True)

    client = TestClient(server_app.create_app())
    r = client.delete(f"/v1/jobs/{server_job_id}", headers={"X-API-Key": "test-key"})

    assert r.status_code == 200
    assert len(backend_subprocess.calls) == 1
    assert backend_subprocess.calls[0]["args"] == ["scancel", "9001"]

    updated = json.loads(job_path.read_text(encoding="utf-8"))
    assert updated["state"] == "canceled"
    assert isinstance(updated.get("canceled_at"), str)
