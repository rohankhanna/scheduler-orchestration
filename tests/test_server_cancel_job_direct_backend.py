from __future__ import annotations

import json
from pathlib import Path
from subprocess import CompletedProcess

from fastapi.testclient import TestClient


def _write_direct_job(runtime_dir: Path, server_job_id: str) -> None:
    jobs_dir = runtime_dir / "scheduler-job-ledger" / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    (jobs_dir / f"{server_job_id}.json").write_text(
        json.dumps(
            {
                "server_job_id": server_job_id,
                "created_at": "2026-05-09T00:00:00Z",
                "spec_sha256": "x" * 64,
                "state": "submitted",
                "scheduler_job_id": None,
                "execution_backend": "direct",
                "direct_scope_name": f"sched-orch-job-{server_job_id}.service",
                "accepted": True,
                "reason": "ok",
                "command": ["echo", "hello"],
                "spec": {"job_name": "demo"},
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def test_cancel_direct_job_executes_systemctl_kill(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("SCHED_ORCH_ENABLE_DIRECT_CANCEL", "1")

    server_job_id = "job-123"
    _write_direct_job(tmp_path, server_job_id)

    calls = []

    def fake_run(args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    import dispatch.server.app as app_mod

    monkeypatch.setattr(app_mod.subprocess, "run", fake_run, raising=True)

    from dispatch.server.app import create_app

    client = TestClient(create_app())
    r = client.delete(f"/v1/jobs/{server_job_id}", headers={"X-API-Key": "test-key"})
    assert r.status_code == 200
    assert r.json() == {"server_job_id": server_job_id, "canceled": True}

    assert len(calls) == 1
    argv = calls[0]["args"]
    assert argv == ["systemctl", "--user", "stop", f"sched-orch-job-{server_job_id}.service"]

    record_path = tmp_path / "scheduler-job-ledger" / "jobs" / f"{server_job_id}.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["state"] == "canceled"
    assert isinstance(record.get("canceled_at"), str) and record["canceled_at"].endswith("Z")


def test_cancel_direct_job_requires_explicit_enable(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    server_job_id = "job-123"
    _write_direct_job(tmp_path, server_job_id)

    from dispatch.server.app import create_app

    client = TestClient(create_app())
    r = client.delete(f"/v1/jobs/{server_job_id}", headers={"X-API-Key": "test-key"})
    assert r.status_code == 400
    assert r.json() == {"error": "bad_request"}
