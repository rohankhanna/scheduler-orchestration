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
                "direct_scope_name": f"sched-orch-job-{server_job_id}.scope",
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


def test_direct_refresh_can_promote_state_to_running(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("SCHED_ORCH_ENABLE_DIRECT_REFRESH", "1")
    monkeypatch.setenv("SCHED_ORCH_ENABLE_DIRECT_STATE_INFERENCE", "1")

    server_job_id = "job-123"
    _write_direct_job(tmp_path, server_job_id)

    def fake_run(args, **kwargs):
        assert args[:3] == ["systemctl", "show", f"sched-orch-job-{server_job_id}.scope"]
        return CompletedProcess(
            args=args,
            returncode=0,
            stdout="ActiveState=active\nSubState=running\nResult=success\n",
            stderr="",
        )

    import dispatch.server.app as app_mod

    monkeypatch.setattr(app_mod.subprocess, "run", fake_run, raising=True)

    from dispatch.server.app import create_app

    client = TestClient(create_app())
    r = client.get(f"/v1/jobs/{server_job_id}?refresh=1", headers={"X-API-Key": "test-key"})
    assert r.status_code == 200

    record_path = tmp_path / "scheduler-job-ledger" / "jobs" / f"{server_job_id}.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["state"] == "running"


def test_direct_refresh_can_promote_state_to_unknown_when_inactive_without_exit_code(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("SCHED_ORCH_ENABLE_DIRECT_REFRESH", "1")
    monkeypatch.setenv("SCHED_ORCH_ENABLE_DIRECT_STATE_INFERENCE", "1")

    server_job_id = "job-123"
    _write_direct_job(tmp_path, server_job_id)

    def fake_run(args, **kwargs):
        assert args[:3] == ["systemctl", "show", f"sched-orch-job-{server_job_id}.scope"]
        return CompletedProcess(
            args=args,
            returncode=0,
            stdout="ActiveState=inactive\nSubState=dead\nResult=success\n",
            stderr="",
        )

    import dispatch.server.app as app_mod

    monkeypatch.setattr(app_mod.subprocess, "run", fake_run, raising=True)

    from dispatch.server.app import create_app

    client = TestClient(create_app())
    r = client.get(f"/v1/jobs/{server_job_id}?refresh=1", headers={"X-API-Key": "test-key"})
    assert r.status_code == 200

    record_path = tmp_path / "scheduler-job-ledger" / "jobs" / f"{server_job_id}.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["state"] == "succeeded"
