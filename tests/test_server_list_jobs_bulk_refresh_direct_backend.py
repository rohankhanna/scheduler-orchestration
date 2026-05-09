from __future__ import annotations

import json
import subprocess
from pathlib import Path

from fastapi.testclient import TestClient

from scheduler_orchestration.server.app import create_app


def _write_job(runtime_dir: Path, server_job_id: str) -> None:
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


def test_list_jobs_refresh_direct_persists_scope_state_when_enabled(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("SCHED_ORCH_ENABLE_BULK_REFRESH", "1")
    monkeypatch.setenv("SCHED_ORCH_BULK_REFRESH_MAX_JOBS", "10")
    monkeypatch.setenv("SCHED_ORCH_ENABLE_DIRECT_REFRESH", "1")

    server_job_id = "job-123"
    _write_job(tmp_path, server_job_id)

    def fake_run(cmd, check, capture_output, text, timeout):
        assert cmd[:3] == ["systemctl", "show", f"sched-orch-job-{server_job_id}.scope"]
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=0,
            stdout="ActiveState=active\nSubState=running\n",
            stderr="",
        )

    import scheduler_orchestration.server.app as app_mod

    monkeypatch.setattr(app_mod.subprocess, "run", fake_run, raising=True)

    client = TestClient(create_app())
    resp = client.get("/v1/jobs?refresh=1", headers={"X-API-Key": "test-key"})
    assert resp.status_code == 200

    rec = json.loads(
        (tmp_path / "scheduler-job-ledger" / "jobs" / f"{server_job_id}.json").read_text(encoding="utf-8")
    )
    assert rec["direct_scope_active_state"] == "active"
    assert rec["direct_scope_sub_state"] == "running"
    assert isinstance(rec.get("last_refresh_at"), str) and rec["last_refresh_at"].endswith("Z")


def test_list_jobs_refresh_direct_is_best_effort_when_not_enabled(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("SCHED_ORCH_ENABLE_BULK_REFRESH", "1")
    monkeypatch.setenv("SCHED_ORCH_BULK_REFRESH_MAX_JOBS", "10")

    server_job_id = "job-123"
    _write_job(tmp_path, server_job_id)

    client = TestClient(create_app())
    resp = client.get("/v1/jobs?refresh=1", headers={"X-API-Key": "test-key"})
    assert resp.status_code == 200

    rec = json.loads(
        (tmp_path / "scheduler-job-ledger" / "jobs" / f"{server_job_id}.json").read_text(encoding="utf-8")
    )
    assert "direct_scope_active_state" not in rec
    assert "direct_scope_sub_state" not in rec
