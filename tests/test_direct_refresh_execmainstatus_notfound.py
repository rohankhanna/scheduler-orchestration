from __future__ import annotations

import json
from pathlib import Path
from subprocess import CompletedProcess

from fastapi.testclient import TestClient


def _write_direct_job(runtime_dir: Path, server_job_id: str, *, state: str, exit_code: int | None) -> None:
    jobs_dir = runtime_dir / "scheduler-job-ledger" / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    (jobs_dir / f"{server_job_id}.json").write_text(
        json.dumps(
            {
                "server_job_id": server_job_id,
                "created_at": "2026-05-09T00:00:00Z",
                "spec_sha256": "x" * 64,
                "state": state,
                "scheduler_job_id": None,
                "exit_code": exit_code,
                "execution_backend": "direct",
                "direct_scope_name": f"sched-orch-job-{server_job_id}.service",
                "accepted": True,
                "reason": "ok",
                "command": ["bash", "-lc", "echo hello"],
                "spec": {"job_name": "demo"},
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def test_direct_refresh_not_found_with_execmainstatus_sets_terminal_state(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("SCHED_ORCH_ENABLE_DIRECT_REFRESH", "1")
    monkeypatch.setenv("SCHED_ORCH_ENABLE_DIRECT_STATE_INFERENCE", "1")

    server_job_id = "job-123"
    _write_direct_job(tmp_path, server_job_id, state="submitted", exit_code=None)

    def fake_run(args, **kwargs):
        assert args[:4] == ["systemctl", "--user", "show", f"sched-orch-job-{server_job_id}.service"]
        # This mirrors the real failure mode: the unit is already gone, but systemd still
        # reports the last ExecMainStatus.
        return CompletedProcess(
            args=args,
            returncode=3,
            stdout="LoadState=not-found\nExecMainStatus=0\n",
            stderr="",
        )

    import dispatch.direct_observer as observer_mod

    monkeypatch.setattr(observer_mod.subprocess, "run", fake_run, raising=True)

    from dispatch.server.app import create_app

    client = TestClient(create_app())
    r = client.get(f"/v1/jobs/{server_job_id}?refresh=1", headers={"X-API-Key": "test-key"})
    assert r.status_code == 200

    record_path = tmp_path / "scheduler-job-ledger" / "jobs" / f"{server_job_id}.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["exit_code"] == 0
    assert record["state"] == "succeeded"


def test_direct_refresh_does_not_regress_succeeded_to_unknown_when_not_found(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("SCHED_ORCH_ENABLE_DIRECT_REFRESH", "1")
    monkeypatch.setenv("SCHED_ORCH_ENABLE_DIRECT_STATE_INFERENCE", "1")

    server_job_id = "job-123"
    _write_direct_job(tmp_path, server_job_id, state="succeeded", exit_code=0)

    def fake_run(args, **kwargs):
        assert args[:4] == ["systemctl", "--user", "show", f"sched-orch-job-{server_job_id}.service"]
        # Observation is incomplete (unit already gone). We must not degrade state.
        return CompletedProcess(
            args=args,
            returncode=3,
            stdout="LoadState=not-found\n",
            stderr="",
        )

    import dispatch.direct_observer as observer_mod

    monkeypatch.setattr(observer_mod.subprocess, "run", fake_run, raising=True)

    from dispatch.server.app import create_app

    client = TestClient(create_app())
    r = client.get(f"/v1/jobs/{server_job_id}?refresh=1", headers={"X-API-Key": "test-key"})
    assert r.status_code == 200

    record_path = tmp_path / "scheduler-job-ledger" / "jobs" / f"{server_job_id}.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["exit_code"] == 0
    assert record["state"] == "succeeded"
