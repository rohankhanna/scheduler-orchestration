from __future__ import annotations

import json
import subprocess
from pathlib import Path

from fastapi.testclient import TestClient

import scheduler_orchestration.backends
from scheduler_orchestration.job_ledger import write_job_record
from scheduler_orchestration.server.app import create_app


def _write_job(runtime_dir: Path, server_job_id: str, scheduler_job_id: str) -> None:
    jobs_dir = runtime_dir / "scheduler-job-ledger" / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    (jobs_dir / f"{server_job_id}.json").write_text(
        json.dumps(
            {
                "server_job_id": server_job_id,
                "created_at": "2026-05-09T00:00:00Z",
                "spec_sha256": "x" * 64,
                "state": "submitted",
                "scheduler_job_id": scheduler_job_id,
                "execution_backend": "slurm",
                "accepted": True,
                "reason": "ok",
                "command": ["sbatch", "--wrap", "true"],
                "spec": {"job_name": "demo"},
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def test_startup_reconciles_slurm_jobs_via_backend_ops(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("SCHED_ORCH_ENABLE_STARTUP_RECONCILE", "1")

    _write_job(tmp_path, "job-a", "111")
    _write_job(tmp_path, "job-b", "222")

    called: dict[str, object] = {}

    def fake_bulk_refresh(runtime_dir: Path, records: list[dict[str, object]], max_jobs: int, terminal_states: set[str]) -> None:
        called["runtime_dir"] = runtime_dir
        called["records"] = records
        called["max_jobs"] = max_jobs
        called["terminal_states"] = terminal_states

        for record in records:
            if record.get("server_job_id") == "job-a":
                record["scheduler_state"] = "RUNNING"
                record["last_refresh_at"] = "2026-05-10T00:00:00Z"
                write_job_record(runtime_dir, record)
            elif record.get("server_job_id") == "job-b":
                record["scheduler_state"] = "not_in_queue"
                record["last_refresh_at"] = "2026-05-10T00:00:01Z"
                write_job_record(runtime_dir, record)

    original_factory = scheduler_orchestration.backends.slurm_backend_ops

    def fake_slurm_backend_ops(drain_state_path: Path):
        ops = original_factory(drain_state_path)
        return ops.__class__(
            name=ops.name,
            build_plan=ops.build_plan,
            build_cancel_command=ops.build_cancel_command,
            refresh_job=ops.refresh_job,
            bulk_refresh=fake_bulk_refresh,
            get_logs=ops.get_logs,
        )

    monkeypatch.setattr(scheduler_orchestration.backends, "slurm_backend_ops", fake_slurm_backend_ops)

    def fail_if_squeue_called(cmd, check, capture_output, text, timeout):
        raise AssertionError(f"startup reconcile should route through backend ops, got subprocess.run({cmd!r})")

    monkeypatch.setattr(subprocess, "run", fail_if_squeue_called)

    # Startup handlers run when entering the TestClient context manager.
    with TestClient(create_app()):
        pass

    assert called["runtime_dir"] == tmp_path
    assert called["max_jobs"] == 50
    assert called["terminal_states"] == {"blocked", "failed", "canceled", "succeeded"}
    assert [record.get("server_job_id") for record in called["records"]] == ["job-a", "job-b"]

    rec_a = json.loads((tmp_path / "scheduler-job-ledger" / "jobs" / "job-a.json").read_text(encoding="utf-8"))
    rec_b = json.loads((tmp_path / "scheduler-job-ledger" / "jobs" / "job-b.json").read_text(encoding="utf-8"))

    assert rec_a["scheduler_state"] == "RUNNING"
    assert rec_a["last_refresh_at"] == "2026-05-10T00:00:00Z"

    assert rec_b["scheduler_state"] == "not_in_queue"
    assert rec_b["last_refresh_at"] == "2026-05-10T00:00:01Z"
