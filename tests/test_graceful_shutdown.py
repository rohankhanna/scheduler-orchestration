from __future__ import annotations

from pathlib import Path

import pytest


def test_graceful_shutdown_sets_drain_and_attempts_cancel(monkeypatch, tmp_path: Path):
    from dispatch import graceful_shutdown

    runtime_dir = tmp_path / "runtime"

    # Seed one non-terminal job record.
    jobs_dir = runtime_dir / "scheduler-job-ledger" / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    (jobs_dir / "job1.json").write_text(
        '{"server_job_id":"job1","state":"running","execution_backend":"slurm","scheduler_job_id":"123"}\n',
        encoding="utf-8",
    )

    # Capture drain writes.
    drained: list[bool] = []

    def fake_set_drain_mode(path: Path, enabled: bool):
        drained.append(bool(enabled))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"enabled": true, "updated_at": "t"}\n', encoding="utf-8")
        return type("S", (), {"enabled": True, "updated_at": "t"})()

    monkeypatch.setattr(graceful_shutdown, "set_drain_mode", fake_set_drain_mode)

    # Stub backend ops.
    class Ops:
        def build_cancel_command(self, record):
            return ["scancel", str(record.get("scheduler_job_id"))]

        def refresh_job(self, runtime_dir, record, terminal_states):
            # converge to canceled
            record["state"] = "canceled"
            record["canceled_at"] = "t"
            from dispatch.job_ledger import write_job_record

            write_job_record(runtime_dir, record)

    def fake_get_backend_ops(*args, **kwargs):
        return Ops()

    monkeypatch.setenv("SCHED_ORCH_ENABLE_SCHEDULER_EXEC", "1")

    # Patch the backend factory used inside graceful_shutdown().
    monkeypatch.setattr("dispatch.backends.get_backend_ops", fake_get_backend_ops)

    # Cancel execution path: make subprocess.run succeed if used.
    monkeypatch.setattr("subprocess.run", lambda *a, **k: None, raising=False)

    result = graceful_shutdown.graceful_shutdown(runtime_dir, graceful_timeout_s=1.0, poll_interval_s=0.05)

    assert drained == [True]
    assert result.drained is True
    assert result.cancel_attempted == 1
    assert result.canceled == 1
    assert result.timed_out is False


def test_graceful_shutdown_times_out(monkeypatch, tmp_path: Path):
    from dispatch import graceful_shutdown

    runtime_dir = tmp_path / "runtime"
    jobs_dir = runtime_dir / "scheduler-job-ledger" / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    (jobs_dir / "job1.json").write_text(
        '{"server_job_id":"job1","state":"running","execution_backend":"slurm","scheduler_job_id":"123"}\n',
        encoding="utf-8",
    )

    monkeypatch.setenv("SCHED_ORCH_ENABLE_SCHEDULER_EXEC", "1")

    class Ops:
        def build_cancel_command(self, record):
            return ["scancel", "123"]

        def refresh_job(self, runtime_dir, record, terminal_states):
            # never transitions
            return

    monkeypatch.setattr("dispatch.backends.get_backend_ops", lambda *a, **k: Ops())
    monkeypatch.setattr("subprocess.run", lambda *a, **k: None, raising=False)

    result = graceful_shutdown.graceful_shutdown(runtime_dir, graceful_timeout_s=0.05, poll_interval_s=0.01)
    assert result.timed_out is True
    assert result.remaining >= 1
