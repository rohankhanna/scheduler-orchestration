from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import subprocess

import os

from scheduler_orchestration.direct_backend import (
    build_direct_submission_plan,
    build_systemctl_cancel_direct_command,
    build_systemd_run_direct_command,
    direct_scope_name_from_record,
)
from scheduler_orchestration.direct_observer import refresh_direct_record_from_systemctl_show
from scheduler_orchestration.job_ledger import utc_now_rfc3339, write_job_record
from scheduler_orchestration.slurm_adapter import (
    build_scancel_command,
    build_squeue_job_query_command,
    build_submission_plan,
    parse_sbatch_submission_stdout,
)
from scheduler_orchestration.slurm_observer import parse_squeue_output, refresh_slurm_records_from_squeue_list


@dataclass(frozen=True)
class BackendOps:
    name: str

    # Build a plan dict: {allowed: bool, reason: str, command: list[str] | None}
    build_plan: Callable[[dict[str, Any], Path], dict[str, Any]]

    # Return cancellation argv for a record, or None if not applicable.
    build_cancel_command: Callable[[dict[str, Any]], list[str] | None]

    # Execute a submission plan. Should return a dict of record updates
    # (for example: scheduler_job_id, exit_code, state, log_capture).
    execute_submission: Callable[[Path, str, dict[str, Any], bool], dict[str, Any]] | None = None

    # Execute cancellation for a record.
    execute_cancel: Callable[[dict[str, Any], bool], None] | None = None

    # Refresh a single job record best-effort. Should not raise for observation failures.
    refresh_job: Callable[[Path, dict[str, Any], bool], dict[str, Any]] | None = None

    # Bulk refresh a list of records best-effort.
    bulk_refresh: Callable[[Path, list[dict[str, Any]], int, set[str]], None] | None = None

    # Return logs for the job, or None if unsupported/unavailable.
    get_logs: Callable[[Path, dict[str, Any], str], dict[str, str] | None] | None = None


def slurm_backend_ops(drain_state_path: Path) -> BackendOps:
    def _plan(spec: dict[str, Any], drain_path: Path) -> dict[str, Any]:
        return build_submission_plan(spec, drain_state_path=drain_path)

    def _cancel(record: dict[str, Any]) -> list[str] | None:
        job_id = record.get("scheduler_job_id")
        if not isinstance(job_id, str) or not job_id.strip():
            return None
        return build_scancel_command(job_id)

    def _refresh_one(runtime_dir: Path, record: dict[str, Any], *, refresh_requested: bool) -> dict[str, Any]:
        scheduler_job_id = record.get("scheduler_job_id")
        if not isinstance(scheduler_job_id, str) or not scheduler_job_id.strip():
            return record

        # Best-effort observation; never raise.
        try:
            cmd = build_squeue_job_query_command(scheduler_job_id)
            proc = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            )
            items = parse_squeue_output(proc.stdout)
            if items:
                record["scheduler_state"] = items[0].get("state")
                if refresh_requested:
                    from scheduler_orchestration.job_ledger import utc_now_rfc3339, write_job_record

                    record["last_refresh_at"] = utc_now_rfc3339()
                    write_job_record(runtime_dir, record)
        except Exception:
            return record

        return record

    def _bulk_refresh(runtime_dir: Path, records: list[dict[str, Any]], max_jobs: int, terminal_states: set[str]) -> None:
        if max_jobs <= 0:
            return

        candidates: list[dict[str, Any]] = []
        for record in records:
            if record.get("execution_backend") != "slurm":
                continue

            scheduler_job_id = record.get("scheduler_job_id")
            if not isinstance(scheduler_job_id, str) or not scheduler_job_id.strip():
                continue

            if record.get("state") in terminal_states:
                continue

            candidates.append(record)
            if len(candidates) >= max_jobs:
                break

        try:
            refresh_slurm_records_from_squeue_list(runtime_dir, candidates)
        except Exception:
            return

    def _execute_submission(runtime_dir: Path, server_job_id: str, plan: dict[str, Any], execution_enabled: bool) -> dict[str, Any]:
        if not execution_enabled:
            return {}
        if not bool(plan.get("allowed")):
            return {}

        cmd = plan.get("command")
        if not isinstance(cmd, list) or not cmd or cmd[0] != "sbatch":
            raise ValueError("bad_plan")

        try:
            proc = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
            )
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
            return {"state": "failed"}

        scheduler_job_id = parse_sbatch_submission_stdout(proc.stdout)
        if not scheduler_job_id:
            return {"state": "failed"}

        return {"scheduler_job_id": scheduler_job_id, "state": "submitted"}

    def _execute_cancel(record: dict[str, Any], execution_enabled: bool) -> None:
        if not execution_enabled:
            raise ValueError("cancel_disabled")

        cmd = _cancel(record)
        if not cmd:
            raise ValueError("bad_request")

        subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def _get_logs(runtime_dir: Path, record: dict[str, Any], server_job_id: str) -> dict[str, str] | None:
        return None

    return BackendOps(
        name="slurm",
        build_plan=lambda spec, _: _plan(spec, drain_state_path),
        build_cancel_command=_cancel,
        execute_submission=_execute_submission,
        execute_cancel=_execute_cancel,
        refresh_job=_refresh_one,
        bulk_refresh=_bulk_refresh,
        get_logs=_get_logs,
    )


def direct_backend_ops(
    drain_state_path: Path,
    *,
    payload_execution_enabled: bool,
    refresh_enabled: bool,
    state_inference_enabled: bool,
) -> BackendOps:
    def _plan(spec: dict[str, Any], drain_path: Path) -> dict[str, Any]:
        return build_direct_submission_plan(spec, drain_path, payload_execution_enabled=payload_execution_enabled)

    def _cancel(record: dict[str, Any]) -> list[str] | None:
        scope_name = direct_scope_name_from_record(record)
        if not scope_name:
            return None
        return build_systemctl_cancel_direct_command(scope_name)

    def _refresh_one(runtime_dir: Path, record: dict[str, Any], refresh_requested: bool) -> dict[str, Any]:
        if not refresh_requested:
            return record
        if not refresh_enabled:
            # Preserve existing API behavior: refresh=1 without enable gate -> 400.
            raise ValueError("refresh_disabled")

        scope_name = direct_scope_name_from_record(record)
        if not scope_name:
            # Preserve existing API behavior: no scope info -> 400.
            raise ValueError("bad_request")

        try:
            refresh_direct_record_from_systemctl_show(
                runtime_dir,
                record,
                direct_state_inference_enabled=state_inference_enabled,
            )
        except Exception:
            # Best-effort; ignore observation failures.
            return record

        return record

    def _bulk_refresh(runtime_dir: Path, records: list[dict[str, Any]], max_jobs: int, terminal_states: set[str]) -> None:
        if not refresh_enabled:
            return
        if max_jobs <= 0:
            return

        candidates: list[dict[str, Any]] = []
        for record in records:
            if record.get("execution_backend") != "direct":
                continue
            if record.get("state") in terminal_states:
                continue
            candidates.append(record)
            if len(candidates) >= max_jobs:
                break

        for rec in candidates:
            try:
                refresh_direct_record_from_systemctl_show(
                    runtime_dir,
                    rec,
                    direct_state_inference_enabled=state_inference_enabled,
                )
            except Exception:
                continue

    def _direct_log_capture_max_bytes() -> int:
        raw = os.environ.get("SCHED_ORCH_DIRECT_LOG_CAPTURE_MAX_BYTES", "65536").strip()
        try:
            n = int(raw)
        except ValueError:
            return 65536

        if n < 0:
            return 0
        if n > 10 * 1024 * 1024:
            return 10 * 1024 * 1024
        return n

    def _write_job_log_text(runtime_dir: Path, server_job_id: str, stream: str, text: str) -> None:
        path = runtime_dir / "scheduler-job-ledger" / "logs" / server_job_id / f"{stream}.txt"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        try:
            path.chmod(0o600)
        except PermissionError:
            pass

    def _execute_submission(runtime_dir: Path, server_job_id: str, plan: dict[str, Any], execution_enabled: bool) -> dict[str, Any]:
        if not execution_enabled:
            return {}
        if not bool(plan.get("allowed")):
            return {}

        payload_argv = plan.get("command")
        if not isinstance(payload_argv, list) or not payload_argv or not all(isinstance(x, str) for x in payload_argv):
            return {"state": "failed"}

        cmd = build_systemd_run_direct_command(server_job_id, payload_argv)
        try:
            proc = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
                timeout=60,
            )
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return {"exit_code": None, "state": "failed"}

        max_bytes = _direct_log_capture_max_bytes()
        raw_stdout = proc.stdout or ""
        raw_stderr = proc.stderr or ""
        stdout_text = raw_stdout[:max_bytes]
        stderr_text = raw_stderr[:max_bytes]

        _write_job_log_text(runtime_dir, server_job_id, "stdout", stdout_text)
        _write_job_log_text(runtime_dir, server_job_id, "stderr", stderr_text)

        return {
            "exit_code": int(proc.returncode),
            "log_capture": {
                "stdout": True,
                "stderr": True,
                "truncated_stdout": len(raw_stdout) > len(stdout_text),
                "truncated_stderr": len(raw_stderr) > len(stderr_text),
                "max_bytes": max_bytes,
            },
            "state": "succeeded" if proc.returncode == 0 else "failed",
        }

    def _execute_cancel(record: dict[str, Any], execution_enabled: bool) -> None:
        if not execution_enabled:
            raise ValueError("cancel_disabled")

        cmd = _cancel(record)
        if not cmd:
            raise ValueError("bad_request")

        subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def _get_logs(runtime_dir: Path, record: dict[str, Any], server_job_id: str) -> dict[str, str] | None:
        # Direct backend captures stdout/stderr into the runtime ledger logs directory.
        logs_dir = runtime_dir / "scheduler-job-ledger" / "logs" / server_job_id
        stdout_path = logs_dir / "stdout.txt"
        stderr_path = logs_dir / "stderr.txt"

        stdout = stdout_path.read_text(encoding="utf-8") if stdout_path.exists() else None
        stderr = stderr_path.read_text(encoding="utf-8") if stderr_path.exists() else None

        if stdout is None and stderr is None:
            return None

        return {"stdout": stdout or "", "stderr": stderr or ""}

    return BackendOps(
        name="direct",
        build_plan=lambda spec, _: _plan(spec, drain_state_path),
        build_cancel_command=_cancel,
        execute_submission=_execute_submission,
        execute_cancel=_execute_cancel,
        refresh_job=_refresh_one,
        bulk_refresh=_bulk_refresh,
        get_logs=_get_logs,
    )


def get_backend_ops(
    backend_name: str,
    *,
    drain_state_path: Path,
    direct_payload_execution_enabled: bool,
    direct_refresh_enabled: bool,
    direct_state_inference_enabled: bool,
) -> BackendOps | None:
    name = str(backend_name or "").strip().lower()
    if name == "slurm":
        return slurm_backend_ops(drain_state_path)
    if name == "direct":
        return direct_backend_ops(
            drain_state_path,
            payload_execution_enabled=direct_payload_execution_enabled,
            refresh_enabled=direct_refresh_enabled,
            state_inference_enabled=direct_state_inference_enabled,
        )
    return None
