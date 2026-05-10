from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import subprocess

from scheduler_orchestration.direct_backend import (
    build_direct_submission_plan,
    build_systemctl_cancel_direct_command,
    direct_scope_name_from_record,
)
from scheduler_orchestration.direct_observer import refresh_direct_record_from_systemctl_show
from scheduler_orchestration.slurm_adapter import build_scancel_command, build_squeue_job_query_command, build_submission_plan
from scheduler_orchestration.slurm_observer import parse_squeue_output, refresh_slurm_records_from_squeue_list


@dataclass(frozen=True)
class BackendOps:
    name: str

    # Build a plan dict: {allowed: bool, reason: str, command: list[str] | None}
    build_plan: Callable[[dict[str, Any], Path], dict[str, Any]]

    # Return cancellation argv for a record, or None if not applicable.
    build_cancel_command: Callable[[dict[str, Any]], list[str] | None]

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

    def _get_logs(runtime_dir: Path, record: dict[str, Any], server_job_id: str) -> dict[str, str] | None:
        return None

    return BackendOps(
        name="slurm",
        build_plan=lambda spec, _: _plan(spec, drain_state_path),
        build_cancel_command=_cancel,
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
