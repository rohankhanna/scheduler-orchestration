from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dispatch.drain_state import set_drain_mode
from dispatch.job_ledger import list_job_paths, read_job_record_from_path, utc_now_rfc3339, write_job_record


TERMINAL_STATES = {"blocked", "failed", "canceled", "succeeded"}


@dataclass(frozen=True)
class ShutdownResult:
    drained: bool
    cancel_attempted: int
    canceled: int
    terminal: int
    remaining: int
    timed_out: bool


def _drain_state_path(runtime_dir: Path) -> Path:
    return runtime_dir / "operator" / "drain_state.json"


def _bool_env(name: str, default: str = "0") -> bool:
    import os

    val = os.environ.get(name, default).strip().lower()
    return val in {"1", "true", "yes", "on"}


def scheduler_execution_enabled() -> bool:
    return _bool_env("SCHED_ORCH_ENABLE_SCHEDULER_EXEC", "0")


def direct_cancel_enabled() -> bool:
    return _bool_env("SCHED_ORCH_ENABLE_DIRECT_CANCEL", "0")


def direct_refresh_enabled() -> bool:
    return _bool_env("SCHED_ORCH_ENABLE_DIRECT_REFRESH", "0")


def direct_state_inference_enabled() -> bool:
    return _bool_env("SCHED_ORCH_ENABLE_DIRECT_STATE_INFERENCE", "0")


def direct_payload_execution_enabled() -> bool:
    return _bool_env("SCHED_ORCH_ENABLE_DIRECT_PAYLOAD_EXEC", "0")


def _parse_rfc3339_epoch_s(ts: str | None) -> float | None:
    if not ts or not isinstance(ts, str):
        return None
    raw = ts.strip()
    if not raw:
        return None
    try:
        from datetime import datetime

        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            return None
        return dt.timestamp()
    except Exception:
        return None


def get_non_terminal_records(runtime_dir: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in list_job_paths(runtime_dir):
        record = read_job_record_from_path(path)
        state = str(record.get("state") or "").strip().lower()
        if state and state in TERMINAL_STATES:
            continue
        records.append(record)
    return records


def graceful_shutdown(
    runtime_dir: Path,
    *,
    cancel_inflight: bool = True,
    drain: bool = True,
    graceful_timeout_s: float = 60.0,
    poll_interval_s: float = 1.0,
    allow_slurm_cancel: bool = True,
    allow_direct_cancel: bool = True,
    ignore_older_than_hours: float | None = None,
) -> ShutdownResult:
    """Attempt to drain + cancel in-flight jobs and wait for terminal states.

    This function is intentionally supervisor-agnostic: it does not assume any
    particular service manager. Callers can block shutdown by waiting for this
    function to return.

    Timeout semantics:
    - `graceful_timeout_s` is a hard cap (the shutdown will not wait longer).
    - Jobs may take longer in reality; in that case `timed_out=True` and
      `remaining>0` are reported.
    """

    drained_ok = False
    if drain:
        set_drain_mode(_drain_state_path(runtime_dir), enabled=True)
        drained_ok = True

    # Import here to keep startup light for CLI importers.
    from dispatch.backends import get_backend_ops

    cancel_attempted = 0
    canceled = 0

    records = get_non_terminal_records(runtime_dir)

    now_s = time.time()
    ignore_cutoff_s: float | None = None
    if ignore_older_than_hours is not None:
        try:
            ignore_hours = float(ignore_older_than_hours)
        except Exception:
            ignore_hours = 0.0
        if ignore_hours > 0:
            ignore_cutoff_s = now_s - (ignore_hours * 3600.0)

    def _should_ignore_record(record: dict[str, Any]) -> bool:
        if ignore_cutoff_s is None:
            return False
        created_at = str(record.get("created_at") or "").strip()
        created_s = _parse_rfc3339_epoch_s(created_at)
        if created_s is None:
            return False
        return created_s < ignore_cutoff_s

    if cancel_inflight:
        for record in records:
            if _should_ignore_record(record):
                continue
            server_job_id = str(record.get("server_job_id") or "").strip()
            if not server_job_id:
                continue

            backend = str(record.get("execution_backend") or "").strip().lower()
            if not backend:
                continue

            record_state = str(record.get("state") or "").strip().lower()
            if record_state in TERMINAL_STATES:
                continue

            # Shutdown is an explicit operator action. Allow cancellation by default.
            if backend == "slurm" and not allow_slurm_cancel:
                record["cancel_failed_at"] = utc_now_rfc3339()
                record["cancel_failed_reason"] = "slurm_cancel_disabled"
                write_job_record(runtime_dir, record)
                continue

            if backend == "direct" and not allow_direct_cancel:
                record["cancel_failed_at"] = utc_now_rfc3339()
                record["cancel_failed_reason"] = "direct_cancel_disabled"
                write_job_record(runtime_dir, record)
                continue

            backend_ops = get_backend_ops(
                backend,
                drain_state_path=_drain_state_path(runtime_dir),
                direct_payload_execution_enabled=direct_payload_execution_enabled(),
                direct_refresh_enabled=direct_refresh_enabled(),
                direct_state_inference_enabled=direct_state_inference_enabled(),
            )
            if not backend_ops:
                record["cancel_failed_at"] = utc_now_rfc3339()
                record["cancel_failed_reason"] = "backend_ops_unavailable"
                write_job_record(runtime_dir, record)
                continue

            cmd = backend_ops.build_cancel_command(record)
            if not cmd:
                record["cancel_failed_at"] = utc_now_rfc3339()
                record["cancel_failed_reason"] = "cancel_command_unavailable"
                write_job_record(runtime_dir, record)
                continue

            cancel_attempted += 1

            execute_cancel = getattr(backend_ops, "execute_cancel", None)
            try:
                if execute_cancel:
                    # backend adapters may consult the enable flag; for explicit shutdown we pass True.
                    execute_cancel(record, True)
                else:
                    import subprocess

                    subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=10)
            except Exception:
                record["cancel_failed_at"] = utc_now_rfc3339()
                record["cancel_failed_reason"] = "cancel_error"
                write_job_record(runtime_dir, record)
                continue

            # Mark cancellation requested; refresh below will converge to a scheduler terminal state.
            # We intentionally do not mark this as canceled yet because a scheduler may still show
            # the job as running/pending until the cancellation takes effect.
            record["cancel_requested_at"] = utc_now_rfc3339()
            write_job_record(runtime_dir, record)
            canceled += 1

    deadline = time.time() + float(max(0.0, graceful_timeout_s))

    # Wait loop: refresh records best-effort until all terminal or timeout.
    while True:
        remaining_records = [r for r in get_non_terminal_records(runtime_dir) if not _should_ignore_record(r)]
        if not remaining_records:
            return ShutdownResult(
                drained=drained_ok,
                cancel_attempted=cancel_attempted,
                canceled=canceled,
                terminal=0,
                remaining=0,
                timed_out=False,
            )

        now = time.time()
        if now >= deadline:
            return ShutdownResult(
                drained=drained_ok,
                cancel_attempted=cancel_attempted,
                canceled=canceled,
                terminal=0,
                remaining=len(remaining_records),
                timed_out=True,
            )

        # Refresh each job individually; avoid bulk refresh gating.
        for record in remaining_records:
            backend = str(record.get("execution_backend") or "").strip().lower()
            if not backend:
                continue

            backend_ops = get_backend_ops(
                backend,
                drain_state_path=_drain_state_path(runtime_dir),
                direct_payload_execution_enabled=direct_payload_execution_enabled(),
                direct_refresh_enabled=direct_refresh_enabled(),
                direct_state_inference_enabled=direct_state_inference_enabled(),
            )
            if not backend_ops or not backend_ops.refresh_job:
                continue

            try:
                backend_ops.refresh_job(runtime_dir, record, TERMINAL_STATES)
            except Exception:
                # Best-effort: continue.
                pass

        time.sleep(float(max(0.05, poll_interval_s)))
