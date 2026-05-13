from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dispatch.drain_state import is_drain_enabled


def direct_systemd_scope_name(server_job_id: str) -> str:
    # Despite the name, we use a transient *service* unit (not a scope).
    # Rationale: systemd-run --pipe/--pty are not compatible with --scope on
    # modern systemd, and for long-running jobs we want asynchronous execution.
    return f"sched-orch-job-{server_job_id}.service"


def direct_scope_name_from_record(record: dict[str, Any]) -> str | None:
    scope_name = record.get("direct_scope_name")
    if isinstance(scope_name, str) and scope_name.strip():
        return scope_name.strip()

    server_job_id = str(record.get("server_job_id", "")).strip()
    if not server_job_id:
        return None
    return direct_systemd_scope_name(server_job_id)


def build_systemctl_cancel_direct_command(scope_name: str) -> list[str]:
    # User-mode unit, started via systemd-run --user.
    return ["systemctl", "--user", "stop", scope_name]


def build_systemd_run_direct_command(
    server_job_id: str,
    payload_argv: list[str],
    *,
    stdout_path: Path,
    stderr_path: Path,
) -> list[str]:
    # Use argv list; no shell.
    # This is the "cage": a transient systemd *service* unit with cgroup properties applied.

    slice_name = os.environ.get("SCHED_ORCH_JOBS_SLICE", "sched-orch-jobs.slice").strip()
    allowed_cpus = os.environ.get("SCHED_ORCH_JOBS_ALLOWED_CPUS", "2-19").strip()
    memory_high = os.environ.get("SCHED_ORCH_JOBS_MEMORY_HIGH", "108G").strip()
    memory_max = os.environ.get("SCHED_ORCH_JOBS_MEMORY_MAX", "113G").strip()
    cpu_weight = os.environ.get("SCHED_ORCH_JOBS_CPU_WEIGHT", "80").strip()
    io_weight = os.environ.get("SCHED_ORCH_JOBS_IO_WEIGHT", "80").strip()

    unit_name = direct_systemd_scope_name(server_job_id)

    return [
        "systemd-run",
        "--user",
        f"--unit={unit_name}",
        "--no-block",
        f"--slice={slice_name}",
        f"--property=AllowedCPUs={allowed_cpus}",
        f"--property=MemoryHigh={memory_high}",
        f"--property=MemoryMax={memory_max}",
        f"--property=CPUWeight={cpu_weight}",
        f"--property=IOWeight={io_weight}",
        f"--property=StandardOutput=append:{stdout_path}",
        f"--property=StandardError=append:{stderr_path}",
        "--",
        *payload_argv,
    ]


def direct_payload_argv_from_spec(spec: dict[str, Any]) -> list[str] | None:
    payload = spec.get("payload")
    if not isinstance(payload, dict):
        return None

    argv = payload.get("argv")
    if not isinstance(argv, list) or not argv:
        return None

    out: list[str] = []
    for item in argv:
        if not isinstance(item, str):
            return None
        s = item.strip()
        if not s:
            return None
        if "\x00" in s:
            return None
        out.append(s)

    if not out:
        return None

    # Basic sanity limit to avoid pathological request sizes.
    if len(out) > 64:
        return None

    return out


def build_direct_submission_plan(
    spec: dict[str, Any],
    drain_state_path: Path,
    *,
    payload_execution_enabled: bool,
) -> dict[str, Any]:
    """Build a plan dict for the direct execution backend.

    This mirrors the slurm plan shape so the server can stay uniform.

    Conservative defaults:
    - obey operator drain: when enabled, reject with a machine-readable reason
    - if payload execution is not enabled, return a placeholder command
    """

    if is_drain_enabled(drain_state_path):
        return {"allowed": False, "reason": "drain_enabled", "command": None}

    payload_argv = direct_payload_argv_from_spec(spec)
    if payload_argv and payload_execution_enabled:
        return {"allowed": True, "reason": "ok", "command": payload_argv}

    return {"allowed": True, "reason": "ok", "command": ["true"]}
