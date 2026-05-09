from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from scheduler_orchestration.direct_backend import (
    build_direct_submission_plan,
    build_systemctl_cancel_direct_command,
    direct_scope_name_from_record,
)
from scheduler_orchestration.slurm_adapter import build_scancel_command, build_submission_plan


@dataclass(frozen=True)
class BackendOps:
    name: str

    # Build a plan dict: {allowed: bool, reason: str, command: list[str] | None}
    build_plan: Callable[[dict[str, Any], Path], dict[str, Any]]

    # Return cancellation argv for a record, or None if not applicable.
    build_cancel_command: Callable[[dict[str, Any]], list[str] | None]


def slurm_backend_ops(drain_state_path: Path) -> BackendOps:
    def _plan(spec: dict[str, Any], drain_path: Path) -> dict[str, Any]:
        return build_submission_plan(spec, drain_state_path=drain_path)

    def _cancel(record: dict[str, Any]) -> list[str] | None:
        job_id = record.get("scheduler_job_id")
        if not isinstance(job_id, str) or not job_id.strip():
            return None
        return build_scancel_command(job_id)

    return BackendOps(name="slurm", build_plan=lambda spec, _: _plan(spec, drain_state_path), build_cancel_command=_cancel)


def direct_backend_ops(drain_state_path: Path, *, payload_execution_enabled: bool) -> BackendOps:
    def _plan(spec: dict[str, Any], drain_path: Path) -> dict[str, Any]:
        return build_direct_submission_plan(spec, drain_path, payload_execution_enabled=payload_execution_enabled)

    def _cancel(record: dict[str, Any]) -> list[str] | None:
        scope_name = direct_scope_name_from_record(record)
        if not scope_name:
            return None
        return build_systemctl_cancel_direct_command(scope_name)

    return BackendOps(name="direct", build_plan=lambda spec, _: _plan(spec, drain_state_path), build_cancel_command=_cancel)


def get_backend_ops(
    backend_name: str,
    *,
    drain_state_path: Path,
    direct_payload_execution_enabled: bool,
) -> BackendOps | None:
    name = str(backend_name or "").strip().lower()
    if name == "slurm":
        return slurm_backend_ops(drain_state_path)
    if name == "direct":
        return direct_backend_ops(drain_state_path, payload_execution_enabled=direct_payload_execution_enabled)
    return None
