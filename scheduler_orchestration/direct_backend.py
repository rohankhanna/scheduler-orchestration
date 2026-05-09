from __future__ import annotations

from pathlib import Path
from typing import Any

from scheduler_orchestration.drain_state import is_drain_enabled


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
