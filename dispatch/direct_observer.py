from __future__ import annotations

import subprocess
from typing import Any

from dispatch.direct_backend import direct_scope_name_from_record
from dispatch.job_ledger import utc_now_rfc3339, write_job_record


def parse_systemctl_show_properties(stdout: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip()
        if k:
            out[k] = v
    return out


def infer_direct_job_state_from_scope(record: dict[str, Any]) -> str | None:
    # Only infer terminal state when systemd says the unit is not active anymore.
    load = str(record.get("direct_scope_load_state") or "").strip().lower()
    active = str(record.get("direct_scope_active_state") or "").strip().lower()
    sub = str(record.get("direct_scope_sub_state") or "").strip().lower()
    result = str(record.get("direct_scope_result") or "").strip().lower()

    if load in {"not-found", "masked"}:
        return "unknown"

    if active in {"active", "activating", "reloading"}:
        return "running"

    if active in {"inactive", "failed", "deactivating"}:
        # If we have an exit code, trust it.
        exit_code = record.get("exit_code")
        if isinstance(exit_code, int):
            return "succeeded" if exit_code == 0 else "failed"

        # Fall back to systemd 'Result' when present.
        if result in {"success", "exit-code"}:
            # 'exit-code' may still be failure, but we don't know the code.
            return "failed" if result == "exit-code" else "succeeded"
        if result in {"timeout", "signal", "core-dump", "watchdog", "resources"}:
            return "failed"

        # If the scope is dead/inactive but we have no exit code, record unknown.
        if sub in {"dead", "failed"}:
            return "unknown"

    return None


def refresh_direct_record_from_systemctl_show(
    runtime_dir,
    record: dict[str, Any],
    *,
    direct_state_inference_enabled: bool,
) -> None:
    scope_name = direct_scope_name_from_record(record)
    if not scope_name:
        return

    cmd = [
        "systemctl",
        "show",
        scope_name,
        "--property=LoadState",
        "--property=ActiveState",
        "--property=SubState",
        "--property=Result",
        "--property=ExecMainStatus",
        "--no-pager",
    ]
    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )

    props = parse_systemctl_show_properties(proc.stdout)
    record["direct_scope_load_state"] = props.get("LoadState")
    record["direct_scope_active_state"] = props.get("ActiveState") or ("not-found" if props.get("LoadState") == "not-found" else None)
    record["direct_scope_sub_state"] = props.get("SubState")
    record["direct_scope_result"] = props.get("Result")
    exec_main_status = props.get("ExecMainStatus")
    if isinstance(exec_main_status, str) and exec_main_status.strip().isdigit():
        record["exit_code"] = int(exec_main_status.strip())
    record["last_refresh_at"] = utc_now_rfc3339()

    if direct_state_inference_enabled:
        inferred = infer_direct_job_state_from_scope(record)
        if inferred:
            record["state"] = inferred

    write_job_record(runtime_dir, record)
