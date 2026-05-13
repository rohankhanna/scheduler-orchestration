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
    """Infer a Dispatch job state from cached `systemctl show` properties.

    Key invariants:
    - If ExecMainStatus is present (and we persisted it as `exit_code`), prefer it for
      terminal state even if LoadState becomes `not-found` for short-lived units.
    - Do not regress a terminal `succeeded` job to `failed`/`unknown` on later refreshes
      when observation becomes incomplete (common for transient units).
    """

    load = str(record.get("direct_scope_load_state") or "").strip().lower()
    active = str(record.get("direct_scope_active_state") or "").strip().lower()
    sub = str(record.get("direct_scope_sub_state") or "").strip().lower()
    result = str(record.get("direct_scope_result") or "").strip().lower()

    prior_state = str(record.get("state") or "").strip().lower()

    # If we have an exit code, trust it for terminal inference as long as the unit is
    # not currently active.
    exit_code = record.get("exit_code")
    if not (active in {"active", "activating", "reloading"}):
        if isinstance(exit_code, int):
            inferred = "succeeded" if exit_code == 0 else "failed"
            # Never regress a known success.
            if prior_state == "succeeded" and inferred != "succeeded":
                return "succeeded"
            return inferred

    if load in {"not-found", "masked"}:
        # Unit metadata is gone; keep prior terminal success if we had it.
        if prior_state == "succeeded":
            return "succeeded"
        return "unknown"

    if active in {"active", "activating", "reloading"}:
        return "running"

    if active in {"inactive", "failed", "deactivating"}:
        # Fall back to systemd 'Result' when present.
        if result in {"success", "exit-code"}:
            # 'exit-code' may still be failure, but we don't know the code.
            inferred = "failed" if result == "exit-code" else "succeeded"
            if prior_state == "succeeded" and inferred != "succeeded":
                return "succeeded"
            return inferred
        if result in {"timeout", "signal", "core-dump", "watchdog", "resources"}:
            if prior_state == "succeeded":
                return "succeeded"
            return "failed"

        # If the unit is dead/inactive but we have no exit code, record unknown.
        if sub in {"dead", "failed"}:
            if prior_state == "succeeded":
                return "succeeded"
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

    # Direct backend uses systemd-run --user; observation must use systemctl --user.
    cmd = [
        "systemctl",
        "--user",
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
