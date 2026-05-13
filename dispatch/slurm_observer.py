from __future__ import annotations

import subprocess
from typing import Any

from dispatch.job_ledger import utc_now_rfc3339, write_job_record
from dispatch.slurm_adapter import (
    build_sacct_job_query_command,
    build_squeue_list_command,
    dispatch_state_from_slurm_state,
    parse_sacct_output,
)


def parse_squeue_output(stdout: str) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        parts = line.split("|", 7)
        if len(parts) != 8:
            continue

        job_id, name, state, elapsed, nodes, cpus, memory, reason = [p.strip() for p in parts]
        items.append(
            {
                "job_id": job_id,
                "name": name,
                "state": state,
                "time": elapsed,
                "nodes": nodes,
                "cpus": cpus,
                "memory": memory,
                "reason": reason,
            }
        )

    return items


def refresh_slurm_records_from_squeue_list(runtime_dir, records: list[dict[str, Any]]) -> None:
    if not records:
        return

    cmd = build_squeue_list_command()
    proc = subprocess.run(
        cmd,
        check=True,
        capture_output=True,
        text=True,
        timeout=5,
    )

    state_by_job_id: dict[str, str] = {}
    for item in parse_squeue_output(proc.stdout):
        job_id = str(item.get("job_id", "")).strip()
        state = str(item.get("state", "")).strip()
        if job_id and state:
            state_by_job_id[job_id] = state

    # Best-effort: for jobs no longer in squeue, try sacct for final state/exit code.
    sacct_by_job_id: dict[str, dict[str, str]] = {}
    missing_job_ids: list[str] = []
    for record in records:
        scheduler_job_id = str(record.get("scheduler_job_id", "")).strip()
        if not scheduler_job_id:
            continue
        if scheduler_job_id in state_by_job_id:
            continue
        missing_job_ids.append(scheduler_job_id)

    for job_id in sorted(set(missing_job_ids)):
        try:
            cmd = build_sacct_job_query_command(job_id)
            proc = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            )
            for item in parse_sacct_output(proc.stdout):
                if str(item.get("job_id") or "").strip() == job_id:
                    sacct_by_job_id[job_id] = item
                    break
        except Exception:
            continue

    now = utc_now_rfc3339()
    for record in records:
        scheduler_job_id = str(record.get("scheduler_job_id", "")).strip()

        slurm_state = state_by_job_id.get(scheduler_job_id)
        if slurm_state:
            record["scheduler_state"] = slurm_state
            mapped = dispatch_state_from_slurm_state(slurm_state)
            if mapped and record.get("state") != "canceled":
                record["state"] = mapped
        else:
            record["scheduler_state"] = "not_in_queue"
            item = sacct_by_job_id.get(scheduler_job_id)
            if item:
                mapped = dispatch_state_from_slurm_state(str(item.get("state") or ""))
                exit_code = str(item.get("exit_code") or "").strip()
                if exit_code and exit_code.split(":", 1)[0].isdigit():
                    record["exit_code"] = int(exit_code.split(":", 1)[0])
                if mapped and record.get("state") != "canceled":
                    record["state"] = mapped

        record["last_refresh_at"] = now
        write_job_record(runtime_dir, record)
