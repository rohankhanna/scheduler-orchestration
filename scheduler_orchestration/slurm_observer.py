from __future__ import annotations

import subprocess
from typing import Any

from scheduler_orchestration.job_ledger import utc_now_rfc3339, write_job_record
from scheduler_orchestration.slurm_adapter import build_squeue_list_command


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

    now = utc_now_rfc3339()
    for record in records:
        scheduler_job_id = str(record.get("scheduler_job_id", "")).strip()
        record["scheduler_state"] = state_by_job_id.get(scheduler_job_id, "not_in_queue")
        record["last_refresh_at"] = now
        write_job_record(runtime_dir, record)
