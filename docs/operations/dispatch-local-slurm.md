# Dispatch + local Slurm operator note

This note captures the minimum submission contract for running Dispatch with the `slurm` backend on a single Linux machine.

## Scope

- Goal: reliable local submission/refresh/logs with Slurm as execution backend.
- Not in scope: multi-node cluster tuning, accounting database setup, production HPC hardening.

## Required binaries in server environment

Dispatch server process must have these on PATH:

- `sbatch`
- `squeue`
- `scancel`
- `scontrol`

Check:

- `command -v sbatch squeue scancel scontrol`

## Minimal job spec (required fields)

`slurm` submission requires all of these fields:

- `job_name` (string)
- `payload.argv` (non-empty list of non-empty strings)
- `resources.central_processing_unit_cores` (int)
- `resources.memory_gibibytes` (int)
- `resources.graphics_processing_units` (int; use `0` for CPU-only)
- `dependencies.parents` (list; use `[]` when none)
- `dependencies.policy` (string; usually `afterok` when parents are present)

If any required field is missing or malformed, plan/build behavior is undefined and may fail closed.

## Minimal example (CPU)

{
  "spec": {
    "job_name": "dispatch-slurm-cpu-smoke",
    "payload": {
      "argv": ["/usr/bin/env", "bash", "-lc", "echo hello; echo err >&2"]
    },
    "resources": {
      "central_processing_unit_cores": 1,
      "memory_gibibytes": 1,
      "graphics_processing_units": 0
    },
    "dependencies": {
      "parents": [],
      "policy": "afterok"
    }
  }
}

## Majority-hardware portability rules

- Use self-contained payload argv (absolute paths or explicit interpreter).
- Do not rely on interactive-shell PATH assumptions.
- Keep CPU/memory requests conservative for single-machine local runs.
- Use `graphics_processing_units=0` unless GPU scheduling is intentionally requested.

## Local Slurm without accounting

Some local installs use `AccountingStorageType=none`.

In that mode:

- `sacct` may be unavailable for terminal-state lookup.
- Dispatch refresh uses `squeue` first, then falls back to `scontrol show job <id>` for terminal state + exit code.

This enables terminal-state resolution (`COMPLETED`, `FAILED`, etc.) without requiring Slurm accounting.

## Verify end-to-end

1) Start server with slurm backend enabled.
2) Submit a job and capture `server_job_id`.
3) Confirm `/v1/jobs/{id}` has:
   - `execution_backend = "slurm"`
   - non-null `scheduler_job_id`
4) Confirm `/v1/jobs/{id}/logs` returns stdout/stderr.

## Notes

- Logs are written under runtime ledger paths for the server job id.
- For direct backend operator guidance, see `docs/operations/restart-recovery-and-logs.md`.
