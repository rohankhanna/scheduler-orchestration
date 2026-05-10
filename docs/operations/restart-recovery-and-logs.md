# Restart, recovery, and logs

This document describes the durable state owned by the orchestration layer and how operators should reason about restart safety.

## Goals

- Job submission should be durable: once accepted, there is an on-disk record.
- A server restart (or machine reboot) should not lose operator state.
- Observation should be best-effort: the server should reconcile job records against the scheduler when possible.
- Failure investigation should be possible after the fact.

## Runtime directory

All durable artifacts are stored under `SCHED_ORCH_RUNTIME_DIR`.

If unset, the default is a relative `runtime/` directory.

Recommended: set `SCHED_ORCH_RUNTIME_DIR` to an absolute path on persistent storage.

## Durable layout

- `runtime/operator/drain_state.json`
  - Operator drain / resume state.

- `runtime/scheduler-job-ledger/jobs/<server_job_id>.json`
  - One JSON record per job.
  - Records are written atomically with private permissions (0600).

- `runtime/scheduler-job-ledger/logs/<server_job_id>/`
  - Backend-provided logs.
  - The direct backend captures `stdout.txt` and `stderr.txt` during submission execution.

## Recovery model

On server startup, the server loads existing job records from the ledger.

For each record, the server attempts to refresh the record from the backend observer surfaces. Observation is designed to fail closed to the server process (that is: observation failures should not crash the service). Records may remain stale until the next successful observation.

### What is guaranteed

- Accepted submissions have a durable record on disk.
- The server can list and return those records after restart.

### What is best-effort

- Mapping a record to a live scheduler state depends on backend observation.
- Log retrieval depends on backend support.

## Logs retrieval story (iteration 1)

- Direct backend:
  - On execution, submission captures stdout/stderr into the ledger logs directory.
  - The server serves these logs through the job logs API.

- Slurm backend:
  - Iteration 1 does not yet define a single canonical log capture mechanism (Slurm output paths vary by site policy and job script conventions).
  - For now, `get_logs` may return `None` for Slurm jobs.

Planned direction (when needed): add a minimal, explicit log contract for Slurm jobs (either via declared output paths in the job spec, or via a fixed spool capture policy) so that the server can surface logs uniformly.

## Operator failure feedback loop

The system is intended to be usable before it is perfect.

The job ledger record is the primary feedback surface:

- it records submission intent (spec hash, resource request)
- it records scheduler identifiers (when known)
- it records state transitions and timestamps
- it records failure evidence available at the orchestration layer

External applications should treat the server job id as the stable handle, and report failures by referencing that id.
