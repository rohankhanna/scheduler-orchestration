# Dispatch graceful shutdown (drain + cancel + bounded wait)

This note documents the `dispatch server shutdown` command.

## Goal

Allow a supervisor (or a human) to block process shutdown until Dispatch has:

1) enabled drain mode (no new admissions)
2) requested cancellation for in-flight jobs
3) waited a bounded amount of time for jobs to converge to terminal state
4) then stopped the Dispatch server process

This feature is implemented entirely in Dispatch. It does not assume or reference any specific external service manager.

## Command

- `dispatch server shutdown --runtime-dir <RUNTIME_DIR>`

Key flags:

- `--graceful-timeout 60` (default: 60s)
  - maximum time to wait for jobs to reach terminal state
- `--poll-interval 1.0`
  - refresh cadence while waiting
- `--stop-timeout 5.0`
  - max time to wait for the server process to exit before SIGKILL
- `--no-drain`
  - skip enabling drain mode
- `--no-cancel`
  - do not request cancellation; only wait for terminal states

## Timeout semantics

- The graceful timeout is a hard cap for this invocation.
- If jobs do not reach terminal state before the deadline, the command exits non-zero.

## Cancellation gating

Cancellation is a control action and is intentionally gated by environment variables:

- slurm backend cancellation requires: `SCHED_ORCH_ENABLE_SCHEDULER_EXEC=1`
- direct backend cancellation requires: `SCHED_ORCH_ENABLE_DIRECT_CANCEL=1`

If cancellation is not enabled, Dispatch records `cancel_failed_reason` in the job ledger and may time out.
