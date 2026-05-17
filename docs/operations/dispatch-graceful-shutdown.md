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

## Cancellation behavior

`dispatch server shutdown` is an explicit operator action. By default it will attempt to cancel non-terminal jobs via the relevant backend adapters.

You can disable cancellation per-backend:

- `--no-slurm-cancel`
- `--no-direct-cancel`

Or disable cancellation entirely:

- `--no-cancel`

Dispatch records `cancel_failed_reason` in the job ledger when cancellation is skipped or fails.

## Stale/non-terminal ledger entries

If the ledger contains very old non-terminal entries (for example, from a previous machine run where the scheduler state is no longer queryable), shutdown may time out even though no real work is still running.

To avoid that, you can ignore ancient entries when deciding whether shutdown completed:

- `--ignore-older-than-hours N`

This does not delete or rewrite the ledger entry; it only excludes it from the shutdown completion decision for that invocation.
