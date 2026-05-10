# Backend ops execution contract

This document defines the server-to-backend execution interface for `scheduler-orchestration`.

The orchestration server is intentionally thin. It owns API semantics and durable state, but delegates scheduler-specific command construction and (optionally) execution to a backend implementation.

This contract exists to make the system usable early: other applications can submit jobs against a stable local API even while individual backends are still evolving.

## Terms

- server job id: the orchestration server's stable identifier (`server_job_id`).
- scheduler job id: the scheduler's identifier (`scheduler_job_id`, for example a Slurm job id).
- plan: the backend-generated submission plan built from a job specification.

## Backend ops surface

Backends are represented by `scheduler_orchestration.backends.BackendOps`.

Required:

- `build_plan(spec: dict[str, Any], execution_enabled: bool) -> dict[str, Any]`
- `build_cancel_command(record: dict[str, Any]) -> list[str] | None`

Optional execution hooks:

- `execute_submission(runtime_dir: Path, server_job_id: str, plan: dict[str, Any], execution_enabled: bool) -> dict[str, Any] | None`
- `execute_cancel(record: dict[str, Any], execution_enabled: bool) -> None`

Observation hooks (already present):

- `refresh_job(runtime_dir: Path, record: dict[str, Any], observation_enabled: bool) -> dict[str, Any] | None`
- `bulk_refresh(runtime_dir: Path, records: list[dict[str, Any]], observation_enabled: bool) -> dict[str, Any] | None`
- `get_logs(runtime_dir: Path, record: dict[str, Any], server_job_id: str) -> dict[str, str] | None`

## Execution gating rules

The server may construct a plan even when execution is disabled.

The server passes `execution_enabled` into backend hooks as the final operator gate. Backends must treat `execution_enabled=False` as a hard no-op for execution.

Separately, submission plans carry `plan["allowed"]`:

- If `allowed` is false, the server persists the job record but must not execute.
- If `allowed` is true, execution still only occurs if `execution_enabled` is true.

The intended behavior is:

- plan allowed + execution disabled => accept + persist (dry-run semantics)
- plan blocked + execution enabled => block + persist (fail closed)

Backends should also fail closed if a plan is malformed.

## execute_submission

Signature:

- `execute_submission(runtime_dir, server_job_id, plan, execution_enabled) -> updates`

Responsibilities:

- If execution is disabled, return `{}` (or `None`) and do not attempt to execute.
- If the plan is not allowed, return `{}` (or `None`) and do not attempt to execute.
- If execution is attempted, run the backend-specific command.
- Return a dict of record updates.

Allowed updates (server will merge best-effort):

- `scheduler_job_id: str` (for scheduler backends)
- `direct_scope_name: str` (for direct backend)
- `exit_code: int | None` (only meaningful for "direct" immediate execution)
- `log_capture: dict[str, Any]` (backend-defined, but must be JSON-serializable)
- `state: str` (server-valid job state string)

Errors:

- If the plan is malformed in a way that indicates a server bug (not user input), raise `ValueError`. The server treats this as `500 server_error`.
- If the execution attempt fails (timeout, command not found, non-zero exit when that indicates submission failure), do not raise. Return updates that put the record in a failed state.

## execute_cancel

Signature:

- `execute_cancel(record, execution_enabled) -> None`

Responsibilities:

- If execution is disabled, raise `ValueError` (server maps to `400 bad_request`).
- If the record cannot be canceled (missing scheduler id / malformed), raise `ValueError` (server maps to `400 bad_request`).
- On success, return `None`.

Unexpected exceptions are treated as server errors and recorded as a cancel failure.

## Runtime directory and durable artifacts

The server and backends must only persist state under `runtime_dir`.

Current durable layout (subject to revision, but must remain backward compatible):

- `runtime/scheduler-job-ledger/jobs/<server_job_id>.json`
- `runtime/scheduler-job-ledger/logs/<server_job_id>/{stdout,stderr}.txt` (direct backend capture)
- `runtime/operator/drain_state.json`

## Backward compatibility note

The server may include fallbacks for tests/stubs when a backend does not implement an optional hook.

Production backends should implement the optional hooks as the contract stabilizes so that server code stays simple and behavior stays uniform across backends.
