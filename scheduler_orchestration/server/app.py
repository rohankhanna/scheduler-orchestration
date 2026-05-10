from __future__ import annotations

import hmac
import os
import re
import subprocess
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from scheduler_orchestration.backends import get_backend_ops
from scheduler_orchestration.direct_backend import (
    build_systemd_run_direct_command,
    direct_scope_name_from_record,
    direct_systemd_scope_name,
)
from scheduler_orchestration.direct_observer import refresh_direct_record_from_systemctl_show
from scheduler_orchestration.drain_state import is_drain_enabled, set_drain_mode
from scheduler_orchestration.job_ledger import (
    list_job_paths,
    read_job_record,
    read_job_record_from_path,
    spec_sha256,
    utc_now_rfc3339,
    write_job_record,
)
from scheduler_orchestration.slurm_adapter import (
    build_squeue_job_query_command,
    build_squeue_list_command,
)
from scheduler_orchestration.slurm_observer import parse_squeue_output, refresh_slurm_records_from_squeue_list


def _runtime_dir() -> Path:
    return Path(os.environ.get("SCHED_ORCH_RUNTIME_DIR", "runtime"))


def _drain_state_path() -> Path:
    return _runtime_dir() / "operator" / "drain_state.json"


def _job_logs_dir(server_job_id: str) -> Path:
    return _runtime_dir() / "scheduler-job-ledger" / "logs" / server_job_id


def _job_log_path(server_job_id: str, stream: str) -> Path:
    return _job_logs_dir(server_job_id) / f"{stream}.txt"


def _write_job_log_text(server_job_id: str, stream: str, text: str) -> None:
    path = _job_log_path(server_job_id, stream)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except PermissionError:
        pass


def _read_job_log_text(server_job_id: str, stream: str) -> str | None:
    path = _job_log_path(server_job_id, stream)
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8")


def _require_api_key(x_api_key: str | None) -> None:
    expected = os.environ.get("SCHED_ORCH_API_KEY")
    if not expected:
        # Fail closed: server should not run without an explicit key.
        raise HTTPException(status_code=500, detail="server_misconfigured")

    if not x_api_key or not hmac.compare_digest(x_api_key, expected):
        raise HTTPException(status_code=401, detail="unauthorized")


def _parse_sbatch_submission_stdout(stdout: str) -> str | None:
    m = re.search(r"Submitted batch job\s+(\d+)", stdout)
    if not m:
        return None
    return m.group(1)


def _scheduler_execution_enabled() -> bool:
    # Safety default: dry-run unless explicitly enabled.
    val = os.environ.get("SCHED_ORCH_ENABLE_SCHEDULER_EXEC", "0").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _direct_cancel_enabled() -> bool:
    # Cancellation is a control action; keep it explicitly gated for direct execution backend.
    val = os.environ.get("SCHED_ORCH_ENABLE_DIRECT_CANCEL", "0").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _direct_execution_enabled() -> bool:
    # Safety default: do not execute direct workloads unless explicitly enabled.
    val = os.environ.get("SCHED_ORCH_ENABLE_DIRECT_EXEC", "0").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _direct_payload_execution_enabled() -> bool:
    # Conservative rollout: keep direct backend "real payload" execution gated separately.
    # If this is not enabled, direct execution will run a placeholder payload ("true").
    val = os.environ.get("SCHED_ORCH_ENABLE_DIRECT_PAYLOAD_EXEC", "0").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _direct_log_capture_max_bytes() -> int:
    raw = os.environ.get("SCHED_ORCH_DIRECT_LOG_CAPTURE_MAX_BYTES", "65536").strip()
    try:
        n = int(raw)
    except ValueError:
        return 65536

    if n < 0:
        return 0
    if n > 10 * 1024 * 1024:
        return 10 * 1024 * 1024
    return n


def _execution_backend() -> str:
    return os.environ.get("SCHED_ORCH_EXECUTION_BACKEND", "slurm").strip().lower()


def _startup_reconcile_enabled() -> bool:
    # Safety default: do not run any startup reconciliation unless explicitly enabled.
    val = os.environ.get("SCHED_ORCH_ENABLE_STARTUP_RECONCILE", "0").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _startup_reconcile_max_jobs() -> int:
    raw = os.environ.get("SCHED_ORCH_STARTUP_RECONCILE_MAX_JOBS", "50").strip()
    try:
        n = int(raw)
    except ValueError:
        return 50
    return _cap_reconcile_job_limit(n)


def _bulk_refresh_enabled() -> bool:
    # Safety default: bulk refresh is an active scheduler query + ledger mutation, so keep it gated.
    val = os.environ.get("SCHED_ORCH_ENABLE_BULK_REFRESH", "0").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _bulk_refresh_max_jobs() -> int:
    raw = os.environ.get("SCHED_ORCH_BULK_REFRESH_MAX_JOBS", "200").strip()
    try:
        n = int(raw)
    except ValueError:
        return 200
    return _cap_reconcile_job_limit(n)


def _cap_reconcile_job_limit(n: int) -> int:
    if n < 0:
        return 0
    if n > 1000:
        return 1000
    return n


def _direct_refresh_enabled() -> bool:
    # Refresh is an observation/control-plane query; keep it explicitly gated for direct backend.
    val = os.environ.get("SCHED_ORCH_ENABLE_DIRECT_REFRESH", "0").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _direct_state_inference_enabled() -> bool:
    # Optional promotion from low-level scope state to high-level job state.
    val = os.environ.get("SCHED_ORCH_ENABLE_DIRECT_STATE_INFERENCE", "0").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _terminal_job_states() -> set[str]:
    return {"blocked", "failed", "canceled", "succeeded"}


def create_app() -> FastAPI:
    # Fail closed: do not create an app without an explicit API key.
    if not os.environ.get("SCHED_ORCH_API_KEY"):
        raise RuntimeError("Missing required environment variable: SCHED_ORCH_API_KEY")

    def _startup_reconcile_ledger_best_effort() -> None:
        if not _startup_reconcile_enabled():
            return

        max_jobs = _startup_reconcile_max_jobs()
        if max_jobs <= 0:
            return

        # Best-effort reconciliation only: never fail server startup due to scheduler/tooling issues.
        try:
            runtime_dir = _runtime_dir()
            paths = list_job_paths(runtime_dir)
            if not paths:
                return

            terminal_states = _terminal_job_states()

            candidates: list[dict[str, Any]] = []
            for path in paths:
                record = read_job_record_from_path(path)
                if record.get("execution_backend") != "slurm":
                    continue

                scheduler_job_id = record.get("scheduler_job_id")
                if not isinstance(scheduler_job_id, str) or not scheduler_job_id.strip():
                    continue

                if record.get("state") in terminal_states:
                    continue

                candidates.append(record)
                if len(candidates) >= max_jobs:
                    break

            refresh_slurm_records_from_squeue_list(runtime_dir, candidates)
        except Exception:
            return

    @asynccontextmanager
    async def _lifespan(app: FastAPI):
        _startup_reconcile_ledger_best_effort()
        yield

    app = FastAPI(title="scheduler-orchestration", version="0.1.0", lifespan=_lifespan)

    def _error_code_for_http(status_code: int) -> str:
        if status_code == 400:
            return "bad_request"
        if status_code == 401:
            return "unauthorized"
        if status_code == 404:
            return "not_found"
        if status_code == 500:
            return "server_error"
        return "error"

    @app.exception_handler(HTTPException)
    def _handle_http_exception(request: Request, exc: HTTPException) -> JSONResponse:
        # Security best practice: stable, non-leaky error bodies.
        code = _error_code_for_http(exc.status_code)
        return JSONResponse(status_code=exc.status_code, content={"error": code})

    @app.exception_handler(RequestValidationError)
    def _handle_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(status_code=400, content={"error": "bad_request"})

    @app.get("/v1/queue")
    def list_queue(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> dict[str, Any]:
        _require_api_key(x_api_key)

        cmd = build_squeue_list_command()
        try:
            proc = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
            raise HTTPException(status_code=500, detail="server_error")

        return {"items": parse_squeue_output(proc.stdout)}

    @app.post("/v1/drain")
    def set_drain(
        body: dict[str, Any],
        x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    ) -> dict[str, Any]:
        _require_api_key(x_api_key)

        enabled = bool(body.get("enabled"))
        state = set_drain_mode(_drain_state_path(), enabled=enabled)
        return {"enabled": state.enabled, "updated_at": state.updated_at}

    @app.post("/v1/jobs", status_code=202)
    def submit_job(
        body: dict[str, Any],
        x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    ) -> dict[str, Any]:
        _require_api_key(x_api_key)

        spec = body.get("spec")
        if not isinstance(spec, dict):
            raise HTTPException(status_code=400, detail="bad_request")

        server_job_id = str(uuid.uuid4())
        backend = _execution_backend()

        backend_ops = get_backend_ops(
            backend,
            drain_state_path=_drain_state_path(),
            direct_payload_execution_enabled=_direct_payload_execution_enabled(),
        )
        if not backend_ops:
            raise HTTPException(status_code=400, detail="bad_request")

        plan = backend_ops.build_plan(spec, _drain_state_path())

        created_at = utc_now_rfc3339()
        spec_hash = spec_sha256(spec)

        scheduler_job_id: str | None = None
        exit_code: int | None = None
        direct_scope_name: str | None = direct_systemd_scope_name(server_job_id) if backend == "direct" else None
        log_capture = {"stdout": False, "stderr": False}
        state = "accepted" if plan["allowed"] else "blocked"

        if plan["allowed"] and backend == "direct" and _direct_execution_enabled():
            payload_argv = plan.get("command")
            if not isinstance(payload_argv, list) or not payload_argv or not all(isinstance(x, str) for x in payload_argv):
                # Plan should always contain an argv list for direct backend.
                state = "failed"
            else:
                cmd = build_systemd_run_direct_command(server_job_id, payload_argv)
                try:
                    proc = subprocess.run(
                        cmd,
                        check=False,
                        capture_output=True,
                        text=True,
                        timeout=60,
                    )
                except (subprocess.TimeoutExpired, FileNotFoundError):
                    exit_code = None
                    state = "failed"
                else:
                    exit_code = int(proc.returncode)

                    max_bytes = _direct_log_capture_max_bytes()
                    raw_stdout = proc.stdout or ""
                    raw_stderr = proc.stderr or ""
                    stdout_text = raw_stdout[:max_bytes]
                    stderr_text = raw_stderr[:max_bytes]

                    _write_job_log_text(server_job_id, "stdout", stdout_text)
                    _write_job_log_text(server_job_id, "stderr", stderr_text)

                    log_capture = {
                        "stdout": True,
                        "stderr": True,
                        "truncated_stdout": len(raw_stdout) > len(stdout_text),
                        "truncated_stderr": len(raw_stderr) > len(stderr_text),
                        "max_bytes": max_bytes,
                    }

                    state = "succeeded" if proc.returncode == 0 else "failed"

        if plan["allowed"] and backend != "direct" and _scheduler_execution_enabled():
            cmd = plan.get("command")
            if not isinstance(cmd, list) or not cmd or cmd[0] != "sbatch":
                raise HTTPException(status_code=500, detail="server_error")

            try:
                proc = subprocess.run(
                    cmd,
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
            except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
                # Persist the request, but fail closed to the client.
                state = "failed"
            else:
                scheduler_job_id = _parse_sbatch_submission_stdout(proc.stdout)
                if scheduler_job_id:
                    state = "submitted"
                else:
                    state = "failed"

        record = {
            "server_job_id": server_job_id,
            "created_at": created_at,
            "spec_sha256": spec_hash,
            "state": state,
            "scheduler_job_id": scheduler_job_id,
            "exit_code": exit_code,
            "execution_backend": backend,
            "direct_scope_name": direct_scope_name,
            "log_capture": log_capture,
            "accepted": bool(plan["allowed"]),
            "reason": plan["reason"],
            "command": plan["command"],
            "spec": spec,
        }
        write_job_record(_runtime_dir(), record)

        if plan["allowed"] and backend != "direct" and _scheduler_execution_enabled() and state != "submitted":
            raise HTTPException(status_code=500, detail="server_error")

        return {
            "server_job_id": server_job_id,
            "accepted": bool(plan["allowed"]),
            "reason": plan["reason"],
            "command": plan["command"],
        }

    @app.get("/v1/jobs")
    def list_jobs(
        refresh: bool = Query(default=False),
        x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    ) -> dict[str, Any]:
        _require_api_key(x_api_key)

        runtime_dir = _runtime_dir()

        if refresh:
            if not _bulk_refresh_enabled():
                raise HTTPException(status_code=400, detail="bad_request")

            max_jobs = _bulk_refresh_max_jobs()
            if max_jobs > 0:
                try:
                    paths = list_job_paths(runtime_dir)

                    terminal_states = _terminal_job_states()

                    # Refresh slurm jobs (one squeue call).
                    slurm_candidates: list[dict[str, Any]] = []
                    for path in paths:
                        record = read_job_record_from_path(path)
                        if record.get("execution_backend") != "slurm":
                            continue

                        scheduler_job_id = record.get("scheduler_job_id")
                        if not isinstance(scheduler_job_id, str) or not scheduler_job_id.strip():
                            continue

                        if record.get("state") in terminal_states:
                            continue

                        slurm_candidates.append(record)
                        if len(slurm_candidates) >= max_jobs:
                            break

                    refresh_slurm_records_from_squeue_list(runtime_dir, slurm_candidates)

                    # Refresh direct jobs (bounded; one systemctl call per job).
                    if _direct_refresh_enabled():
                        direct_candidates: list[dict[str, Any]] = []
                        for path in paths:
                            record = read_job_record_from_path(path)
                            if record.get("execution_backend") != "direct":
                                continue

                            if record.get("state") in terminal_states:
                                continue

                            direct_candidates.append(record)
                            if len(direct_candidates) >= max_jobs:
                                break

                        for record in direct_candidates:
                            try:
                                refresh_direct_record_from_systemctl_show(
                                    runtime_dir,
                                    record,
                                    direct_state_inference_enabled=_direct_state_inference_enabled(),
                                )
                            except (subprocess.TimeoutExpired, FileNotFoundError):
                                continue
                except Exception:
                    # Best-effort only: do not fail the list endpoint for refresh failures.
                    pass

        items: list[dict[str, Any]] = []
        for path in reversed(list_job_paths(runtime_dir)):
            record = read_job_record_from_path(path)
            items.append(
                {
                    "server_job_id": record.get("server_job_id"),
                    "created_at": record.get("created_at"),
                    "state": record.get("state", "unknown"),
                    "scheduler_job_id": record.get("scheduler_job_id"),
                    "execution_backend": record.get("execution_backend"),
                    "accepted": record.get("accepted"),
                    "reason": record.get("reason"),
                    "spec_sha256": record.get("spec_sha256"),
                    "job_name": (record.get("spec") or {}).get("job_name") if isinstance(record.get("spec"), dict) else None,
                }
            )

        return {"items": items}

    @app.get("/v1/jobs/{server_job_id}")
    def get_job(
        server_job_id: str,
        refresh: bool = Query(default=False),
        x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    ) -> dict[str, Any]:
        _require_api_key(x_api_key)

        record = read_job_record(_runtime_dir(), server_job_id)
        if not record:
            raise HTTPException(status_code=404, detail="not_found")

        backend = record.get("execution_backend")

        scheduler_job_id = record.get("scheduler_job_id")
        if isinstance(scheduler_job_id, str) and scheduler_job_id.strip():
            cmd = build_squeue_job_query_command(scheduler_job_id)
            try:
                proc = subprocess.run(
                    cmd,
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
            except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
                # Do not fail the request; return the durable record as-is.
                pass
            else:
                items = parse_squeue_output(proc.stdout)
                if items:
                    record["scheduler_state"] = items[0].get("state")
                    if refresh:
                        record["last_refresh_at"] = utc_now_rfc3339()
                        write_job_record(_runtime_dir(), record)

        if backend == "direct" and refresh:
            if not _direct_refresh_enabled():
                raise HTTPException(status_code=400, detail="bad_request")

            scope_name = direct_scope_name_from_record(record)
            if not scope_name:
                raise HTTPException(status_code=400, detail="bad_request")

            try:
                refresh_direct_record_from_systemctl_show(
                    _runtime_dir(),
                    record,
                    direct_state_inference_enabled=_direct_state_inference_enabled(),
                )
            except (subprocess.TimeoutExpired, FileNotFoundError):
                # Best-effort; do not fail the request for observation errors.
                pass

        return {
            "server_job_id": record.get("server_job_id", server_job_id),
            "state": record.get("state", "unknown"),
            "scheduler_job_id": record.get("scheduler_job_id"),
            "detail": record,
        }

    @app.get("/v1/jobs/{server_job_id}/logs")
    def get_job_logs(
        server_job_id: str,
        x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    ) -> dict[str, Any]:
        _require_api_key(x_api_key)

        record = read_job_record(_runtime_dir(), server_job_id)
        if not record:
            raise HTTPException(status_code=404, detail="not_found")

        backend = record.get("execution_backend")
        if backend != "direct":
            raise HTTPException(status_code=400, detail="bad_request")

        stdout = _read_job_log_text(server_job_id, "stdout")
        stderr = _read_job_log_text(server_job_id, "stderr")
        if stdout is None and stderr is None:
            raise HTTPException(status_code=404, detail="not_found")

        return {
            "server_job_id": server_job_id,
            "execution_backend": backend,
            "stdout": stdout or "",
            "stderr": stderr or "",
        }

    @app.delete("/v1/jobs/{server_job_id}")
    def cancel_job(
        server_job_id: str,
        x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    ) -> dict[str, Any]:
        _require_api_key(x_api_key)

        record = read_job_record(_runtime_dir(), server_job_id)
        if not record:
            raise HTTPException(status_code=404, detail="not_found")

        backend = record.get("execution_backend")

        # Conservative policy: cancellation is an execution/control action, so keep it gated.
        if backend == "slurm" and not _scheduler_execution_enabled():
            raise HTTPException(status_code=400, detail="bad_request")
        if backend == "direct" and not _direct_cancel_enabled():
            raise HTTPException(status_code=400, detail="bad_request")

        backend_ops = get_backend_ops(
            str(backend or ""),
            drain_state_path=_drain_state_path(),
            direct_payload_execution_enabled=_direct_payload_execution_enabled(),
        )
        if not backend_ops:
            raise HTTPException(status_code=400, detail="bad_request")

        cmd = backend_ops.build_cancel_command(record)
        if not cmd:
            raise HTTPException(status_code=400, detail="bad_request")

        try:
            subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
            )
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
            record["state"] = "failed"
            record["cancel_failed_at"] = utc_now_rfc3339()
            write_job_record(_runtime_dir(), record)
            raise HTTPException(status_code=500, detail="server_error")

        record["state"] = "canceled"
        record["canceled_at"] = utc_now_rfc3339()
        write_job_record(_runtime_dir(), record)

        return {"server_job_id": server_job_id, "canceled": True}

    return app
