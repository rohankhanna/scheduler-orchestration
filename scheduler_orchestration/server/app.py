from __future__ import annotations

import hmac
import json
import os
import re
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from scheduler_orchestration.drain_state import is_drain_enabled, set_drain_mode
from scheduler_orchestration.slurm_adapter import (
    build_submission_plan,
    build_squeue_job_query_command,
    build_squeue_list_command,
)


def _runtime_dir() -> Path:
    return Path(os.environ.get("SCHED_ORCH_RUNTIME_DIR", "runtime"))


def _drain_state_path() -> Path:
    return _runtime_dir() / "operator" / "drain_state.json"


def _jobs_dir() -> Path:
    return _runtime_dir() / "scheduler-job-ledger" / "jobs"


def _job_path(server_job_id: str) -> Path:
    return _jobs_dir() / f"{server_job_id}.json"


def _write_private_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Best-effort private permissions; respect umask, then tighten.
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except PermissionError:
        # If chmod is blocked by filesystem policy, keep going.
        pass


def _require_api_key(x_api_key: str | None) -> None:
    expected = os.environ.get("SCHED_ORCH_API_KEY")
    if not expected:
        # Fail closed: server should not run without an explicit key.
        raise HTTPException(status_code=500, detail="server_misconfigured")

    if not x_api_key or not hmac.compare_digest(x_api_key, expected):
        raise HTTPException(status_code=401, detail="unauthorized")


def _parse_squeue_output(stdout: str) -> list[dict[str, str]]:
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


def _parse_sbatch_submission_stdout(stdout: str) -> str | None:
    m = re.search(r"Submitted batch job\s+(\d+)", stdout)
    if not m:
        return None
    return m.group(1)


def _scheduler_execution_enabled() -> bool:
    # Safety default: dry-run unless explicitly enabled.
    val = os.environ.get("SCHED_ORCH_ENABLE_SCHEDULER_EXEC", "0").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _direct_execution_enabled() -> bool:
    # Safety default: do not execute direct workloads unless explicitly enabled.
    val = os.environ.get("SCHED_ORCH_ENABLE_DIRECT_EXEC", "0").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _execution_backend() -> str:
    return os.environ.get("SCHED_ORCH_EXECUTION_BACKEND", "slurm").strip().lower()


def _build_systemd_run_direct_placeholder_command() -> list[str]:
    # Conservative rollout: placeholder payload only.
    # Use argv list; no shell.

    slice_name = os.environ.get("SCHED_ORCH_JOBS_SLICE", "sched-orch-jobs.slice").strip()
    allowed_cpus = os.environ.get("SCHED_ORCH_JOBS_ALLOWED_CPUS", "2-19").strip()
    memory_high = os.environ.get("SCHED_ORCH_JOBS_MEMORY_HIGH", "108G").strip()
    memory_max = os.environ.get("SCHED_ORCH_JOBS_MEMORY_MAX", "113G").strip()
    cpu_weight = os.environ.get("SCHED_ORCH_JOBS_CPU_WEIGHT", "80").strip()
    io_weight = os.environ.get("SCHED_ORCH_JOBS_IO_WEIGHT", "80").strip()

    return [
        "systemd-run",
        "--scope",
        "--wait",
        "--pipe",
        f"--slice={slice_name}",
        f"--property=AllowedCPUs={allowed_cpus}",
        f"--property=MemoryHigh={memory_high}",
        f"--property=MemoryMax={memory_max}",
        f"--property=CPUWeight={cpu_weight}",
        f"--property=IOWeight={io_weight}",
        "--",
        "true",
    ]


def create_app() -> FastAPI:
    # Fail closed: do not create an app without an explicit API key.
    if not os.environ.get("SCHED_ORCH_API_KEY"):
        raise RuntimeError("Missing required environment variable: SCHED_ORCH_API_KEY")

    app = FastAPI(title="scheduler-orchestration", version="0.1.0")

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

        return {"items": _parse_squeue_output(proc.stdout)}

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

        # Build a submission plan.
        # - slurm: existing sbatch-based plan builder
        # - direct: conservative placeholder plan (systemd-run true)
        if backend == "direct":
            if is_drain_enabled(_drain_state_path()):
                plan = {"allowed": False, "reason": "drain_enabled", "command": None}
            else:
                plan = {"allowed": True, "reason": "ok", "command": ["true"]}
        else:
            plan = build_submission_plan(spec, drain_state_path=_drain_state_path())

        created_at = datetime.now(timezone.utc).isoformat()

        scheduler_job_id: str | None = None
        exit_code: int | None = None
        state = "accepted" if plan["allowed"] else "blocked"

        if plan["allowed"] and backend == "direct" and _direct_execution_enabled():
            cmd = _build_systemd_run_direct_placeholder_command()
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
            "state": state,
            "scheduler_job_id": scheduler_job_id,
            "exit_code": exit_code,
            "execution_backend": backend,
            "accepted": bool(plan["allowed"]),
            "reason": plan["reason"],
            "command": plan["command"],
            "spec": spec,
        }
        _write_private_json(_job_path(server_job_id), record)

        if plan["allowed"] and backend != "direct" and _scheduler_execution_enabled() and state != "submitted":
            raise HTTPException(status_code=500, detail="server_error")

        return {
            "server_job_id": server_job_id,
            "accepted": bool(plan["allowed"]),
            "reason": plan["reason"],
            "command": plan["command"],
        }

    @app.get("/v1/jobs/{server_job_id}")
    def get_job(
        server_job_id: str,
        x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    ) -> dict[str, Any]:
        _require_api_key(x_api_key)

        path = _job_path(server_job_id)
        if not path.exists():
            raise HTTPException(status_code=404, detail="not_found")

        record = json.loads(path.read_text(encoding="utf-8"))

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
                items = _parse_squeue_output(proc.stdout)
                if items:
                    record["scheduler_state"] = items[0].get("state")

        return {
            "server_job_id": record.get("server_job_id", server_job_id),
            "state": record.get("state", "unknown"),
            "scheduler_job_id": record.get("scheduler_job_id"),
            "detail": record,
        }

    return app
