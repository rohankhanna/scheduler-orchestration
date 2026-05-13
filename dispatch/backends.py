from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import subprocess

import os

from dispatch.direct_backend import (
    build_direct_submission_plan,
    build_systemctl_cancel_direct_command,
    build_systemd_run_direct_command,
    direct_scope_name_from_record,
    direct_systemd_scope_name,
)
from dispatch.direct_observer import refresh_direct_record_from_systemctl_show
from dispatch.job_ledger import utc_now_rfc3339, write_job_record
from dispatch.slurm_adapter import (
    build_sacct_job_query_command,
    build_scancel_command,
    build_squeue_job_query_command,
    build_submission_plan,
    dispatch_state_from_slurm_state,
    parse_sacct_output,
    payload_argv_from_spec,
    parse_sbatch_submission_stdout,
)
from dispatch.slurm_observer import parse_squeue_output, refresh_slurm_records_from_squeue_list


@dataclass(frozen=True)
class BackendOps:
    name: str

    # Build a plan dict: {allowed: bool, reason: str, command: list[str] | None}
    build_plan: Callable[[dict[str, Any], Path], dict[str, Any]]

    # Return cancellation argv for a record, or None if not applicable.
    build_cancel_command: Callable[[dict[str, Any]], list[str] | None]

    # Execute a submission plan. Should return a dict of record updates
    # (for example: scheduler_job_id, exit_code, state, log_capture).
    execute_submission: Callable[[Path, str, dict[str, Any], bool], dict[str, Any]] | None = None

    # Execute cancellation for a record.
    execute_cancel: Callable[[dict[str, Any], bool], None] | None = None

    # Refresh a single job record best-effort. Should not raise for observation failures.
    refresh_job: Callable[[Path, dict[str, Any], bool], dict[str, Any]] | None = None

    # Bulk refresh a list of records best-effort.
    bulk_refresh: Callable[[Path, list[dict[str, Any]], int, set[str]], None] | None = None

    # Return logs for the job, or None if unsupported/unavailable.
    get_logs: Callable[[Path, dict[str, Any], str], dict[str, str] | None] | None = None


def slurm_backend_ops(drain_state_path: Path) -> BackendOps:
    def _plan(spec: dict[str, Any], drain_path: Path) -> dict[str, Any]:
        return build_submission_plan(spec, drain_state_path=drain_path)

    def _cancel(record: dict[str, Any]) -> list[str] | None:
        job_id = record.get("scheduler_job_id")
        if not isinstance(job_id, str) or not job_id.strip():
            return None
        return build_scancel_command(job_id)

    def _refresh_one(runtime_dir: Path, record: dict[str, Any], *, refresh_requested: bool) -> dict[str, Any]:
        scheduler_job_id = record.get("scheduler_job_id")
        if not isinstance(scheduler_job_id, str) or not scheduler_job_id.strip():
            return record

        # Best-effort observation; never raise.
        slurm_state: str | None = None
        try:
            cmd = build_squeue_job_query_command(scheduler_job_id)
            proc = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            )
            items = parse_squeue_output(proc.stdout)
            if items:
                slurm_state = str(items[0].get("state") or "").strip() or None
        except Exception:
            slurm_state = None

        if not slurm_state:
            # Not in queue anymore; attempt to resolve final state via sacct.
            record["scheduler_state"] = "not_in_queue"
            try:
                cmd = build_sacct_job_query_command(scheduler_job_id)
                proc = subprocess.run(
                    cmd,
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                items = parse_sacct_output(proc.stdout)
                for item in items:
                    if str(item.get("job_id") or "").strip() != str(scheduler_job_id).strip():
                        continue
                    slurm_state = str(item.get("state") or "").strip() or None
                    exit_code = str(item.get("exit_code") or "").strip()
                    if exit_code and exit_code.split(":", 1)[0].isdigit():
                        record["exit_code"] = int(exit_code.split(":", 1)[0])
                    break
            except Exception:
                slurm_state = None
        else:
            record["scheduler_state"] = slurm_state

        if slurm_state:
            mapped = dispatch_state_from_slurm_state(slurm_state)
            if mapped:
                # Do not override explicit cancel state recorded by the API.
                if record.get("state") != "canceled":
                    record["state"] = mapped

        if refresh_requested:
            record["last_refresh_at"] = utc_now_rfc3339()
            write_job_record(runtime_dir, record)

        return record

    def _bulk_refresh(runtime_dir: Path, records: list[dict[str, Any]], max_jobs: int, terminal_states: set[str]) -> None:
        if max_jobs <= 0:
            return

        candidates: list[dict[str, Any]] = []
        for record in records:
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

        try:
            refresh_slurm_records_from_squeue_list(runtime_dir, candidates)
        except Exception:
            return

    def _execute_submission(runtime_dir: Path, server_job_id: str, plan: dict[str, Any], execution_enabled: bool) -> dict[str, Any]:
        if not execution_enabled:
            return {}
        if not bool(plan.get("allowed")):
            return {}

        cmd = plan.get("command")
        if not isinstance(cmd, list) or not cmd or cmd[0] != "sbatch":
            raise ValueError("bad_plan")

        spec = plan.get("spec")
        if not isinstance(spec, dict):
            raise ValueError("bad_plan")

        payload_argv = payload_argv_from_spec(spec)
        if not payload_argv:
            raise ValueError("bad_plan")

        env_overrides = spec.get("environment_overrides")
        if env_overrides is None:
            env_overrides = {}
        if not isinstance(env_overrides, dict):
            raise ValueError("bad_plan")

        import shlex

        export_lines: list[str] = []
        for k, v in env_overrides.items():
            if not isinstance(k, str) or not isinstance(v, str):
                raise ValueError("bad_plan")
            key = k.strip()
            if not key:
                raise ValueError("bad_plan")
            # Only allow normal shell variable names.
            if not key.replace("_", "A").isalnum() or not (key[0].isalpha() or key[0] == "_"):
                raise ValueError("bad_plan")
            export_lines.append(f"export {key}={shlex.quote(v)}")

        argv_str = " ".join(shlex.quote(x) for x in payload_argv)
        script_lines = ["#!/usr/bin/env bash", "set -euo pipefail", ""]
        script_lines.extend(export_lines)
        if export_lines:
            script_lines.append("")
        script_lines.append(f"exec {argv_str}")
        script_lines.append("")
        script_text = "\n".join(script_lines)

        logs_dir = runtime_dir / "scheduler-job-ledger" / "logs" / server_job_id
        logs_dir.mkdir(parents=True, exist_ok=True)

        script_path = logs_dir / "slurm-job.sh"
        script_path.write_text(script_text, encoding="utf-8")
        try:
            script_path.chmod(0o600)
        except PermissionError:
            pass

        stdout_path = logs_dir / "stdout.txt"
        stderr_path = logs_dir / "stderr.txt"

        full_cmd = list(cmd)
        full_cmd.append(f"--output={stdout_path}")
        full_cmd.append(f"--error={stderr_path}")

        try:
            proc = subprocess.run(
                full_cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
                input=script_text,
            )
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
            return {"state": "failed"}

        scheduler_job_id = parse_sbatch_submission_stdout(proc.stdout)
        if not scheduler_job_id:
            return {"state": "failed"}

        return {
            "scheduler_job_id": scheduler_job_id,
            "state": "submitted",
            "log_capture": {"stdout": True, "stderr": True},
        }

    def _execute_cancel(record: dict[str, Any], execution_enabled: bool) -> None:
        if not execution_enabled:
            raise ValueError("cancel_disabled")

        cmd = _cancel(record)
        if not cmd:
            raise ValueError("bad_request")

        subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def _get_logs(runtime_dir: Path, record: dict[str, Any], server_job_id: str) -> dict[str, str] | None:
        logs_dir = runtime_dir / "scheduler-job-ledger" / "logs" / server_job_id
        stdout_path = logs_dir / "stdout.txt"
        stderr_path = logs_dir / "stderr.txt"

        stdout = stdout_path.read_text(encoding="utf-8") if stdout_path.exists() else None
        stderr = stderr_path.read_text(encoding="utf-8") if stderr_path.exists() else None

        if stdout is None and stderr is None:
            return None

        return {"stdout": stdout or "", "stderr": stderr or ""}

    return BackendOps(
        name="slurm",
        build_plan=lambda spec, _: {**_plan(spec, drain_state_path), "spec": spec},
        build_cancel_command=_cancel,
        execute_submission=_execute_submission,
        execute_cancel=_execute_cancel,
        refresh_job=_refresh_one,
        bulk_refresh=_bulk_refresh,
        get_logs=_get_logs,
    )


def direct_backend_ops(
    drain_state_path: Path,
    *,
    payload_execution_enabled: bool,
    refresh_enabled: bool,
    state_inference_enabled: bool,
) -> BackendOps:
    def _plan(spec: dict[str, Any], drain_path: Path) -> dict[str, Any]:
        return build_direct_submission_plan(spec, drain_path, payload_execution_enabled=payload_execution_enabled)

    def _cancel(record: dict[str, Any]) -> list[str] | None:
        scope_name = direct_scope_name_from_record(record)
        if not scope_name:
            return None
        return build_systemctl_cancel_direct_command(scope_name)

    def _refresh_one(runtime_dir: Path, record: dict[str, Any], refresh_requested: bool) -> dict[str, Any]:
        if not refresh_requested:
            return record
        if not refresh_enabled:
            # Preserve existing API behavior: refresh=1 without enable gate -> 400.
            raise ValueError("refresh_disabled")

        scope_name = direct_scope_name_from_record(record)
        if not scope_name:
            # Preserve existing API behavior: no scope info -> 400.
            raise ValueError("bad_request")

        try:
            refresh_direct_record_from_systemctl_show(
                runtime_dir,
                record,
                direct_state_inference_enabled=state_inference_enabled,
            )
        except Exception:
            # Best-effort; ignore observation failures.
            return record

        return record

    def _bulk_refresh(runtime_dir: Path, records: list[dict[str, Any]], max_jobs: int, terminal_states: set[str]) -> None:
        if not refresh_enabled:
            return
        if max_jobs <= 0:
            return

        candidates: list[dict[str, Any]] = []
        for record in records:
            if record.get("execution_backend") != "direct":
                continue
            if record.get("state") in terminal_states:
                continue
            candidates.append(record)
            if len(candidates) >= max_jobs:
                break

        for rec in candidates:
            try:
                refresh_direct_record_from_systemctl_show(
                    runtime_dir,
                    rec,
                    direct_state_inference_enabled=state_inference_enabled,
                )
            except Exception:
                continue

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

    def _write_job_log_text(runtime_dir: Path, server_job_id: str, stream: str, text: str) -> None:
        path = runtime_dir / "scheduler-job-ledger" / "logs" / server_job_id / f"{stream}.txt"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        try:
            path.chmod(0o600)
        except PermissionError:
            pass

    def _execute_submission(runtime_dir: Path, server_job_id: str, plan: dict[str, Any], execution_enabled: bool) -> dict[str, Any]:
        if not execution_enabled:
            return {}
        if not bool(plan.get("allowed")):
            return {}

        payload_argv = plan.get("command")
        if not isinstance(payload_argv, list) or not payload_argv or not all(isinstance(x, str) for x in payload_argv):
            return {"state": "failed"}

        logs_dir = runtime_dir / "scheduler-job-ledger" / "logs" / server_job_id
        logs_dir.mkdir(parents=True, exist_ok=True)

        stdout_path = logs_dir / "stdout.txt"
        stderr_path = logs_dir / "stderr.txt"

        # Create the files up front so /logs can return something immediately.
        for p in (stdout_path, stderr_path):
            if not p.exists():
                p.write_text("", encoding="utf-8")
            try:
                p.chmod(0o600)
            except PermissionError:
                pass

        cmd = build_systemd_run_direct_command(
            server_job_id,
            payload_argv,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )
        try:
            proc = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return {"state": "failed"}

        if proc.returncode != 0:
            # systemd-run itself failed (job never started).
            return {"state": "failed"}

        return {
            "direct_scope_name": direct_systemd_scope_name(server_job_id),
            "log_capture": {"stdout": True, "stderr": True},
            "state": "submitted",
        }

    def _execute_cancel(record: dict[str, Any], execution_enabled: bool) -> None:
        if not execution_enabled:
            raise ValueError("cancel_disabled")

        cmd = _cancel(record)
        if not cmd:
            raise ValueError("bad_request")

        subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def _get_logs(runtime_dir: Path, record: dict[str, Any], server_job_id: str) -> dict[str, str] | None:
        # Direct backend captures stdout/stderr into the runtime ledger logs directory.
        # To avoid unbounded API responses, we apply a configurable max-bytes cap at read time.
        logs_dir = runtime_dir / "scheduler-job-ledger" / "logs" / server_job_id
        stdout_path = logs_dir / "stdout.txt"
        stderr_path = logs_dir / "stderr.txt"

        stdout = stdout_path.read_text(encoding="utf-8") if stdout_path.exists() else None
        stderr = stderr_path.read_text(encoding="utf-8") if stderr_path.exists() else None

        if stdout is None and stderr is None:
            return None

        max_bytes = _direct_log_capture_max_bytes()
        if max_bytes <= 0:
            return {"stdout": "", "stderr": ""}

        out_stdout = (stdout or "")[:max_bytes]
        out_stderr = (stderr or "")[:max_bytes]
        return {"stdout": out_stdout, "stderr": out_stderr}

    return BackendOps(
        name="direct",
        build_plan=lambda spec, _: _plan(spec, drain_state_path),
        build_cancel_command=_cancel,
        execute_submission=_execute_submission,
        execute_cancel=_execute_cancel,
        refresh_job=_refresh_one,
        bulk_refresh=_bulk_refresh,
        get_logs=_get_logs,
    )


def get_backend_ops(
    backend_name: str,
    *,
    drain_state_path: Path,
    direct_payload_execution_enabled: bool,
    direct_refresh_enabled: bool,
    direct_state_inference_enabled: bool,
) -> BackendOps | None:
    name = str(backend_name or "").strip().lower()
    if name == "slurm":
        return slurm_backend_ops(drain_state_path)
    if name == "direct":
        return direct_backend_ops(
            drain_state_path,
            payload_execution_enabled=direct_payload_execution_enabled,
            refresh_enabled=direct_refresh_enabled,
            state_inference_enabled=direct_state_inference_enabled,
        )
    return None
