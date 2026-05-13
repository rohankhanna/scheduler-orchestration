from __future__ import annotations

from pathlib import Path
from typing import Any

from dispatch.drain_state import is_drain_enabled


_DEFAULT_SQUEUE_FORMAT = "%i|%j|%T|%M|%D|%C|%m|%R"
# fields: jobid|name|state|time|nodes|cpus|min_memory|reason


def payload_argv_from_spec(spec: dict[str, Any]) -> list[str] | None:
    payload = spec.get("payload")
    if not isinstance(payload, dict):
        return None

    argv = payload.get("argv")
    if not isinstance(argv, list) or not argv:
        return None

    out: list[str] = []
    for item in argv:
        if not isinstance(item, str):
            return None
        s = item.strip()
        if not s:
            return None
        if "\x00" in s:
            return None
        out.append(s)

    if not out:
        return None
    if len(out) > 64:
        return None

    return out


def build_sbatch_command(spec: dict[str, Any]) -> list[str]:
    """Construct an sbatch command list for the given SchedulerJobSpec.

    This is intentionally "dry-run" logic: it only constructs the command.
    Execution is the responsibility of a higher-level runner.
    """

    job_name = str(spec["job_name"])

    resources = spec["resources"]
    gpus = int(resources["graphics_processing_units"])
    cpus = int(resources["central_processing_unit_cores"])
    mem_gib = int(resources["memory_gibibytes"])

    dep = spec["dependencies"]
    parents = list(dep["parents"])  # list[str]
    policy = str(dep["policy"])

    cmd: list[str] = [
        "sbatch",
        f"--job-name={job_name}",
        f"--cpus-per-task={cpus}",
        f"--mem={mem_gib}G",
    ]

    if gpus > 0:
        cmd.append(f"--gpus={gpus}")

    if parents:
        cmd.append(f"--dependency={policy}:{':'.join(parents)}")

    # The payload is executed via a generated batch script at submission time.
    # This function constructs the sbatch flags only; the caller is responsible
    # for providing the script (as a file argument or via stdin).

    return cmd


def build_squeue_job_query_command(job_id: str) -> list[str]:
    """Construct an squeue command that returns one job (machine-readable)."""

    job_id = str(job_id)
    return [
        "squeue",
        f"--jobs={job_id}",
        "--noheader",
        f"--format={_DEFAULT_SQUEUE_FORMAT}",
    ]


def build_squeue_list_command() -> list[str]:
    """Construct an squeue command that lists the queue (machine-readable)."""

    return [
        "squeue",
        "--noheader",
        f"--format={_DEFAULT_SQUEUE_FORMAT}",
    ]


def build_sacct_job_query_command(job_id: str) -> list[str]:
    # Use a machine-readable format. sacct output can contain extra lines for steps;
    # we only need the top-level job row.
    job_id = str(job_id)
    return [
        "sacct",
        f"--jobs={job_id}",
        "--noheader",
        "--parsable2",
        "--format=JobIDRaw,State,ExitCode",
    ]


def parse_sacct_output(stdout: str) -> list[dict[str, str]]:
    # parsable2 uses | delimiters.
    items: list[dict[str, str]] = []
    for raw_line in str(stdout or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 3:
            continue

        job_id, state, exit_code = parts[0], parts[1], parts[2]
        if not job_id or not state:
            continue

        # Only accept the top-level job row (ignore .batch, .extern, etc.).
        if "." in job_id:
            continue

        items.append({"job_id": job_id, "state": state, "exit_code": exit_code})

    return items


def dispatch_state_from_slurm_state(slurm_state: str) -> str | None:
    s = str(slurm_state or "").strip().upper()
    if not s:
        return None

    if s in {"PENDING", "CONFIGURING"}:
        return "submitted"
    if s in {"RUNNING", "COMPLETING"}:
        return "running"
    if s in {"COMPLETED"}:
        return "succeeded"
    if s in {"CANCELLED", "CANCELED"}:
        return "canceled"

    if s in {
        "FAILED",
        "TIMEOUT",
        "OUT_OF_MEMORY",
        "NODE_FAIL",
        "BOOT_FAIL",
        "DEADLINE",
        "PREEMPTED",
    }:
        return "failed"

    return None


def build_scancel_command(job_id: str) -> list[str]:
    """Construct an scancel command for a given job id."""

    return ["scancel", str(job_id)]


def parse_sbatch_submission_stdout(stdout: str) -> str | None:
    """Parse sbatch stdout to extract the scheduler job id."""

    import re

    m = re.search(r"Submitted batch job\s+(\d+)", str(stdout))
    if not m:
        return None
    return m.group(1)


def build_submission_plan(spec: dict[str, Any], drain_state_path: Path) -> dict[str, Any]:
    """Build a submission plan for a spec.

    This enforces operator-level drain semantics:
    - when drain is enabled, submission is blocked (no sbatch command returned)
    - when drain is disabled, sbatch command construction proceeds

    Returns a small machine-readable dict so a CLI can emit JSON without parsing
    human-formatted text.
    """

    if is_drain_enabled(drain_state_path):
        return {
            "allowed": False,
            "reason": "drain_enabled",
            "command": None,
        }

    if not payload_argv_from_spec(spec):
        return {
            "allowed": False,
            "reason": "missing_payload",
            "command": None,
        }

    return {
        "allowed": True,
        "reason": "ok",
        "command": build_sbatch_command(spec),
    }
