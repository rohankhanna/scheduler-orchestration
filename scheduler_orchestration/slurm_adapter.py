from __future__ import annotations

from pathlib import Path
from typing import Any

from scheduler_orchestration.drain_state import is_drain_enabled


_DEFAULT_SQUEUE_FORMAT = "%i|%j|%T|%M|%D|%C|%m|%R"
# fields: jobid|name|state|time|nodes|cpus|min_memory|reason


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

    # Placeholder payload: we keep this as a minimal wrap for now.
    # The real payload wrapper will be introduced as a later task.
    cmd.extend(["--wrap", "true"])

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

    return {
        "allowed": True,
        "reason": "ok",
        "command": build_sbatch_command(spec),
    }
