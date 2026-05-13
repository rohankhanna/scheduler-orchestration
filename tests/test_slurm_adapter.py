import json
import shlex
from pathlib import Path

import pytest


def _load_example_spec_dict() -> dict:
    example_path = Path("schemas/examples/scheduler_job_spec.example.json")
    return json.loads(example_path.read_text(encoding="utf-8"))


def test_build_sbatch_command_includes_job_name_and_resources_and_no_wrap():
    # This test is deliberately about command construction only (dry-run behavior).
    spec = _load_example_spec_dict()

    from dispatch.slurm_adapter import build_sbatch_command

    cmd = build_sbatch_command(spec)

    # Validate shape
    assert cmd[0] == "sbatch"

    joined = " ".join(shlex.quote(x) for x in cmd)
    assert "--job-name=" in joined
    assert "--cpus-per-task=" in joined
    assert "--mem=" in joined
    assert "--wrap" not in cmd


def test_build_dependency_flag_afterok():
    spec = _load_example_spec_dict()
    spec["dependencies"]["parents"] = ["123", "456"]
    spec["dependencies"]["policy"] = "afterok"

    from dispatch.slurm_adapter import build_sbatch_command

    cmd = build_sbatch_command(spec)
    joined = " ".join(cmd)

    assert "--dependency=afterok:123:456" in joined
