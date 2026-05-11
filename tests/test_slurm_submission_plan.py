import json
from pathlib import Path


def _load_example_spec_dict() -> dict:
    example_path = Path("schemas/examples/scheduler_job_spec.example.json")
    return json.loads(example_path.read_text(encoding="utf-8"))


def test_submission_blocked_when_drain_enabled(tmp_path: Path):
    from dispatch.drain_state import set_drain_mode
    from dispatch.slurm_adapter import build_submission_plan

    spec = _load_example_spec_dict()
    drain_state_path = tmp_path / "drain_state.json"
    set_drain_mode(drain_state_path, enabled=True)

    plan = build_submission_plan(spec, drain_state_path=drain_state_path)

    assert plan["allowed"] is False
    assert plan["reason"] == "drain_enabled"
    assert plan["command"] is None


def test_submission_allowed_when_drain_disabled(tmp_path: Path):
    from dispatch.drain_state import set_drain_mode
    from dispatch.slurm_adapter import build_submission_plan

    spec = _load_example_spec_dict()
    drain_state_path = tmp_path / "drain_state.json"
    set_drain_mode(drain_state_path, enabled=False)

    plan = build_submission_plan(spec, drain_state_path=drain_state_path)

    assert plan["allowed"] is True
    assert plan["reason"] == "ok"
    assert isinstance(plan["command"], list)
    assert plan["command"][0] == "sbatch"
