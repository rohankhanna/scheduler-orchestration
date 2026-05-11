from pathlib import Path


def test_drain_state_defaults_to_disabled_when_file_missing(tmp_path: Path):
    from dispatch.drain_state import is_drain_enabled

    state_path = tmp_path / "drain_state.json"
    assert is_drain_enabled(state_path) is False


def test_set_drain_mode_true_persists_and_reads_back(tmp_path: Path):
    from dispatch.drain_state import is_drain_enabled, set_drain_mode

    state_path = tmp_path / "drain_state.json"

    set_drain_mode(state_path, enabled=True)

    assert state_path.exists()
    assert is_drain_enabled(state_path) is True


def test_set_drain_mode_false_persists_and_reads_back(tmp_path: Path):
    from dispatch.drain_state import is_drain_enabled, set_drain_mode

    state_path = tmp_path / "drain_state.json"

    set_drain_mode(state_path, enabled=True)
    assert is_drain_enabled(state_path) is True

    set_drain_mode(state_path, enabled=False)
    assert is_drain_enabled(state_path) is False
