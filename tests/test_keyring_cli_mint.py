import json
import os
import subprocess
import sys


def test_keyring_cli_mint_writes_keyring_and_prints_key(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "scheduler_orchestration.keyring",
            "mint",
            "--ttl",
            "1d",
            "--label",
            "test",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
        env={**dict(os.environ)},
    )

    api_key = proc.stdout.strip().splitlines()[-1].strip()
    assert api_key

    keyring_path = tmp_path / "auth" / "api_keys.json"
    data = json.loads(keyring_path.read_text(encoding="utf-8"))
    assert data["version"] == 1
    assert len(data["keys"]) == 1
    assert data["keys"][0]["label"] == "test"
