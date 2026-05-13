from __future__ import annotations

from subprocess import CompletedProcess

from fastapi.testclient import TestClient


def _example_spec_dict():
    return {
        "job_name": "log-limit-job",
        "workflow_preset": "embedding:v1",
        "device_preference": "gpu",
        "payload": {"argv": ["python3", "-c", "print('x')"]},
        "resources": {
            "graphics_processing_units": 0,
            "central_processing_unit_cores": 1,
            "memory_gibibytes": 1,
        },
        "dependencies": {"parents": [], "policy": "afterok"},
        "environment_overrides": {},
        "artifacts_root": "EXAMPLE_ONLY_/tmp/artifacts",
        "retry_policy": {"max_attempts": 1},
        "drain_behavior": {"honor_drain": True},
    }


def test_direct_log_capture_is_truncated_by_configured_limit(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("SCHED_ORCH_EXECUTION_BACKEND", "direct")
    monkeypatch.setenv("SCHED_ORCH_ENABLE_DIRECT_EXEC", "1")
    monkeypatch.setenv("SCHED_ORCH_ENABLE_DIRECT_PAYLOAD_EXEC", "1")
    monkeypatch.setenv("SCHED_ORCH_DIRECT_LOG_CAPTURE_MAX_BYTES", "8")

    def _extract_append_path(argv: list[str], key: str):
        prefix = f"--property={key}=append:"
        for item in argv:
            if isinstance(item, str) and item.startswith(prefix):
                return item[len(prefix) :]
        return None

    def fake_run(args, **kwargs):
        # Simulate systemd writing longer-than-limit logs to the configured files.
        out_path = _extract_append_path(list(args), "StandardOutput")
        err_path = _extract_append_path(list(args), "StandardError")
        if out_path:
            from pathlib import Path

            Path(out_path).write_text("0123456789", encoding="utf-8")
        if err_path:
            from pathlib import Path

            Path(err_path).write_text("abcdefghij", encoding="utf-8")
        return CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    import dispatch.server.app as app_mod

    monkeypatch.setattr(app_mod.subprocess, "run", fake_run, raising=True)

    from dispatch.server.app import create_app

    client = TestClient(create_app())
    submit = client.post("/v1/jobs", headers={"X-API-Key": "test-key"}, json={"spec": _example_spec_dict()})
    assert submit.status_code == 202
    server_job_id = submit.json()["server_job_id"]

    logs = client.get(f"/v1/jobs/{server_job_id}/logs", headers={"X-API-Key": "test-key"})
    assert logs.status_code == 200
    assert logs.json()["stdout"] == "01234567"
    assert logs.json()["stderr"] == "abcdefgh"

    detail = client.get(f"/v1/jobs/{server_job_id}", headers={"X-API-Key": "test-key"})
    assert detail.status_code == 200
    assert detail.json()["detail"]["log_capture"] == {
        "stdout": True,
        "stderr": True,
    }

