import os


def _app(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    from dispatch.server.app import create_app

    return create_app()


def _example_spec_dict():
    return {
        "job_name": "embedding-shard-0001",
        "workflow_preset": "embedding:v1",
        "device_preference": "gpu",
        "payload": {"argv": ["/usr/bin/env", "bash", "-lc", "echo hello-from-server-jobs"]},
        "resources": {
            "graphics_processing_units": 1,
            "central_processing_unit_cores": 4,
            "memory_gibibytes": 24,
        },
        "dependencies": {"parents": [], "policy": "afterok"},
        "environment_overrides": {"EXAMPLE_ONLY_ENV": "example"},
        "artifacts_root": "EXAMPLE_ONLY_/absolute/path/to/artifacts",
        "retry_policy": {"max_attempts": 3},
        "drain_behavior": {"honor_drain": True},
    }


def test_submit_job_returns_202_and_server_job_id(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    app = _app(tmp_path, monkeypatch)
    client = TestClient(app)

    r = client.post(
        "/v1/jobs",
        headers={"X-API-Key": "test-key"},
        json={"spec": _example_spec_dict()},
    )

    assert r.status_code == 202
    data = r.json()
    assert data["accepted"] is True
    assert data["reason"] == "ok"
    assert isinstance(data["server_job_id"], str)


def test_submit_job_blocked_when_drain_enabled(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    app = _app(tmp_path, monkeypatch)
    client = TestClient(app)

    # enable drain
    r = client.post(
        "/v1/drain",
        headers={"X-API-Key": "test-key"},
        json={"enabled": True},
    )
    assert r.status_code == 200
    assert r.json()["enabled"] is True

    r = client.post(
        "/v1/jobs",
        headers={"X-API-Key": "test-key"},
        json={"spec": _example_spec_dict()},
    )

    assert r.status_code == 202
    data = r.json()
    assert data["accepted"] is False
    assert data["reason"] == "drain_enabled"
    assert data["command"] is None


def test_get_job_404_for_unknown_job(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    app = _app(tmp_path, monkeypatch)
    client = TestClient(app)

    r = client.get("/v1/jobs/does-not-exist", headers={"X-API-Key": "test-key"})
    assert r.status_code == 404


def test_submit_then_get_job_round_trip(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    app = _app(tmp_path, monkeypatch)
    client = TestClient(app)

    r = client.post(
        "/v1/jobs",
        headers={"X-API-Key": "test-key"},
        json={"spec": _example_spec_dict()},
    )
    server_job_id = r.json()["server_job_id"]

    r = client.get(f"/v1/jobs/{server_job_id}", headers={"X-API-Key": "test-key"})
    assert r.status_code == 200
    data = r.json()
    assert data["server_job_id"] == server_job_id
    assert isinstance(data["state"], str)
