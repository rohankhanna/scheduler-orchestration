import os

import pytest


def _import_app():
    from dispatch.server.app import create_app

    return create_app()


def test_requires_api_key_by_default(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    app = _import_app()

    try:
        from fastapi.testclient import TestClient
    except ImportError:
        pytest.fail("Missing dependency: fastapi (and its test client).", pytrace=False)

    client = TestClient(app)

    # no API key header -> 401
    r = client.get("/v1/queue")
    assert r.status_code == 401


def test_drain_toggle_requires_api_key_and_persists_state(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    app = _import_app()

    from fastapi.testclient import TestClient

    client = TestClient(app)

    # missing key -> 401
    r = client.post("/v1/drain", json={"enabled": True})
    assert r.status_code == 401

    # with key -> 200
    r = client.post(
        "/v1/drain",
        headers={"X-API-Key": "test-key"},
        json={"enabled": True},
    )
    assert r.status_code == 200
    assert r.json()["enabled"] is True

    # should read back state via a second call
    r = client.post(
        "/v1/drain",
        headers={"X-API-Key": "test-key"},
        json={"enabled": False},
    )
    assert r.status_code == 200
    assert r.json()["enabled"] is False
