import pytest


def _app(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    from dispatch.server.app import create_app

    return create_app()


def test_unauthorized_errors_have_stable_json_shape(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    client = TestClient(_app(tmp_path, monkeypatch))

    r = client.get("/v1/queue")
    assert r.status_code == 401
    assert r.json() == {"error": "unauthorized"}


def test_bad_request_errors_have_stable_json_shape(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    client = TestClient(_app(tmp_path, monkeypatch))

    r = client.post("/v1/jobs", headers={"X-API-Key": "test-key"}, json={})
    assert r.status_code == 400
    assert r.json() == {"error": "bad_request"}


def test_not_found_errors_have_stable_json_shape(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    client = TestClient(_app(tmp_path, monkeypatch))

    r = client.get("/v1/jobs/does-not-exist", headers={"X-API-Key": "test-key"})
    assert r.status_code == 404
    assert r.json() == {"error": "not_found"}
