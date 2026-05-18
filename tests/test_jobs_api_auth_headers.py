from __future__ import annotations

from fastapi.testclient import TestClient


def _client(tmp_path, monkeypatch) -> TestClient:
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    from dispatch.server.app import create_app

    return TestClient(create_app())


def test_jobs_endpoint_accepts_x_api_key_header(tmp_path, monkeypatch):
    client = _client(tmp_path, monkeypatch)

    r = client.get("/v1/queue", headers={"X-API-Key": "test-key"})

    assert r.status_code == 200


def test_jobs_endpoint_accepts_authorization_bearer_api_key(tmp_path, monkeypatch):
    client = _client(tmp_path, monkeypatch)

    r = client.get("/v1/queue", headers={"Authorization": "Bearer test-key"})

    assert r.status_code == 200


def test_jobs_endpoint_accepts_authorization_apikey_api_key(tmp_path, monkeypatch):
    client = _client(tmp_path, monkeypatch)

    r = client.get("/v1/queue", headers={"Authorization": "ApiKey test-key"})

    assert r.status_code == 200


def test_jobs_endpoint_401_includes_missing_header_hint(tmp_path, monkeypatch):
    client = _client(tmp_path, monkeypatch)

    r = client.get("/v1/queue")

    assert r.status_code == 401
    assert r.json() == {
        "error": "unauthorized",
        "message": "missing X-API-Key header (jobs endpoints also accept Authorization: Bearer <api_key>)",
    }
