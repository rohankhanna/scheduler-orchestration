import hashlib
import json
import time


def _write_keyring(path, *, key: str, expires_at: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    key_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "keys": [
                    {
                        "key_id": "k1",
                        "key_hash": key_hash,
                        "created_at": "2026-01-01T00:00:00Z",
                        "expires_at": expires_at,
                        "label": "test",
                    }
                ],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _client(tmp_path, monkeypatch, *, key: str, expires_at: str):
    from fastapi.testclient import TestClient

    # Keep legacy env key set so server can start even before keyring support lands.
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "root-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    keyring_path = tmp_path / "auth" / "api_keys.json"
    _write_keyring(keyring_path, key=key, expires_at=expires_at)
    monkeypatch.setenv("SCHED_ORCH_API_KEYRING_PATH", str(keyring_path))

    from scheduler_orchestration.server.app import create_app

    return TestClient(create_app())


def test_expired_api_key_mentions_how_to_regenerate(tmp_path, monkeypatch):
    client = _client(tmp_path, monkeypatch, key="expired-key", expires_at="2000-01-01T00:00:00Z")

    r = client.get("/v1/queue", headers={"X-API-Key": "expired-key"})
    assert r.status_code == 401

    body = r.json()
    assert body["error"] == "unauthorized"
    assert "message" in body
    assert "python -m scheduler_orchestration.keyring mint" in body["message"]


def test_unexpired_keyring_api_key_is_authorized(tmp_path, monkeypatch):
    client = _client(tmp_path, monkeypatch, key="live-key", expires_at="2999-01-01T00:00:00Z")

    r = client.get("/v1/jobs", headers={"X-API-Key": "live-key"})
    assert r.status_code == 200


def test_unknown_api_key_keeps_generic_unauthorized_error(tmp_path, monkeypatch):
    client = _client(tmp_path, monkeypatch, key="live-key", expires_at="2999-01-01T00:00:00Z")

    r = client.get("/v1/queue", headers={"X-API-Key": "wrong-key"})
    assert r.status_code == 401
    assert r.json() == {"error": "unauthorized"}
