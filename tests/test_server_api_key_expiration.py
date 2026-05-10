import hashlib
import json
from pathlib import Path
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


def test_expired_api_key_message_is_loaded_from_markdown_file(tmp_path, monkeypatch):
    md_path = tmp_path / "docs" / "expired_api_key.md"
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_text = "API key expired. Regenerate with: python -m scheduler_orchestration.keyring mint --ttl 30d --label my-client\n"
    md_path.write_text(md_text, encoding="utf-8")
    monkeypatch.setenv("SCHED_ORCH_EXPIRED_API_KEY_MESSAGE_MD_PATH", str(md_path))

    client = _client(tmp_path, monkeypatch, key="expired-key", expires_at="2000-01-01T00:00:00Z")

    r = client.get("/v1/queue", headers={"X-API-Key": "expired-key"})
    assert r.status_code == 401

    body = r.json()
    assert body["error"] == "unauthorized"
    assert body.get("message") == md_text.strip()


def test_unexpired_keyring_api_key_is_authorized(tmp_path, monkeypatch):
    client = _client(tmp_path, monkeypatch, key="live-key", expires_at="2999-01-01T00:00:00Z")

    r = client.get("/v1/jobs", headers={"X-API-Key": "live-key"})
    assert r.status_code == 200


def test_unknown_api_key_keeps_generic_unauthorized_error(tmp_path, monkeypatch):
    client = _client(tmp_path, monkeypatch, key="live-key", expires_at="2999-01-01T00:00:00Z")

    r = client.get("/v1/queue", headers={"X-API-Key": "wrong-key"})
    assert r.status_code == 401
    assert r.json() == {"error": "unauthorized"}


def test_expired_key_message_default_source_is_repo_doc_markdown(tmp_path, monkeypatch):
    # This test is intentionally "policy-like": it ensures the server's default
    # expired-key regeneration message is sourced from the repo documentation file,
    # so that doc edits propagate to runtime behavior.

    repo_doc = Path("docs/auth/expired_api_key.md")
    assert repo_doc.exists(), "docs/auth/expired_api_key.md must exist"

    expected_message = repo_doc.read_text(encoding="utf-8").strip()
    assert expected_message, "expired_api_key.md must not be empty"

    # No override env var: server must use the repo doc by default.
    monkeypatch.delenv("SCHED_ORCH_EXPIRED_API_KEY_MESSAGE_MD_PATH", raising=False)

    client = _client(tmp_path, monkeypatch, key="expired-key", expires_at="2000-01-01T00:00:00Z")

    r = client.get("/v1/queue", headers={"X-API-Key": "expired-key"})
    assert r.status_code == 401
    body = r.json()
    assert body["error"] == "unauthorized"
    assert body.get("message") == expected_message
