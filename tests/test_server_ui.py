def test_ui_endpoint_serves_html(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    from scheduler_orchestration.server.app import create_app

    client = TestClient(create_app())
    r = client.get("/ui")
    assert r.status_code == 200
    assert "text/html" in r.headers.get("content-type", "")
    assert "Dispatch" in r.text
