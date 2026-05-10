import uuid


def _example_spec_dict():
    return {
        "job_name": "unit-test-job",
        "workflow_preset": "unit-test",
        "device_preference": "cpu",
        "resources": {"cpus": 1, "memory_mb": 128},
        "dependencies": [],
        "environment_overrides": {},
        "artifacts_root": "/tmp",
        "retry_policy": {"max_retries": 0},
        "drain_behavior": {"on_drain": "block"},
    }


def test_get_job_refresh_is_routed_through_backend_ops(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    # Gate needs to be enabled for refresh requests to be allowed.
    monkeypatch.setenv("SCHED_ORCH_ENABLE_DIRECT_REFRESH", "1")

    from scheduler_orchestration.server import app as server_app

    calls = {"refresh_job": 0}

    class StubOps:
        def __init__(self):
            self.name = "direct"

        def refresh_job(self, runtime_dir, record, *, refresh_requested):
            calls["refresh_job"] += 1
            record["touched_by_stub"] = True
            return record

    def _stub_get_backend_ops(*args, **kwargs):
        return StubOps()

    monkeypatch.setattr(server_app, "get_backend_ops", _stub_get_backend_ops)

    client = TestClient(server_app.create_app())

    server_job_id = str(uuid.uuid4())
    record = {
        "server_job_id": server_job_id,
        "execution_backend": "direct",
        "state": "running",
        "accepted": True,
        "reason": "ok",
        "spec": _example_spec_dict(),
        "direct_scope_name": "dummy.scope",
    }

    from scheduler_orchestration.job_ledger import write_job_record

    write_job_record(tmp_path, record)

    r = client.get(f"/v1/jobs/{server_job_id}?refresh=1", headers={"X-API-Key": "test-key"})
    assert r.status_code == 200
    assert calls["refresh_job"] == 1


def test_get_job_logs_is_routed_through_backend_ops(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    from scheduler_orchestration.server import app as server_app

    calls = {"get_logs": 0}

    class StubOps:
        def __init__(self):
            self.name = "direct"

        def get_logs(self, runtime_dir, record, *, server_job_id):
            calls["get_logs"] += 1
            return {"stdout": "hello", "stderr": ""}

    def _stub_get_backend_ops(*args, **kwargs):
        return StubOps()

    monkeypatch.setattr(server_app, "get_backend_ops", _stub_get_backend_ops)

    client = TestClient(server_app.create_app())

    server_job_id = str(uuid.uuid4())
    record = {
        "server_job_id": server_job_id,
        "execution_backend": "direct",
        "state": "succeeded",
        "accepted": True,
        "reason": "ok",
        "spec": _example_spec_dict(),
    }

    from scheduler_orchestration.job_ledger import write_job_record

    write_job_record(tmp_path, record)

    r = client.get(f"/v1/jobs/{server_job_id}/logs", headers={"X-API-Key": "test-key"})
    assert r.status_code == 200
    assert r.json()["stdout"] == "hello"
    assert calls["get_logs"] == 1


def test_list_jobs_bulk_refresh_is_routed_through_backend_ops(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    # Bulk refresh gate for list endpoint.
    monkeypatch.setenv("SCHED_ORCH_ENABLE_BULK_REFRESH", "1")

    from scheduler_orchestration.server import app as server_app

    calls = {"bulk_refresh": 0}

    class StubOps:
        def __init__(self):
            self.name = "direct"

        def bulk_refresh(self, runtime_dir, records, max_jobs, terminal_states):
            calls["bulk_refresh"] += 1

    def _stub_get_backend_ops(*args, **kwargs):
        return StubOps()

    monkeypatch.setattr(server_app, "get_backend_ops", _stub_get_backend_ops)

    client = TestClient(server_app.create_app())

    server_job_id = str(uuid.uuid4())
    record = {
        "server_job_id": server_job_id,
        "execution_backend": "direct",
        "state": "running",
        "accepted": True,
        "reason": "ok",
        "spec": _example_spec_dict(),
        "direct_scope_name": "dummy.scope",
    }

    from scheduler_orchestration.job_ledger import write_job_record

    write_job_record(tmp_path, record)

    r = client.get("/v1/jobs?refresh=1", headers={"X-API-Key": "test-key"})
    assert r.status_code == 200
    assert calls["bulk_refresh"] >= 1
