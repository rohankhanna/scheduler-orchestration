from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient


def _example_spec_dict() -> dict[str, object]:
    return {
        "job_name": "client-test-job",
        "workflow_preset": "unit-test",
        "device_preference": "cpu",
        "payload": {"argv": ["/usr/bin/env", "bash", "-lc", "echo hello-from-client"]},
        "resources": {"graphics_processing_units": 0, "central_processing_unit_cores": 1, "memory_gibibytes": 1},
        "dependencies": {"parents": [], "policy": "afterok"},
        "environment_overrides": {},
        "artifacts_root": "/tmp/dispatch-client-tests",
        "retry_policy": {"max_attempts": 0},
        "drain_behavior": {"honor_drain": True},
    }


class FakeOps:
    name = "fake"

    def build_plan(self, spec, drain_state_path):
        return {"allowed": True, "reason": "ok", "command": ["fake-submit", spec["job_name"]]}

    def execute_submission(self, runtime_dir, server_job_id, plan, execution_enabled):
        return {
            "scheduler_job_id": "fake-123",
            "state": "submitted",
            "log_capture": {"stdout": False, "stderr": False},
        }

    def build_cancel_command(self, record):
        scheduler_job_id = record.get("scheduler_job_id")
        return ["fake-cancel", str(scheduler_job_id)] if scheduler_job_id else None

    def refresh_job(self, runtime_dir, record, terminal_states):
        record["state"] = "running"
        record["scheduler_state"] = "RUNNING"
        from dispatch.job_ledger import write_job_record

        write_job_record(runtime_dir, record)

    def get_logs(self, runtime_dir, record, *, server_job_id):
        return {"server_job_id": server_job_id, "execution_backend": "fake", "stdout": "hello\n", "stderr": ""}

    def bulk_refresh(self, runtime_dir, records, max_jobs, terminal_states):
        return None


@pytest.fixture
def test_app(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("SCHED_ORCH_EXECUTION_BACKEND", "fake")
    monkeypatch.setenv("SCHED_ORCH_ENABLE_SCHEDULER_EXEC", "1")
    monkeypatch.setenv("SCHED_ORCH_ENABLE_BULK_REFRESH", "1")

    from dispatch.server import app as server_app

    monkeypatch.setattr(server_app, "get_backend_ops", lambda *args, **kwargs: FakeOps())
    return server_app.create_app()


@pytest.fixture
def dispatch_client(test_app):
    from dispatch_client import DispatchClient, DispatchClientConfig

    tc = TestClient(test_app)
    http = httpx.Client(transport=tc._transport, base_url=str(tc.base_url))
    return DispatchClient(config=DispatchClientConfig(base_url=str(tc.base_url), api_key="test-key"), http=http)


def test_client_rejects_missing_api_key(test_app):
    from dispatch_client import DispatchClient, DispatchClientConfig

    tc = TestClient(test_app)
    http = httpx.Client(transport=tc._transport, base_url=str(tc.base_url))

    with pytest.raises(ValueError):
        DispatchClient(config=DispatchClientConfig(base_url=str(tc.base_url), api_key=""), http=http)


def test_client_raises_typed_401_on_invalid_key(test_app):
    from dispatch_client import DispatchAPIError, DispatchClient, DispatchClientConfig

    tc = TestClient(test_app)
    http = httpx.Client(transport=tc._transport, base_url=str(tc.base_url))
    client = DispatchClient(config=DispatchClientConfig(base_url=str(tc.base_url), api_key="wrong-key"), http=http)

    with pytest.raises(DispatchAPIError) as exc:
        client.list_jobs()

    assert exc.value.status_code == 401
    assert exc.value.error == "unauthorized"


def test_submit_job_returns_expected_fields(dispatch_client):
    body = dispatch_client.submit_job(_example_spec_dict())
    assert set(body.keys()) == {"server_job_id", "accepted", "reason", "command"}
    assert body["accepted"] is True
    assert body["reason"] == "ok"


def test_get_job_supports_refresh_and_parses_response(dispatch_client):
    submit = dispatch_client.submit_job(_example_spec_dict())
    body = dispatch_client.get_job(submit["server_job_id"], refresh=True)
    assert body["server_job_id"] == submit["server_job_id"]
    assert body["detail"]["scheduler_job_id"] == "fake-123"


def test_get_job_logs_works(dispatch_client):
    submit = dispatch_client.submit_job(_example_spec_dict())
    body = dispatch_client.get_job_logs(submit["server_job_id"])
    assert body["server_job_id"] == submit["server_job_id"]
    assert body["stdout"] == "hello\n"


def test_list_jobs_returns_items_shape(dispatch_client):
    dispatch_client.submit_job(_example_spec_dict())
    body = dispatch_client.list_jobs(refresh=True)
    assert "items" in body
    assert isinstance(body["items"], list)
    assert len(body["items"]) == 1
