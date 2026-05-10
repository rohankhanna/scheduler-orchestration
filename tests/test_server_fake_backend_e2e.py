def _example_spec_dict():
    return {
        "job_name": "api-e2e-job",
        "workflow_preset": "unit-test",
        "device_preference": "cpu",
        "resources": {"cpus": 1, "memory_mb": 128},
        "dependencies": [],
        "environment_overrides": {},
        "artifacts_root": "/tmp",
        "retry_policy": {"max_retries": 0},
        "drain_behavior": {"on_drain": "block"},
    }


def test_submit_observe_cancel_e2e_uses_fake_backend_ops(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))
    monkeypatch.setenv("SCHED_ORCH_EXECUTION_BACKEND", "fake")
    monkeypatch.setenv("SCHED_ORCH_ENABLE_SCHEDULER_EXEC", "1")
    monkeypatch.setenv("SCHED_ORCH_ENABLE_BULK_REFRESH", "1")
    monkeypatch.setenv("SCHED_ORCH_ENABLE_DIRECT_CANCEL", "1")

    from scheduler_orchestration.job_ledger import write_job_record
    from scheduler_orchestration.server import app as server_app

    calls = {"submit": 0, "refresh": 0, "cancel": 0}

    class FakeOps:
        name = "fake"

        def build_plan(self, spec, drain_state_path):
            return {
                "allowed": True,
                "reason": "ok",
                "command": ["fake-submit", spec["job_name"]],
            }

        def build_cancel_command(self, record):
            scheduler_job_id = record.get("scheduler_job_id")
            if not scheduler_job_id:
                return None
            return ["fake-cancel", str(scheduler_job_id)]

        def execute_submission(self, runtime_dir, server_job_id, plan, execution_enabled):
            assert execution_enabled is True
            calls["submit"] += 1
            return {
                "scheduler_job_id": "fake-123",
                "state": "submitted",
                "log_capture": {"stdout": False, "stderr": False},
            }

        def refresh_job(self, runtime_dir, record, *, refresh_requested):
            assert refresh_requested is True
            calls["refresh"] += 1
            record["state"] = "running"
            record["scheduler_state"] = "RUNNING"
            record["observed_by_fake"] = True
            write_job_record(runtime_dir, record)
            return record

        def execute_cancel(self, record, execution_enabled):
            assert execution_enabled is True
            calls["cancel"] += 1

        def get_logs(self, runtime_dir, record, *, server_job_id):
            return {"stdout": "", "stderr": ""}

    def _fake_backend_ops(*args, **kwargs):
        return FakeOps()

    monkeypatch.setattr(server_app, "get_backend_ops", _fake_backend_ops)

    client = TestClient(server_app.create_app())
    headers = {"X-API-Key": "test-key"}

    submit = client.post("/v1/jobs", json={"spec": _example_spec_dict()}, headers=headers)
    assert submit.status_code == 202
    server_job_id = submit.json()["server_job_id"]
    assert calls["submit"] == 1

    observed = client.get(f"/v1/jobs/{server_job_id}?refresh=1", headers=headers)
    assert observed.status_code == 200
    observed_body = observed.json()
    assert observed_body["scheduler_job_id"] == "fake-123"
    assert observed_body["detail"]["state"] == "running"
    assert observed_body["detail"]["scheduler_state"] == "RUNNING"
    assert calls["refresh"] == 1

    canceled = client.delete(f"/v1/jobs/{server_job_id}", headers=headers)
    assert canceled.status_code == 200
    assert canceled.json()["canceled"] is True
    assert calls["cancel"] == 1

    after_cancel = client.get(f"/v1/jobs/{server_job_id}", headers=headers)
    assert after_cancel.status_code == 200
    assert after_cancel.json()["detail"]["state"] == "canceled"
