import pytest


def _example_spec_dict():
    return {
        "job_name": "embedding-shard-0001",
        "workflow_preset": "embedding:v1",
        "device_preference": "gpu",
        "payload": {"argv": ["/usr/bin/env", "bash", "-lc", "echo hello-from-python-client"]},
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


@pytest.mark.anyio
async def test_python_async_client_can_submit_and_get_job(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    from dispatch.server.app import create_app

    app = create_app()

    import httpx

    from clients.python.scheduler_orchestration_client.client import AsyncSchedulerOrchestrationClient

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        client = AsyncSchedulerOrchestrationClient(http=http, api_key="test-key")

        submit = await client.submit_job(_example_spec_dict())
        assert submit["accepted"] is True
        server_job_id = submit["server_job_id"]

        job = await client.get_job(server_job_id)
        assert job["server_job_id"] == server_job_id


def test_python_client_enforces_timeouts_by_default():
    # unit-level: no transport needed
    from clients.python.scheduler_orchestration_client.client import AsyncSchedulerOrchestrationClient

    # we only validate default value, not I/O behavior
    assert AsyncSchedulerOrchestrationClient.DEFAULT_TIMEOUT_SECONDS <= 10.0
