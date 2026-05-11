def test_signup_login_create_project_and_mint_key_then_submit_job(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    # Keep scheduler execution disabled (dry-run) for test.
    monkeypatch.setenv("SCHED_ORCH_ENABLE_SCHEDULER_EXEC", "0")

    from dispatch.server.app import create_app

    client = TestClient(create_app())

    # Signup
    r = client.post("/v1/users", json={"username": "alice", "password": "very-strong-password"})
    assert r.status_code == 201
    user_id = r.json()["user_id"]
    assert user_id

    # Login
    r = client.post("/v1/login", json={"username": "alice", "password": "very-strong-password"})
    assert r.status_code == 200
    access_token = r.json()["access_token"]
    assert access_token

    authz = {"Authorization": f"Bearer {access_token}"}

    # Create project
    r = client.post("/v1/projects", json={"name": "example-program"}, headers=authz)
    assert r.status_code == 201
    project_id = r.json()["project_id"]
    assert project_id

    # Mint API key
    r = client.post(
        f"/v1/projects/{project_id}/api-keys",
        json={"label": "example-program-key", "ttl_seconds": 3600},
        headers=authz,
    )
    assert r.status_code == 201
    api_key = r.json()["api_key"]
    assert api_key

    # Submit a job using the project API key.
    spec = {
        "job_name": "unit-test-job",
        "workflow_preset": "unit-test",
        "device_preference": "cpu",
        "resources": {"graphics_processing_units": 0, "central_processing_unit_cores": 1, "memory_gibibytes": 1},
        "dependencies": {"parents": [], "policy": "afterok"},
        "environment_overrides": {},
        "artifacts_root": "/tmp",
        "retry_policy": {"max_attempts": 1},
        "drain_behavior": {"honor_drain": True},
    }

    r = client.post("/v1/jobs", json={"spec": spec}, headers={"X-API-Key": api_key})
    assert r.status_code == 202
    assert r.json()["accepted"] in {True, False}
