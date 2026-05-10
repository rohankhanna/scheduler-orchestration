from pathlib import Path

import yaml


def test_openapi_security_is_api_key_and_server_is_loopback_only():
    spec = yaml.safe_load(Path("api/openapi.yaml").read_text(encoding="utf-8"))

    servers = spec.get("servers")
    assert isinstance(servers, list)
    assert len(servers) >= 1

    # Security best practice: loopback-only default.
    for s in servers:
        url = s.get("url", "")
        assert url.startswith("http://127.0.0.1") or url.startswith("http://localhost")
        assert "0.0.0.0" not in url

    schemes = spec["components"]["securitySchemes"]
    assert "ApiKeyAuth" in schemes
    assert schemes["ApiKeyAuth"]["type"] == "apiKey"
    assert schemes["ApiKeyAuth"]["in"] == "header"
    assert schemes["ApiKeyAuth"]["name"] == "X-API-Key"

    # Security scheme presence
    assert "BearerAuth" in schemes
    assert schemes["BearerAuth"]["type"] == "http"
    assert schemes["BearerAuth"]["scheme"] == "bearer"

    # Per-route security requirements (no global security).
    paths = spec["paths"]

    # Job endpoints require API key auth.
    assert {"ApiKeyAuth": []} in paths["/v1/jobs"]["get"].get("security", [])
    assert {"ApiKeyAuth": []} in paths["/v1/jobs"]["post"].get("security", [])
    assert {"ApiKeyAuth": []} in paths["/v1/jobs/{job_id}"]["get"].get("security", [])
    assert {"ApiKeyAuth": []} in paths["/v1/jobs/{job_id}"]["delete"].get("security", [])

    # Project/key administration requires bearer session.
    assert {"BearerAuth": []} in paths["/v1/projects"]["post"].get("security", [])
    assert {"BearerAuth": []} in paths["/v1/projects/{project_id}/api-keys"]["post"].get("security", [])
    assert {"BearerAuth": []} in paths["/v1/projects/{project_id}/api-keys"]["get"].get("security", [])
    assert {"BearerAuth": []} in paths["/v1/api-keys/{key_id}"]["delete"].get("security", [])

    # Signup/login/refresh are intentionally open (no security requirement).
    assert "security" not in paths["/v1/users"]["post"]
    assert "security" not in paths["/v1/login"]["post"]
    assert "security" not in paths["/v1/token/refresh"]["post"]


def test_openapi_schema_tightening_basics():
    spec = yaml.safe_load(Path("api/openapi.yaml").read_text(encoding="utf-8"))
    schemas = spec["components"]["schemas"]

    # command is nullable
    cmd = schemas["SubmitJobResponse"]["properties"]["command"]
    assert "anyOf" in cmd
    assert any(x.get("type") == "null" for x in cmd["anyOf"])

    # drain updated_at is date-time
    updated_at = schemas["SetDrainResponse"]["properties"]["updated_at"]
    assert updated_at.get("format") == "date-time"

    # job state is an enum
    state = schemas["GetJobResponse"]["properties"]["state"]
    assert "enum" in state
    assert "accepted" in state["enum"]
    assert "blocked" in state["enum"]
