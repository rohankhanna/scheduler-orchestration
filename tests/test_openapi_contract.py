from pathlib import Path

import yaml


def test_openapi_contract_exists_and_has_required_routes():
    path = Path("api/openapi.yaml")
    assert path.exists(), "OpenAPI contract missing"

    spec = yaml.safe_load(path.read_text(encoding="utf-8"))

    assert spec["openapi"].startswith("3.")
    assert spec["info"]["title"]
    assert spec["info"]["version"]

    paths = spec["paths"]

    # Auth + key management
    assert "/v1/users" in paths
    assert "post" in paths["/v1/users"]

    assert "/v1/login" in paths
    assert "post" in paths["/v1/login"]

    assert "/v1/token/refresh" in paths
    assert "post" in paths["/v1/token/refresh"]

    assert "/v1/projects" in paths
    assert "post" in paths["/v1/projects"]

    assert "/v1/projects/{project_id}/api-keys" in paths
    assert "post" in paths["/v1/projects/{project_id}/api-keys"]
    assert "get" in paths["/v1/projects/{project_id}/api-keys"]

    assert "/v1/api-keys/{key_id}" in paths
    assert "delete" in paths["/v1/api-keys/{key_id}"]

    # Job control
    assert "/v1/jobs" in paths
    assert "post" in paths["/v1/jobs"]
    assert "get" in paths["/v1/jobs"]

    assert "/v1/jobs/{job_id}" in paths
    assert "get" in paths["/v1/jobs/{job_id}"]
    assert "delete" in paths["/v1/jobs/{job_id}"]

    assert "/v1/queue" in paths
    assert "get" in paths["/v1/queue"]

    assert "/v1/drain" in paths
    assert "post" in paths["/v1/drain"]
