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
    assert "/v1/jobs" in paths
    assert "post" in paths["/v1/jobs"]
    assert "get" in paths["/v1/jobs"]

    assert "/v1/jobs/{job_id}" in paths
    assert "get" in paths["/v1/jobs/{job_id}"]

    assert "/v1/queue" in paths
    assert "get" in paths["/v1/queue"]

    assert "/v1/drain" in paths
    assert "post" in paths["/v1/drain"]
