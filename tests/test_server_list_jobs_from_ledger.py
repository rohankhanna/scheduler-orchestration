from __future__ import annotations

import json

from fastapi.testclient import TestClient


def test_list_jobs_reads_from_ledger_and_returns_most_recent_first(tmp_path, monkeypatch):
    monkeypatch.setenv("SCHED_ORCH_API_KEY", "test-key")
    monkeypatch.setenv("SCHED_ORCH_RUNTIME_DIR", str(tmp_path))

    jobs_dir = tmp_path / "scheduler-job-ledger" / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)

    # Seed two records. The server implementation returns reverse-sorted paths.
    (jobs_dir / "a.json").write_text(
        json.dumps(
            {
                "server_job_id": "a",
                "created_at": "2026-01-01T00:00:00Z",
                "spec_sha256": "a" * 64,
                "state": "submitted",
                "scheduler_job_id": "9001",
                "execution_backend": "slurm",
                "accepted": True,
                "reason": "ok",
                "spec": {"job_name": "job-a"},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    (jobs_dir / "b.json").write_text(
        json.dumps(
            {
                "server_job_id": "b",
                "created_at": "2026-01-01T00:00:01Z",
                "spec_sha256": "b" * 64,
                "state": "succeeded",
                "scheduler_job_id": None,
                "execution_backend": "direct",
                "accepted": True,
                "reason": "ok",
                "spec": {"job_name": "job-b"},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    from scheduler_orchestration.server.app import create_app

    client = TestClient(create_app())

    r = client.get("/v1/jobs", headers={"X-API-Key": "test-key"})
    assert r.status_code == 200

    items = r.json()["items"]
    assert [i["server_job_id"] for i in items] == ["b", "a"]
    assert items[0]["job_name"] == "job-b"
    assert items[1]["job_name"] == "job-a"
