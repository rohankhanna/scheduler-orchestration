from __future__ import annotations

import os
from subprocess import CompletedProcess

import pytest
from fastapi.testclient import TestClient

from dispatch.server.app import create_app


def test_queue_calls_squeue_and_parses_items(tmp_path, monkeypatch):
    os.environ["SCHED_ORCH_API_KEY"] = "[REDACTED]"
    os.environ["SCHED_ORCH_RUNTIME_DIR"] = str(tmp_path)

    calls = []

    def fake_run(args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        stdout = (
            "123|job-a|RUNNING|00:01:02|1|4|4G|None\n"
            "124|job-b|PENDING|00:00:00|1|1|1G|Resources\n"
        )
        return CompletedProcess(args=args, returncode=0, stdout=stdout, stderr="")

    # Patch subprocess.run inside the server module.
    import dispatch.server.app as app_mod

    monkeypatch.setattr(app_mod.subprocess, "run", fake_run, raising=True)

    client = TestClient(create_app())
    r = client.get("/v1/queue", headers={"X-API-Key": "[REDACTED]"})

    assert r.status_code == 200
    body = r.json()

    assert body["items"] == [
        {
            "job_id": "123",
            "name": "job-a",
            "state": "RUNNING",
            "time": "00:01:02",
            "nodes": "1",
            "cpus": "4",
            "memory": "4G",
            "reason": "None",
        },
        {
            "job_id": "124",
            "name": "job-b",
            "state": "PENDING",
            "time": "00:00:00",
            "nodes": "1",
            "cpus": "1",
            "memory": "1G",
            "reason": "Resources",
        },
    ]

    assert len(calls) == 1
    assert calls[0]["args"] == [
        "squeue",
        "--noheader",
        "--format=%i|%j|%T|%M|%D|%C|%m|%R",
    ]

    kwargs = calls[0]["kwargs"]
    assert kwargs.get("check") is True
    assert kwargs.get("text") is True
    assert kwargs.get("capture_output") is True
    assert isinstance(kwargs.get("timeout"), (int, float))
