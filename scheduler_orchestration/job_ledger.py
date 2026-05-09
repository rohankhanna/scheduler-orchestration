from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_rfc3339() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def jobs_dir(runtime_dir: Path) -> Path:
    return runtime_dir / "scheduler-job-ledger" / "jobs"


def job_path(runtime_dir: Path, server_job_id: str) -> Path:
    return jobs_dir(runtime_dir) / f"{server_job_id}.json"


def spec_sha256(spec: dict[str, Any]) -> str:
    encoded = json.dumps(spec, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def atomic_write_private_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(payload, indent=2, sort_keys=True) + "\n"

    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())

        try:
            os.chmod(tmp_path, 0o600)
        except PermissionError:
            pass

        os.replace(tmp_path, path)

        try:
            dir_fd = os.open(str(path.parent), os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def read_job_record(runtime_dir: Path, server_job_id: str) -> dict[str, Any] | None:
    path = job_path(runtime_dir, server_job_id)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_job_record(runtime_dir: Path, record: dict[str, Any]) -> None:
    server_job_id = str(record.get("server_job_id", "")).strip()
    if not server_job_id:
        raise ValueError("record missing server_job_id")
    atomic_write_private_json(job_path(runtime_dir, server_job_id), record)
