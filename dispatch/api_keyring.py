from __future__ import annotations

import hashlib
import json
import os
import secrets
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ApiKeyCheckResult:
    ok: bool
    expired: bool


def default_keyring_path(runtime_dir: Path) -> Path:
    return runtime_dir / "auth" / "api_keys.json"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_rfc3339_utc(s: str) -> datetime | None:
    raw = s.strip()
    if not raw:
        return None

    # Accept a minimal subset: either ISO with offset, or trailing "Z".
    try:
        if raw.endswith("Z"):
            return datetime.fromisoformat(raw[:-1] + "+00:00")
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _format_rfc3339_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compute_api_key_hash(api_key: str) -> str:
    # Deliberately simple: key values are expected to be high-entropy.
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def load_keyring(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "keys": []}

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"version": 1, "keys": []}

    if not isinstance(data, dict):
        return {"version": 1, "keys": []}

    keys = data.get("keys")
    if not isinstance(keys, list):
        keys = []

    return {"version": int(data.get("version", 1) or 1), "keys": keys}


def save_keyring(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)

    try:
        os.chmod(path, 0o600)
    except PermissionError:
        pass


def check_api_key_against_keyring(path: Path, api_key: str, *, now: datetime | None = None) -> ApiKeyCheckResult:
    now_dt = now or _utcnow()
    key_hash = compute_api_key_hash(api_key)

    data = load_keyring(path)
    keys = data.get("keys", [])

    saw_expired_match = False

    for entry in keys:
        if not isinstance(entry, dict):
            continue

        entry_hash = entry.get("key_hash")
        if not isinstance(entry_hash, str):
            continue

        # Hashes are fixed-size; compare directly.
        if entry_hash != key_hash:
            continue

        expires_at = entry.get("expires_at")
        if not isinstance(expires_at, str):
            # No expiry -> treat as expired (force explicit TTL).
            saw_expired_match = True
            continue

        expires_dt = _parse_rfc3339_utc(expires_at)
        if not expires_dt:
            saw_expired_match = True
            continue

        if now_dt >= expires_dt:
            saw_expired_match = True
            continue

        return ApiKeyCheckResult(ok=True, expired=False)

    if saw_expired_match:
        return ApiKeyCheckResult(ok=False, expired=True)

    return ApiKeyCheckResult(ok=False, expired=False)


def mint_api_key(*, label: str | None, ttl_seconds: int, runtime_dir: Path, keyring_path: Path | None = None) -> str:
    if ttl_seconds <= 0:
        raise ValueError("ttl_seconds must be > 0")

    path = keyring_path or default_keyring_path(runtime_dir)

    key_value = secrets.token_urlsafe(32)
    key_hash = compute_api_key_hash(key_value)

    now = _utcnow()
    expires_at = now + timedelta(seconds=ttl_seconds)

    data = load_keyring(path)
    keys = data.get("keys")
    if not isinstance(keys, list):
        keys = []

    keys.append(
        {
            "key_id": str(uuid.uuid4()),
            "key_hash": key_hash,
            "created_at": _format_rfc3339_utc(now),
            "expires_at": _format_rfc3339_utc(expires_at),
            "label": label or "",
        }
    )

    data["version"] = 1
    data["keys"] = keys

    save_keyring(path, data)

    return key_value
