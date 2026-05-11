from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _format_rfc3339_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_rfc3339_utc(s: str) -> datetime | None:
    raw = str(s or "").strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            return datetime.fromisoformat(raw[:-1] + "+00:00")
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _atomic_write_private_json(path: Path, payload: dict[str, Any]) -> None:
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


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode("ascii").rstrip("=")


def _b64url_decode(s: str) -> bytes:
    raw = s.strip()
    pad = "=" * ((4 - (len(raw) % 4)) % 4)
    return base64.urlsafe_b64decode(raw + pad)


def hash_password(password: str) -> str:
    # Local-first baseline: PBKDF2-HMAC-SHA256.
    # Format: pbkdf2_sha256$<iters>$<salt_b64url>$<hash_b64url>
    iters = 310_000
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iters)
    return f"pbkdf2_sha256${iters}${_b64url(salt)}${_b64url(dk)}"


def verify_password(password: str, stored: str) -> bool:
    parts = str(stored or "").split("$")
    if len(parts) != 4:
        return False
    alg, iters_s, salt_s, hash_s = parts
    if alg != "pbkdf2_sha256":
        return False
    try:
        iters = int(iters_s)
    except ValueError:
        return False
    if iters <= 0:
        return False

    try:
        salt = _b64url_decode(salt_s)
        expected = _b64url_decode(hash_s)
    except Exception:
        return False

    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iters)
    return hmac.compare_digest(dk, expected)


@dataclass(frozen=True)
class AuthConfig:
    access_ttl_seconds: int
    refresh_ttl_seconds: int
    refresh_cookie_name: str


def default_auth_config() -> AuthConfig:
    return AuthConfig(
        access_ttl_seconds=int(os.environ.get("DISPATCH_ACCESS_TTL_SECONDS", "900")),
        refresh_ttl_seconds=int(os.environ.get("DISPATCH_REFRESH_TTL_SECONDS", str(30 * 24 * 3600))),
        refresh_cookie_name=os.environ.get("DISPATCH_REFRESH_COOKIE_NAME", "dispatch_refresh_token").strip() or "dispatch_refresh_token",
    )


def users_path(runtime_dir: Path) -> Path:
    return runtime_dir / "auth" / "users.json"


def sessions_path(runtime_dir: Path) -> Path:
    return runtime_dir / "auth" / "sessions.json"


def projects_path(runtime_dir: Path) -> Path:
    return runtime_dir / "auth" / "projects.json"


def api_keys_path(runtime_dir: Path) -> Path:
    return runtime_dir / "auth" / "api_keys.json"


def _load_json_or_default(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default
    if not isinstance(data, dict):
        return default
    return data


def create_user(runtime_dir: Path, *, username: str, password: str) -> dict[str, Any]:
    u = username.strip()
    if not u:
        raise ValueError("bad_request")
    if len(u) > 64:
        raise ValueError("bad_request")
    if not password:
        raise ValueError("bad_request")

    path = users_path(runtime_dir)
    data = _load_json_or_default(path, {"version": 1, "users": []})
    users = data.get("users")
    if not isinstance(users, list):
        users = []

    for existing in users:
        if not isinstance(existing, dict):
            continue
        if str(existing.get("username") or "").strip().lower() == u.lower():
            raise ValueError("conflict")

    user_id = str(uuid.uuid4())
    now = _utcnow()
    record = {
        "user_id": user_id,
        "username": u,
        "password_hash": hash_password(password),
        "created_at": _format_rfc3339_utc(now),
    }
    users.append(record)
    data["version"] = 1
    data["users"] = users
    _atomic_write_private_json(path, data)

    return {"user_id": user_id, "username": u, "created_at": record["created_at"]}


def _find_user(runtime_dir: Path, *, username: str) -> dict[str, Any] | None:
    path = users_path(runtime_dir)
    data = _load_json_or_default(path, {"version": 1, "users": []})
    users = data.get("users")
    if not isinstance(users, list):
        return None

    u = username.strip().lower()
    for entry in users:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("username") or "").strip().lower() == u:
            return entry
    return None


def _mint_token_value() -> str:
    return secrets.token_urlsafe(32)


def _store_session_token(
    runtime_dir: Path,
    *,
    token_type: str,
    token_value: str,
    user_id: str,
    ttl_seconds: int,
) -> dict[str, Any]:
    if token_type not in {"access", "refresh"}:
        raise ValueError("bad_request")

    now = _utcnow()
    expires_at = now + timedelta(seconds=ttl_seconds)

    path = sessions_path(runtime_dir)
    data = _load_json_or_default(path, {"version": 1, "tokens": []})
    tokens = data.get("tokens")
    if not isinstance(tokens, list):
        tokens = []

    token_id = str(uuid.uuid4())
    entry = {
        "token_id": token_id,
        "token_type": token_type,
        "token_hash": _sha256_hex(token_value),
        "user_id": user_id,
        "created_at": _format_rfc3339_utc(now),
        "expires_at": _format_rfc3339_utc(expires_at),
        "revoked_at": None,
        "last_used_at": None,
    }
    tokens.append(entry)
    data["version"] = 1
    data["tokens"] = tokens
    _atomic_write_private_json(path, data)

    return {"token_id": token_id, "expires_at": entry["expires_at"]}


def _revoke_token_hash(runtime_dir: Path, token_hash: str) -> None:
    path = sessions_path(runtime_dir)
    data = _load_json_or_default(path, {"version": 1, "tokens": []})
    tokens = data.get("tokens")
    if not isinstance(tokens, list):
        tokens = []

    now_s = _format_rfc3339_utc(_utcnow())
    changed = False
    for entry in tokens:
        if not isinstance(entry, dict):
            continue
        if entry.get("token_hash") != token_hash:
            continue
        if entry.get("revoked_at") is None:
            entry["revoked_at"] = now_s
            changed = True

    if changed:
        data["tokens"] = tokens
        _atomic_write_private_json(path, data)


def issue_login_tokens(runtime_dir: Path, *, username: str, password: str, cfg: AuthConfig) -> dict[str, Any]:
    user = _find_user(runtime_dir, username=username)
    if not user:
        raise ValueError("unauthorized")

    stored = str(user.get("password_hash") or "")
    if not verify_password(password, stored):
        raise ValueError("unauthorized")

    user_id = str(user.get("user_id") or "").strip()
    if not user_id:
        raise ValueError("unauthorized")

    access_token = _mint_token_value()
    refresh_token = _mint_token_value()

    access_meta = _store_session_token(runtime_dir, token_type="access", token_value=access_token, user_id=user_id, ttl_seconds=cfg.access_ttl_seconds)
    refresh_meta = _store_session_token(runtime_dir, token_type="refresh", token_value=refresh_token, user_id=user_id, ttl_seconds=cfg.refresh_ttl_seconds)

    return {
        "user": {"user_id": user_id, "username": str(user.get("username") or "")},
        "access_token": access_token,
        "access_expires_at": access_meta["expires_at"],
        "refresh_token": refresh_token,
        "refresh_expires_at": refresh_meta["expires_at"],
    }


def _check_token(runtime_dir: Path, *, token_type: str, token_value: str) -> dict[str, Any] | None:
    if token_type not in {"access", "refresh"}:
        return None

    token_hash = _sha256_hex(token_value)
    path = sessions_path(runtime_dir)
    data = _load_json_or_default(path, {"version": 1, "tokens": []})
    tokens = data.get("tokens")
    if not isinstance(tokens, list):
        return None

    now = _utcnow()
    now_s = _format_rfc3339_utc(now)

    changed = False
    for entry in tokens:
        if not isinstance(entry, dict):
            continue
        if entry.get("token_type") != token_type:
            continue
        if entry.get("token_hash") != token_hash:
            continue

        if entry.get("revoked_at") is not None:
            return None

        expires_at = entry.get("expires_at")
        if not isinstance(expires_at, str):
            return None
        expires_dt = _parse_rfc3339_utc(expires_at)
        if not expires_dt:
            return None
        if now >= expires_dt:
            return None

        if entry.get("last_used_at") != now_s:
            entry["last_used_at"] = now_s
            changed = True

        if changed:
            data["tokens"] = tokens
            _atomic_write_private_json(path, data)

        return entry

    return None


def authenticate_access_token(runtime_dir: Path, access_token: str) -> dict[str, Any] | None:
    token = _check_token(runtime_dir, token_type="access", token_value=access_token)
    if not token:
        return None

    user_id = str(token.get("user_id") or "").strip()
    if not user_id:
        return None

    return {"user_id": user_id}


def refresh_session(runtime_dir: Path, *, refresh_token: str, cfg: AuthConfig) -> dict[str, Any]:
    token = _check_token(runtime_dir, token_type="refresh", token_value=refresh_token)
    if not token:
        raise ValueError("unauthorized")

    user_id = str(token.get("user_id") or "").strip()
    if not user_id:
        raise ValueError("unauthorized")

    # Rotate refresh token: revoke the old refresh token hash.
    _revoke_token_hash(runtime_dir, _sha256_hex(refresh_token))

    access_token = _mint_token_value()
    new_refresh = _mint_token_value()

    access_meta = _store_session_token(runtime_dir, token_type="access", token_value=access_token, user_id=user_id, ttl_seconds=cfg.access_ttl_seconds)
    refresh_meta = _store_session_token(runtime_dir, token_type="refresh", token_value=new_refresh, user_id=user_id, ttl_seconds=cfg.refresh_ttl_seconds)

    return {
        "user": {"user_id": user_id},
        "access_token": access_token,
        "access_expires_at": access_meta["expires_at"],
        "refresh_token": new_refresh,
        "refresh_expires_at": refresh_meta["expires_at"],
    }


def create_project(runtime_dir: Path, *, owner_user_id: str, name: str) -> dict[str, Any]:
    n = name.strip()
    if not n:
        raise ValueError("bad_request")
    if len(n) > 128:
        raise ValueError("bad_request")

    path = projects_path(runtime_dir)
    data = _load_json_or_default(path, {"version": 1, "projects": []})
    projects = data.get("projects")
    if not isinstance(projects, list):
        projects = []

    project_id = str(uuid.uuid4())
    now_s = _format_rfc3339_utc(_utcnow())
    rec = {
        "project_id": project_id,
        "owner_user_id": owner_user_id,
        "name": n,
        "created_at": now_s,
    }
    projects.append(rec)
    data["version"] = 1
    data["projects"] = projects
    _atomic_write_private_json(path, data)

    return {"project_id": project_id, "name": n, "created_at": now_s}


def mint_project_api_key(runtime_dir: Path, *, project_id: str, label: str, ttl_seconds: int) -> dict[str, Any]:
    pid = project_id.strip()
    if not pid:
        raise ValueError("bad_request")
    if ttl_seconds <= 0:
        raise ValueError("bad_request")

    key_value = secrets.token_urlsafe(32)
    key_hash = _sha256_hex(key_value)

    now = _utcnow()
    expires_at = now + timedelta(seconds=ttl_seconds)

    path = api_keys_path(runtime_dir)
    data = _load_json_or_default(path, {"version": 1, "keys": []})
    keys = data.get("keys")
    if not isinstance(keys, list):
        keys = []

    key_id = str(uuid.uuid4())
    entry = {
        "key_id": key_id,
        "project_id": pid,
        "label": (label or "").strip(),
        "key_hash": key_hash,
        "created_at": _format_rfc3339_utc(now),
        "expires_at": _format_rfc3339_utc(expires_at),
        "revoked_at": None,
    }
    keys.append(entry)
    data["version"] = 1
    data["keys"] = keys
    _atomic_write_private_json(path, data)

    return {
        "key_id": key_id,
        "project_id": pid,
        "label": entry["label"],
        "created_at": entry["created_at"],
        "expires_at": entry["expires_at"],
        "api_key": key_value,
    }


def check_project_api_key(runtime_dir: Path, api_key: str) -> dict[str, Any] | None:
    token = str(api_key or "").strip()
    if not token:
        return None

    token_hash = _sha256_hex(token)
    path = api_keys_path(runtime_dir)
    data = _load_json_or_default(path, {"version": 1, "keys": []})
    keys = data.get("keys")
    if not isinstance(keys, list):
        return None

    now = _utcnow()
    for entry in keys:
        if not isinstance(entry, dict):
            continue
        if entry.get("key_hash") != token_hash:
            continue
        if entry.get("revoked_at") is not None:
            continue
        expires_at = entry.get("expires_at")
        if not isinstance(expires_at, str):
            continue
        expires_dt = _parse_rfc3339_utc(expires_at)
        if not expires_dt:
            continue
        if now >= expires_dt:
            continue
        return {"project_id": str(entry.get("project_id") or "").strip(), "key_id": str(entry.get("key_id") or "").strip()}

    return None
