from __future__ import annotations

import hmac
import os
import re
import subprocess
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dispatch.api_keyring import check_api_key_against_keyring
from dispatch.auth_store import (
    authenticate_access_token,
    check_project_api_key,
    create_project,
    create_user,
    default_auth_config,
    issue_login_tokens,
    mint_project_api_key,
    refresh_session,
)

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse

from dispatch.backends import get_backend_ops
from dispatch.direct_backend import (
    build_systemd_run_direct_command,
    direct_systemd_scope_name,
)
from dispatch.drain_state import set_drain_mode
from dispatch.job_ledger import (
    list_job_paths,
    read_job_record,
    read_job_record_from_path,
    spec_sha256,
    utc_now_rfc3339,
    write_job_record,
)
from dispatch.slurm_adapter import build_squeue_list_command
from dispatch.slurm_observer import parse_squeue_output


def _runtime_dir() -> Path:
    return Path(os.environ.get("SCHED_ORCH_RUNTIME_DIR", "runtime"))


def _drain_state_path() -> Path:
    return _runtime_dir() / "operator" / "drain_state.json"


def _job_logs_dir(server_job_id: str) -> Path:
    return _runtime_dir() / "scheduler-job-ledger" / "logs" / server_job_id


def _job_log_path(server_job_id: str, stream: str) -> Path:
    return _job_logs_dir(server_job_id) / f"{stream}.txt"


def _write_job_log_text(server_job_id: str, stream: str, text: str) -> None:
    path = _job_log_path(server_job_id, stream)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except PermissionError:
        pass


def _read_job_log_text(server_job_id: str, stream: str) -> str | None:
    path = _job_log_path(server_job_id, stream)
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8")


def _api_keyring_path() -> Path:
    configured = os.environ.get("SCHED_ORCH_API_KEYRING_PATH")
    if configured:
        return Path(configured)
    return _runtime_dir() / "auth" / "api_keys.json"


def _expired_api_key_message() -> str:
    # This message is sourced from a markdown file so it stays "documentation-driven"
    # (system-wide doc edits can update the runtime error message).
    configured = os.environ.get("SCHED_ORCH_EXPIRED_API_KEY_MESSAGE_MD_PATH", "").strip()
    candidates: list[Path] = []
    if configured:
        candidates.append(Path(configured))

    # Default to a repo-local docs path when running from source.
    repo_root = Path(__file__).resolve().parents[2]
    candidates.append(repo_root / "docs" / "auth" / "expired_api_key.md")

    for p in candidates:
        try:
            if p.exists():
                return p.read_text(encoding="utf-8").strip()
        except OSError:
            continue

    # Last-resort fallback if the markdown file is unavailable.
    return "API key expired. Regenerate a local key with: python -m dispatch.keyring mint --ttl 30d --label example-client"


def _require_api_key(x_api_key: str | None) -> None:
    # Dispatch job submission uses project API keys.
    # Legacy env-var and legacy keyring remain accepted for backward compatibility,
    # but are not required for server startup.
    if not x_api_key:
        raise HTTPException(status_code=401, detail="unauthorized")

    expected = os.environ.get("SCHED_ORCH_API_KEY")
    if expected and hmac.compare_digest(x_api_key, expected):
        return

    # Prefer Dispatch project key store.
    if check_project_api_key(_runtime_dir(), x_api_key):
        return

    # Legacy keyring auth (format-compatible with project key store, but not required).
    keyring_path = _api_keyring_path()
    if keyring_path.exists():
        result = check_api_key_against_keyring(keyring_path, x_api_key, now=datetime.now(timezone.utc))
        if result.ok:
            return
        if result.expired:
            raise HTTPException(
                status_code=401,
                detail={"message": _expired_api_key_message()},
            )

    raise HTTPException(status_code=401, detail="unauthorized")


def _parse_sbatch_submission_stdout(stdout: str) -> str | None:
    m = re.search(r"Submitted batch job\s+(\d+)", stdout)
    if not m:
        return None
    return m.group(1)


def _scheduler_execution_enabled() -> bool:
    # Safety default: dry-run unless explicitly enabled.
    val = os.environ.get("SCHED_ORCH_ENABLE_SCHEDULER_EXEC", "0").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _direct_cancel_enabled() -> bool:
    # Cancellation is a control action; keep it explicitly gated for direct execution backend.
    val = os.environ.get("SCHED_ORCH_ENABLE_DIRECT_CANCEL", "0").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _direct_execution_enabled() -> bool:
    # Safety default: do not execute direct workloads unless explicitly enabled.
    val = os.environ.get("SCHED_ORCH_ENABLE_DIRECT_EXEC", "0").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _direct_payload_execution_enabled() -> bool:
    # Conservative rollout: keep direct backend "real payload" execution gated separately.
    # If this is not enabled, direct execution will run a placeholder payload ("true").
    val = os.environ.get("SCHED_ORCH_ENABLE_DIRECT_PAYLOAD_EXEC", "0").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _direct_log_capture_max_bytes() -> int:
    raw = os.environ.get("SCHED_ORCH_DIRECT_LOG_CAPTURE_MAX_BYTES", "65536").strip()
    try:
        n = int(raw)
    except ValueError:
        return 65536

    if n < 0:
        return 0
    if n > 10 * 1024 * 1024:
        return 10 * 1024 * 1024
    return n


def _execution_backend() -> str:
    return os.environ.get("SCHED_ORCH_EXECUTION_BACKEND", "slurm").strip().lower()


def _startup_reconcile_enabled() -> bool:
    # Safety default: do not run any startup reconciliation unless explicitly enabled.
    val = os.environ.get("SCHED_ORCH_ENABLE_STARTUP_RECONCILE", "0").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _startup_reconcile_max_jobs() -> int:
    raw = os.environ.get("SCHED_ORCH_STARTUP_RECONCILE_MAX_JOBS", "50").strip()
    try:
        n = int(raw)
    except ValueError:
        return 50
    return _cap_reconcile_job_limit(n)


def _bulk_refresh_enabled() -> bool:
    # Safety default: bulk refresh is an active scheduler query + ledger mutation, so keep it gated.
    val = os.environ.get("SCHED_ORCH_ENABLE_BULK_REFRESH", "0").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _bulk_refresh_max_jobs() -> int:
    raw = os.environ.get("SCHED_ORCH_BULK_REFRESH_MAX_JOBS", "200").strip()
    try:
        n = int(raw)
    except ValueError:
        return 200
    return _cap_reconcile_job_limit(n)


def _cap_reconcile_job_limit(n: int) -> int:
    if n < 0:
        return 0
    if n > 1000:
        return 1000
    return n


def _direct_refresh_enabled() -> bool:
    # Refresh is an observation/control-plane query; keep it explicitly gated for direct backend.
    val = os.environ.get("SCHED_ORCH_ENABLE_DIRECT_REFRESH", "0").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _direct_state_inference_enabled() -> bool:
    # Optional promotion from low-level scope state to high-level job state.
    val = os.environ.get("SCHED_ORCH_ENABLE_DIRECT_STATE_INFERENCE", "0").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _terminal_job_states() -> set[str]:
    return {"blocked", "failed", "canceled", "succeeded"}


def _execution_backend_for_record(record: dict[str, Any]) -> str | None:
    # Backward-compatibility for older ledger entries.
    backend = record.get("execution_backend")
    if isinstance(backend, str) and backend.strip():
        return backend.strip().lower()

    scheduler_job_id = record.get("scheduler_job_id")
    if isinstance(scheduler_job_id, str) and scheduler_job_id.strip():
        return "slurm"

    scope_name = record.get("direct_scope_name")
    if isinstance(scope_name, str) and scope_name.strip():
        return "direct"

    return None


def create_app() -> FastAPI:
    # Dispatch starts even when no users or API keys exist yet.
    # Job submission endpoints will return 401 until a project API key is minted.

    def _startup_reconcile_ledger_best_effort() -> None:
        if not _startup_reconcile_enabled():
            return

        max_jobs = _startup_reconcile_max_jobs()
        if max_jobs <= 0:
            return

        # Best-effort reconciliation only: never fail server startup due to scheduler/tooling issues.
        try:
            runtime_dir = _runtime_dir()
            paths = list_job_paths(runtime_dir)
            if not paths:
                return

            terminal_states = _terminal_job_states()

            records = [read_job_record_from_path(path) for path in paths]

            backend_names = sorted({str(r.get("execution_backend") or "").strip().lower() for r in records})
            for backend_name in backend_names:
                if not backend_name:
                    continue

                backend_ops = get_backend_ops(
                    backend_name,
                    drain_state_path=_drain_state_path(),
                    direct_payload_execution_enabled=_direct_payload_execution_enabled(),
                    direct_refresh_enabled=_direct_refresh_enabled(),
                    direct_state_inference_enabled=_direct_state_inference_enabled(),
                )
                if backend_ops and backend_ops.bulk_refresh:
                    backend_ops.bulk_refresh(runtime_dir, records, max_jobs, terminal_states)
        except Exception:
            return

    @asynccontextmanager
    async def _lifespan(app: FastAPI):
        _startup_reconcile_ledger_best_effort()
        yield

    app = FastAPI(title="scheduler-orchestration", version="0.1.0", lifespan=_lifespan)

    def _error_code_for_http(status_code: int) -> str:
        if status_code == 400:
            return "bad_request"
        if status_code == 401:
            return "unauthorized"
        if status_code == 404:
            return "not_found"
        if status_code == 500:
            return "server_error"
        return "error"

    @app.exception_handler(HTTPException)
    def _handle_http_exception(request: Request, exc: HTTPException) -> JSONResponse:
        # Security best practice: stable, non-leaky error bodies.
        code = _error_code_for_http(exc.status_code)
        payload: dict[str, Any] = {"error": code}

        if isinstance(exc.detail, dict):
            message = exc.detail.get("message")
            if isinstance(message, str) and message.strip():
                payload["message"] = message

        return JSONResponse(status_code=exc.status_code, content=payload)

    @app.exception_handler(RequestValidationError)
    def _handle_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(status_code=400, content={"error": "bad_request"})

    def _bearer_token_from_header(authorization: str | None) -> str | None:
        raw = str(authorization or "").strip()
        if not raw:
            return None
        parts = raw.split(" ", 1)
        if len(parts) != 2:
            return None
        if parts[0].lower() != "bearer":
            return None
        token = parts[1].strip()
        if not token:
            return None
        return token

    def _require_user_session(authorization: str | None) -> dict[str, Any]:
        token = _bearer_token_from_header(authorization)
        if not token:
            raise HTTPException(status_code=401, detail="unauthorized")

        session = authenticate_access_token(_runtime_dir(), token)
        if not session:
            raise HTTPException(status_code=401, detail="unauthorized")
        return session

    @app.post("/v1/users", status_code=201)
    def create_user_endpoint(body: dict[str, Any]) -> dict[str, Any]:
        username = body.get("username")
        password = body.get("password")
        if not isinstance(username, str) or not isinstance(password, str):
            raise HTTPException(status_code=400, detail="bad_request")
        try:
            return create_user(_runtime_dir(), username=username, password=password)
        except ValueError as exc:
            if str(exc) == "conflict":
                raise HTTPException(status_code=409, detail="conflict")
            raise HTTPException(status_code=400, detail="bad_request")

    @app.post("/v1/login")
    def login_endpoint(body: dict[str, Any], request: Request) -> dict[str, Any]:
        username = body.get("username")
        password = body.get("password")
        if not isinstance(username, str) or not isinstance(password, str):
            raise HTTPException(status_code=400, detail="bad_request")

        cfg = default_auth_config()
        try:
            tokens = issue_login_tokens(_runtime_dir(), username=username, password=password, cfg=cfg)
        except ValueError:
            raise HTTPException(status_code=401, detail="unauthorized")

        response = {
            "access_token": tokens["access_token"],
            "access_expires_at": tokens["access_expires_at"],
            "refresh_token": tokens["refresh_token"],
            "refresh_expires_at": tokens["refresh_expires_at"],
            "user": tokens.get("user") or {},
        }

        # Set refresh cookie for browser clients.
        from fastapi.responses import JSONResponse as _JSONResponse

        res = _JSONResponse(status_code=200, content=response)
        res.set_cookie(
            key=cfg.refresh_cookie_name,
            value=tokens["refresh_token"],
            httponly=True,
            samesite="lax",
            secure=False,
            path="/",
        )
        return res

    @app.post("/v1/token/refresh")
    def refresh_endpoint(request: Request, body: dict[str, Any] | None = None) -> dict[str, Any]:
        cfg = default_auth_config()

        provided = ""
        if isinstance(body, dict):
            val = body.get("refresh_token")
            if isinstance(val, str):
                provided = val

        cookie_val = request.cookies.get(cfg.refresh_cookie_name, "")
        refresh_token = cookie_val or provided
        if not refresh_token:
            raise HTTPException(status_code=401, detail="unauthorized")

        try:
            tokens = refresh_session(_runtime_dir(), refresh_token=refresh_token, cfg=cfg)
        except ValueError:
            raise HTTPException(status_code=401, detail="unauthorized")

        response = {
            "access_token": tokens["access_token"],
            "access_expires_at": tokens["access_expires_at"],
            "refresh_token": tokens["refresh_token"],
            "refresh_expires_at": tokens["refresh_expires_at"],
            "user": tokens.get("user") or {},
        }

        from fastapi.responses import JSONResponse as _JSONResponse

        res = _JSONResponse(status_code=200, content=response)
        res.set_cookie(
            key=cfg.refresh_cookie_name,
            value=tokens["refresh_token"],
            httponly=True,
            samesite="lax",
            secure=False,
            path="/",
        )
        return res

    @app.get("/ui", response_class=HTMLResponse)
    def ui() -> str:
        # Minimal GUI shell. It intentionally calls the same /v1 API endpoints.
        return """<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Dispatch</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 24px; max-width: 980px; }
    h1 { margin: 0 0 12px 0; }
    .row { display: flex; gap: 16px; flex-wrap: wrap; }
    .card { border: 1px solid #ddd; padding: 12px; border-radius: 8px; min-width: 320px; }
    label { display: block; font-size: 12px; margin-top: 8px; }
    input { width: 100%; padding: 6px; }
    button { margin-top: 10px; padding: 8px 12px; }
    pre { background: #f7f7f7; padding: 10px; border-radius: 8px; overflow-x: auto; }
  </style>
</head>
<body>
  <h1>Dispatch</h1>
  <p>Local-first orchestration server. This UI calls the same /v1 API endpoints used by programs.</p>

  <div class=\"row\">
    <div class=\"card\">
      <h3>Sign up</h3>
      <label>Username</label><input id=\"su_user\" />
      <label>Password</label><input id=\"su_pass\" type=\"password\" />
      <button onclick=\"signup()\">Create user</button>
    </div>

    <div class=\"card\">
      <h3>Login</h3>
      <label>Username</label><input id=\"li_user\" />
      <label>Password</label><input id=\"li_pass\" type=\"password\" />
      <button onclick=\"login()\">Login</button>
      <div style=\"margin-top:8px; font-size: 12px\">Access token is kept in memory; refresh token is stored as an HttpOnly cookie.</div>
    </div>

    <div class=\"card\">
      <h3>Project and API key</h3>
      <label>Project name</label><input id=\"pr_name\" />
      <button onclick=\"createProject()\">Create project</button>
      <label style=\"margin-top:12px\">Project id</label><input id=\"pr_id\" />
      <label>Key label</label><input id=\"key_label\" value=\"default\" />
      <button onclick=\"mintKey()\">Mint API key</button>
    </div>
  </div>

  <h3>Output</h3>
  <pre id=\"out\"></pre>

<script>
let accessToken = null;

function show(obj) {
  document.getElementById('out').textContent = JSON.stringify(obj, null, 2);
}

async function signup() {
  const username = document.getElementById('su_user').value;
  const password = document.getElementById('su_pass').value;
  const r = await fetch('/v1/users', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({username, password})});
  show({status: r.status, body: await r.json()});
}

async function login() {
  const username = document.getElementById('li_user').value;
  const password = document.getElementById('li_pass').value;
  const r = await fetch('/v1/login', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({username, password})});
  const body = await r.json();
  if (r.status === 200) {
    accessToken = body.access_token;
  }
  show({status: r.status, body});
}

async function createProject() {
  const name = document.getElementById('pr_name').value;
  const r = await fetch('/v1/projects', {method:'POST', headers:{'Content-Type':'application/json', 'Authorization': 'Bearer ' + accessToken}, body: JSON.stringify({name})});
  const body = await r.json();
  if (r.status === 201) {
    document.getElementById('pr_id').value = body.project_id;
  }
  show({status: r.status, body});
}

async function mintKey() {
  const project_id = document.getElementById('pr_id').value;
  const label = document.getElementById('key_label').value;
  const r = await fetch('/v1/projects/' + project_id + '/api-keys', {method:'POST', headers:{'Content-Type':'application/json', 'Authorization': 'Bearer ' + accessToken}, body: JSON.stringify({label, ttl_seconds: 3600})});
  show({status: r.status, body: await r.json()});
}
</script>
</body>
</html>"""

    @app.post("/v1/projects", status_code=201)
    def create_project_endpoint(body: dict[str, Any], authorization: str | None = Header(default=None, alias="Authorization")) -> dict[str, Any]:
        session = _require_user_session(authorization)
        name = body.get("name")
        if not isinstance(name, str):
            raise HTTPException(status_code=400, detail="bad_request")
        try:
            return create_project(_runtime_dir(), owner_user_id=str(session["user_id"]), name=name)
        except ValueError:
            raise HTTPException(status_code=400, detail="bad_request")

    @app.post("/v1/projects/{project_id}/api-keys", status_code=201)
    def mint_api_key_endpoint(
        project_id: str,
        body: dict[str, Any],
        authorization: str | None = Header(default=None, alias="Authorization"),
    ) -> dict[str, Any]:
        _require_user_session(authorization)
        label = body.get("label")
        ttl_seconds = body.get("ttl_seconds", 30 * 24 * 3600)
        if not isinstance(label, str):
            raise HTTPException(status_code=400, detail="bad_request")
        if not isinstance(ttl_seconds, int):
            raise HTTPException(status_code=400, detail="bad_request")

        try:
            return mint_project_api_key(_runtime_dir(), project_id=project_id, label=label, ttl_seconds=ttl_seconds)
        except ValueError:
            raise HTTPException(status_code=400, detail="bad_request")

    @app.get("/v1/projects/{project_id}/api-keys")
    def list_api_keys_endpoint(project_id: str, authorization: str | None = Header(default=None, alias="Authorization")) -> dict[str, Any]:
        _require_user_session(authorization)
        keyring_path = _api_keyring_path()
        items: list[dict[str, Any]] = []
        if keyring_path.exists():
            try:
                import json as _json

                data = _json.loads(keyring_path.read_text(encoding="utf-8"))
                keys = data.get("keys") if isinstance(data, dict) else []
                if isinstance(keys, list):
                    for rec in keys:
                        if not isinstance(rec, dict):
                            continue
                        if str(rec.get("project_id") or "") != str(project_id):
                            continue
                        if rec.get("revoked_at") is not None:
                            continue
                        items.append(
                            {
                                "key_id": str(rec.get("key_id") or ""),
                                "project_id": str(rec.get("project_id") or ""),
                                "label": str(rec.get("label") or ""),
                                "created_at": str(rec.get("created_at") or ""),
                                "expires_at": str(rec.get("expires_at") or ""),
                            }
                        )
            except Exception:
                pass
        return {"items": items}

    @app.delete("/v1/api-keys/{key_id}")
    def revoke_api_key_endpoint(key_id: str, authorization: str | None = Header(default=None, alias="Authorization")) -> dict[str, Any]:
        _require_user_session(authorization)

        keyring_path = _api_keyring_path()
        if not keyring_path.exists():
            raise HTTPException(status_code=404, detail="not_found")

        try:
            import json as _json

            data = _json.loads(keyring_path.read_text(encoding="utf-8"))
        except Exception:
            raise HTTPException(status_code=404, detail="not_found")

        if not isinstance(data, dict):
            raise HTTPException(status_code=404, detail="not_found")

        keys = data.get("keys")
        if not isinstance(keys, list):
            raise HTTPException(status_code=404, detail="not_found")

        found = False
        now_s = utc_now_rfc3339()
        for rec in keys:
            if not isinstance(rec, dict):
                continue
            if str(rec.get("key_id") or "") != str(key_id):
                continue
            if rec.get("revoked_at") is None:
                rec["revoked_at"] = now_s
            found = True
            break

        if not found:
            raise HTTPException(status_code=404, detail="not_found")

        try:
            keyring_path.parent.mkdir(parents=True, exist_ok=True)
            keyring_path.write_text(__import__("json").dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            try:
                os.chmod(keyring_path, 0o600)
            except PermissionError:
                pass
        except Exception:
            raise HTTPException(status_code=500, detail="server_error")

        return {"key_id": str(key_id), "revoked": True}

    @app.get("/v1/queue")
    def list_queue(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> dict[str, Any]:
        _require_api_key(x_api_key)

        cmd = build_squeue_list_command()
        try:
            proc = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
            raise HTTPException(status_code=500, detail="server_error")

        return {"items": parse_squeue_output(proc.stdout)}

    @app.post("/v1/drain")
    def set_drain(
        body: dict[str, Any],
        x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    ) -> dict[str, Any]:
        _require_api_key(x_api_key)

        enabled = bool(body.get("enabled"))
        state = set_drain_mode(_drain_state_path(), enabled=enabled)
        return {"enabled": state.enabled, "updated_at": state.updated_at}

    @app.post("/v1/jobs", status_code=202)
    def submit_job(
        body: dict[str, Any],
        x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    ) -> dict[str, Any]:
        _require_api_key(x_api_key)

        spec = body.get("spec")
        if not isinstance(spec, dict):
            raise HTTPException(status_code=400, detail="bad_request")

        server_job_id = str(uuid.uuid4())
        backend = _execution_backend()

        backend_ops = get_backend_ops(
            backend,
            drain_state_path=_drain_state_path(),
            direct_payload_execution_enabled=_direct_payload_execution_enabled(),
            direct_refresh_enabled=_direct_refresh_enabled(),
            direct_state_inference_enabled=_direct_state_inference_enabled(),
        )
        if not backend_ops:
            raise HTTPException(status_code=400, detail="bad_request")

        plan = backend_ops.build_plan(spec, _drain_state_path())

        created_at = utc_now_rfc3339()
        spec_hash = spec_sha256(spec)

        scheduler_job_id: str | None = None
        exit_code: int | None = None
        direct_scope_name: str | None = direct_systemd_scope_name(server_job_id) if backend == "direct" else None
        log_capture: dict[str, Any] = {"stdout": False, "stderr": False}
        state = "accepted" if plan["allowed"] else "blocked"

        execution_enabled = _direct_execution_enabled() if backend == "direct" else _scheduler_execution_enabled()

        execute_submission = getattr(backend_ops, "execute_submission", None)
        if execute_submission:
            try:
                updates = execute_submission(_runtime_dir(), server_job_id, plan, execution_enabled)
            except ValueError:
                raise HTTPException(status_code=500, detail="server_error")

            if isinstance(updates, dict):
                scheduler_job_id = updates.get("scheduler_job_id") if isinstance(updates.get("scheduler_job_id"), str) else scheduler_job_id
                exit_code = updates.get("exit_code") if isinstance(updates.get("exit_code"), int) else exit_code
                direct_scope_name = (
                    updates.get("direct_scope_name") if isinstance(updates.get("direct_scope_name"), str) else direct_scope_name
                )
                if isinstance(updates.get("log_capture"), dict):
                    log_capture = updates["log_capture"]
                if isinstance(updates.get("state"), str) and updates.get("state"):
                    state = updates["state"]


        record = {
            "server_job_id": server_job_id,
            "created_at": created_at,
            "spec_sha256": spec_hash,
            "state": state,
            "scheduler_job_id": scheduler_job_id,
            "exit_code": exit_code,
            "execution_backend": backend,
            "direct_scope_name": direct_scope_name,
            "log_capture": log_capture,
            "accepted": bool(plan["allowed"]),
            "reason": plan["reason"],
            "command": plan["command"],
            "spec": spec,
        }
        write_job_record(_runtime_dir(), record)

        if plan["allowed"] and backend != "direct" and _scheduler_execution_enabled() and state != "submitted":
            raise HTTPException(status_code=500, detail="server_error")

        return {
            "server_job_id": server_job_id,
            "accepted": bool(plan["allowed"]),
            "reason": plan["reason"],
            "command": plan["command"],
        }

    @app.get("/v1/jobs")
    def list_jobs(
        refresh: bool = Query(default=False),
        x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    ) -> dict[str, Any]:
        _require_api_key(x_api_key)

        runtime_dir = _runtime_dir()

        if refresh:
            if not _bulk_refresh_enabled():
                raise HTTPException(status_code=400, detail="bad_request")

            max_jobs = _bulk_refresh_max_jobs()
            if max_jobs > 0:
                try:
                    records = [read_job_record_from_path(path) for path in list_job_paths(runtime_dir)]
                    terminal_states = _terminal_job_states()

                    backend_names = sorted({str(r.get("execution_backend") or "").strip().lower() for r in records})
                    for backend_name in backend_names:
                        if not backend_name:
                            continue

                        backend_ops = get_backend_ops(
                            backend_name,
                            drain_state_path=_drain_state_path(),
                            direct_payload_execution_enabled=_direct_payload_execution_enabled(),
                            direct_refresh_enabled=_direct_refresh_enabled(),
                            direct_state_inference_enabled=_direct_state_inference_enabled(),
                        )
                        if backend_ops and backend_ops.bulk_refresh:
                            backend_ops.bulk_refresh(runtime_dir, records, max_jobs, terminal_states)
                except Exception:
                    # Best-effort only: do not fail the list endpoint for refresh failures.
                    pass

        items: list[dict[str, Any]] = []
        for path in reversed(list_job_paths(runtime_dir)):
            record = read_job_record_from_path(path)
            items.append(
                {
                    "server_job_id": record.get("server_job_id"),
                    "created_at": record.get("created_at"),
                    "state": record.get("state", "unknown"),
                    "scheduler_job_id": record.get("scheduler_job_id"),
                    "execution_backend": record.get("execution_backend"),
                    "accepted": record.get("accepted"),
                    "reason": record.get("reason"),
                    "spec_sha256": record.get("spec_sha256"),
                    "job_name": (record.get("spec") or {}).get("job_name") if isinstance(record.get("spec"), dict) else None,
                }
            )

        return {"items": items}

    @app.get("/v1/jobs/{server_job_id}")
    def get_job(
        server_job_id: str,
        refresh: bool = Query(default=False),
        x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    ) -> dict[str, Any]:
        _require_api_key(x_api_key)

        record = read_job_record(_runtime_dir(), server_job_id)
        if not record:
            raise HTTPException(status_code=404, detail="not_found")

        backend = _execution_backend_for_record(record)

        backend_ops = get_backend_ops(
            str(backend or ""),
            drain_state_path=_drain_state_path(),
            direct_payload_execution_enabled=_direct_payload_execution_enabled(),
            direct_refresh_enabled=_direct_refresh_enabled(),
            direct_state_inference_enabled=_direct_state_inference_enabled(),
        )
        if backend_ops and backend_ops.refresh_job:
            try:
                backend_ops.refresh_job(_runtime_dir(), record, refresh_requested=refresh)
            except ValueError:
                # Backend uses ValueError for request-level policy violations.
                raise HTTPException(status_code=400, detail="bad_request")
            except Exception:
                # Best-effort; do not fail request for observation errors.
                pass

        return {
            "server_job_id": record.get("server_job_id", server_job_id),
            "state": record.get("state", "unknown"),
            "scheduler_job_id": record.get("scheduler_job_id"),
            "detail": record,
        }

    @app.get("/v1/jobs/{server_job_id}/logs")
    def get_job_logs(
        server_job_id: str,
        x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    ) -> dict[str, Any]:
        _require_api_key(x_api_key)

        record = read_job_record(_runtime_dir(), server_job_id)
        if not record:
            raise HTTPException(status_code=404, detail="not_found")

        backend = _execution_backend_for_record(record)

        backend_ops = get_backend_ops(
            str(backend or ""),
            drain_state_path=_drain_state_path(),
            direct_payload_execution_enabled=_direct_payload_execution_enabled(),
            direct_refresh_enabled=_direct_refresh_enabled(),
            direct_state_inference_enabled=_direct_state_inference_enabled(),
        )
        if not backend_ops or not backend_ops.get_logs:
            raise HTTPException(status_code=400, detail="bad_request")

        logs = backend_ops.get_logs(_runtime_dir(), record, server_job_id=server_job_id)
        if not logs:
            raise HTTPException(status_code=404, detail="not_found")

        return {
            "server_job_id": server_job_id,
            "execution_backend": str(backend or ""),
            "stdout": str(logs.get("stdout") or ""),
            "stderr": str(logs.get("stderr") or ""),
        }

    @app.delete("/v1/jobs/{server_job_id}")
    def cancel_job(
        server_job_id: str,
        x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    ) -> dict[str, Any]:
        _require_api_key(x_api_key)

        record = read_job_record(_runtime_dir(), server_job_id)
        if not record:
            raise HTTPException(status_code=404, detail="not_found")

        backend = _execution_backend_for_record(record)

        # Conservative policy: cancellation is an execution/control action, so keep it gated.
        if backend == "slurm" and not _scheduler_execution_enabled():
            raise HTTPException(status_code=400, detail="bad_request")
        if backend == "direct" and not _direct_cancel_enabled():
            raise HTTPException(status_code=400, detail="bad_request")

        backend_ops = get_backend_ops(
            str(backend or ""),
            drain_state_path=_drain_state_path(),
            direct_payload_execution_enabled=_direct_payload_execution_enabled(),
            direct_refresh_enabled=_direct_refresh_enabled(),
            direct_state_inference_enabled=_direct_state_inference_enabled(),
        )
        if not backend_ops:
            raise HTTPException(status_code=400, detail="bad_request")

        cmd = backend_ops.build_cancel_command(record)
        if not cmd:
            raise HTTPException(status_code=400, detail="bad_request")

        execute_cancel = getattr(backend_ops, "execute_cancel", None)

        # Backward-compatible fallback for tests/stubs: if backend ops does not provide
        # an execution function, execute the cancel command directly here.
        if not execute_cancel:
            try:
                subprocess.run(
                    cmd,
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
            except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
                record["state"] = "failed"
                record["cancel_failed_at"] = utc_now_rfc3339()
                write_job_record(_runtime_dir(), record)
                raise HTTPException(status_code=500, detail="server_error")
        else:
            try:
                execute_cancel(record, _scheduler_execution_enabled() if backend == "slurm" else _direct_cancel_enabled())
            except ValueError:
                raise HTTPException(status_code=400, detail="bad_request")
            except Exception:
                record["state"] = "failed"
                record["cancel_failed_at"] = utc_now_rfc3339()
                write_job_record(_runtime_dir(), record)
                raise HTTPException(status_code=500, detail="server_error")

        record["state"] = "canceled"
        record["canceled_at"] = utc_now_rfc3339()
        write_job_record(_runtime_dir(), record)

        return {"server_job_id": server_job_id, "canceled": True}

    return app
