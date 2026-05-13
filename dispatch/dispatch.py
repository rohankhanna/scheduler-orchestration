from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8780
DEFAULT_LOG_LINES = 200


def _runtime_dir_from_env() -> Path:
    return Path(os.environ.get("SCHED_ORCH_RUNTIME_DIR", "runtime"))


def _runtime_dir(arg: str | None) -> Path:
    if arg:
        return Path(arg)
    return _runtime_dir_from_env()


def _server_state_dir(runtime_dir: Path) -> Path:
    return runtime_dir / "operator" / "server"


def _default_pid_path(runtime_dir: Path) -> Path:
    return _server_state_dir(runtime_dir) / "dispatch-server.pid"


def _default_log_path(runtime_dir: Path) -> Path:
    return _server_state_dir(runtime_dir) / "dispatch-server.log"


def _default_meta_path(runtime_dir: Path) -> Path:
    return _server_state_dir(runtime_dir) / "dispatch-server.meta.json"


def _read_pid(path: Path) -> int | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _pid_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # If we cannot signal it, treat it as running.
        return True
    return True


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _write_text_atomic(path: Path, text: str, mode: int = 0o600) -> None:
    _ensure_parent_dir(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    try:
        os.chmod(tmp, mode)
    except PermissionError:
        pass
    tmp.replace(path)


def _best_effort_remove(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return
    except OSError:
        return


def _tail_lines(path: Path, n: int) -> list[str]:
    if n <= 0:
        return []
    try:
        data = path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return []
    except OSError:
        return []
    lines = data.splitlines()
    return lines[-n:]


def _follow_file(path: Path) -> int:
    # Simple follow implementation: seek to end, then poll for new lines.
    try:
        f = path.open("r", encoding="utf-8", errors="replace")
    except FileNotFoundError:
        sys.stderr.write(f"log file not found: {path}\n")
        return 2

    with f:
        f.seek(0, 2)
        while True:
            chunk = f.readline()
            if chunk:
                sys.stdout.write(chunk)
                sys.stdout.flush()
                continue
            time.sleep(0.25)
    return 0


def _python_exe() -> str:
    return sys.executable or "python"


def _uvicorn_argv(host: str, port: int, reload: bool) -> list[str]:
    # Use the app factory so the server module doesn't need a global `app`.
    argv = [
        _python_exe(),
        "-m",
        "uvicorn",
        "dispatch.server.app:create_app",
        "--factory",
        "--host",
        host,
        "--port",
        str(port),
    ]
    if reload:
        argv.append("--reload")
    return argv


@dataclass(frozen=True)
class ServerPaths:
    pid_path: Path
    log_path: Path
    meta_path: Path


def _server_paths(runtime_dir: Path, pid_path: str | None, log_path: str | None) -> ServerPaths:
    pid = Path(pid_path) if pid_path else _default_pid_path(runtime_dir)
    log = Path(log_path) if log_path else _default_log_path(runtime_dir)
    meta = _default_meta_path(runtime_dir)
    return ServerPaths(pid_path=pid, log_path=log, meta_path=meta)


def _now_epoch_s() -> int:
    return int(time.time())


def _write_server_meta(
    path: Path,
    *,
    runtime_dir: Path,
    host: str,
    port: int,
    foreground: bool,
    reload: bool,
    pid: int | None,
    pid_file: Path,
    log_file: Path,
) -> None:
    # Meta is intentionally non-secret: it contains only process/runtime topology.
    payload = {
        "runtime_dir": str(runtime_dir),
        "host": host,
        "port": int(port),
        "foreground": bool(foreground),
        "reload": bool(reload),
        "pid": int(pid) if pid is not None else None,
        "pid_file": str(pid_file),
        "log_file": str(log_file),
        "base_url": f"http://{host}:{int(port)}",
        "started_at_epoch_s": _now_epoch_s(),
    }
    _write_text_atomic(path, json.dumps(payload, sort_keys=True) + "\n", mode=0o600)


def _read_server_meta(path: Path) -> dict[str, Any] | None:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    return data


def _is_dispatch_server_at(base_url: str) -> bool:
    # Best-effort only; used for diagnostics. Must not require auth and must not print response bodies.
    try:
        import httpx

        with httpx.Client(base_url=base_url.rstrip("/"), timeout=2.0) as http:
            r = http.get("/openapi.json")
            if r.status_code != 200:
                return False
            body = r.json()
            if not isinstance(body, dict):
                return False
            info = body.get("info")
            if not isinstance(info, dict):
                return False
            title = str(info.get("title") or "").strip()
            # Current FastAPI app title is "scheduler-orchestration".
            return title == "scheduler-orchestration"
    except Exception:
        return False


def _cmd_server_start(args: argparse.Namespace) -> int:
    runtime_dir = _runtime_dir(args.runtime_dir)
    paths = _server_paths(runtime_dir, args.pid_file, args.log_file)

    existing_pid = _read_pid(paths.pid_path)
    if existing_pid is not None and _pid_is_running(existing_pid):
        sys.stderr.write(f"dispatch server already running (pid {existing_pid})\n")
        return 2

    # Ensure runtime dir exists.
    runtime_dir.mkdir(parents=True, exist_ok=True)

    # The server reads SCHED_ORCH_RUNTIME_DIR; set it explicitly for the child process.
    env = dict(os.environ)
    env["SCHED_ORCH_RUNTIME_DIR"] = str(runtime_dir)

    argv = _uvicorn_argv(host=args.host, port=args.port, reload=args.reload)

    if args.foreground:
        sys.stderr.write("starting dispatch server (foreground)\n")
        sys.stderr.write(f"runtime_dir: {runtime_dir}\n")
        sys.stderr.write(f"listen: http://{args.host}:{args.port}\n")
        sys.stderr.write(f"ui:     http://{args.host}:{args.port}/ui\n")
        sys.stderr.write("\n")
        sys.stderr.write("NOTE: foreground mode is for development only.\n")
        sys.stderr.write("      It does NOT write pid/log files, so `dispatch server status/stop` cannot manage it.\n")
        sys.stderr.write("      For a managed server, omit --foreground.\n")
        sys.stderr.write("\n")

        # Write non-secret meta for later diagnostics (still no pid/log files).
        try:
            _write_server_meta(
                paths.meta_path,
                runtime_dir=runtime_dir,
                host=str(args.host),
                port=int(args.port),
                foreground=True,
                reload=bool(args.reload),
                pid=None,
                pid_file=paths.pid_path,
                log_file=paths.log_path,
            )
        except Exception:
            pass

        # In foreground mode, do not write PID/log files.
        proc = subprocess.run(argv, env=env)
        return int(proc.returncode)

    _ensure_parent_dir(paths.log_path)
    log_f = paths.log_path.open("a", encoding="utf-8")
    try:
        try:
            os.chmod(paths.log_path, 0o600)
        except PermissionError:
            pass

        proc = subprocess.Popen(
            argv,
            env=env,
            stdout=log_f,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    finally:
        # Keep file descriptor open for the child; parent can close its handle.
        log_f.close()

    _write_text_atomic(paths.pid_path, f"{proc.pid}\n", mode=0o600)

    try:
        _write_server_meta(
            paths.meta_path,
            runtime_dir=runtime_dir,
            host=str(args.host),
            port=int(args.port),
            foreground=False,
            reload=bool(args.reload),
            pid=int(proc.pid),
            pid_file=paths.pid_path,
            log_file=paths.log_path,
        )
    except Exception:
        pass

    sys.stdout.write("dispatch server started\n")
    sys.stdout.write(f"pid:        {proc.pid}\n")
    sys.stdout.write(f"runtime_dir: {runtime_dir}\n")
    sys.stdout.write(f"log:        {paths.log_path}\n")
    sys.stdout.write(f"listen:     http://{args.host}:{args.port}\n")
    sys.stdout.write(f"ui:         http://{args.host}:{args.port}/ui\n")
    return 0


def _cmd_server_status(args: argparse.Namespace) -> int:
    runtime_dir = _runtime_dir(args.runtime_dir)
    paths = _server_paths(runtime_dir, args.pid_file, args.log_file)

    pid = _read_pid(paths.pid_path)
    if pid is None:
        sys.stdout.write("dispatch server: not running (no pid file)\n")
        sys.stdout.write(f"pid_file: {paths.pid_path}\n")

        # Diagnostics for common mismatch: the port is serving Dispatch, but not managed under this runtime dir.
        meta = _read_server_meta(paths.meta_path)
        base_url = None
        foreground = None
        if meta:
            base_url = str(meta.get("base_url") or "").strip() or None
            foreground = bool(meta.get("foreground")) if "foreground" in meta else None

        # Fall back to the default listen address, which is the common local dev/operator default.
        if not base_url:
            base_url = f"http://{DEFAULT_HOST}:{DEFAULT_PORT}"

        if _is_dispatch_server_at(base_url):
            sys.stdout.write("WARNING: dispatch server appears to be running outside this runtime dir or outside managed mode.\n")
            sys.stdout.write(f"detected_url: {base_url}/ui\n")
            if foreground is True:
                sys.stdout.write("note: the last recorded start under this runtime dir was foreground mode (dev-only).\n")
            sys.stdout.write("hint: start managed mode with: dispatch server start --runtime-dir /ABS/PATH\n")

        return 1

    if not _pid_is_running(pid):
        sys.stdout.write("dispatch server: not running (stale pid file)\n")
        sys.stdout.write(f"pid_file: {paths.pid_path}\n")
        sys.stdout.write(f"pid: {pid}\n")
        return 1

    sys.stdout.write("dispatch server: running\n")
    sys.stdout.write(f"pid: {pid}\n")
    sys.stdout.write(f"pid_file: {paths.pid_path}\n")
    sys.stdout.write(f"log: {paths.log_path}\n")
    return 0


def _cmd_server_stop(args: argparse.Namespace) -> int:
    runtime_dir = _runtime_dir(args.runtime_dir)
    paths = _server_paths(runtime_dir, args.pid_file, args.log_file)

    pid = _read_pid(paths.pid_path)
    if pid is None:
        sys.stderr.write("dispatch server not running (no pid file)\n")
        sys.stderr.write(f"pid_file: {paths.pid_path}\n")
        return 1

    if not _pid_is_running(pid):
        sys.stderr.write("dispatch server not running (stale pid file)\n")
        sys.stderr.write(f"pid: {pid}\n")
        _best_effort_remove(paths.pid_path)
        sys.stderr.write("removed stale pid file\n")
        return 0

    sig = signal.SIGTERM
    sys.stdout.write(f"stopping dispatch server (pid {pid})\n")
    try:
        os.kill(pid, sig)
    except ProcessLookupError:
        _best_effort_remove(paths.pid_path)
        return 0

    deadline = time.time() + float(args.timeout)
    while time.time() < deadline:
        if not _pid_is_running(pid):
            _best_effort_remove(paths.pid_path)
            sys.stdout.write("stopped\n")
            return 0
        time.sleep(0.1)

    sys.stderr.write("timeout waiting for server to stop; sending SIGKILL\n")
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass

    _best_effort_remove(paths.pid_path)
    return 0


def _cmd_server_logs(args: argparse.Namespace) -> int:
    runtime_dir = _runtime_dir(args.runtime_dir)
    paths = _server_paths(runtime_dir, args.pid_file, args.log_file)

    if args.follow:
        return _follow_file(paths.log_path)

    lines = _tail_lines(paths.log_path, int(args.lines))
    if not lines:
        sys.stdout.write(f"<no logs> ({paths.log_path})\n")
        return 0

    for line in lines:
        sys.stdout.write(line + "\n")
    return 0


def _http_client(base_url: str):
    import httpx

    # keep timeouts short; bootstrap is local-only.
    return httpx.Client(base_url=base_url, timeout=5.0)


def _cmd_bootstrap(args: argparse.Namespace) -> int:
    base_url = args.base_url.rstrip("/")

    # Avoid echoing secrets back to the user; only print the API key as the last line.
    username = args.username
    password = args.password
    project_name = args.project_name

    if not username or not password or not project_name:
        sys.stderr.write("bootstrap requires: --username, --password, --project-name\n")
        return 2

    with _http_client(base_url) as http:
        # 1) user (idempotent-ish; if conflict, continue)
        r = http.post("/v1/users", json={"username": username, "password": password})
        if r.status_code not in (201, 409):
            sys.stderr.write(f"create user failed: HTTP {r.status_code}\n")
            return 1

        # 2) login
        r = http.post("/v1/login", json={"username": username, "password": password})
        if r.status_code != 200:
            sys.stderr.write(f"login failed: HTTP {r.status_code}\n")
            return 1
        access_token = str(r.json().get("access_token") or "").strip()
        if not access_token:
            sys.stderr.write("login failed: missing access_token\n")
            return 1

        # 3) create project
        r = http.post(
            "/v1/projects",
            json={"name": project_name},
            headers={"Authorization": f"Bearer {access_token}"},
        )
        if r.status_code not in (201, 409):
            sys.stderr.write(f"create project failed: HTTP {r.status_code}\n")
            return 1

        # If conflict, we need the project id to mint a key; for now require success path.
        if r.status_code == 409:
            sys.stderr.write(
                "project already exists (409). For now, please mint a key via the UI or add a list-projects endpoint.\n"
            )
            return 1

        project_body = r.json() if hasattr(r, "json") else {}
        project_id = str((project_body or {}).get("project_id") or (project_body or {}).get("id") or "").strip()
        if not project_id:
            sys.stderr.write("create project failed: missing project_id in response\n")
            return 1

        # 4) mint api key
        body: dict[str, Any] = {}
        if args.key_label:
            body["label"] = args.key_label

        r = http.post(
            f"/v1/projects/{project_id}/api-keys",
            json=body,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        if r.status_code != 201:
            sys.stderr.write(f"mint api key failed: HTTP {r.status_code}\n")
            return 1

        api_key = str(r.json().get("api_key") or "").strip()
        if not api_key:
            sys.stderr.write("mint api key failed: missing api_key\n")
            return 1

        # Print only the key as last line (script-friendly, avoids leaking other info).
        if args.print_summary:
            sys.stdout.write("bootstrap ok\n")
            sys.stdout.write(f"base_url: {base_url}\n")
            sys.stdout.write(f"ui: {base_url}/ui\n")
            sys.stdout.write(f"project_id: {project_id}\n")
            sys.stdout.write("api_key:\n")

        sys.stdout.write(f"{api_key}\n")
        return 0


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="dispatch",
        description="Dispatch operator CLI (server lifecycle + bootstrap).",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    server = sub.add_parser("server", help="Start/stop/status/logs for the Dispatch server")
    server_sub = server.add_subparsers(dest="server_cmd", required=True)

    start = server_sub.add_parser("start", help="Start the server")
    start.add_argument("--runtime-dir", default="", help="Runtime dir (defaults to $SCHED_ORCH_RUNTIME_DIR or ./runtime)")
    start.add_argument("--host", default=DEFAULT_HOST)
    start.add_argument("--port", type=int, default=DEFAULT_PORT)
    start.add_argument("--pid-file", default="", help="Override pid file path")
    start.add_argument("--log-file", default="", help="Override log file path")
    start.add_argument("--reload", action="store_true", help="Enable uvicorn --reload")
    start.add_argument(
        "--foreground",
        action="store_true",
        help="Development-only: run in foreground (does not write pid/log files; status/stop cannot manage it)",
    )
    start.set_defaults(func=_cmd_server_start)

    stop = server_sub.add_parser("stop", help="Stop the server")
    stop.add_argument("--runtime-dir", default="")
    stop.add_argument("--pid-file", default="")
    stop.add_argument("--log-file", default="")
    stop.add_argument("--timeout", type=float, default=5.0, help="Seconds to wait before SIGKILL")
    stop.set_defaults(func=_cmd_server_stop)

    status = server_sub.add_parser("status", help="Check whether the server is running")
    status.add_argument("--runtime-dir", default="")
    status.add_argument("--pid-file", default="")
    status.add_argument("--log-file", default="")
    status.set_defaults(func=_cmd_server_status)

    logs = server_sub.add_parser("logs", help="Print server logs")
    logs.add_argument("--runtime-dir", default="")
    logs.add_argument("--pid-file", default="")
    logs.add_argument("--log-file", default="")
    logs.add_argument("--lines", type=int, default=DEFAULT_LOG_LINES)
    logs.add_argument("-f", "--follow", action="store_true")
    logs.set_defaults(func=_cmd_server_logs)

    bootstrap = sub.add_parser("bootstrap", help="Bootstrap: create user, login, create project, mint api key")
    bootstrap.add_argument("--base-url", default=f"http://{DEFAULT_HOST}:{DEFAULT_PORT}")
    bootstrap.add_argument("--username", required=True)
    bootstrap.add_argument("--password", required=True)
    bootstrap.add_argument("--project-name", required=True)
    bootstrap.add_argument("--key-label", default="")
    bootstrap.add_argument("--print-summary", action="store_true", help="Print non-secret summary before printing key")
    bootstrap.set_defaults(func=_cmd_bootstrap)

    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    func = getattr(args, "func", None)
    if not func:
        raise RuntimeError("missing handler")
    return int(func(args))


if __name__ == "__main__":
    raise SystemExit(main())
