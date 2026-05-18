from __future__ import annotations

import argparse
import json
import os
import signal
import socket
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


def _port_listening_pid(host: str, port: int) -> int | None:
    """Best-effort: return the PID listening on host:port, if we can determine it.

    Used for:
    - server-start preflight (avoid runtime-dir mismatches on the same port)
    - status/doctor diagnostics

    If we cannot determine the PID (permissions, missing tools, etc.), return None.
    """

    host = str(host)
    port = int(port)

    # Fast occupancy test: if we can bind, the port isn't in use.
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((host, port))
            return None
        finally:
            try:
                s.close()
            except Exception:
                pass
    except OSError:
        # likely in use
        pass

    # Best-effort PID discovery via `ss -ltnp`.
    try:
        proc = subprocess.run(
            ["ss", "-ltnp"],
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except Exception:
        return None

    import re

    target = f"{host}:{port}"
    for line in str(proc.stdout or "").splitlines():
        if target not in line:
            continue
        m = re.search(r"pid=(\d+)", line)
        if not m:
            continue
        try:
            return int(m.group(1))
        except ValueError:
            continue

    return None


def _runtime_dir_from_pid_environ(pid: int) -> Path | None:
    """Best-effort read of SCHED_ORCH_RUNTIME_DIR from /proc/<pid>/environ."""

    try:
        data = Path(f"/proc/{int(pid)}/environ").read_bytes()
    except (FileNotFoundError, PermissionError, OSError):
        return None

    for raw in data.split(b"\x00"):
        if not raw:
            continue
        if raw.startswith(b"SCHED_ORCH_RUNTIME_DIR="):
            try:
                value = raw.split(b"=", 1)[1].decode("utf-8", errors="replace").strip()
            except Exception:
                return None
            if not value:
                return None
            return Path(value)

    return None


def _execution_backend_from_pid_environ(pid: int) -> str | None:
    """Best-effort read of SCHED_ORCH_EXECUTION_BACKEND from /proc/<pid>/environ."""

    try:
        data = Path(f"/proc/{int(pid)}/environ").read_bytes()
    except (FileNotFoundError, PermissionError, OSError):
        return None

    for raw in data.split(b"\x00"):
        if not raw:
            continue
        if raw.startswith(b"SCHED_ORCH_EXECUTION_BACKEND="):
            try:
                value = raw.split(b"=", 1)[1].decode("utf-8", errors="replace").strip()
            except Exception:
                return None
            if not value:
                return None
            return value

    return None


def _slurm_gpu_gres_available() -> bool | None:
    """Return True/False if Slurm appears to advertise GPU GRES; None if unknown.

    This is a best-effort operator hint only (doctor output); it should not fail the server.
    """

    import shutil

    if shutil.which("sinfo") is None:
        return None

    try:
        proc = subprocess.run(
            ["sinfo", "-h", "-o", "%G"],
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except Exception:
        return None

    text = str(proc.stdout or "").strip()
    if not text:
        return None

    # Common outputs:
    # - "(null)"
    # - "gpu:1"
    # - "gpu:gb10:1" (if Type is used)
    if "(null)" in text:
        return False
    if "gpu" in text.lower():
        return True
    return False


def _cmd_server_start(args: argparse.Namespace) -> int:
    runtime_dir = _runtime_dir(args.runtime_dir)
    paths = _server_paths(runtime_dir, args.pid_file, args.log_file)

    # Preflight: refuse to start when a Dispatch server on this port already
    # belongs to a different runtime dir. This prevents confusing auth failures
    # caused by minting keys under runtime Y while requests hit runtime X.
    listening_pid = _port_listening_pid(args.host, args.port)
    if listening_pid is not None:
        base_url = f"http://{args.host}:{int(args.port)}"
        if _is_dispatch_server_at(base_url):
            active_runtime = _runtime_dir_from_pid_environ(listening_pid)
            if active_runtime is not None and active_runtime.resolve() != runtime_dir.resolve():
                sys.stderr.write("ERROR: dispatch server port already in use by another runtime.\n")
                sys.stderr.write(f"listen:            {base_url}\n")
                sys.stderr.write(f"existing_pid:      {listening_pid}\n")
                sys.stderr.write(f"existing_runtime:  {active_runtime}\n")
                sys.stderr.write(f"requested_runtime: {runtime_dir}\n")
                sys.stderr.write("\n")
                sys.stderr.write("This commonly appears as API key auth failures (unauthorized) when keys were minted under a different runtime dir.\n")
                sys.stderr.write("\n")
                sys.stderr.write("Next steps (pick one):\n")
                sys.stderr.write(f"- stop old server: dispatch server stop --runtime-dir {active_runtime}\n")
                sys.stderr.write(f"- or use existing runtime: dispatch server start --runtime-dir {active_runtime}\n")
                sys.stderr.write(f"- or choose another port: dispatch server start --runtime-dir {runtime_dir} --port <PORT>\n")
                return 2

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

    show_non_terminal = bool(getattr(args, "show_non_terminal", False))
    non_terminal_max = int(getattr(args, "non_terminal_max", 50) or 50)

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

        # If another Dispatch is running on the default port but under a different
        # runtime dir, it's very likely the root cause of confusing "unauthorized"
        # failures (keys minted under one runtime, requests going to another).
        base_url = f"http://{DEFAULT_HOST}:{DEFAULT_PORT}"
        other_pid = _port_listening_pid(DEFAULT_HOST, DEFAULT_PORT)
        if other_pid is not None and _is_dispatch_server_at(base_url):
            other_runtime = _runtime_dir_from_pid_environ(other_pid)
            if other_runtime is not None and other_runtime.resolve() != runtime_dir.resolve():
                sys.stdout.write("\n")
                sys.stdout.write("WARNING: wrong runtime on same port (likely causes unauthorized API key errors).\n")
                sys.stdout.write(f"active_pid: {other_pid}\n")
                sys.stdout.write(f"active_runtime_dir: {other_runtime}\n")
                sys.stdout.write(f"status_runtime_dir: {runtime_dir}\n")
                sys.stdout.write(f"detected_url: {base_url}/ui\n")
                sys.stdout.write("\n")
                sys.stdout.write("Remediation:\n")
                sys.stdout.write(f"- stop the active server: dispatch server stop --runtime-dir {other_runtime}\n")
                sys.stdout.write(f"- or run status against that runtime: dispatch server status --runtime-dir {other_runtime}\n")

        return 1

    sys.stdout.write("dispatch server: running\n")
    sys.stdout.write(f"pid: {pid}\n")
    sys.stdout.write(f"pid_file: {paths.pid_path}\n")
    sys.stdout.write(f"log: {paths.log_path}\n")

    if show_non_terminal:
        try:
            from dispatch.graceful_shutdown import get_non_terminal_records

            records = get_non_terminal_records(runtime_dir)
            sys.stdout.write(f"non_terminal_count: {len(records)}\n")
            for record in records[: max(0, non_terminal_max)]:
                server_job_id = str(record.get("server_job_id") or "").strip()
                state = str(record.get("state") or "").strip().lower() or "unknown"
                backend = str(record.get("execution_backend") or "").strip().lower() or "unknown"
                sys.stdout.write(f"- {server_job_id} state={state} backend={backend}\n")
        except Exception:
            sys.stdout.write("non_terminal_count: <unavailable>\n")

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


def _cmd_server_shutdown(args: argparse.Namespace) -> int:
    """Graceful shutdown: drain + cancel in-flight jobs, wait (bounded), then stop server."""

    runtime_dir = _runtime_dir(args.runtime_dir)

    from dispatch.graceful_shutdown import graceful_shutdown

    result = graceful_shutdown(
        runtime_dir,
        cancel_inflight=not bool(args.no_cancel),
        drain=not bool(args.no_drain),
        graceful_timeout_s=float(args.graceful_timeout),
        poll_interval_s=float(args.poll_interval),
        allow_slurm_cancel=not bool(args.no_slurm_cancel),
        allow_direct_cancel=not bool(args.no_direct_cancel),
        ignore_older_than_hours=(float(args.ignore_older_than_hours) if args.ignore_older_than_hours is not None else None),
    )

    sys.stdout.write("dispatch server shutdown\n")
    sys.stdout.write(f"runtime_dir: {runtime_dir}\n")
    sys.stdout.write(f"drained: {result.drained}\n")
    sys.stdout.write(f"cancel_attempted: {result.cancel_attempted}\n")
    sys.stdout.write(f"cancel_requested: {result.canceled}\n")
    sys.stdout.write(f"remaining_non_terminal: {result.remaining}\n")
    sys.stdout.write(f"timed_out: {result.timed_out}\n")

    stop_args = argparse.Namespace(
        runtime_dir=args.runtime_dir,
        pid_file=getattr(args, "pid_file", ""),
        log_file=getattr(args, "log_file", ""),
        timeout=float(args.stop_timeout),
    )
    _cmd_server_stop(stop_args)

    if result.timed_out or result.remaining:
        return 1
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


def _cmd_doctor(args: argparse.Namespace) -> int:
    if bool(getattr(args, "auth_hints", False)):
        sys.stdout.write("dispatch doctor auth-hints\n")
        sys.stdout.write("Use X-API-Key for /v1/jobs endpoints\n")
        sys.stdout.write("Use Authorization Bearer access_token for /v1/projects, /v1/login, /v1/users, /v1/api-keys endpoints\n")
        return 0

    base_url = str(args.base_url or "").strip().rstrip("/")
    if not base_url:
        sys.stderr.write("doctor requires --base-url\n")
        return 2

    expected_runtime_dir = _runtime_dir(getattr(args, "runtime_dir", "") or None)

    # Parse host/port from the base URL.
    try:
        from urllib.parse import urlparse

        u = urlparse(base_url)
        host = u.hostname or DEFAULT_HOST
        port = int(u.port or DEFAULT_PORT)
    except Exception:
        host = DEFAULT_HOST
        port = DEFAULT_PORT

    active_pid = _port_listening_pid(host, port)
    dispatch_detected = _is_dispatch_server_at(base_url)

    active_runtime_dir: Path | None = None
    if active_pid is not None:
        active_runtime_dir = _runtime_dir_from_pid_environ(active_pid)

    active_execution_backend: str | None = None
    if active_pid is not None:
        active_execution_backend = _execution_backend_from_pid_environ(active_pid)

    sys.stdout.write("dispatch doctor\n")
    sys.stdout.write(f"base_url: {base_url}\n")
    sys.stdout.write(f"expected_runtime_dir: {expected_runtime_dir}\n")
    sys.stdout.write(f"expected_auth_store_path: {expected_runtime_dir / 'auth' / 'api_keys.json'}\n")
    sys.stdout.write("\n")

    if active_pid is None:
        sys.stdout.write("active_pid: <unknown>\n")
    else:
        sys.stdout.write(f"active_pid: {active_pid}\n")

    if active_runtime_dir is None:
        sys.stdout.write("active_runtime_dir: <unknown>\n")
    else:
        sys.stdout.write(f"active_runtime_dir: {active_runtime_dir}\n")
        sys.stdout.write(f"active_auth_store_path: {active_runtime_dir / 'auth' / 'api_keys.json'}\n")

    if active_execution_backend:
        sys.stdout.write(f"active_execution_backend: {active_execution_backend}\n")

    sys.stdout.write("\n")

    if not dispatch_detected:
        sys.stdout.write("FAIL: no Dispatch server detected at base_url.\n")
        sys.stdout.write("Remediation:\n")
        sys.stdout.write(f"- start server: dispatch server start --runtime-dir {expected_runtime_dir} --host {host} --port {port}\n")
        return 1

    if active_runtime_dir is None:
        sys.stdout.write("FAIL: Dispatch detected but runtime dir could not be determined from the owning process.\n")
        sys.stdout.write("Remediation:\n")
        sys.stdout.write(f"- run status: dispatch server status --runtime-dir {expected_runtime_dir}\n")
        return 1

    if active_runtime_dir.resolve() != expected_runtime_dir.resolve():
        sys.stdout.write("FAIL: runtime-dir mismatch detected (likely why auth returns unauthorized).\n")
        sys.stdout.write("Remediation (pick one):\n")
        sys.stdout.write(f"- stop active server: dispatch server stop --runtime-dir {active_runtime_dir}\n")
        sys.stdout.write(f"- use active runtime: dispatch server status --runtime-dir {active_runtime_dir}\n")
        sys.stdout.write(f"- or start on another port: dispatch server start --runtime-dir {expected_runtime_dir} --port <PORT>\n")
        return 1

    sys.stdout.write("PASS: active server runtime dir matches expected runtime dir.\n")

    # Operator hint: if using slurm backend, warn if Slurm isn't advertising GPU GRES.
    if (active_execution_backend or "").strip().lower() == "slurm":
        avail = _slurm_gpu_gres_available()
        if avail is False:
            sys.stdout.write(
                "WARN: slurm backend is active but Slurm does not appear to advertise GPU GRES (sinfo %G shows (null)).\n"
            )
            sys.stdout.write(
                "      Jobs with resources.graphics_processing_units>0 may remain pending or fail to schedule until GRES is configured.\n"
            )

    return 0


def _cmd_bootstrap(args: argparse.Namespace) -> int:
    base_url = args.base_url.rstrip("/")

    runtime_dir_hint = _runtime_dir(getattr(args, "runtime_dir", "") or None)

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
        body: dict[str, Any] = {"label": args.key_label, "ttl_seconds": 30 * 24 * 3600}

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
            sys.stdout.write("\n")
            sys.stdout.write("Env hint (non-secret; keep runtime dir consistent between server and keyring):\n")
            sys.stdout.write(f"DISPATCH_BASE_URL={base_url}\n")
            sys.stdout.write(f"DISPATCH_RUNTIME_DIR={runtime_dir_hint}\n")
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

    shutdown = server_sub.add_parser(
        "shutdown",
        help="Graceful shutdown: drain + cancel in-flight jobs, wait (bounded), then stop server",
    )
    shutdown.add_argument("--runtime-dir", default="")
    shutdown.add_argument("--pid-file", default="")
    shutdown.add_argument("--log-file", default="")
    shutdown.add_argument("--graceful-timeout", type=float, default=60.0, help="Seconds to wait for jobs to reach terminal state")
    shutdown.add_argument("--poll-interval", type=float, default=1.0, help="Seconds between refresh attempts")
    shutdown.add_argument("--stop-timeout", type=float, default=5.0, help="Seconds to wait for server process before SIGKILL")
    shutdown.add_argument("--no-drain", action="store_true", help="Do not enable drain mode before waiting/canceling")
    shutdown.add_argument("--no-cancel", action="store_true", help="Do not attempt cancel; only wait for jobs to become terminal")
    shutdown.add_argument("--no-slurm-cancel", action="store_true", help="Do not attempt to cancel slurm backend jobs during shutdown")
    shutdown.add_argument("--no-direct-cancel", action="store_true", help="Do not attempt to cancel direct backend jobs during shutdown")
    shutdown.add_argument(
        "--ignore-older-than-hours",
        type=float,
        default=None,
        help="Ignore non-terminal ledger entries older than this many hours when deciding whether shutdown completed",
    )
    shutdown.set_defaults(func=_cmd_server_shutdown)

    status = server_sub.add_parser("status", help="Check whether the server is running")
    status.add_argument("--runtime-dir", default="")
    status.add_argument("--pid-file", default="")
    status.add_argument("--log-file", default="")
    status.add_argument("--show-non-terminal", action="store_true", help="Print non-terminal ledger entries (best-effort)")
    status.add_argument("--non-terminal-max", type=int, default=50, help="Max non-terminal records to print")
    status.set_defaults(func=_cmd_server_status)

    doctor_server = server_sub.add_parser("doctor", help="Diagnose server/runtime-dir/keyring mismatches")
    doctor_server.add_argument("--runtime-dir", default="")
    doctor_server.add_argument("--base-url", default=f"http://{DEFAULT_HOST}:{DEFAULT_PORT}")
    doctor_server.add_argument("--auth-hints", action="store_true", help="Print auth header guidance for jobs vs session endpoints")
    doctor_server.set_defaults(func=_cmd_doctor)

    logs = server_sub.add_parser("logs", help="Print server logs")
    logs.add_argument("--runtime-dir", default="")
    logs.add_argument("--pid-file", default="")
    logs.add_argument("--log-file", default="")
    logs.add_argument("--lines", type=int, default=DEFAULT_LOG_LINES)
    logs.add_argument("-f", "--follow", action="store_true")
    logs.set_defaults(func=_cmd_server_logs)

    doctor = sub.add_parser("doctor", help="Diagnose server/runtime-dir/keyring mismatches")
    doctor.add_argument("--runtime-dir", default="")
    doctor.add_argument("--base-url", default=f"http://{DEFAULT_HOST}:{DEFAULT_PORT}")
    doctor.add_argument("--auth-hints", action="store_true", help="Print auth header guidance for jobs vs session endpoints")
    doctor.set_defaults(func=_cmd_doctor)

    bootstrap = sub.add_parser("bootstrap", help="Bootstrap: create user, login, create project, mint api key")
    bootstrap.add_argument("--base-url", default=f"http://{DEFAULT_HOST}:{DEFAULT_PORT}")
    bootstrap.add_argument("--runtime-dir", default="", help="Runtime dir hint to print with env guidance")
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
