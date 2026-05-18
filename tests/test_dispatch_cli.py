import os


def test_dispatch_server_start_background_writes_pid_logs_and_meta(tmp_path, monkeypatch):
    from dispatch import dispatch

    calls = {}

    class DummyProc:
        pid = 4321

    def fake_popen(argv, env=None, stdout=None, stderr=None, start_new_session=None):
        calls["argv"] = list(argv)
        calls["env"] = dict(env or {})
        calls["start_new_session"] = start_new_session
        # Ensure the CLI is passing a file handle for logging.
        assert stdout is not None
        return DummyProc()

    monkeypatch.setattr(dispatch.subprocess, "Popen", fake_popen)

    rc = dispatch.main(
        [
            "server",
            "start",
            "--runtime-dir",
            str(tmp_path),
            "--host",
            "127.0.0.1",
            "--port",
            "9999",
        ]
    )
    assert rc == 0

    pid_path = tmp_path / "operator" / "server" / "dispatch-server.pid"
    log_path = tmp_path / "operator" / "server" / "dispatch-server.log"
    meta_path = tmp_path / "operator" / "server" / "dispatch-server.meta.json"

    assert pid_path.exists()
    assert pid_path.read_text(encoding="utf-8").strip() == "4321"
    assert log_path.exists()
    assert meta_path.exists()

    meta = dispatch._read_server_meta(meta_path)
    assert meta
    assert meta.get("pid") == 4321
    assert meta.get("host") == "127.0.0.1"
    assert meta.get("port") == 9999
    assert meta.get("foreground") is False

    assert calls["start_new_session"] is True
    assert calls["env"]["SCHED_ORCH_RUNTIME_DIR"] == str(tmp_path)
    assert calls["argv"][0:3] == [os.environ.get("PYTHON", None) or dispatch.sys.executable, "-m", "uvicorn"]


def test_dispatch_server_status_stale_pid_file(tmp_path, monkeypatch):
    from dispatch import dispatch

    pid_path = tmp_path / "operator" / "server" / "dispatch-server.pid"
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text("999999\n", encoding="utf-8")

    def fake_kill(pid, sig):
        raise ProcessLookupError()

    monkeypatch.setattr(dispatch.os, "kill", fake_kill)

    rc = dispatch.main(["server", "status", "--runtime-dir", str(tmp_path)])
    assert rc == 1


def test_dispatch_server_status_running_only_when_pid_alive(tmp_path, monkeypatch, capsys):
    from dispatch import dispatch

    pid_path = tmp_path / "operator" / "server" / "dispatch-server.pid"
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text("1234\n", encoding="utf-8")

    def fake_kill(pid, sig):
        return None

    monkeypatch.setattr(dispatch.os, "kill", fake_kill)

    rc = dispatch.main(["server", "status", "--runtime-dir", str(tmp_path)])
    assert rc == 0

    out = capsys.readouterr().out
    assert "dispatch server: running" in out


def test_dispatch_server_stop_removes_pid_file(tmp_path, monkeypatch):
    from dispatch import dispatch

    pid_path = tmp_path / "operator" / "server" / "dispatch-server.pid"
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text("1234\n", encoding="utf-8")

    state = {"running": True}

    def fake_kill(pid, sig):
        if sig == 0:
            if not state["running"]:
                raise ProcessLookupError()
            return None
        # first SIGTERM makes it stop
        state["running"] = False
        return None

    monkeypatch.setattr(dispatch.os, "kill", fake_kill)

    rc = dispatch.main(["server", "stop", "--runtime-dir", str(tmp_path), "--timeout", "0.2"])
    assert rc == 0
    assert not pid_path.exists()


def test_dispatch_server_stop_stale_pid_file_cleans_up(tmp_path, monkeypatch):
    from dispatch import dispatch

    pid_path = tmp_path / "operator" / "server" / "dispatch-server.pid"
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text("999999\n", encoding="utf-8")

    def fake_kill(pid, sig):
        raise ProcessLookupError()

    monkeypatch.setattr(dispatch.os, "kill", fake_kill)

    rc = dispatch.main(["server", "stop", "--runtime-dir", str(tmp_path)])
    assert rc == 0
    assert not pid_path.exists()


def test_dispatch_server_start_foreground_is_dev_only_and_unmanaged(tmp_path, monkeypatch, capsys):
    from dispatch import dispatch

    calls = {}

    class DummyResult:
        returncode = 0

    def fake_run(argv, env=None):
        calls["argv"] = list(argv)
        calls["env"] = dict(env or {})
        return DummyResult()

    monkeypatch.setattr(dispatch.subprocess, "run", fake_run)

    rc = dispatch.main(
        [
            "server",
            "start",
            "--foreground",
            "--runtime-dir",
            str(tmp_path),
            "--host",
            "127.0.0.1",
            "--port",
            "9999",
        ]
    )
    assert rc == 0

    pid_path = tmp_path / "operator" / "server" / "dispatch-server.pid"
    log_path = tmp_path / "operator" / "server" / "dispatch-server.log"
    meta_path = tmp_path / "operator" / "server" / "dispatch-server.meta.json"

    assert not pid_path.exists()
    assert not log_path.exists()
    assert meta_path.exists()

    err = capsys.readouterr().err
    assert "does NOT write pid/log files" in err
    assert "cannot manage it" in err


def test_dispatch_server_status_warns_if_port_serving_but_no_pid(tmp_path, monkeypatch, capsys):
    from dispatch import dispatch

    # No pid file.
    (tmp_path / "operator" / "server").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(dispatch, "_is_dispatch_server_at", lambda base_url: True)

    rc = dispatch.main(["server", "status", "--runtime-dir", str(tmp_path)])
    assert rc == 1
    out = capsys.readouterr().out
    assert "WARNING: dispatch server appears to be running outside this runtime dir" in out


def test_dispatch_server_start_aborts_on_port_collision_runtime_mismatch_no_pid_written(tmp_path, monkeypatch, capsys):
    from dispatch import dispatch

    other_pid = 424242
    requested_runtime = tmp_path / "requested"
    active_runtime = tmp_path / "active"

    # Simulate port already occupied by a Dispatch server owned by other_pid.
    monkeypatch.setattr(dispatch, "_port_listening_pid", lambda host, port: other_pid)
    monkeypatch.setattr(dispatch, "_is_dispatch_server_at", lambda base_url: True)
    monkeypatch.setattr(dispatch, "_runtime_dir_from_pid_environ", lambda pid: active_runtime)

    calls = {"popen": 0}

    def fake_popen(*args, **kwargs):
        calls["popen"] += 1
        raise AssertionError("must not attempt to start when port is already owned by another runtime")

    monkeypatch.setattr(dispatch.subprocess, "Popen", fake_popen)

    rc = dispatch.main(
        [
            "server",
            "start",
            "--runtime-dir",
            str(requested_runtime),
            "--host",
            "127.0.0.1",
            "--port",
            "9999",
        ]
    )
    assert rc == 2

    err = capsys.readouterr().err
    assert "port already in use" in err
    assert str(other_pid) in err
    assert str(active_runtime) in err
    assert str(requested_runtime) in err

    # No pid/log should be written for the requested runtime dir.
    pid_path = requested_runtime / "operator" / "server" / "dispatch-server.pid"
    log_path = requested_runtime / "operator" / "server" / "dispatch-server.log"
    assert not pid_path.exists()
    assert not log_path.exists()
    assert calls["popen"] == 0


def test_dispatch_server_status_reports_wrong_runtime_on_same_port_when_pid_stale(tmp_path, monkeypatch, capsys):
    from dispatch import dispatch

    # Stale pid file under this runtime.
    pid_path = tmp_path / "operator" / "server" / "dispatch-server.pid"
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text("999999\n", encoding="utf-8")

    # pid is stale
    monkeypatch.setattr(dispatch, "_pid_is_running", lambda pid: False)

    other_pid = 12345
    other_runtime = tmp_path / "other"

    monkeypatch.setattr(dispatch, "_port_listening_pid", lambda host, port: other_pid)
    monkeypatch.setattr(dispatch, "_is_dispatch_server_at", lambda base_url: True)
    monkeypatch.setattr(dispatch, "_runtime_dir_from_pid_environ", lambda pid: other_runtime)

    rc = dispatch.main(["server", "status", "--runtime-dir", str(tmp_path)])
    assert rc == 1

    out = capsys.readouterr().out
    assert "wrong runtime on same port" in out
    assert str(other_pid) in out
    assert str(other_runtime) in out
    assert str(tmp_path) in out


def test_dispatch_doctor_reports_mismatch_and_no_secrets(tmp_path, monkeypatch, capsys):
    from dispatch import dispatch

    expected_runtime = tmp_path / "expected"
    expected_runtime.mkdir(parents=True, exist_ok=True)

    active_pid = 2222
    active_runtime = tmp_path / "active"

    monkeypatch.setattr(dispatch, "_port_listening_pid", lambda host, port: active_pid)
    monkeypatch.setattr(dispatch, "_is_dispatch_server_at", lambda base_url: True)
    monkeypatch.setattr(dispatch, "_runtime_dir_from_pid_environ", lambda pid: active_runtime)

    rc = dispatch.main(["doctor", "--runtime-dir", str(expected_runtime), "--base-url", "http://127.0.0.1:9999"])
    assert rc == 1

    out = capsys.readouterr().out
    assert "FAIL" in out
    assert str(active_pid) in out
    assert str(active_runtime) in out
    assert str(expected_runtime) in out
    assert "unauthorized" in out.lower()


def test_dispatch_doctor_reports_pass_when_runtime_matches(tmp_path, monkeypatch, capsys):
    from dispatch import dispatch

    runtime = tmp_path / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)

    active_pid = 3333

    monkeypatch.setattr(dispatch, "_port_listening_pid", lambda host, port: active_pid)
    monkeypatch.setattr(dispatch, "_is_dispatch_server_at", lambda base_url: True)
    monkeypatch.setattr(dispatch, "_runtime_dir_from_pid_environ", lambda pid: runtime)

    rc = dispatch.main(["doctor", "--runtime-dir", str(runtime), "--base-url", "http://127.0.0.1:9999"])
    assert rc == 0

    out = capsys.readouterr().out
    assert "PASS" in out
    assert str(active_pid) in out


def test_dispatch_doctor_warns_when_slurm_backend_has_no_gpu_gres(tmp_path, monkeypatch, capsys):
    from dispatch import dispatch

    runtime = tmp_path / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)

    active_pid = 4444

    monkeypatch.setattr(dispatch, "_port_listening_pid", lambda host, port: active_pid)
    monkeypatch.setattr(dispatch, "_is_dispatch_server_at", lambda base_url: True)
    monkeypatch.setattr(dispatch, "_runtime_dir_from_pid_environ", lambda pid: runtime)
    monkeypatch.setattr(dispatch, "_execution_backend_from_pid_environ", lambda pid: "slurm")
    monkeypatch.setattr(dispatch, "_slurm_gpu_gres_available", lambda: False)

    rc = dispatch.main(["doctor", "--runtime-dir", str(runtime), "--base-url", "http://127.0.0.1:9999"])
    assert rc == 0

    out = capsys.readouterr().out
    assert "PASS" in out
    assert "active_execution_backend: slurm" in out
    assert "WARN: slurm backend is active but Slurm does not appear to advertise GPU GRES" in out


def test_dispatch_doctor_auth_hints_prints_header_guidance(capsys):
    from dispatch import dispatch

    rc = dispatch.main(["doctor", "--auth-hints"])
    assert rc == 0

    out = capsys.readouterr().out
    assert "Use X-API-Key for /v1/jobs endpoints" in out
    assert "Use Authorization Bearer access_token for /v1/projects, /v1/login" in out


def test_dispatch_server_shutdown_reports_summary_and_stops_server(monkeypatch, tmp_path, capsys):
    from dispatch import dispatch

    runtime = tmp_path / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)

    class R:
        drained = True
        cancel_attempted = 2
        canceled = 2
        remaining = 0
        timed_out = False

    monkeypatch.setattr("dispatch.graceful_shutdown.graceful_shutdown", lambda *a, **k: R())

    stopped: list[bool] = []

    def fake_stop(args):
        stopped.append(True)
        return 0

    monkeypatch.setattr(dispatch, "_cmd_server_stop", fake_stop)

    rc = dispatch.main([
        "server",
        "shutdown",
        "--runtime-dir",
        str(runtime),
        "--graceful-timeout",
        "60",
    ])
    assert rc == 0

    out = capsys.readouterr().out
    assert "dispatch server shutdown" in out
    assert "cancel_attempted: 2" in out
    assert "remaining_non_terminal: 0" in out
    assert stopped == [True]


def test_dispatch_server_shutdown_returns_nonzero_on_timeout(monkeypatch, tmp_path):
    from dispatch import dispatch

    runtime = tmp_path / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)

    class R:
        drained = True
        cancel_attempted = 0
        canceled = 0
        remaining = 1
        timed_out = True

    monkeypatch.setattr("dispatch.graceful_shutdown.graceful_shutdown", lambda *a, **k: R())
    monkeypatch.setattr(dispatch, "_cmd_server_stop", lambda args: 0)

    rc = dispatch.main(["server", "shutdown", "--runtime-dir", str(runtime)])
    assert rc == 1
