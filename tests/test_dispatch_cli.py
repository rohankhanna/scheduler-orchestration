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
