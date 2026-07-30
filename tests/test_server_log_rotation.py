"""server.log rolls at startup — and only at startup.

Full LLM request logging writes the entire prompt on every Claude call, so the
file grows in steps. The rotation has to happen BEFORE stdout/stderr are dup2'd
onto it: the descriptors follow the inode through a rename, so rolling a file
that already has fds bound sends every later traceback to the renamed copy, and
eventually to an unlinked inode nothing can read.
"""

from __future__ import annotations

import os
from pathlib import Path

from yt_scheduler.main import SERVER_LOG_KEEP, SERVER_LOG_MAX_BYTES, _rotate_server_log


def test_small_log_is_left_alone(tmp_path: Path):
    log = tmp_path / "server.log"
    log.write_text("still small")
    _rotate_server_log(log)
    assert log.read_text() == "still small"
    assert not (tmp_path / "server.log.1").exists()


def test_oversized_log_rolls_aside(tmp_path: Path):
    log = tmp_path / "server.log"
    log.write_bytes(b"x" * (SERVER_LOG_MAX_BYTES + 1))
    _rotate_server_log(log)
    assert not log.exists(), "the live path is freed for a fresh file"
    assert (tmp_path / "server.log.1").stat().st_size == SERVER_LOG_MAX_BYTES + 1


def test_oldest_generation_is_dropped(tmp_path: Path):
    log = tmp_path / "server.log"
    log.write_bytes(b"x" * (SERVER_LOG_MAX_BYTES + 1))
    for n in range(1, SERVER_LOG_KEEP + 1):
        (tmp_path / f"server.log.{n}").write_text(f"gen{n}")
    _rotate_server_log(log)
    # gen1 shifted to .2, gen2 to .3, and the former .3 is gone.
    assert (tmp_path / "server.log.2").read_text() == "gen1"
    assert (tmp_path / f"server.log.{SERVER_LOG_KEEP}").read_text() == "gen2"


def test_rotation_failure_does_not_stop_startup(tmp_path: Path, monkeypatch):
    """Best-effort: the server must boot even if the log can't be rolled."""
    log = tmp_path / "server.log"
    log.write_bytes(b"x" * (SERVER_LOG_MAX_BYTES + 1))

    def boom(*args, **kwargs):
        raise OSError("read-only filesystem")

    monkeypatch.setattr(Path, "rename", boom)
    _rotate_server_log(log)  # must not raise
    assert log.exists()


def test_descriptors_bound_after_rotation_still_see_the_live_file(tmp_path: Path):
    """The ordering guarantee, exercised the way main.py uses it.

    Rotate first, THEN open and dup2. Bytes written through the descriptor
    must land in the file a reader of server.log actually sees.
    """
    log = tmp_path / "server.log"
    log.write_bytes(b"x" * (SERVER_LOG_MAX_BYTES + 1))
    _rotate_server_log(log)

    fd = os.open(str(log), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, b"after-rotation traceback\n")
    finally:
        os.close(fd)

    assert "after-rotation traceback" in log.read_text()
    assert os.stat(str(log)).st_ino != os.stat(str(tmp_path / "server.log.1")).st_ino


def test_rotation_runs_before_the_descriptors_are_bound(tmp_path, monkeypatch):
    """The ordering IS the design.

    fds 1/2 follow the inode through a rename, so rotating after the dup2
    would send every later traceback to the renamed file and eventually to an
    unlinked one. This asserts the order inside the function under test — the
    other test only demonstrates the property in its own body, which would
    stay green if main.py reordered.
    """
    import io

    from yt_scheduler import main

    order: list[str] = []
    monkeypatch.setattr(main, "LOG_DIR", tmp_path)
    monkeypatch.setattr(main, "ensure_dirs", lambda: None)
    monkeypatch.setattr(main, "_rotate_server_log", lambda p: order.append("rotate"))
    monkeypatch.setattr(main.os, "open", lambda *a, **k: (order.append("open"), 99)[1])
    monkeypatch.setattr(main.os, "dup2", lambda *a: order.append("dup2"))
    monkeypatch.setattr(main.os, "close", lambda fd: None)
    monkeypatch.setattr(main.os, "fdopen", lambda *a, **k: io.StringIO())

    main._redirect_stdio_to_log()

    assert order[0] == "rotate"
    assert order.index("rotate") < order.index("open") < order.index("dup2")


def test_rotation_failure_is_reported_into_the_log_it_rolled(tmp_path, monkeypatch, capsys):
    """The complaint about server.log has to land IN server.log.

    _rotate_server_log runs before the redirect, when stderr is still
    launchd's — so it returns the message and the caller emits it afterwards.
    """
    log = tmp_path / "server.log"
    log.write_bytes(b"x" * (SERVER_LOG_MAX_BYTES + 1))

    def boom(*args, **kwargs):
        raise OSError("read-only filesystem")

    monkeypatch.setattr(Path, "rename", boom)
    message = _rotate_server_log(log)
    assert message is not None and "rotation skipped" in message
    # Nothing was printed from inside the function — it is the caller's job,
    # after stderr points at the file.
    assert capsys.readouterr().err == ""
