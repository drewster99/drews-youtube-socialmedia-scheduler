"""Shared pytest fixtures, plus a hard guard against touching the real database."""

from __future__ import annotations

import asyncio
import importlib
import os
import sqlite3
import sys
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import aiosqlite
import pytest

# Make `src/` importable so tests can `from yt_scheduler import ...`
SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


# --- production-data guard ------------------------------------------------
#
# config.DATA_DIR / DB_PATH freeze at import time. A test that reaches
# yt_scheduler.database without first pointing DYS_DATA_DIR at a tmp dir opens
# the user's REAL publisher.db read/write — and with the menubar app running it
# also blocks on the SQLite write lock. Neither is acceptable, and neither is
# visible from the test's source, so enforce it at the only three places a
# database is ever opened rather than trusting each test to isolate itself.

_BUNDLE_ID = "com.nuclearcyborg.drews-socialmedia-scheduler"

#: Every location the app would put real data on any platform, resolved without
#: consulting DYS_DATA_DIR — a test that forgot to set it lands here.
_PRODUCTION_DIRS: tuple[Path, ...] = (
    Path.home() / "Library" / "Application Support" / _BUNDLE_ID,
    Path(os.getenv("XDG_DATA_HOME") or (Path.home() / ".local" / "share")) / _BUNDLE_ID,
)


def _is_production_path(database: object) -> bool:
    """True when ``database`` resolves inside a real data dir."""
    if not isinstance(database, (str, os.PathLike)):
        return False
    target = str(database)
    if target in ("", ":memory:") or target.startswith("file::memory:"):
        return False
    try:
        resolved = Path(target).expanduser().resolve()
    except OSError:
        return False
    return any(
        resolved == real or real in resolved.parents for real in _PRODUCTION_DIRS
    )


class ProductionDatabaseAccess(RuntimeError):
    """A test tried to open the user's real database."""


def _refuse(database: object) -> None:
    raise ProductionDatabaseAccess(
        f"Test tried to open the production database at {database!r}.\n"
        "Set DYS_DATA_DIR to a tmp dir BEFORE importing yt_scheduler.config "
        "(purging yt_scheduler.* from sys.modules first), or use the "
        "`isolated_db` / `isolated_data_dir` fixture."
    )


#: Every aiosqlite connection opened during a test, so teardown can close the
#: ones a test leaked. Tracking here rather than via ``database._db`` is
#: essential: tests routinely purge ``yt_scheduler.*`` from sys.modules, which
#: orphans the old module object and its still-running connection.
_OPENED_CONNECTIONS: list[aiosqlite.Connection] = []


@pytest.fixture(autouse=True)
def _close_leaked_db_connections() -> Iterator[None]:
    """Close any aiosqlite connection a test left open.

    ``aiosqlite.Connection`` is a non-daemon thread, so a test that calls
    ``get_db()`` without a matching ``close_db()`` passes and then wedges the
    whole pytest process: the interpreter parks in ``threading._shutdown()``
    waiting to join a worker thread that never exits. A connection's futures
    bind to whatever loop is running when ``close()`` is called, so a fresh
    ``asyncio.run`` here is enough to drain and stop the worker.
    """
    _OPENED_CONNECTIONS.clear()
    yield

    leaked = [c for c in _OPENED_CONNECTIONS if getattr(c, "_connection", None) is not None]
    _OPENED_CONNECTIONS.clear()

    for connection in leaked:
        try:
            asyncio.run(connection.close())
        except Exception:  # pragma: no cover - teardown must never mask a failure
            pass

    module = sys.modules.get("yt_scheduler.database")
    if module is not None:
        module._db = None
        reset = getattr(module, "reset_write_txn_flag", None)
        if reset is not None:
            reset()


@pytest.fixture(scope="session", autouse=True)
def _block_production_database() -> Iterator[None]:
    real_sqlite_connect = sqlite3.connect
    real_aiosqlite_connect = aiosqlite.connect

    def guarded_sqlite_connect(database, *args, **kwargs):
        if _is_production_path(database):
            _refuse(database)
        return real_sqlite_connect(database, *args, **kwargs)

    def guarded_aiosqlite_connect(database, *args, **kwargs):
        if _is_production_path(database):
            _refuse(database)
        connection = real_aiosqlite_connect(database, *args, **kwargs)
        _OPENED_CONNECTIONS.append(connection)
        return connection

    sqlite3.connect = guarded_sqlite_connect
    aiosqlite.connect = guarded_aiosqlite_connect
    try:
        yield
    finally:
        sqlite3.connect = real_sqlite_connect
        aiosqlite.connect = real_aiosqlite_connect


def _purge_yt_scheduler_modules() -> None:
    """Drop every cached ``yt_scheduler`` module.

    ``config.DATA_DIR``/``DB_PATH`` are computed at import time, so a module left
    over from an earlier test still points at whatever ``DYS_DATA_DIR`` was set
    then — in the worst case the user's real ``publisher.db``.
    """
    for name in list(sys.modules):
        if name.startswith("yt_scheduler"):
            sys.modules.pop(name, None)


@pytest.fixture
def isolated_data_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Path]:
    """Point ``yt_scheduler`` at a throwaway data dir and force the file keychain.

    Import-time frozen config is why both halves are needed: the env var must be
    set *before* ``yt_scheduler.config`` is (re)imported, and the module cache
    must be purged so that re-import actually happens. Forcing ``_is_macos``
    false keeps every test off the real login Keychain.
    """
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    _purge_yt_scheduler_modules()

    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)

    yield tmp_path

    _purge_yt_scheduler_modules()


@pytest.fixture
async def isolated_db(isolated_data_dir: Path) -> AsyncIterator:
    """An open aiosqlite connection to a fresh database, with a default project.

    ``get_db()`` creates the schema and applies migrations on first connect, so
    no separate migration step is needed. A ``projects`` row with id 1 is seeded
    because most tables carry a ``project_id`` foreign key.
    """
    db_module = importlib.import_module("yt_scheduler.database")
    conn = await db_module.get_db()
    await conn.execute(
        "INSERT INTO projects (id, name, slug) VALUES (1, 'default', 'default')"
        " ON CONFLICT DO NOTHING"
    )
    await conn.commit()

    yield conn

    # A test that raised mid-transaction can leave _in_write_txn set, which would
    # make the next write_transaction in this worker silently join a transaction
    # that no longer exists.
    db_module.reset_write_txn_flag()
    await db_module.close_db()
