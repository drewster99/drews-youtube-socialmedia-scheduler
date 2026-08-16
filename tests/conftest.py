"""Shared pytest fixtures, plus a hard guard against touching the real database."""

from __future__ import annotations

import asyncio
import ctypes
import ctypes.util
import importlib
import os
import sqlite3
import subprocess
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


# --- real-Keychain guard --------------------------------------------------
#
# The database guard above has no counterpart for secrets, and the gap was not
# theoretical: `test_project_url_invariants` starts the app through TestClient's
# lifespan, which calls repair_keychain_acls() and then the channel backfills.
# With no in-memory Keychain installed those ran against the user's real login
# Keychain, fetched the live YouTube channel with real OAuth tokens, and wrote
# that channel's id and thumbnail into the test's tmp database — where an
# assertion that a fresh install has no channel binding then failed, describing
# the developer's account rather than the code.
#
# Two costs, neither visible from the test's source: real API quota spent by a
# test run, and a modal Keychain password prompt that can wedge the suite.
# There are TWO ways down to the real Keychain, and guarding only the obvious
# one proves nothing: `_keychain_get` reads through the Security framework via
# ctypes and never spawns a process, so blocking the `security` CLI alone left
# the test binding the live channel exactly as before. Both are blocked here:
#
#   1. ctypes  — `find_library("Security")` answers None and a direct CDLL load
#      raises OSError, which is precisely what `_get_sec_lib` already treats as
#      "framework unavailable" (it catches OSError and returns None).
#   2. subprocess — `_keychain_get` then falls through to `_keychain_get_cli`,
#      which is where the refusal below fires. `_keychain_get_cli` catches only
#      FileNotFoundError and TimeoutExpired, so it propagates.
#
# So the framework path degrades quietly to the CLI path, and the CLI path is
# loud. There is no route left that silently returns a real secret.
#
# install_in_memory_keychain() patches well above both levels, so every properly
# isolated test never reaches either.


#: The one documented way to turn the guard off. Named here rather than in the
#: test module that wants it so the variable has a single spelling.
LIVE_API_TESTS_ENV = "DYS_RUN_LIVE_API_TESTS"


def live_api_tests_opted_in() -> bool:
    """True when the user has explicitly asked for real, billed API calls.

    ``test_template_render_live`` says it is "segregated from the default suite",
    but it gated on whether an Anthropic key happened to be in the Keychain —
    which on the developer's own machine is always true. So four tests that each
    cost real tokens ran on every single ``pytest`` invocation, and were roughly
    half the suite's wall-clock. An opt-in has to be an opt-in.
    """
    return os.getenv(LIVE_API_TESTS_ENV) == "1"


class ProductionKeychainAccess(RuntimeError):
    """A test tried to read or write the user's real login Keychain."""


class _SecurityFrameworkBlocked(OSError):
    """Refusal shaped as the failure ``_get_sec_lib`` already handles.

    OSError is what a failed ``dlopen`` raises, so the module takes its own
    documented "framework can't be loaded" branch rather than meeting an
    exception type it has no answer for.
    """


def _is_security_cli(args: object) -> bool:
    """True when ``args`` invokes the macOS ``security`` binary."""
    if isinstance(args, (str, os.PathLike)):
        argv0 = str(args)
    elif isinstance(args, (list, tuple)) and args:
        first = args[0]
        if not isinstance(first, (str, os.PathLike)):
            return False
        argv0 = str(first)
    else:
        return False
    return argv0 == "security" or argv0.endswith("/security")


def _is_security_framework(path: object) -> bool:
    """True when ``path`` names the macOS Security framework binary."""
    if not isinstance(path, (str, os.PathLike)):
        return False
    return "Security.framework" in str(path)


@pytest.fixture(scope="session", autouse=True)
def _block_real_keychain() -> Iterator[None]:
    # The live-API tests need the real Anthropic key, and they are the only
    # thing in this suite that legitimately does. Asking for them is asking for
    # real credentials, real money, and possibly a Keychain password prompt —
    # which is exactly why it has to be said out loud rather than inferred from
    # a key being present.
    if live_api_tests_opted_in():
        yield
        return

    real_run = subprocess.run
    real_popen = subprocess.Popen
    real_find_library = ctypes.util.find_library
    real_cdll = ctypes.CDLL

    def guarded_find_library(name):
        if name == "Security":
            return None
        return real_find_library(name)

    def guarded_cdll(name=None, *rest, **kwargs):
        if _is_security_framework(name):
            raise _SecurityFrameworkBlocked(
                f"Test tried to load the macOS Security framework ({name!r}) to "
                "reach the real login Keychain."
            )
        return real_cdll(name, *rest, **kwargs)

    def _refuse_keychain(args: object) -> None:
        raise ProductionKeychainAccess(
            f"Test tried to run the macOS `security` CLI: {args!r}.\n"
            "That reads or writes the user's real login Keychain and can raise "
            "a modal password prompt mid-run.\n"
            "Use tests.conftest.install_in_memory_keychain(monkeypatch, "
            "keychain_module), or the `isolated_data_dir` / `isolated_db` "
            "fixture, which installs it for you."
        )

    def guarded_run(args, *rest, **kwargs):
        if _is_security_cli(args):
            _refuse_keychain(args)
        return real_run(args, *rest, **kwargs)

    def guarded_popen(args, *rest, **kwargs):
        if _is_security_cli(args):
            _refuse_keychain(args)
        return real_popen(args, *rest, **kwargs)

    subprocess.run = guarded_run
    subprocess.Popen = guarded_popen
    ctypes.util.find_library = guarded_find_library
    ctypes.CDLL = guarded_cdll
    try:
        yield
    finally:
        subprocess.run = real_run
        subprocess.Popen = real_popen
        ctypes.util.find_library = real_find_library
        ctypes.CDLL = real_cdll


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


@pytest.fixture(autouse=True)
def _clear_mastodon_instance_limits_cache() -> Iterator[None]:
    """Keep MastodonPoster's per-instance limits cache from crossing tests.

    It is class-level with a 6-hour TTL, so an entry written by one test
    outlives the whole run and is served to any later test asking the same
    instance — including one that never patched httpx and believes it is
    exercising the fallback path. Resolved through ``sys.modules`` rather than
    imported: importing here would freeze ``config.DATA_DIR`` against the real
    data dir for every test in the session.
    """
    def clear() -> None:
        module = sys.modules.get("yt_scheduler.services.social")
        if module is not None:
            module.MastodonPoster._instance_limits_cache.clear()

    clear()
    yield
    clear()


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


def install_in_memory_keychain(
    monkeypatch: pytest.MonkeyPatch, keychain_module
) -> dict[tuple[str, str], str]:
    """Swap the macOS Keychain primitives for a dict, and return it.

    Tests need a *fake secret*, never a real one — nothing in this suite makes a
    real API call. The substitution happens at the three Keychain primitives, so
    everything above them (the key index, the legacy-namespace migration, the
    public API) is the same code that runs in production; only the system call
    at the bottom is faked.

    ``_is_macos`` is forced TRUE rather than false. There is no longer a
    non-macOS store to fall back to — off macOS the module raises — and pinning
    it here means the suite behaves identically wherever it runs. Nothing
    touches the real login Keychain, and no secret is written to disk.
    """
    store: dict[tuple[str, str], str] = {}

    def _set(service: str, account: str, value: str) -> bool:
        store[(service, account)] = value
        return True

    def _get(service: str, account: str) -> str | None:
        return store.get((service, account))

    def _delete(service: str, account: str) -> bool:
        return store.pop((service, account), None) is not None

    monkeypatch.setattr(keychain_module, "_is_macos", lambda: True)
    monkeypatch.setattr(keychain_module, "_keychain_set", _set)
    monkeypatch.setattr(keychain_module, "_keychain_get", _get)
    monkeypatch.setattr(keychain_module, "_keychain_delete", _delete)
    return store


@pytest.fixture
def isolated_data_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Path]:
    """Point ``yt_scheduler`` at a throwaway data dir with a fake Keychain.

    Import-time frozen config is why both halves are needed: the env var must be
    set *before* ``yt_scheduler.config`` is (re)imported, and the module cache
    must be purged so that re-import actually happens. The in-memory keychain
    keeps every test off the real login Keychain.
    """
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    _purge_yt_scheduler_modules()

    keychain = importlib.import_module("yt_scheduler.services.keychain")
    install_in_memory_keychain(monkeypatch, keychain)

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
