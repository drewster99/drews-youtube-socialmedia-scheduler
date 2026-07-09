"""The conftest guard must refuse any connection to the real data dir.

config.DATA_DIR/DB_PATH freeze at import, so a test that reaches
yt_scheduler.database without pointing DYS_DATA_DIR at a tmp dir opens the
user's real publisher.db read/write. That is invisible in the test's own source,
so the guard sits at the three places a database is ever opened.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import aiosqlite
import pytest

from tests.conftest import _PRODUCTION_DIRS, ProductionDatabaseAccess, _is_production_path

REAL_DB = _PRODUCTION_DIRS[0] / "publisher.db"


def test_production_paths_are_recognised() -> None:
    assert _is_production_path(REAL_DB)
    assert _is_production_path(str(REAL_DB))
    assert _is_production_path(_PRODUCTION_DIRS[0] / "secrets.json")


def test_tmp_and_memory_paths_are_allowed(tmp_path: Path) -> None:
    assert not _is_production_path(":memory:")
    assert not _is_production_path(tmp_path / "publisher.db")
    assert not _is_production_path(None)


def test_sqlite3_connect_to_real_db_is_refused() -> None:
    with pytest.raises(ProductionDatabaseAccess, match="production database"):
        sqlite3.connect(str(REAL_DB))


async def test_aiosqlite_connect_to_real_db_is_refused() -> None:
    with pytest.raises(ProductionDatabaseAccess, match="production database"):
        await aiosqlite.connect(str(REAL_DB))


def test_sqlite3_still_works_for_tmp_paths(tmp_path: Path) -> None:
    with sqlite3.connect(str(tmp_path / "scratch.db")) as conn:
        conn.execute("CREATE TABLE t (id INTEGER)")
    assert (tmp_path / "scratch.db").exists()


async def test_isolated_db_fixture_is_not_the_real_db(isolated_db, tmp_path: Path) -> None:
    """Belt and braces: the fixture must resolve to tmp, not app support."""
    import yt_scheduler.config as config

    assert not _is_production_path(config.DB_PATH)
    assert config.DB_PATH.is_relative_to(tmp_path)
