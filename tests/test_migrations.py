"""Migration runner tests."""

from __future__ import annotations

from pathlib import Path

import aiosqlite
import pytest

from yt_scheduler.migrations import (
    MIGRATIONS_DIR,
    apply_migrations,
    discover_migrations,
)


@pytest.mark.asyncio
async def test_baseline_migration_applies_to_empty_db(tmp_path: Path) -> None:
    db_path = tmp_path / "publisher.db"
    async with aiosqlite.connect(str(db_path)) as conn:
        applied = await apply_migrations(conn)
        assert 1 in applied

        cursor = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "ORDER BY name"
        )
        rows = await cursor.fetchall()
        names = {row[0] for row in rows}
        assert {"videos", "social_posts", "templates", "blocklist", "moderation_log",
                "settings", "schema_migrations"}.issubset(names)


@pytest.mark.asyncio
async def test_running_again_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "publisher.db"
    async with aiosqlite.connect(str(db_path)) as conn:
        await apply_migrations(conn)
        applied_again = await apply_migrations(conn)
        assert applied_again == []


@pytest.mark.asyncio
async def test_existing_db_stamped_at_baseline(tmp_path: Path) -> None:
    """A pre-existing DB created via raw CREATE TABLE should be stamped, not re-run.

    Baseline (v1) is recorded as applied without re-executing its SQL; later
    migrations apply normally on top.
    """
    db_path = tmp_path / "publisher.db"
    async with aiosqlite.connect(str(db_path)) as conn:
        # Simulate the legacy startup: create the baseline tables directly.
        baseline_sql = (MIGRATIONS_DIR / "001_baseline.sql").read_text()
        await conn.executescript(baseline_sql)
        await conn.commit()

        applied = await apply_migrations(conn)
        # Baseline must NOT appear in newly_applied (it was stamped, not re-run).
        assert 1 not in applied
        cursor = await conn.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        )
        rows = await cursor.fetchall()
        recorded = [row[0] for row in rows]
        assert recorded[0] == 1
        assert sorted(recorded) == recorded  # contiguous, sorted


@pytest.mark.asyncio
async def test_checksum_mismatch_raises(tmp_path: Path) -> None:
    db_path = tmp_path / "publisher.db"
    async with aiosqlite.connect(str(db_path)) as conn:
        await apply_migrations(conn)

        # Manually corrupt the recorded checksum
        await conn.execute(
            "UPDATE schema_migrations SET checksum = 'bogus' WHERE version = 1"
        )
        await conn.commit()

        with pytest.raises(RuntimeError, match="checksum mismatch"):
            await apply_migrations(conn)


def test_discover_rejects_non_contiguous(tmp_path: Path) -> None:
    (tmp_path / "001_baseline.sql").write_text("-- noop")
    (tmp_path / "003_skip.sql").write_text("-- noop")

    with pytest.raises(ValueError, match="contiguous"):
        discover_migrations(tmp_path)


def test_discover_rejects_bad_filename(tmp_path: Path) -> None:
    (tmp_path / "001_baseline.sql").write_text("-- noop")
    (tmp_path / "not-a-migration.sql").write_text("-- noop")

    with pytest.raises(ValueError, match="does not match"):
        discover_migrations(tmp_path)


def test_discover_rejects_duplicate_version(tmp_path: Path) -> None:
    (tmp_path / "001_baseline.sql").write_text("-- noop")
    (tmp_path / "001_other.sql").write_text("-- noop")

    with pytest.raises(ValueError, match="Duplicate"):
        discover_migrations(tmp_path)


def test_discover_returns_sorted(tmp_path: Path) -> None:
    (tmp_path / "002_b.sql").write_text("-- b")
    (tmp_path / "001_a.sql").write_text("-- a")
    (tmp_path / "003_c.sql").write_text("-- c")

    migrations = discover_migrations(tmp_path)
    assert [m.version for m in migrations] == [1, 2, 3]
    assert [m.name for m in migrations] == ["a", "b", "c"]
