"""Migration runner tests."""

from __future__ import annotations

import shutil
from pathlib import Path

import aiosqlite
import pytest

from yt_scheduler.migrations import (
    MIGRATIONS_DIR,
    _split_statements,
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
async def test_existing_db_runs_idempotent_baseline(tmp_path: Path) -> None:
    """A pre-existing DB created via raw CREATE TABLE migrates cleanly.

    The baseline (v1) is entirely CREATE TABLE IF NOT EXISTS, so it re-runs as a
    no-op and is recorded; later migrations apply normally on top. (We no longer
    stamp v1 by table-name match, which could falsely mark a partial schema as
    migrated.)
    """
    db_path = tmp_path / "publisher.db"
    async with aiosqlite.connect(str(db_path)) as conn:
        # Simulate the legacy startup: create the baseline tables directly.
        baseline_sql = (MIGRATIONS_DIR / "001_baseline.sql").read_text()
        await conn.executescript(baseline_sql)
        await conn.commit()

        await apply_migrations(conn)
        cursor = await conn.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        )
        rows = await cursor.fetchall()
        recorded = [row[0] for row in rows]
        assert recorded[0] == 1  # baseline recorded
        assert sorted(recorded) == recorded  # contiguous, sorted
        # Schema still intact and usable after the idempotent re-run.
        cursor = await conn.execute("SELECT COUNT(*) FROM videos")
        assert (await cursor.fetchone())[0] == 0


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


def test_split_statements_handles_semicolons_in_literals() -> None:
    """A semicolon or `--` inside a string literal (e.g. a seeded prompt body)
    must not mis-split the statement."""
    from yt_scheduler.migrations import _split_statements

    sql = (
        "PRAGMA foreign_keys = OFF;\n"
        "CREATE TABLE t (id INTEGER, body TEXT);\n"
        "INSERT INTO t (id, body) VALUES (1, 'Step 1; do X -- not a comment');\n"
        "PRAGMA foreign_keys = ON;\n"
    )
    leading, txn = _split_statements(sql)
    assert any(s.upper().startswith("PRAGMA") and "OFF" in s.upper() for s in leading)
    insert_stmts = [s for s in txn if s.upper().startswith("INSERT")]
    assert len(insert_stmts) == 1
    assert "'Step 1; do X -- not a comment'" in insert_stmts[0]
    # Trailing PRAGMA ON is excluded (restored by the finally block instead).
    assert not any("ON" in s.upper() and s.upper().startswith("PRAGMA") for s in txn)


def test_split_statements_classifies_pragma_after_comment() -> None:
    """A leading `-- comment` line before PRAGMA foreign_keys=OFF must still be
    classified as a leading PRAGMA (run outside the transaction)."""
    from yt_scheduler.migrations import _split_statements

    sql = (
        "-- rebuild needs FKs off\n"
        "PRAGMA foreign_keys = OFF;\n"
        "CREATE TABLE t (id INTEGER);\n"
    )
    leading, txn = _split_statements(sql)
    assert len(leading) == 1 and leading[0].upper().startswith("PRAGMA")
    assert any(s.upper().startswith("CREATE TABLE") for s in txn)


def test_discover_returns_sorted(tmp_path: Path) -> None:
    (tmp_path / "002_b.sql").write_text("-- b")
    (tmp_path / "001_a.sql").write_text("-- a")
    (tmp_path / "003_c.sql").write_text("-- c")

    migrations = discover_migrations(tmp_path)
    assert [m.version for m in migrations] == [1, 2, 3]
    assert [m.name for m in migrations] == ["a", "b", "c"]


@pytest.mark.asyncio
async def test_failed_migration_is_fully_rolled_back(tmp_path: Path) -> None:
    """A migration that fails mid-way must leave no schema changes and no stamp.

    This is the core atomicity guarantee: the DB is identical before and after
    a failed migration run, so the next startup can safely retry.
    """
    db_path = tmp_path / "publisher.db"
    async with aiosqlite.connect(str(db_path)) as conn:
        # Apply the baseline so schema_migrations exists (version 1 stamped).
        await apply_migrations(conn, target_version=1)

        # Build an isolated migrations dir holding the REAL baseline (so the
        # runner discovers a contiguous [1, 2] set — otherwise discover_migrations
        # rejects a lone version 2 and we'd never reach the migration at all) plus
        # a 002 that adds a column then re-adds it (duplicate → fails mid-script).
        mig_dir = tmp_path / "migrations"
        mig_dir.mkdir()
        shutil.copy(MIGRATIONS_DIR / "001_baseline.sql", mig_dir / "001_baseline.sql")
        (mig_dir / "002_partial_fail.sql").write_text(
            "ALTER TABLE videos ADD COLUMN canary_col TEXT;\n"
            "ALTER TABLE videos ADD COLUMN canary_col TEXT;\n"  # duplicate → fail
        )

        # Version 1 is already applied, so only 002 runs — and must raise for the
        # migration's own reason, NOT a discovery/contiguity error.
        with pytest.raises(Exception) as exc_info:
            await apply_migrations(conn, directory=mig_dir, target_version=2)
        assert "canary_col" in str(exc_info.value).lower() or (
            "duplicate column" in str(exc_info.value).lower()
        ), f"failed for the wrong reason: {exc_info.value!r}"

        # The partial column must NOT have been committed.
        cursor = await conn.execute("PRAGMA table_info(videos)")
        col_names = {row[1] for row in await cursor.fetchall()}
        assert "canary_col" not in col_names, (
            "Failed migration left a partial schema change — rollback did not work"
        )

        # Version 2 must NOT be stamped — next startup can retry.
        cursor = await conn.execute("SELECT version FROM schema_migrations")
        versions = {row[0] for row in await cursor.fetchall()}
        assert 2 not in versions, (
            "Failed migration was stamped as applied — would be skipped on retry"
        )


def test_split_statements_strips_semicolons_in_comments(tmp_path: Path) -> None:
    """Semicolons inside -- comment text must not produce spurious statement fragments."""
    sql = (
        "-- duration < 50s; the UI shows a picker.\n"
        "ALTER TABLE t ADD COLUMN x INTEGER;\n"
        "ALTER TABLE t ADD COLUMN y INTEGER;\n"
    )
    leading_pragmas, txn_stmts = _split_statements(sql)
    assert leading_pragmas == []
    assert len(txn_stmts) == 2
    assert all(s.upper().startswith("ALTER") for s in txn_stmts)


def test_split_statements_leading_pragma_excluded_from_txn(tmp_path: Path) -> None:
    """Leading PRAGMA lines are separated from transactional statements."""
    sql = (
        "PRAGMA foreign_keys = OFF;\n"
        "CREATE TABLE t (x INTEGER);\n"
        "INSERT INTO t VALUES (1);\n"
        "PRAGMA foreign_keys = ON;\n"
    )
    leading_pragmas, txn_stmts = _split_statements(sql)
    assert len(leading_pragmas) == 1
    assert "OFF" in leading_pragmas[0]
    # Trailing PRAGMA must be excluded from txn_stmts.
    assert all("PRAGMA" not in s.upper() for s in txn_stmts)
    assert len(txn_stmts) == 2
