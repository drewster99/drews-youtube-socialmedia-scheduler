"""Tests for the project-layer migration (002_projects.sql)."""

from __future__ import annotations

from pathlib import Path

import aiosqlite
import pytest

from yt_scheduler.migrations import MIGRATIONS_DIR, apply_migrations


async def _seed_baseline_with_data(conn: aiosqlite.Connection) -> None:
    """Create the baseline schema and insert sample rows representative of legacy data."""
    baseline = (MIGRATIONS_DIR / "001_baseline.sql").read_text()
    await conn.executescript(baseline)
    await conn.execute(
        """INSERT INTO videos (id, title, description, tags, status)
           VALUES ('vid_abc', 'Hello World', 'Old description', '["foo"]', 'uploaded')"""
    )
    await conn.execute(
        """INSERT INTO templates (name, description, platforms)
           VALUES ('new_video', 'Default', '{"twitter": {"template": "hi"}}')"""
    )
    await conn.execute(
        "INSERT INTO blocklist (keyword, is_regex) VALUES ('spam', 0)"
    )
    await conn.execute(
        """INSERT INTO moderation_log (video_id, comment_id, author, comment_text,
                                       matched_keyword, action)
           VALUES ('vid_abc', 'cmt1', 'troll', 'spammy text', 'spam', 'deleted')"""
    )
    await conn.commit()


@pytest.mark.asyncio
async def test_migration_002_creates_project_tables(tmp_path: Path) -> None:
    db = tmp_path / "p.db"
    async with aiosqlite.connect(str(db)) as conn:
        await apply_migrations(conn)
        cursor = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        names = {row[0] for row in await cursor.fetchall()}
        assert {
            "projects", "social_accounts", "project_social_accounts",
            "project_settings",
        }.issubset(names)


@pytest.mark.asyncio
async def test_migration_002_backfills_existing_data_into_default(tmp_path: Path) -> None:
    db = tmp_path / "p.db"
    async with aiosqlite.connect(str(db)) as conn:
        # Pretend we're an existing install: create baseline schema directly.
        await _seed_baseline_with_data(conn)
        # Now run migrations: this stamps v1 and applies v2 with the backfill.
        await apply_migrations(conn)

        cursor = await conn.execute(
            "SELECT id, name, slug FROM projects"
        )
        rows = await cursor.fetchall()
        assert rows == [(1, "Default", "default")]

        for table in ("videos", "templates", "blocklist", "moderation_log"):
            cursor = await conn.execute(
                f"SELECT project_id FROM {table}"
            )
            project_ids = {row[0] for row in await cursor.fetchall()}
            assert project_ids == {1}, f"{table} rows should all be in project 1"


@pytest.mark.asyncio
async def test_migration_002_default_project_unique(tmp_path: Path) -> None:
    db = tmp_path / "p.db"
    async with aiosqlite.connect(str(db)) as conn:
        await apply_migrations(conn)
        with pytest.raises(aiosqlite.IntegrityError):
            await conn.execute(
                "INSERT INTO projects (name, slug) VALUES ('Other', 'default')"
            )
            await conn.commit()


@pytest.mark.asyncio
async def test_migration_002_cascade_delete_project_clears_videos(tmp_path: Path) -> None:
    db = tmp_path / "p.db"
    async with aiosqlite.connect(str(db)) as conn:
        await _seed_baseline_with_data(conn)
        await apply_migrations(conn)
        await conn.execute("PRAGMA foreign_keys = ON")
        await conn.execute("DELETE FROM projects WHERE id = 1")
        await conn.commit()

        for table in ("videos", "templates", "blocklist", "moderation_log",
                      "project_settings", "project_social_accounts"):
            cursor = await conn.execute(f"SELECT COUNT(*) FROM {table}")
            count = (await cursor.fetchone())[0]
            assert count == 0, f"{table} should be empty after project delete"


@pytest.mark.asyncio
async def test_migration_002_social_accounts_survive_project_delete(tmp_path: Path) -> None:
    """social_accounts rows themselves should not cascade away when a project is deleted."""
    db = tmp_path / "p.db"
    async with aiosqlite.connect(str(db)) as conn:
        await apply_migrations(conn)
        await conn.execute("PRAGMA foreign_keys = ON")
        # Add a project + social_account + attachment.
        await conn.execute(
            "INSERT INTO projects (name, slug) VALUES ('Other', 'other')"
        )
        await conn.execute(
            "INSERT INTO social_accounts (platform, username, credentials_ref) "
            "VALUES ('bluesky', 'me.bsky.social', 'bluesky:me.bsky.social')"
        )
        await conn.execute(
            "INSERT INTO project_social_accounts (project_id, social_account_id) "
            "VALUES (2, 1)"
        )
        await conn.commit()

        await conn.execute("DELETE FROM projects WHERE id = 2")
        await conn.commit()

        cursor = await conn.execute("SELECT COUNT(*) FROM social_accounts")
        assert (await cursor.fetchone())[0] == 1
        cursor = await conn.execute("SELECT COUNT(*) FROM project_social_accounts")
        assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_migration_002_template_applies_to_default(tmp_path: Path) -> None:
    db = tmp_path / "p.db"
    async with aiosqlite.connect(str(db)) as conn:
        await _seed_baseline_with_data(conn)
        await apply_migrations(conn)
        cursor = await conn.execute("SELECT applies_to FROM templates")
        row = await cursor.fetchone()
        assert row[0] == '["hook","short","segment","video"]'
