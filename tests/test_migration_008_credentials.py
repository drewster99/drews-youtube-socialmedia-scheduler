"""Tests for migration 008 (per-project credentials reshape).

Verifies the schema reshape: ``social_accounts`` gains ``uuid``,
``provider_account_id``, ``deleted_at``; ``project_social_defaults``
replaces ``project_social_accounts``; ``template_slots`` replaces
``templates.platforms`` JSON; the ``announce_video``/``send_message``
rename runs.
"""

from __future__ import annotations

from pathlib import Path

import aiosqlite
import pytest

from yt_scheduler.migrations import MIGRATIONS_DIR, apply_migrations


async def _seed_pre_008(conn: aiosqlite.Connection) -> None:
    """Apply migrations 001..007 and insert representative legacy rows."""
    await apply_migrations(conn, target_version=7)
    await conn.execute("PRAGMA foreign_keys = ON")
    await conn.execute(
        "INSERT INTO social_accounts (platform, username, credentials_ref) "
        "VALUES ('twitter', 'olduser', 'twitter:olduser')"
    )
    await conn.execute(
        "INSERT INTO social_accounts (platform, username, credentials_ref) "
        "VALUES ('bluesky', 'me.bsky.social', 'bluesky:me.bsky.social')"
    )
    await conn.execute(
        "INSERT INTO project_social_accounts (project_id, social_account_id) "
        "VALUES (1, 1)"
    )
    await conn.execute(
        "INSERT INTO templates (project_id, name, description, platforms, applies_to) "
        "VALUES (1, 'new_video', 'announce', "
        "'{\"twitter\":{\"template\":\"hi\",\"max_chars\":280},"
        " \"bluesky\":{\"template\":\"yo\",\"max_chars\":300}}', "
        "'[\"hook\",\"video\"]')"
    )
    await conn.execute(
        "INSERT INTO templates (project_id, name, description, platforms, applies_to) "
        "VALUES (1, 'new_message', 'send', "
        "'{\"linkedin\":{\"template\":\"hello\"}}', "
        "'[\"video\"]')"
    )
    await conn.commit()


@pytest.mark.asyncio
async def test_migration_008_creates_new_tables(tmp_path: Path) -> None:
    db = tmp_path / "p.db"
    async with aiosqlite.connect(str(db)) as conn:
        await apply_migrations(conn, target_version=8)
        cursor = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = {r[0] for r in await cursor.fetchall()}
        assert "project_social_defaults" in tables
        assert "template_slots" in tables
        assert "project_social_accounts" not in tables, (
            "project_social_accounts should be replaced"
        )


@pytest.mark.asyncio
async def test_migration_008_social_accounts_columns(tmp_path: Path) -> None:
    db = tmp_path / "p.db"
    async with aiosqlite.connect(str(db)) as conn:
        await apply_migrations(conn, target_version=8)
        cursor = await conn.execute("PRAGMA table_info(social_accounts)")
        cols = {row[1] for row in await cursor.fetchall()}
        for required in {
            "uuid", "platform", "provider_account_id", "username",
            "display_name", "is_nickname", "credentials_ref",
            "created_at", "deleted_at",
        }:
            assert required in cols, f"social_accounts missing column {required}"


@pytest.mark.asyncio
async def test_migration_008_inserts_pending_placeholders(tmp_path: Path) -> None:
    db = tmp_path / "p.db"
    async with aiosqlite.connect(str(db)) as conn:
        conn.row_factory = aiosqlite.Row
        await _seed_pre_008(conn)
        await apply_migrations(conn, target_version=8)

        cursor = await conn.execute(
            "SELECT uuid, platform, provider_account_id, credentials_ref, deleted_at "
            "FROM social_accounts ORDER BY id"
        )
        rows = list(await cursor.fetchall())
        assert len(rows) == 2
        for row in rows:
            assert row["uuid"].startswith("__pending__:"), row["uuid"]
            assert row["provider_account_id"].startswith("pending:"), row["provider_account_id"]
            assert row["credentials_ref"].startswith("cred.__pending__:")
            assert row["deleted_at"] is None


@pytest.mark.asyncio
async def test_migration_008_renames_seeded_templates(tmp_path: Path) -> None:
    db = tmp_path / "p.db"
    async with aiosqlite.connect(str(db)) as conn:
        conn.row_factory = aiosqlite.Row
        await _seed_pre_008(conn)
        await apply_migrations(conn, target_version=8)

        cursor = await conn.execute(
            "SELECT name, is_builtin FROM templates ORDER BY name"
        )
        rows = list(await cursor.fetchall())
        names = [r["name"] for r in rows]
        assert "announce_video" in names
        assert "send_message" in names
        assert "new_video" not in names
        assert "new_message" not in names
        for r in rows:
            if r["name"] in ("announce_video", "send_message"):
                assert r["is_builtin"] == 1


@pytest.mark.asyncio
async def test_migration_008_drops_platforms_column(tmp_path: Path) -> None:
    db = tmp_path / "p.db"
    async with aiosqlite.connect(str(db)) as conn:
        await apply_migrations(conn, target_version=8)
        cursor = await conn.execute("PRAGMA table_info(templates)")
        cols = {row[1] for row in await cursor.fetchall()}
        assert "platforms" not in cols, (
            "templates.platforms JSON column should be removed"
        )


@pytest.mark.asyncio
async def test_migration_008_backfills_template_slots_from_platforms(
    tmp_path: Path,
) -> None:
    db = tmp_path / "p.db"
    async with aiosqlite.connect(str(db)) as conn:
        conn.row_factory = aiosqlite.Row
        await _seed_pre_008(conn)
        await apply_migrations(conn, target_version=8)

        cursor = await conn.execute(
            "SELECT s.platform, s.body, s.max_chars, s.is_builtin, t.name "
            "FROM template_slots s JOIN templates t ON t.id = s.template_id "
            "ORDER BY t.name, s.platform"
        )
        rows = list(await cursor.fetchall())
        slots_by_template = {}
        for r in rows:
            slots_by_template.setdefault(r["name"], []).append(
                (r["platform"], r["body"], r["max_chars"], r["is_builtin"])
            )

        assert "announce_video" in slots_by_template
        announce = dict((p, (b, m, ib)) for p, b, m, ib in slots_by_template["announce_video"])
        assert announce["twitter"] == ("hi", 280, 1)
        assert announce["bluesky"] == ("yo", 300, 1)

        assert "send_message" in slots_by_template
        send = dict((p, (b, m, ib)) for p, b, m, ib in slots_by_template["send_message"])
        assert "linkedin" in send
        assert send["linkedin"][0] == "hello"
        assert send["linkedin"][2] == 1, "renamed builtin should keep is_builtin=1"


@pytest.mark.asyncio
async def test_migration_008_youtube_channel_unique_index(tmp_path: Path) -> None:
    """Two projects can't claim the same YouTube channel; NULL is fine."""
    db = tmp_path / "p.db"
    async with aiosqlite.connect(str(db)) as conn:
        await apply_migrations(conn, target_version=8)
        await conn.execute("PRAGMA foreign_keys = ON")
        await conn.execute(
            "INSERT INTO projects (name, slug, youtube_channel_id) "
            "VALUES ('A', 'a', 'UC123')"
        )
        await conn.execute(
            "INSERT INTO projects (name, slug, youtube_channel_id) "
            "VALUES ('B', 'b', NULL)"
        )
        await conn.execute(
            "INSERT INTO projects (name, slug, youtube_channel_id) "
            "VALUES ('C', 'c', NULL)"
        )
        await conn.commit()

        with pytest.raises(aiosqlite.IntegrityError):
            await conn.execute(
                "INSERT INTO projects (name, slug, youtube_channel_id) "
                "VALUES ('D', 'd', 'UC123')"
            )
            await conn.commit()


@pytest.mark.asyncio
async def test_migration_008_template_slot_cascade_on_template_delete(
    tmp_path: Path,
) -> None:
    db = tmp_path / "p.db"
    async with aiosqlite.connect(str(db)) as conn:
        await _seed_pre_008(conn)
        await apply_migrations(conn, target_version=8)
        await conn.execute("PRAGMA foreign_keys = ON")

        cursor = await conn.execute(
            "SELECT id FROM templates WHERE name = 'announce_video'"
        )
        tid = (await cursor.fetchone())[0]
        await conn.execute("DELETE FROM templates WHERE id = ?", (tid,))
        await conn.commit()

        cursor = await conn.execute(
            "SELECT COUNT(*) FROM template_slots WHERE template_id = ?", (tid,)
        )
        assert (await cursor.fetchone())[0] == 0, (
            "slots should cascade-delete when their template is deleted"
        )


@pytest.mark.asyncio
async def test_migration_008_default_set_null_on_credential_delete(
    tmp_path: Path,
) -> None:
    """Deleting a social_accounts row nulls the FK in project_social_defaults."""
    db = tmp_path / "p.db"
    async with aiosqlite.connect(str(db)) as conn:
        conn.row_factory = aiosqlite.Row
        await apply_migrations(conn, target_version=8)
        await conn.execute("PRAGMA foreign_keys = ON")
        cursor = await conn.execute(
            "INSERT INTO social_accounts "
            "(uuid, platform, provider_account_id, username, credentials_ref) "
            "VALUES ('u1', 'twitter', 'tw:1', 'me', 'cred.u1')"
        )
        sid = cursor.lastrowid
        await conn.execute(
            "INSERT INTO project_social_defaults (project_id, platform, social_account_id) "
            "VALUES (1, 'twitter', ?)",
            (sid,),
        )
        await conn.commit()

        await conn.execute("DELETE FROM social_accounts WHERE id = ?", (sid,))
        await conn.commit()

        cursor = await conn.execute(
            "SELECT social_account_id FROM project_social_defaults "
            "WHERE project_id = 1 AND platform = 'twitter'"
        )
        row = await cursor.fetchone()
        assert row is not None, "row should remain (only the FK is nulled)"
        assert row["social_account_id"] is None
