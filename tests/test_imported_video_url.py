"""Regression tests for the {{url}} bug — every imported YouTube row must
have ``videos.url`` set, and the social-render context must surface a
warning when a template body references a URL-family variable that
resolved to empty.

The bug: ``services/imports.py`` was INSERTing imported videos without
setting the ``url`` column, so ``video.get("url") or ""`` in
``_build_render_context`` silently returned "" and any ``{{url}}`` in a
template rendered as nothing. Migration 010 had backfilled existing
rows; the code regression had to be guarded both with a code fix and a
catch-up migration (015).
"""

from __future__ import annotations

from pathlib import Path

import aiosqlite
import pytest

from yt_scheduler.migrations import apply_migrations


@pytest.mark.asyncio
async def test_migration_015_backfills_imported_rows_with_null_url(tmp_path: Path) -> None:
    """An imported row that somehow ended up with NULL url must get
    ``https://youtu.be/<id>`` after migration 015 runs."""
    db_path = tmp_path / "p.db"
    async with aiosqlite.connect(str(db_path)) as conn:
        # Apply all migrations up to (but not including) 015. There is no
        # public API for "stop at version N", so we apply everything and
        # then null out the URL on an imported row to simulate the bug
        # state — migration 015 is idempotent so re-running here would
        # be a no-op, but our point is to prove the SELECTed UPDATE is
        # correct.
        await apply_migrations(conn)

        # Insert an imported row, then deliberately clear its url to
        # mimic the bug-era state.
        await conn.execute(
            "INSERT INTO videos (id, title, status, imported_from_youtube) "
            "VALUES (?, ?, 'uploaded', 1)",
            ("BUGTEST1234", "Bug repro"),
        )
        await conn.execute("UPDATE videos SET url = NULL WHERE id = 'BUGTEST1234'")
        await conn.commit()

        # Re-run the 015 SQL by hand (apply_migrations would record it as
        # already applied). This is a pure-SQL idempotency test.
        from yt_scheduler.migrations import MIGRATIONS_DIR
        sql_015 = (MIGRATIONS_DIR / "015_backfill_imported_video_url.sql").read_text()
        await conn.executescript(sql_015)
        await conn.commit()

        cur = await conn.execute("SELECT url FROM videos WHERE id = 'BUGTEST1234'")
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == "https://youtu.be/BUGTEST1234"


@pytest.mark.asyncio
async def test_migration_015_leaves_standalone_items_alone(tmp_path: Path) -> None:
    """A standalone (non-imported) item with NULL url must stay NULL —
    only YouTube imports get the youtu.be/<id> backfill."""
    db_path = tmp_path / "p.db"
    async with aiosqlite.connect(str(db_path)) as conn:
        await apply_migrations(conn)

        await conn.execute(
            "INSERT INTO videos (id, title, status, imported_from_youtube) "
            "VALUES (?, ?, 'uploaded', 0)",
            ("item_standalone_1", "Hand-built item"),
        )
        await conn.execute("UPDATE videos SET url = NULL WHERE id = 'item_standalone_1'")
        await conn.commit()

        from yt_scheduler.migrations import MIGRATIONS_DIR
        sql_015 = (MIGRATIONS_DIR / "015_backfill_imported_video_url.sql").read_text()
        await conn.executescript(sql_015)
        await conn.commit()

        cur = await conn.execute(
            "SELECT url FROM videos WHERE id = 'item_standalone_1'"
        )
        row = await cur.fetchone()
        assert row is not None
        assert row[0] is None


@pytest.mark.asyncio
async def test_render_context_reports_empty_url_keys(tmp_path: Path, monkeypatch) -> None:
    """``_build_render_context`` must return an ``empty_url_keys`` set
    listing which of {url, episode_url, project_url} resolved to empty.
    A slot body that references one of those keys later turns into a
    warning in the generate-posts response."""
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    (tmp_path / "templates").mkdir(parents=True, exist_ok=True)

    # Reimport to pick up the env var.
    import sys
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)

    from yt_scheduler.database import get_db
    from yt_scheduler.routers.social_routes import _build_render_context

    db = await get_db()

    # The "default" project from baseline schema has no project_url set,
    # so {{project_url}} should land in empty_url_keys.
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status) "
        "VALUES (?, 1, ?, 'draft')",
        ("URLLEAKAGE1", "URL leakage test"),
    )
    await db.commit()

    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", ("URLLEAKAGE1",))
    video = dict(rows[0])

    ctx = await _build_render_context(db, video)

    # No url set on this item, no parent item, no project_url set:
    # all three URL keys should be flagged as empty.
    assert ctx["empty_url_keys"] == {"url", "episode_url", "project_url"}

    # Setting an item URL should clear that one but leave the others.
    await db.execute(
        "UPDATE videos SET url = ? WHERE id = ?",
        ("https://example.com/x", "URLLEAKAGE1"),
    )
    await db.commit()
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", ("URLLEAKAGE1",))
    video = dict(rows[0])
    ctx = await _build_render_context(db, video)
    assert "url" not in ctx["empty_url_keys"]
    assert "episode_url" in ctx["empty_url_keys"]
    assert "project_url" in ctx["empty_url_keys"]
