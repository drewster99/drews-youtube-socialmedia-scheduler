"""``PRAGMA foreign_keys`` is a connection-scoped pragma in SQLite —
opening a fresh connection to an already-migrated DB doesn't carry
any prior setting forward. The codebase depends on
``ON DELETE CASCADE`` for several relationships (social_post_traces,
template_slots, project-scoped tables), so the singleton in
``database.get_db()`` must set the pragma on every new connection,
not just rely on the in-migration ``PRAGMA foreign_keys = ON``
running at first-boot time.

This test simulates the production "process restart on a long-running
install" case: migrate, close, reopen, then check that a cascade
still fires.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.mark.asyncio
async def test_fk_pragma_set_on_every_connection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)

    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    db_module = importlib.import_module("yt_scheduler.database")

    # First boot — opens, migrates, closes.
    db = await db_module.get_db()
    cur = await db.execute("PRAGMA foreign_keys")
    assert (await cur.fetchone())[0] == 1
    await db_module.close_db()

    # Reset the module state to mimic a fresh process picking up the
    # already-migrated DB. (close_db sets _db=None internally; we go a
    # step further to mirror what happens in production.)
    db_module._db = None

    # Second boot — must still have foreign_keys ON. Without the
    # database.get_db fix this came back 0 because the pragma is
    # connection-scoped and migration 002 doesn't re-run on a stamped
    # DB.
    db = await db_module.get_db()
    cur = await db.execute("PRAGMA foreign_keys")
    assert (await cur.fetchone())[0] == 1

    # And cascade actually fires across the restart. Seed a videos row
    # + a social_posts row + a social_post_traces row, then delete
    # the post and confirm the trace went with it.
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status, imported_from_youtube) "
        "VALUES ('FKT0000001', 1, 't', 'uploaded', 1)"
    )
    cur = await db.execute(
        "INSERT INTO social_posts (video_id, platform, content, status) "
        "VALUES ('FKT0000001', 'bluesky', 'hi', 'draft')"
    )
    post_id = int(cur.lastrowid)
    await db.execute(
        "INSERT INTO social_post_traces (post_id, trace_json) VALUES (?, ?)",
        (post_id, "[]"),
    )
    await db.commit()

    await db.execute("DELETE FROM social_posts WHERE id = ?", (post_id,))
    await db.commit()

    cur = await db.execute(
        "SELECT 1 FROM social_post_traces WHERE post_id = ?", (post_id,)
    )
    leftover = await cur.fetchone()
    assert leftover is None, "ON DELETE CASCADE didn't fire — FK pragma off?"
