"""Regression test: a post stranded in 'sending' by a crash is recovered.

Both publish_video_job (filters status='approved') and _send_scheduled_post
(early-returns on 'sending') would otherwise skip a stranded 'sending' row
forever, so it would silently never send. restore_scheduled_posts resets any
'sending' row to 'approved' on startup, when nothing is actually in flight.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
async def app_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    scheduler = importlib.import_module("yt_scheduler.services.scheduler")

    db = await database.get_db()
    await projects.ensure_default_project()
    yield scheduler, db
    await database.close_db()


async def test_restore_resets_stranded_sending_to_approved(app_db):
    scheduler, db = app_db
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status) "
        "VALUES ('vidS', 1, 'Strand', 'uploaded')"
    )
    cursor = await db.execute(
        "INSERT INTO social_posts (video_id, platform, content, status) "
        "VALUES ('vidS', 'mastodon', 'hi', 'sending')"
    )
    post_id = cursor.lastrowid
    await db.commit()

    await scheduler.restore_scheduled_posts()

    cursor = await db.execute(
        "SELECT status FROM social_posts WHERE id = ?", (post_id,)
    )
    row = await cursor.fetchone()
    assert row["status"] == "approved"
