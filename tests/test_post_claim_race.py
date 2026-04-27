"""Tests for the atomic post-claim that prevents publish_video_job and
_send_scheduled_post from racing on the same post.

The original bug surfaced as two identical Mastodon posts at the same
timestamp — the per-video publish job and a per-post scheduled job
both transitioned ``approved`` → ``posted`` independently, each calling
``poster.post`` and recording an event.
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


async def _seed_approved_post(db) -> int:
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status) "
        "VALUES ('vidR', 1, 'Race', 'uploaded')"
    )
    cursor = await db.execute(
        "INSERT INTO social_posts (video_id, platform, content, status) "
        "VALUES ('vidR', 'mastodon', 'hello', 'approved')"
    )
    await db.commit()
    return int(cursor.lastrowid)


async def test_claim_succeeds_first_time(app_db) -> None:
    scheduler, db = app_db
    pid = await _seed_approved_post(db)
    won = await scheduler._claim_post_for_send(pid)
    assert won is True

    cursor = await db.execute("SELECT status FROM social_posts WHERE id = ?", (pid,))
    row = await cursor.fetchone()
    assert row["status"] == "sending"


async def test_second_claim_loses(app_db) -> None:
    scheduler, db = app_db
    pid = await _seed_approved_post(db)
    won1 = await scheduler._claim_post_for_send(pid)
    won2 = await scheduler._claim_post_for_send(pid)
    assert won1 is True
    assert won2 is False, "second concurrent caller must NOT also claim"


async def test_release_restores_approved(app_db) -> None:
    scheduler, db = app_db
    pid = await _seed_approved_post(db)
    await scheduler._claim_post_for_send(pid)
    await scheduler._release_post_to_approved(pid)

    cursor = await db.execute("SELECT status FROM social_posts WHERE id = ?", (pid,))
    row = await cursor.fetchone()
    assert row["status"] == "approved"

    # Now claim again — release must let the next worker pick it up.
    assert await scheduler._claim_post_for_send(pid) is True


async def test_release_doesnt_undo_a_posted_post(app_db) -> None:
    """If a post raced through to 'posted' between claim and release
    (e.g. the post succeeded but a later UPDATE failed), the release
    must NOT roll it back to approved — that would cause a real
    duplicate next time the scheduler fires."""
    scheduler, db = app_db
    pid = await _seed_approved_post(db)
    await scheduler._claim_post_for_send(pid)
    # Pretend the send succeeded
    await db.execute("UPDATE social_posts SET status = 'posted' WHERE id = ?", (pid,))
    await db.commit()

    await scheduler._release_post_to_approved(pid)

    cursor = await db.execute("SELECT status FROM social_posts WHERE id = ?", (pid,))
    row = await cursor.fetchone()
    assert row["status"] == "posted"


async def test_claim_ignores_non_approved_posts(app_db) -> None:
    """Posts in 'draft' or 'failed' aren't eligible for the auto-claim
    path — only 'approved' transitions to 'sending'. Manual retries via
    /api/social/posts/{id}/send go through a different code path."""
    scheduler, db = app_db
    cursor = await db.execute(
        "INSERT INTO videos (id, project_id, title, status) "
        "VALUES ('vidD', 1, 'Draft', 'uploaded')"
    )
    cursor = await db.execute(
        "INSERT INTO social_posts (video_id, platform, content, status) "
        "VALUES ('vidD', 'mastodon', 'hello', 'draft')"
    )
    await db.commit()
    pid = int(cursor.lastrowid)
    assert await scheduler._claim_post_for_send(pid) is False
