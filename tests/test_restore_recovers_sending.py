"""A post stranded in 'sending' by a crash is OUTCOME-UNKNOWN, not unsent.

This file used to assert the opposite: that startup reset any 'sending' row to
'approved' so it could be re-claimed and sent, reasoning that "nothing is
actually in flight" on a fresh process. That premise is true and irrelevant.
Nothing is in flight NOW — but the claim was taken before the crash, the request
may have been written in full, and the platform may have accepted it. The only
record that it did was the `mark_posted` the shutdown prevented.

Resetting to 'approved' left `scheduled_at` set (only mark_posted/mark_failed
clear it), so the row fell straight into the overdue-recovery pass and was sent
again. Quitting the menubar app during a large upload the platform had already
accepted was enough to post twice, unattended.

No downstream guard can catch it: `find_recent_duplicate_post` looks for a
'posted'/'sending' row with the same content and excludes the row itself, and
nothing else ever recorded the send. So the ambiguity is stated and handed to a
human — the reasoning behind ThreadsPublishOutcomeUnknown, applied to the
process-crash case.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from tests.conftest import install_in_memory_keychain


@pytest.fixture
async def app_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    install_in_memory_keychain(monkeypatch, keychain)
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    scheduler = importlib.import_module("yt_scheduler.services.scheduler")

    db = await database.get_db()
    await projects.ensure_default_project()
    yield scheduler, db
    await database.close_db()


async def _stranded_post(db, *, scheduled_at: str | None) -> int:
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status) "
        "VALUES ('vidS', 1, 'Strand', 'uploaded')"
    )
    cursor = await db.execute(
        "INSERT INTO social_posts (video_id, platform, content, status, scheduled_at) "
        "VALUES ('vidS', 'mastodon', 'hi', 'sending', ?)",
        (scheduled_at,),
    )
    post_id = cursor.lastrowid
    await db.commit()
    return post_id


async def test_a_post_stranded_mid_send_is_never_re_sent_automatically(app_db):
    """The double-send this prevents: an overdue `scheduled_at` on a row reset
    to 'approved' is exactly what the recovery pass picks up and sends."""
    scheduler, db = app_db
    post_id = await _stranded_post(db, scheduled_at="2020-01-01T00:00:00+00:00")

    await scheduler.restore_scheduled_posts()

    cursor = await db.execute(
        "SELECT status, scheduled_at, scheduler_job_id FROM social_posts WHERE id = ?",
        (post_id,),
    )
    row = await cursor.fetchone()
    assert row["status"] == "failed", "a stranded send must not be re-armed"
    # Cleared by mark_failed — this is what keeps the recovery pass below from
    # ever seeing the row.
    assert row["scheduled_at"] is None
    assert row["scheduler_job_id"] is None


async def test_the_ambiguity_is_stated_rather_than_guessed(app_db):
    """The user has to be told it MIGHT have gone out. A bare "failed" would
    invite precisely the re-send this exists to prevent."""
    scheduler, db = app_db
    post_id = await _stranded_post(db, scheduled_at=None)

    await scheduler.restore_scheduled_posts()

    cursor = await db.execute(
        "SELECT status, error, retryable FROM social_posts WHERE id = ?", (post_id,)
    )
    row = await cursor.fetchone()
    assert row["status"] == "failed"
    assert "unknown whether it went out" in row["error"]
    # And it must not reach the automatic retry job, which would do the
    # re-sending by itself.
    assert not row["retryable"]
