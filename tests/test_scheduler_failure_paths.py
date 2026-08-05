"""Scheduler failure paths must be honest and terminal-or-retryable, not both.

Three defects this covers:

* A duplicate-detected scheduled post used to release to 'approved' and
  re-register itself an hour out. The duplicate window is anchored to the
  ORIGINAL post's posted_at, so it kept matching for up to 30 days — ~720 fires,
  each writing an event and a warning, never self-healing.
* Terminal transitions cleared scheduler_job_id but not scheduled_at, so
  restore_scheduled_posts re-registered a dead job for every failed post on every
  boot.
* publish_video_job fell through to a fabricated project_id=1 / item_type
  'episode' when the video row was gone, then called YouTube on a deleted id.
* A successful send wrote status/posted_at/post_url but left ``error`` holding
  the previous attempt's text, so a delivered post still rendered "use Send to
  retry" — and a Threads post that had timed out at the publish step (published
  anyway, response lost) was re-sent on that advice and went out twice.
"""

from __future__ import annotations

import importlib
import sys
from datetime import datetime, timedelta, timezone
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
    database.reset_write_txn_flag()
    await database.close_db()


async def _seed_video(
    db,
    video_id: str = "vidD",
    privacy: str = "public",
    *,
    youtube_video_id: str | None = "ytDupVideo",
    status: str = "published",
) -> None:
    """Seed a video. ``youtube_video_id=None`` makes it a non-YouTube item,
    for which privacy_status carries no meaning and ``status`` decides liveness."""
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status, privacy_status, youtube_video_id) "
        "VALUES (?, 1, 'Dup', ?, ?, ?)",
        (video_id, status, privacy, youtube_video_id),
    )
    await db.commit()


async def _seed_post(db, *, status: str, content: str = "hello",
                     posted_at: str | None = None, video_id: str = "vidD") -> int:
    cursor = await db.execute(
        "INSERT INTO social_posts "
        "(video_id, platform, content, status, posted_at, scheduled_at, scheduler_job_id) "
        "VALUES (?, 'mastodon', ?, ?, ?, '2030-01-01T00:00:00+00:00', 'job-1')",
        (video_id, content, status, posted_at),
    )
    await db.commit()
    return int(cursor.lastrowid)


async def _row(db, post_id: int) -> dict:
    cursor = await db.execute(
        "SELECT status, error, scheduled_at, scheduler_job_id "
        "FROM social_posts WHERE id = ?",
        (post_id,),
    )
    return dict(await cursor.fetchone())


async def test_duplicate_scheduled_post_is_terminal_and_not_rearmed(
    app_db, monkeypatch
) -> None:
    scheduler, db = app_db
    await _seed_video(db)
    # Relative, not a hardcoded absolute date: find_recent_duplicate_post only
    # matches within the last 30 days, so a fixed past date silently ages out of
    # the window and the dup stops being detected (this test began failing once
    # real time passed the old 2026-07-01 + 30d).
    recent = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    await _seed_post(db, status="posted", posted_at=recent)
    target = await _seed_post(db, status="approved")

    rearmed: list[int] = []
    monkeypatch.setattr(
        scheduler, "schedule_social_post",
        lambda pid, when: rearmed.append(pid),
    )

    await scheduler._send_scheduled_post(target)

    row = await _row(db, target)
    assert row["status"] == "failed"
    assert "already sent" in row["error"]
    # No live trigger and no stale schedule left behind.
    assert row["scheduled_at"] is None
    assert row["scheduler_job_id"] is None
    assert rearmed == [], "duplicate post must not re-arm an hourly retry"

    events = await db.execute_fetchall(
        "SELECT type FROM video_events WHERE type = 'social_post_skipped_duplicate'"
    )
    assert len(events) == 1, "exactly one skipped-duplicate event"


async def test_terminal_failure_clears_scheduled_at(app_db, monkeypatch) -> None:
    """Otherwise restore_scheduled_posts re-registers a dead job on every boot."""
    scheduler, db = app_db
    await _seed_video(db, privacy="unlisted")
    post_id = await _seed_post(db, status="approved")

    await scheduler._send_scheduled_post(post_id)

    row = await _row(db, post_id)
    assert row["status"] == "failed"
    assert "non-public" in row["error"]
    assert row["scheduled_at"] is None
    assert row["scheduler_job_id"] is None


async def test_non_youtube_item_is_not_blocked_by_privacy_status(app_db, monkeypatch) -> None:
    """privacy_status defaults to 'unlisted' on every row, YouTube-backed or not.

    Reading it unconditionally permanently blocked posts for items that never
    carried a YouTube link — they could never become 'public', so every send
    failed with 'refusing to post a link to a non-public video' about a link
    that does not exist.
    """
    scheduler, db = app_db
    await _seed_video(db, privacy="unlisted", youtube_video_id=None)
    post_id = await _seed_post(db, status="approved", content="no youtube here")

    sent: list[str] = []

    class _DeliveringPoster:
        async def is_configured(self) -> bool:
            return True

        async def post(self, content, media_paths=None) -> dict:
            sent.append(content)
            return {"url": "https://example.test/post/2"}

    social = importlib.import_module("yt_scheduler.services.social")
    monkeypatch.setattr(social, "get_poster", lambda platform: _DeliveringPoster())

    await scheduler._send_scheduled_post(post_id)

    assert sent == ["no youtube here"], "a non-YouTube item must not be privacy-gated"
    assert (await _row(db, post_id))["status"] == "posted"


async def test_non_youtube_item_is_blocked_until_published(app_db, monkeypatch) -> None:
    """For an item with no YouTube behind it, `status` is what liveness means.

    Skipping the check entirely for these would announce an unpublished item.
    smart_queue.is_eligible draws the line at status == 'published'; this path
    must draw it in the same place or the two disagree about the same video.
    """
    scheduler, db = app_db
    await _seed_video(db, privacy="unlisted", youtube_video_id=None, status="draft")
    post_id = await _seed_post(db, status="approved", content="not ready")

    class _ExplodingPoster:
        async def is_configured(self) -> bool:
            return True

        async def post(self, content, media_paths=None) -> dict:
            raise AssertionError("must not announce an unpublished item")

    social = importlib.import_module("yt_scheduler.services.social")
    monkeypatch.setattr(social, "get_poster", lambda platform: _ExplodingPoster())

    await scheduler._send_scheduled_post(post_id)

    row = await _row(db, post_id)
    assert row["status"] == "failed"
    assert "not published yet" in row["error"]


async def test_youtube_backed_unlisted_video_is_still_blocked(app_db, monkeypatch) -> None:
    """The guard must keep working for rows that DO have a YouTube video."""
    scheduler, db = app_db
    await _seed_video(db, privacy="unlisted", youtube_video_id="ytRealVideo")
    post_id = await _seed_post(db, status="approved", content="has youtube")

    social = importlib.import_module("yt_scheduler.services.social")

    class _ExplodingPoster:
        async def is_configured(self) -> bool:
            return True

        async def post(self, content, media_paths=None) -> dict:
            raise AssertionError("must not reach the platform for an unlisted video")

    monkeypatch.setattr(social, "get_poster", lambda platform: _ExplodingPoster())

    await scheduler._send_scheduled_post(post_id)

    row = await _row(db, post_id)
    assert row["status"] == "failed"
    assert "non-public" in row["error"]


async def test_successful_send_clears_the_previous_attempt_error(app_db, monkeypatch) -> None:
    """A delivered post must carry no failure text — that text invites a re-send."""
    scheduler, db = app_db
    await _seed_video(db)
    post_id = await _seed_post(db, status="approved", content="second attempt")
    await db.execute(
        "UPDATE social_posts SET error = ? WHERE id = ?",
        (
            "Threads post failed: ReadTimeout: timed out contacting "
            "graph.threads.net. Check your network connection, then use Send to retry.",
            post_id,
        ),
    )
    await db.commit()

    class _DeliveringPoster:
        async def is_configured(self) -> bool:
            return True

        async def post(self, content, media_paths=None) -> dict:
            return {"url": "https://example.test/post/1"}

    # _send_scheduled_post imports get_poster lazily from the service module,
    # so patch it there rather than on the scheduler module object.
    social = importlib.import_module("yt_scheduler.services.social")
    monkeypatch.setattr(social, "get_poster", lambda platform: _DeliveringPoster())

    await scheduler._send_scheduled_post(post_id)

    row = await _row(db, post_id)
    assert row["status"] == "posted"
    assert row["error"] is None, (
        "a posted row still carrying the prior failure text is what produced a "
        "duplicate Threads post"
    )
    assert row["scheduled_at"] is None
    assert row["scheduler_job_id"] is None


async def test_publish_job_aborts_when_video_row_is_gone(app_db, monkeypatch) -> None:
    scheduler, db = app_db

    called: list[str] = []
    monkeypatch.setattr(
        scheduler.youtube, "update_video_metadata",
        lambda *a, **k: called.append("youtube"),
    )

    results = await scheduler.publish_video_job("gone0000000")

    assert results.get("skipped_missing_video") is True
    assert called == [], "must not call YouTube for a deleted video"

    events = await db.execute_fetchall(
        "SELECT type FROM video_events WHERE type = 'credential_invalid'"
    )
    assert events == [], "must not emit a misleading credential_invalid event"


async def test_archived_video_is_still_skipped(app_db) -> None:
    """The pre-existing archived guard must survive the None-row refactor."""
    scheduler, db = app_db
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status, archived) "
        "VALUES ('vidA', 1, 'Archived', 'published', 1)"
    )
    await db.commit()

    results = await scheduler.publish_video_job("vidA")

    assert results.get("skipped_archived") is True


# --- a failed YouTube publish is recorded where a page can see it -------------


async def test_failed_publish_stamps_the_row_and_records_an_event(
    app_db, monkeypatch
) -> None:
    """The publish job's only caller is a timer, so its return value and its
    log line reach nobody. Before the stamp, the video kept status='scheduled'
    with a past publish_at — which no page renders as trouble — while every
    approved social post sat unsent and nothing retried until a restart."""
    scheduler, db = app_db
    await _seed_video(db, "vidF", "unlisted", status="scheduled")

    youtube = importlib.import_module("yt_scheduler.services.youtube")

    def boom(video_id, **kw):
        raise RuntimeError("quotaExceeded: publish refused")

    monkeypatch.setattr(youtube, "update_video_metadata", boom)

    result = await scheduler.publish_video_job("vidF")

    assert "quotaExceeded" in result["publish_error"]
    cursor = await db.execute(
        "SELECT status, publish_failed_at, publish_error FROM videos WHERE id = 'vidF'"
    )
    row = dict(await cursor.fetchone())
    assert row["publish_failed_at"] is not None
    assert "quotaExceeded" in row["publish_error"]
    assert row["status"] == "scheduled", (
        "still scheduled — restart recovery must keep re-attempting it"
    )

    events = await db.execute_fetchall(
        "SELECT type FROM video_events WHERE video_id = 'vidF'"
    )
    assert "publish_failed" in [e["type"] for e in events], (
        "a non-auth failure gets an event too — a quota refusal missed its "
        "date as thoroughly as a revoked token"
    )


async def test_successful_publish_clears_an_earlier_failure(
    app_db, monkeypatch
) -> None:
    """publish_failed_at is the banner's source of truth, so leaving it set
    after a successful retry would keep a resolved failure on every page."""
    scheduler, db = app_db
    await _seed_video(db, "vidF", "unlisted", status="scheduled")
    await db.execute(
        "UPDATE videos SET publish_failed_at = datetime('now'), "
        "publish_error = 'earlier failure' WHERE id = 'vidF'"
    )
    await db.commit()

    youtube = importlib.import_module("yt_scheduler.services.youtube")
    monkeypatch.setattr(youtube, "update_video_metadata", lambda *a, **kw: {})

    result = await scheduler.publish_video_job("vidF")

    assert result["published"] is True
    cursor = await db.execute(
        "SELECT publish_failed_at, publish_error FROM videos WHERE id = 'vidF'"
    )
    row = dict(await cursor.fetchone())
    assert row["publish_failed_at"] is None
    assert row["publish_error"] is None


async def test_cancelling_the_schedule_clears_the_failure(app_db) -> None:
    """Cancel is the banner's give-up action. The failure clears WITH the
    schedule, or the banner would keep a row with nothing left to act on."""
    scheduler, db = app_db
    await _seed_video(db, "vidF", "unlisted", status="scheduled")
    await db.execute(
        "UPDATE videos SET publish_at = '2026-01-01T00:00:00+00:00', "
        "publish_failed_at = datetime('now'), publish_error = 'boom' "
        "WHERE id = 'vidF'"
    )
    await db.commit()

    assert await scheduler.cancel_scheduled_publish("vidF") is True

    cursor = await db.execute(
        "SELECT publish_failed_at, publish_error, status FROM videos WHERE id = 'vidF'"
    )
    row = dict(await cursor.fetchone())
    assert row["publish_failed_at"] is None
    assert row["publish_error"] is None
    assert row["status"] == "ready"


async def test_non_youtube_publish_clears_an_earlier_failure(
    app_db, monkeypatch
) -> None:
    """The else branch is a success writer too.

    A hook with no YouTube video that publishes must clear the stamp, or the
    banner shows a failed row for a published video and its own Retry — which
    re-takes the same branch — can never clear it.
    """
    scheduler, db = app_db
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status, item_type, url) "
        "VALUES ('vidF', 1, 'Hook', 'scheduled', 'hook', '')"
    )
    await db.execute(
        "UPDATE videos SET publish_failed_at = datetime('now'), "
        "publish_error = 'earlier failure' WHERE id = 'vidF'"
    )
    await db.commit()

    youtube = importlib.import_module("yt_scheduler.services.youtube")

    def _must_not_call(*a, **kw):
        raise AssertionError("non-YouTube item must not touch YouTube")

    monkeypatch.setattr(youtube, "update_video_metadata", _must_not_call)

    result = await scheduler.publish_video_job("vidF")

    assert result["published"] is True
    assert result.get("youtube_skipped") is True
    cursor = await db.execute(
        "SELECT status, publish_failed_at, publish_error FROM videos "
        "WHERE id = 'vidF'"
    )
    row = dict(await cursor.fetchone())
    assert row["status"] == "published"
    assert row["publish_failed_at"] is None
    assert row["publish_error"] is None
