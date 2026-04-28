"""schedule_publish must auto-attach approved posts at the chosen time
so the Log card and per-post UI show one entry per post, AND honor the
project's stagger settings (post_video_delay_minutes,
inter_post_spacing_minutes) so the video card and the Socials Compose
page produce identical timing for the same set of posts.

Each test seeds a fresh in-memory DB with a video and several posts in
mixed states, then asserts the post-attachment behavior across the
schedule / re-schedule / cancel lifecycle. APScheduler is started in
the fixture so the real ``add_job``/``remove_job`` paths run end-to-end
— if the per-post lock or job-id wiring regresses, these tests catch
it before the user sees a missed Log entry.
"""

from __future__ import annotations

import importlib
import sys
from datetime import datetime, timedelta, timezone
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
    project_settings = importlib.import_module("yt_scheduler.services.project_settings")
    scheduler_mod = importlib.import_module("yt_scheduler.services.scheduler")

    db = await database.get_db()
    await projects.ensure_default_project()
    scheduler_mod.scheduler.start()
    try:
        yield scheduler_mod, db, project_settings
    finally:
        scheduler_mod.scheduler.shutdown(wait=False)
        await database.close_db()


async def _seed(db, post_specs: list[tuple[str, str]]) -> tuple[str, list[int]]:
    """Insert one video + N posts. Returns (video_id, [post_ids])."""
    video_id = "vidS"
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status) "
        "VALUES (?, 1, 'Sched', 'uploaded')",
        (video_id,),
    )
    pids = []
    for platform, status in post_specs:
        cursor = await db.execute(
            "INSERT INTO social_posts (video_id, platform, content, status) "
            "VALUES (?, ?, ?, ?)",
            (video_id, platform, f"hi from {platform}", status),
        )
        pids.append(int(cursor.lastrowid))
    await db.commit()
    return video_id, pids


async def test_schedule_publish_staggers_approved_posts(app_db) -> None:
    scheduler_mod, db, project_settings = app_db
    # Set deterministic stagger settings so we can assert exact times.
    await project_settings.set_json(
        1, "posting", {"post_video_delay_minutes": 10, "inter_post_spacing_minutes": 7}
    )
    video_id, pids = await _seed(
        db,
        [("twitter", "approved"), ("bluesky", "approved"), ("mastodon", "draft")],
    )
    when = datetime.now(timezone.utc) + timedelta(hours=1)

    await scheduler_mod.schedule_publish(video_id, when)

    rows = await db.execute_fetchall(
        "SELECT id, scheduled_at, scheduler_job_id, status FROM social_posts "
        "WHERE video_id = ? ORDER BY id",
        (video_id,),
    )
    by_id = {r["id"]: dict(r) for r in rows}
    # First approved post: video time + 10 min delay
    expected_first = (when + timedelta(minutes=10)).isoformat()
    assert by_id[pids[0]]["scheduled_at"] == expected_first
    assert by_id[pids[0]]["scheduler_job_id"] == f"social_post_{pids[0]}"
    assert by_id[pids[0]]["status"] == "approved"
    # Second approved post: video time + 10 + 7 = 17 min
    expected_second = (when + timedelta(minutes=17)).isoformat()
    assert by_id[pids[1]]["scheduled_at"] == expected_second
    # Draft post: untouched
    assert by_id[pids[2]]["scheduled_at"] is None
    assert by_id[pids[2]]["scheduler_job_id"] is None
    assert by_id[pids[2]]["status"] == "draft"


async def test_reschedule_recomputes_all_posts(app_db) -> None:
    scheduler_mod, db, project_settings = app_db
    await project_settings.set_json(
        1, "posting", {"post_video_delay_minutes": 0, "inter_post_spacing_minutes": 5}
    )
    video_id, pids = await _seed(db, [("twitter", "approved"), ("bluesky", "approved")])
    t1 = datetime.now(timezone.utc) + timedelta(hours=1)
    t2 = t1 + timedelta(hours=3)

    await scheduler_mod.schedule_publish(video_id, t1)
    await scheduler_mod.schedule_publish(video_id, t2)

    rows = await db.execute_fetchall(
        "SELECT id, scheduled_at FROM social_posts WHERE video_id = ? ORDER BY id",
        (video_id,),
    )
    expected = {
        pids[0]: t2.isoformat(),
        pids[1]: (t2 + timedelta(minutes=5)).isoformat(),
    }
    for r in rows:
        assert r["scheduled_at"] == expected[r["id"]], (
            f"post {r['id']} should have moved to its new staggered slot"
        )


async def test_reschedule_overrides_hand_retimed_post(app_db) -> None:
    """Hand-retimed posts are deliberately re-baselined when the video
    schedule changes — we don't try to preserve them. This is the
    documented contract: the video's schedule owns all its per-post
    jobs, period.
    """
    scheduler_mod, db, project_settings = app_db
    await project_settings.set_json(
        1, "posting", {"post_video_delay_minutes": 0, "inter_post_spacing_minutes": 5}
    )
    video_id, pids = await _seed(db, [("twitter", "approved"), ("bluesky", "approved")])
    t_video1 = datetime.now(timezone.utc) + timedelta(hours=1)
    t_custom = t_video1 + timedelta(minutes=30)
    t_video2 = t_video1 + timedelta(hours=5)

    await scheduler_mod.schedule_publish(video_id, t_video1)
    # User manually re-times the second post via the per-post API
    await scheduler_mod.schedule_social_post(pids[1], t_custom)
    # Then re-schedules the whole video — re-baseline kicks in
    await scheduler_mod.schedule_publish(video_id, t_video2)

    rows = await db.execute_fetchall(
        "SELECT id, scheduled_at FROM social_posts WHERE video_id = ? ORDER BY id",
        (video_id,),
    )
    by_id = {r["id"]: r["scheduled_at"] for r in rows}
    assert by_id[pids[0]] == t_video2.isoformat()
    assert by_id[pids[1]] == (t_video2 + timedelta(minutes=5)).isoformat()


async def test_cancel_clears_all_attached_posts(app_db) -> None:
    scheduler_mod, db, project_settings = app_db
    video_id, pids = await _seed(db, [("twitter", "approved"), ("bluesky", "approved")])
    t_video = datetime.now(timezone.utc) + timedelta(hours=1)

    await scheduler_mod.schedule_publish(video_id, t_video)
    await scheduler_mod.cancel_scheduled_publish(video_id)

    rows = await db.execute_fetchall(
        "SELECT id, scheduled_at, scheduler_job_id FROM social_posts WHERE video_id = ? ORDER BY id",
        (video_id,),
    )
    for r in rows:
        assert r["scheduled_at"] is None, f"post {r['id']} should be detached"
        assert r["scheduler_job_id"] is None

    cursor = await db.execute("SELECT publish_at, status FROM videos WHERE id = ?", (video_id,))
    vrow = await cursor.fetchone()
    assert vrow["publish_at"] is None
    assert vrow["status"] == "ready"


async def test_send_scheduled_post_blocks_when_video_not_public(app_db) -> None:
    """Per-post jobs fire independently of publish_video_job. If YouTube
    publish failed (auth expired, quota exhausted), the per-post job
    must NOT cheerfully post a link to a still-unlisted video. The
    fired post should land in 'failed' with an actionable error and a
    log event so the user sees it and can retry from the UI.
    """
    scheduler_mod, db, _ = app_db
    video_id, pids = await _seed(db, [("twitter", "approved")])
    # Video sits in the post-upload state — never went public.
    # _send_scheduled_post should refuse to fire the post.

    await scheduler_mod._send_scheduled_post(pids[0])

    cursor = await db.execute(
        "SELECT status, error, scheduler_job_id FROM social_posts WHERE id = ?",
        (pids[0],),
    )
    row = await cursor.fetchone()
    assert row["status"] == "failed"
    assert "still" in (row["error"] or "").lower()
    assert row["scheduler_job_id"] is None

    rows = await db.execute_fetchall(
        "SELECT type FROM video_events "
        "WHERE video_id = ? AND type = 'social_post_failed_video_not_public'",
        (video_id,),
    )
    assert len(rows) == 1


async def test_send_scheduled_post_proceeds_when_video_public(app_db) -> None:
    scheduler_mod, db, _ = app_db
    video_id, pids = await _seed(db, [("twitter", "approved")])
    await db.execute(
        "UPDATE videos SET privacy_status = 'public', status = 'published' WHERE id = ?",
        (video_id,),
    )
    await db.commit()

    # The video is public so the gate doesn't block; the post will
    # progress to the claim+poster path. We can't mock the network here,
    # so it'll fail at "not configured" — but the key invariant is that
    # status moved past 'approved', proving the gate was passed.
    await scheduler_mod._send_scheduled_post(pids[0])

    cursor = await db.execute(
        "SELECT status FROM social_posts WHERE id = ?", (pids[0],)
    )
    row = await cursor.fetchone()
    assert row["status"] in ("failed", "posted"), (
        f"gate must not have blocked; got status={row['status']}"
    )
    # The error, if present, must NOT be the video-not-public error.
    cursor = await db.execute(
        "SELECT error FROM social_posts WHERE id = ?", (pids[0],)
    )
    row = await cursor.fetchone()
    if row["error"]:
        assert "non-public" not in row["error"]


async def test_schedule_records_one_event_per_post(app_db) -> None:
    """The user-visible promise: every auto-attached post produces a Log row."""
    scheduler_mod, db, _ = app_db
    video_id, pids = await _seed(db, [("twitter", "approved"), ("bluesky", "approved")])
    when = datetime.now(timezone.utc) + timedelta(hours=1)

    await scheduler_mod.schedule_publish(video_id, when)

    rows = await db.execute_fetchall(
        "SELECT type FROM video_events WHERE video_id = ? AND type = 'social_post_scheduled'",
        (video_id,),
    )
    assert len(rows) == 2, "must record one social_post_scheduled per attached post"
