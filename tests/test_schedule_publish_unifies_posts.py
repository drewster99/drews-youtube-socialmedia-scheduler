"""schedule_publish must auto-attach approved posts at the chosen time
so the Log card and per-post UI show one entry per post, and so users
can later re-time individual posts independently.

Each test seeds a fresh in-memory DB with a video and several posts in
mixed states, then asserts the post-attachment behavior across the
schedule / re-schedule / cancel lifecycle. APScheduler is started in
the fixture so the real ``add_job``/``remove_job`` paths run end-to-end
— if the per-post lock or job-id wiring regresses, these tests catch it
before the user sees a missed Log entry.
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
    scheduler_mod = importlib.import_module("yt_scheduler.services.scheduler")

    db = await database.get_db()
    await projects.ensure_default_project()
    scheduler_mod.scheduler.start()
    try:
        yield scheduler_mod, db
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


async def test_schedule_publish_attaches_all_approved_posts(app_db) -> None:
    scheduler_mod, db = app_db
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
    # Approved posts → attached at the video time, status stays 'approved'
    assert by_id[pids[0]]["scheduled_at"] == when.isoformat()
    assert by_id[pids[0]]["scheduler_job_id"] == f"social_post_{pids[0]}"
    assert by_id[pids[0]]["status"] == "approved"
    assert by_id[pids[1]]["scheduled_at"] == when.isoformat()
    # Draft post → untouched
    assert by_id[pids[2]]["scheduled_at"] is None
    assert by_id[pids[2]]["scheduler_job_id"] is None
    assert by_id[pids[2]]["status"] == "draft"


async def test_reschedule_moves_auto_attached_posts(app_db) -> None:
    scheduler_mod, db = app_db
    video_id, pids = await _seed(db, [("twitter", "approved"), ("bluesky", "approved")])
    t1 = datetime.now(timezone.utc) + timedelta(hours=1)
    t2 = t1 + timedelta(hours=3)

    await scheduler_mod.schedule_publish(video_id, t1)
    await scheduler_mod.schedule_publish(video_id, t2)

    rows = await db.execute_fetchall(
        "SELECT id, scheduled_at FROM social_posts WHERE video_id = ?", (video_id,)
    )
    for r in rows:
        assert r["scheduled_at"] == t2.isoformat(), (
            f"post {r['id']} should have moved from {t1} to {t2} when video re-scheduled"
        )


async def test_reschedule_preserves_hand_retimed_post(app_db) -> None:
    scheduler_mod, db = app_db
    video_id, pids = await _seed(db, [("twitter", "approved"), ("bluesky", "approved")])
    t_video1 = datetime.now(timezone.utc) + timedelta(hours=1)
    t_custom = t_video1 + timedelta(minutes=30)
    t_video2 = t_video1 + timedelta(hours=5)

    await scheduler_mod.schedule_publish(video_id, t_video1)
    # User re-times the second post via the per-post API
    await scheduler_mod.schedule_social_post(pids[1], t_custom)
    # Then re-schedules the whole video
    await scheduler_mod.schedule_publish(video_id, t_video2)

    rows = await db.execute_fetchall(
        "SELECT id, scheduled_at FROM social_posts WHERE video_id = ? ORDER BY id",
        (video_id,),
    )
    by_id = {r["id"]: r["scheduled_at"] for r in rows}
    assert by_id[pids[0]] == t_video2.isoformat()
    assert by_id[pids[1]] == t_custom.isoformat(), (
        "hand-retimed post must NOT be moved when video schedule changes"
    )


async def test_cancel_clears_auto_attached_posts_only(app_db) -> None:
    scheduler_mod, db = app_db
    video_id, pids = await _seed(db, [("twitter", "approved"), ("bluesky", "approved")])
    t_video = datetime.now(timezone.utc) + timedelta(hours=1)
    t_custom = t_video + timedelta(minutes=30)

    await scheduler_mod.schedule_publish(video_id, t_video)
    await scheduler_mod.schedule_social_post(pids[1], t_custom)

    await scheduler_mod.cancel_scheduled_publish(video_id)

    rows = await db.execute_fetchall(
        "SELECT id, scheduled_at, scheduler_job_id FROM social_posts WHERE video_id = ? ORDER BY id",
        (video_id,),
    )
    by_id = {r["id"]: dict(r) for r in rows}
    # Auto-attached post is detached
    assert by_id[pids[0]]["scheduled_at"] is None
    assert by_id[pids[0]]["scheduler_job_id"] is None
    # Hand-retimed post survives the video cancel
    assert by_id[pids[1]]["scheduled_at"] == t_custom.isoformat()
    assert by_id[pids[1]]["scheduler_job_id"] is not None

    # Video state cleared too
    cursor = await db.execute("SELECT publish_at, status FROM videos WHERE id = ?", (video_id,))
    vrow = await cursor.fetchone()
    assert vrow["publish_at"] is None
    assert vrow["status"] == "ready"


async def test_schedule_records_one_event_per_post(app_db) -> None:
    """The user-visible promise: every auto-attached post produces a
    Log row. The video Log card reads the events table directly, so this
    test asserts the per-post events exist after scheduling.
    """
    scheduler_mod, db = app_db
    video_id, pids = await _seed(db, [("twitter", "approved"), ("bluesky", "approved")])
    when = datetime.now(timezone.utc) + timedelta(hours=1)

    await scheduler_mod.schedule_publish(video_id, when)

    rows = await db.execute_fetchall(
        "SELECT type FROM video_events WHERE video_id = ? AND type = 'social_post_scheduled'",
        (video_id,),
    )
    assert len(rows) == 2, "must record one social_post_scheduled per attached post"
