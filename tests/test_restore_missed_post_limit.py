"""Regression tests: restore_scheduled_posts refuses a large missed backlog.

A Mac that slept for a week comes back with every scheduled post overdue.
The old restore pass fired all of them 5 seconds apart, which reads as spam
on the receiving account and can't be undone. Past the limit, none are sent
and each is marked failed so the user can triage them by hand.
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
    await database.close_db()


async def _seed_posts(db, count: int, *, offset: timedelta) -> list[int]:
    """Insert ``count`` approved posts all scheduled ``offset`` from now."""
    when = (datetime.now(timezone.utc) + offset).isoformat()
    await db.execute(
        "INSERT OR IGNORE INTO videos (id, project_id, title, status) "
        "VALUES ('vidM', 1, 'Missed', 'published')"
    )
    ids = []
    for i in range(count):
        cursor = await db.execute(
            "INSERT INTO social_posts "
            "(video_id, platform, content, status, scheduled_at, scheduler_job_id) "
            "VALUES ('vidM', 'mastodon', ?, 'approved', ?, ?)",
            (f"post {i}", when, f"job_{i}"),
        )
        ids.append(cursor.lastrowid)
    await db.commit()
    return ids


@pytest.fixture
def captured_jobs(monkeypatch: pytest.MonkeyPatch):
    """Record add_job calls instead of registering them on the real scheduler."""
    calls: list[dict] = []

    def fake_add_job(*args, **kwargs):
        calls.append(kwargs)
        return None

    return calls, fake_add_job


async def test_backlog_over_limit_sends_nothing_and_marks_failed(
    app_db, captured_jobs, monkeypatch
):
    scheduler, db = app_db
    calls, fake_add_job = captured_jobs
    monkeypatch.setattr(scheduler.scheduler, "add_job", fake_add_job)

    over = scheduler._MAX_MISSED_POST_AUTO_RECOVERY + 1
    ids = await _seed_posts(db, over, offset=timedelta(hours=-6))

    await scheduler.restore_scheduled_posts()

    assert calls == [], "no recovery job may be registered past the limit"

    rows = await db.execute_fetchall(
        "SELECT status, error, scheduled_at, scheduler_job_id FROM social_posts"
    )
    assert len(rows) == over
    for row in rows:
        assert row["status"] == "failed"
        assert str(over) in (row["error"] or "")
        # Cleared so the NEXT restart's restore pass can't resurrect them.
        assert row["scheduled_at"] is None
        assert row["scheduler_job_id"] is None

    events = await db.execute_fetchall(
        "SELECT type FROM video_events WHERE type = 'social_post_recovery_refused'"
    )
    assert len(events) == over, "each refused post gets a visible log entry"
    assert len(ids) == over


async def test_backlog_at_limit_still_recovers(app_db, captured_jobs, monkeypatch):
    """The limit is inclusive — exactly N overdue posts still send."""
    scheduler, db = app_db
    calls, fake_add_job = captured_jobs
    monkeypatch.setattr(scheduler.scheduler, "add_job", fake_add_job)

    at_limit = scheduler._MAX_MISSED_POST_AUTO_RECOVERY
    await _seed_posts(db, at_limit, offset=timedelta(hours=-6))

    await scheduler.restore_scheduled_posts()

    assert len(calls) == at_limit
    rows = await db.execute_fetchall("SELECT status FROM social_posts")
    assert [r["status"] for r in rows] == ["approved"] * at_limit


async def test_future_posts_survive_a_refused_backlog(
    app_db, captured_jobs, monkeypatch
):
    """A future post is re-registered even when the overdue backlog is refused."""
    scheduler, db = app_db
    calls, fake_add_job = captured_jobs
    monkeypatch.setattr(scheduler.scheduler, "add_job", fake_add_job)

    over = scheduler._MAX_MISSED_POST_AUTO_RECOVERY + 1
    await _seed_posts(db, over, offset=timedelta(hours=-6))
    cursor = await db.execute(
        "INSERT INTO social_posts "
        "(video_id, platform, content, status, scheduled_at) "
        "VALUES ('vidM', 'bluesky', 'later', 'approved', ?)",
        ((datetime.now(timezone.utc) + timedelta(days=2)).isoformat(),),
    )
    future_id = cursor.lastrowid
    await db.commit()

    await scheduler.restore_scheduled_posts()

    rows = await db.execute_fetchall(
        "SELECT status, scheduled_at FROM social_posts WHERE id = ?", (future_id,)
    )
    assert rows[0]["status"] == "approved"
    assert rows[0]["scheduled_at"] is not None
    assert len(calls) == 1, "only the future post is registered"
