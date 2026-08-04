"""Missed postings and their disposition.

"Missed" is derived from ``scheduled_at`` being in the past, computed at read
time — there is no stored flag and no background sweeper, so the state cannot
go stale. Recovery is entirely user-initiated by design.
"""

from __future__ import annotations

import importlib
from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture
async def disposition_env(isolated_db, monkeypatch):
    queue_service = importlib.import_module("yt_scheduler.services.smart_queue")
    disposition = importlib.import_module(
        "yt_scheduler.services.smart_queue_disposition"
    )
    scheduler = importlib.import_module("yt_scheduler.services.scheduler")
    db = isolated_db

    sent: list[int] = []
    rescheduled: list[tuple[int, datetime]] = []
    cancelled: list[int] = []

    async def fake_send(post_id):
        sent.append(int(post_id))

    async def fake_schedule(post_id, when):
        rescheduled.append((int(post_id), when))
        return f"job_{post_id}"

    async def fake_cancel(post_id):
        cancelled.append(int(post_id))
        return True

    monkeypatch.setattr(scheduler, "_send_scheduled_post", fake_send)
    monkeypatch.setattr(scheduler, "schedule_social_post", fake_schedule)
    monkeypatch.setattr(scheduler, "cancel_scheduled_post", fake_cancel)

    cursor = await db.execute(
        "INSERT INTO templates (project_id, name, applies_to) "
        "VALUES (1, 't', '[\"hook\"]')"
    )
    template_id = int(cursor.lastrowid)
    await db.execute(
        "INSERT INTO videos (id, youtube_video_id, project_id, title, item_type, "
        "duration_seconds, privacy_status, width, height) "
        "VALUES ('vid00000001', 'vid00000001', 1, 'A clip', 'hook', 60, "
        "'public', 1080, 1920)"
    )
    await db.commit()
    queue_id = await queue_service.create_queue(
        project_id=1, name="Q", template_id=template_id, timezone_name="UTC",
        slots=[{"weekday": d, "time_of_day": "09:00"} for d in range(7)],
        missed_policy="post_late", missed_grace_hours=24,
    )
    return disposition, queue_service, db, queue_id, sent, rescheduled, cancelled


async def _make_missed(db, queue_id, *, hours_ago=5.0, status="approved"):
    when = (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).isoformat()
    cursor = await db.execute(
        "INSERT INTO smart_queue_items (queue_id, video_id, position, "
        "scheduled_at, state) VALUES (?, 'vid00000001', 0, ?, 'scheduled')",
        (queue_id, when),
    )
    item_id = int(cursor.lastrowid)
    cursor = await db.execute(
        "INSERT INTO social_posts (video_id, platform, content, status, "
        "scheduled_at, smart_queue_item_id) "
        "VALUES ('vid00000001', 'bluesky', 'hi', ?, ?, ?)",
        (status, when, item_id),
    )
    post_id = int(cursor.lastrowid)
    await db.commit()
    return item_id, post_id


async def test_past_due_post_is_missed(disposition_env):
    disposition, _qs, db, queue_id, *_ = disposition_env
    await _make_missed(db, queue_id)

    missed = await disposition.missed_items(queue_id)

    assert len(missed) == 1
    assert missed[0]["platform"] == "bluesky"
    assert "time passed" in missed[0]["missed_reason"]


async def test_missed_row_reports_whether_auto_add_has_considered_the_video(
    disposition_env,
):
    """Remove is the one disposition with a consequence past this posting, and
    which consequence depends on this fact: a video auto-add has already
    considered is never picked up again on its own, while one it has not could
    still be added by a later publish. The confirmation must state the row's
    actual case rather than guess, so the row has to carry it."""
    disposition, _qs, db, queue_id, *rest = disposition_env
    await _make_missed(db, queue_id)

    missed = await disposition.missed_items(queue_id)
    assert "auto_add_considered_at" in missed[0]
    assert missed[0]["auto_add_considered_at"] is None

    await db.execute(
        "UPDATE videos SET auto_add_considered_at = '2026-07-27 22:15:21'"
    )
    await db.commit()

    missed = await disposition.missed_items(queue_id)
    assert missed[0]["auto_add_considered_at"] == "2026-07-27 22:15:21"


async def test_future_post_is_not_missed(disposition_env):
    disposition, _qs, db, queue_id, *_ = disposition_env
    await _make_missed(db, queue_id, hours_ago=-48)
    assert await disposition.missed_items(queue_id) == []


async def test_already_posted_is_not_missed(disposition_env):
    disposition, _qs, db, queue_id, *_ = disposition_env
    await _make_missed(db, queue_id, status="posted")
    assert await disposition.missed_items(queue_id) == []


async def test_skipped_is_not_missed(disposition_env):
    """Skipped was a decision, not a failure — it needs no disposition."""
    disposition, _qs, db, queue_id, *_ = disposition_env
    await _make_missed(db, queue_id, status="skipped")
    assert await disposition.missed_items(queue_id) == []


async def test_failed_post_needs_a_decision_even_if_recent(disposition_env):
    disposition, _qs, db, queue_id, *_ = disposition_env
    await _make_missed(db, queue_id, hours_ago=-48, status="failed")
    missed = await disposition.missed_items(queue_id)
    assert len(missed) == 1


async def test_grace_window_boundaries(disposition_env):
    disposition, _qs, db, queue_id, *_ = disposition_env
    queue = await _qs_get(disposition, queue_id)
    recent = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
    stale = (datetime.now(timezone.utc) - timedelta(hours=100)).isoformat()

    assert disposition.within_grace(queue, recent) is True
    assert disposition.within_grace(queue, stale) is False

    queue["missed_policy"] = "remove"
    assert disposition.within_grace(queue, recent) is False


async def _qs_get(disposition, queue_id):
    from yt_scheduler.services import smart_queue as queue_service
    return await queue_service.get_queue(queue_id)


async def test_post_now_sends_through_the_ordinary_path(disposition_env):
    """Not a queue-only sender: the same claim, duplicate check, liveness
    check, and media preparation must apply."""
    disposition, _qs, db, queue_id, sent, _resched, _cancelled = disposition_env
    _item_id, post_id = await _make_missed(db, queue_id, status="failed")

    result = await disposition.dispose(queue_id, post_id, "post_now")

    assert sent == [post_id]
    assert result["action"] == "post_now"
    # A failed post is returned to 'approved' first, or the send path's claim
    # (which only takes 'approved') would refuse it.
    rows = await db.execute_fetchall(
        "SELECT status FROM social_posts WHERE id = ?", (post_id,)
    )
    assert rows[0]["status"] == "approved"


async def test_reschedule_moves_to_the_end_with_a_future_time(disposition_env):
    disposition, _qs, db, queue_id, _sent, rescheduled, _cancelled = disposition_env
    item_id, post_id = await _make_missed(db, queue_id)

    result = await disposition.dispose(queue_id, post_id, "reschedule_end")

    when = datetime.fromisoformat(result["scheduled_at"])
    assert when > datetime.now(timezone.utc)
    assert rescheduled and rescheduled[0][0] == post_id
    rows = await db.execute_fetchall(
        "SELECT state, scheduled_at FROM smart_queue_items WHERE id = ?", (item_id,)
    )
    assert rows[0]["state"] == "scheduled"
    assert rows[0]["scheduled_at"] == result["scheduled_at"]


async def test_remove_makes_the_video_eligible_again(disposition_env):
    """'removed' is neither scheduled nor posted, so the standing filters no
    longer exclude the video from a future Auto-select."""
    disposition, queue_service, db, queue_id, _sent, _resched, cancelled = disposition_env
    item_id, post_id = await _make_missed(db, queue_id)

    await disposition.dispose(queue_id, post_id, "remove")

    assert cancelled == [post_id]
    rows = await db.execute_fetchall(
        "SELECT state FROM smart_queue_items WHERE id = ?", (item_id,)
    )
    assert rows[0]["state"] == "removed"

    queue = await queue_service.get_queue(queue_id)
    candidates = await queue_service.candidate_videos(queue)
    assert [v["id"] for v in candidates["eligible"]] == ["vid00000001"]


async def test_unknown_action_is_rejected(disposition_env):
    disposition, _qs, db, queue_id, *_ = disposition_env
    _item_id, post_id = await _make_missed(db, queue_id)
    with pytest.raises(Exception, match="Unknown action"):
        await disposition.dispose(queue_id, post_id, "explode")


async def test_post_from_another_queue_is_rejected(disposition_env):
    disposition, _qs, db, queue_id, *_ = disposition_env
    _item_id, post_id = await _make_missed(db, queue_id)
    with pytest.raises(Exception, match="not part of"):
        await disposition.dispose(queue_id + 999, post_id, "post_now")
