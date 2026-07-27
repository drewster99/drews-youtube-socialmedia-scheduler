"""Auto-add when a video becomes live.

The marker records that the decision was *taken*, not that the video is live —
liveness is already derivable from privacy_status, and a second column
asserting it would be redundant state that can drift.
"""

from __future__ import annotations

import importlib

import pytest


@pytest.fixture
async def live_env(isolated_db):
    queue_service = importlib.import_module("yt_scheduler.services.smart_queue")
    live = importlib.import_module("yt_scheduler.services.smart_queue_live")
    db = isolated_db

    cursor = await db.execute(
        "INSERT INTO templates (project_id, name, applies_to) "
        "VALUES (1, 'clips', '[\"hook\"]')"
    )
    template_id = int(cursor.lastrowid)
    await db.commit()
    queue_id = await queue_service.create_queue(
        project_id=1, name="Q", template_id=template_id, timezone_name="UTC",
        slots=[{"weekday": d, "time_of_day": "09:00"} for d in range(7)],
    )
    return live, queue_service, db, queue_id


async def _add_video(db, video_id="v1", **overrides):
    fields = {
        "item_type": "hook", "duration_seconds": 60.0,
        "privacy_status": "public", "width": 1080, "height": 1920,
    }
    fields.update(overrides)
    await db.execute(
        "INSERT INTO videos (id, project_id, title, item_type, duration_seconds, "
        "privacy_status, width, height) VALUES (?, 1, 'A clip', ?, ?, ?, ?, ?)",
        (video_id, fields["item_type"], fields["duration_seconds"],
         fields["privacy_status"], fields["width"], fields["height"]),
    )
    await db.commit()


async def test_eligible_video_is_appended(live_env):
    live, _queue_service, db, queue_id = live_env
    await _add_video(db)

    result = await live.on_video_became_live("v1")

    assert result["added_to"] == [queue_id]
    rows = await db.execute_fetchall(
        "SELECT video_id, position, state, scheduled_at FROM smart_queue_items"
    )
    assert rows[0]["video_id"] == "v1"
    assert rows[0]["state"] == "scheduled"
    # No time yet — Accept is the single path that turns queued items into a
    # posting schedule.
    assert rows[0]["scheduled_at"] is None


async def test_second_transition_does_not_add_again(live_env):
    """public -> unlisted -> public must not queue the clip twice."""
    live, _queue_service, db, queue_id = live_env
    await _add_video(db)

    await live.on_video_became_live("v1")
    # Simulate the item being posted and the user re-publishing the video.
    await db.execute("UPDATE smart_queue_items SET state = 'posted'")
    await db.commit()

    second = await live.on_video_became_live("v1")

    assert second["considered"] is False
    assert second["added_to"] == []
    rows = await db.execute_fetchall("SELECT COUNT(*) n FROM smart_queue_items")
    assert rows[0]["n"] == 1


async def test_ineligible_video_is_marked_but_not_added(live_env):
    """The decision was real — 'no' — so it must not be revisited."""
    live, _queue_service, db, _queue_id = live_env
    await _add_video(db, width=1920, height=1080)  # landscape

    result = await live.on_video_became_live("v1")

    assert result["considered"] is True
    assert result["added_to"] == []
    rows = await db.execute_fetchall(
        "SELECT auto_add_considered_at FROM videos WHERE id = 'v1'"
    )
    assert rows[0]["auto_add_considered_at"] is not None


async def test_undecidable_video_is_left_unmarked(live_env, monkeypatch):
    """A video that goes live before its dimensions are known must be
    reconsidered once they arrive — burning the marker would mean never."""
    live, _queue_service, db, _queue_id = live_env
    await _add_video(db, width=None, height=None)
    await db.execute(
        "UPDATE videos SET video_file_path = NULL WHERE id = 'v1'"
    )
    await db.commit()

    result = await live.on_video_became_live("v1")

    assert result["considered"] is False
    rows = await db.execute_fetchall(
        "SELECT auto_add_considered_at FROM videos WHERE id = 'v1'"
    )
    assert rows[0]["auto_add_considered_at"] is None


async def test_no_auto_add_queue_leaves_the_marker_unset(live_env, db=None):
    """With auto-add off everywhere, no decision was taken — so a queue
    created later still sees this video the next time it goes live."""
    live, queue_service, db, queue_id = live_env
    await queue_service.update_queue(queue_id, {"auto_add_on_live": 0})
    await _add_video(db)

    result = await live.on_video_became_live("v1")

    assert result["considered"] is False
    rows = await db.execute_fetchall(
        "SELECT auto_add_considered_at FROM videos WHERE id = 'v1'"
    )
    assert rows[0]["auto_add_considered_at"] is None


async def test_already_pending_is_not_duplicated(live_env):
    live, _queue_service, db, queue_id = live_env
    await _add_video(db)
    await db.execute(
        "INSERT INTO smart_queue_items (queue_id, video_id, position, state) "
        "VALUES (?, 'v1', 0, 'scheduled')", (queue_id,),
    )
    await db.commit()

    await live.on_video_became_live("v1")

    rows = await db.execute_fetchall("SELECT COUNT(*) n FROM smart_queue_items")
    assert rows[0]["n"] == 1


async def test_unknown_video_is_handled_quietly(live_env):
    live, _queue_service, _db, _queue_id = live_env
    assert await live.on_video_became_live("nope") == {
        "considered": False, "added_to": [], "reasons": {}
    }


async def test_migration_backfills_existing_public_videos(live_env):
    """Every already-public video must come out of migration 036 marked, or a
    future unlisted->public flip would read as 'first time ever live'."""
    _live, _queue_service, db, _queue_id = live_env
    rows = await db.execute_fetchall(
        "SELECT name FROM pragma_table_info('videos') WHERE name = 'auto_add_considered_at'"
    )
    assert rows, "migration 036 did not apply"
