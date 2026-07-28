"""Accept turns a selected batch into a scheduled posting plan.

Accepted videos become ordinary ``social_posts`` rows so that send, retry,
duplicate detection, history, and the missed-backlog guard all apply
unchanged — there is no parallel posting path to keep in step.
"""

from __future__ import annotations

import importlib
import json
from datetime import datetime, timezone

import pytest


@pytest.fixture
async def accept_env(isolated_db, monkeypatch):
    """A queue with one Bluesky slot, plus two eligible vertical clips."""
    queue_service = importlib.import_module("yt_scheduler.services.smart_queue")
    accept = importlib.import_module("yt_scheduler.services.smart_queue_accept")
    scheduler = importlib.import_module("yt_scheduler.services.scheduler")
    db = isolated_db

    # Don't register real APScheduler jobs in a test process.
    scheduled: list[tuple[int, datetime]] = []

    async def fake_schedule(post_id, when):
        scheduled.append((int(post_id), when))
        return f"job_{post_id}"

    monkeypatch.setattr(scheduler, "schedule_social_post", fake_schedule)

    cursor = await db.execute(
        "INSERT INTO templates (project_id, name, applies_to) "
        "VALUES (1, 'clips', '[\"hook\"]')"
    )
    template_id = int(cursor.lastrowid)
    await db.execute(
        "INSERT INTO template_slots (template_id, platform, body, media, max_chars) "
        "VALUES (?, 'bluesky', 'Watch: {{title}} {{video}}', 'none', 300)",
        (template_id,),
    )
    for video_id, title in (("v1", "First clip"), ("v2", "Second clip")):
        await db.execute(
            "INSERT INTO videos (id, project_id, title, item_type, duration_seconds, "
            "privacy_status, width, height, url, video_file_path) "
            "VALUES (?, 1, ?, 'hook', 60, 'public', 1080, 1920, 'https://y/x', ?)",
            (video_id, title, f"/tmp/{video_id}.mp4"),
        )
    await db.commit()

    queue_id = await queue_service.create_queue(
        project_id=1, name="Q", template_id=template_id, timezone_name="UTC",
        slots=[{"weekday": d, "time_of_day": "09:00"} for d in range(7)],
    )
    return accept, queue_service, db, queue_id, scheduled


async def test_accept_creates_items_and_approved_posts(accept_env):
    accept, _queue_service, db, queue_id, scheduled = accept_env

    result = await accept.accept_selection(queue_id, ["v1", "v2"])

    assert result["scheduled"] == 2
    items = await db.execute_fetchall(
        "SELECT video_id, position, scheduled_at, state FROM smart_queue_items "
        "ORDER BY position"
    )
    assert [r["video_id"] for r in items] == ["v1", "v2"]
    assert [r["state"] for r in items] == ["scheduled", "scheduled"]
    # Positions are dense and ordered, and times are strictly increasing.
    assert [r["position"] for r in items] == [0, 1]
    assert items[0]["scheduled_at"] < items[1]["scheduled_at"]

    posts = await db.execute_fetchall(
        "SELECT platform, status, content, smart_queue_item_id FROM social_posts"
    )
    assert len(posts) == 2
    assert {p["status"] for p in posts} == {"approved"}
    assert all(p["smart_queue_item_id"] is not None for p in posts)
    # Rendered at Accept, not at fire time.
    assert "First clip" in posts[0]["content"]
    assert len(scheduled) == 2


async def test_order_given_is_the_order_scheduled(accept_env):
    """Shuffle happens in the UI; Accept must not re-sort the batch."""
    accept, _queue_service, db, queue_id, _scheduled = accept_env

    await accept.accept_selection(queue_id, ["v2", "v1"])

    items = await db.execute_fetchall(
        "SELECT video_id FROM smart_queue_items ORDER BY position"
    )
    assert [r["video_id"] for r in items] == ["v2", "v1"]


async def test_video_directive_attaches_the_file(accept_env):
    accept, _queue_service, db, queue_id, _scheduled = accept_env

    await accept.accept_selection(queue_id, ["v1"])

    rows = await db.execute_fetchall("SELECT media_paths FROM social_posts")
    assert json.loads(rows[0]["media_paths"]) == ["/tmp/v1.mp4"]


async def test_second_accept_appends_after_the_existing_schedule(accept_env):
    """Accepting a second batch must not double-book the first batch's slots."""
    accept, _queue_service, db, queue_id, _scheduled = accept_env

    await accept.accept_selection(queue_id, ["v1"])
    first = (await db.execute_fetchall(
        "SELECT scheduled_at FROM smart_queue_items ORDER BY position"
    ))[0]["scheduled_at"]

    await accept.accept_selection(queue_id, ["v2"])
    times = [r["scheduled_at"] for r in await db.execute_fetchall(
        "SELECT scheduled_at FROM smart_queue_items ORDER BY position"
    )]
    assert times[0] == first
    assert times[1] > first


async def test_slot_over_the_platform_duration_cap_is_skipped_not_failed(
    accept_env, monkeypatch
):
    """Skipped means 'known in advance, not attempted'. No encode shortens a
    clip, so scheduling it would fail identically every time it came round."""
    accept, _queue_service, db, queue_id, _scheduled = accept_env
    await db.execute("UPDATE videos SET duration_seconds = 600 WHERE id = 'v1'")
    await db.commit()

    social = importlib.import_module("yt_scheduler.services.social")
    monkeypatch.setitem(
        social.PLATFORM_MEDIA_LIMITS, "bluesky",
        social.media_service.PlatformMediaLimits(max_duration_seconds=180),
    )

    result = await accept.accept_selection(queue_id, ["v1"])

    assert result["scheduled"] == 0
    assert len(result["skipped"]) == 1
    assert "over" in result["skipped"][0]["reason"]

    posts = await db.execute_fetchall("SELECT status, error FROM social_posts")
    assert posts[0]["status"] == "skipped"
    assert "180s limit" in posts[0]["error"]
    # The item itself is skipped too, so it doesn't burn a posting slot.
    items = await db.execute_fetchall("SELECT state FROM smart_queue_items")
    assert items[0]["state"] == "skipped"


async def test_hosted_media_platform_is_skipped_when_hosting_is_unconfigured(
    accept_env, monkeypatch
):
    """Threads can carry video now, but only through media hosting. Without it
    the slot must be skipped up front rather than failing at its posting time
    every day."""
    accept, _queue_service, db, queue_id, _scheduled = accept_env
    await db.execute(
        "UPDATE template_slots SET platform = 'threads' WHERE platform = 'bluesky'"
    )
    await db.commit()

    async def _unconfigured():
        return False

    monkeypatch.setattr(accept.media_hosting, "is_configured", _unconfigured)

    result = await accept.accept_selection(queue_id, ["v1"])

    assert result["scheduled"] == 0
    assert "media hosting isn't configured" in result["skipped"][0]["reason"]


async def test_hosted_media_platform_is_scheduled_once_hosting_is_configured(
    accept_env, monkeypatch
):
    """The mirror image: with hosting set up, a Threads slot is a normal slot."""
    accept, _queue_service, db, queue_id, _scheduled = accept_env
    await db.execute(
        "UPDATE template_slots SET platform = 'threads' WHERE platform = 'bluesky'"
    )
    await db.commit()

    async def _configured():
        return True

    monkeypatch.setattr(accept.media_hosting, "is_configured", _configured)

    result = await accept.accept_selection(queue_id, ["v1"])

    assert result["scheduled"] == 1
    assert result["skipped"] == []


async def test_disabled_slots_produce_no_row_at_all(accept_env):
    """A slot the user turned off isn't 'skipped' — it was never in play."""
    accept, _queue_service, db, queue_id, _scheduled = accept_env
    await db.execute("UPDATE template_slots SET is_disabled = 1")
    await db.commit()

    result = await accept.accept_selection(queue_id, ["v1"])

    assert result["skipped"] == []
    posts = await db.execute_fetchall("SELECT COUNT(*) n FROM social_posts")
    assert posts[0]["n"] == 0


async def test_one_broken_slot_does_not_abort_the_others(accept_env):
    """A render failure is per-slot. The working platforms still go out, and
    the broken one is visible with its real reason rather than missing."""
    accept, _queue_service, db, queue_id, _scheduled = accept_env
    cursor = await db.execute("SELECT template_id FROM template_slots LIMIT 1")
    template_id = (await cursor.fetchone())["template_id"]
    # {{nonexistent}} is undefined, which the strict renderer rejects.
    await db.execute(
        "INSERT INTO template_slots (template_id, platform, body, media, max_chars) "
        "VALUES (?, 'mastodon', 'Broken {{nonexistent}}', 'none', 500)",
        (template_id,),
    )
    await db.commit()

    result = await accept.accept_selection(queue_id, ["v1"])

    assert result["scheduled"] == 1, "the healthy slot still scheduled"
    assert [s["platform"] for s in result["skipped"]] == ["mastodon"]
    assert "nonexistent" in result["skipped"][0]["reason"]

    rows = await db.execute_fetchall(
        "SELECT platform, status FROM social_posts ORDER BY platform"
    )
    assert [(r["platform"], r["status"]) for r in rows] == [
        ("bluesky", "approved"), ("mastodon", "skipped"),
    ]


async def test_accepting_nothing_is_a_no_op(accept_env):
    accept, _queue_service, db, queue_id, _scheduled = accept_env
    assert await accept.accept_selection(queue_id, []) == {
        "scheduled": 0, "items": [], "skipped": [], "errors": []
    }
    rows = await db.execute_fetchall("SELECT COUNT(*) n FROM smart_queue_items")
    assert rows[0]["n"] == 0


async def test_rerender_updates_pending_but_not_posted(accept_env):
    accept, _queue_service, db, queue_id, _scheduled = accept_env
    await accept.accept_selection(queue_id, ["v1", "v2"])

    posts = await db.execute_fetchall("SELECT id FROM social_posts ORDER BY id")
    await db.execute(
        "UPDATE social_posts SET status = 'posted', content = 'already out' "
        "WHERE id = ?", (posts[0]["id"],),
    )
    await db.execute("UPDATE videos SET title = 'Renamed' WHERE id = 'v2'")
    await db.commit()

    result = await accept.rerender_pending(queue_id)

    assert result["updated"] == 1
    rows = await db.execute_fetchall(
        "SELECT id, content FROM social_posts ORDER BY id"
    )
    assert rows[0]["content"] == "already out", "a sent post is history"
    assert "Renamed" in rows[1]["content"]


async def test_dst_boundary_holds_wall_clock_time(isolated_db, monkeypatch):
    """Scheduling across the November change must keep 9am at 9am local."""
    queue_service = importlib.import_module("yt_scheduler.services.smart_queue")
    zone = queue_service.resolve_timezone("America/Los_Angeles")
    instants = queue_service.occurrences(
        [{"weekday": d, "time_of_day": "09:00"} for d in range(7)],
        zone, 10, after=datetime(2026, 10, 29, 0, 0, tzinfo=timezone.utc),
    )
    local_hours = {dt.astimezone(zone).hour for dt in instants}
    assert local_hours == {9}
    # The UTC hour must differ across the boundary, or the zone was ignored.
    assert len({dt.hour for dt in instants}) == 2


class TestQueuedItemsArePromoted:
    """Auto-add appends a `queued` item with no time; Accept is what gives it
    one. Before this existed, auto-add wrote `scheduled` — which candidates
    excluded and Accept only ever INSERTed past — so an auto-added video sat in
    the queue forever and never posted.
    """

    async def test_accept_promotes_a_waiting_item_in_place(self, accept_env):
        accept, queue_service, db, queue_id, _scheduled = accept_env
        await db.execute(
            "INSERT INTO smart_queue_items (queue_id, video_id, position, state) "
            "VALUES (?, 'v1', 0, ?)",
            (queue_id, queue_service.ITEM_STATE_QUEUED),
        )
        await db.commit()

        # No explicit ids: the waiting item alone must be picked up.
        result = await accept.accept_selection(queue_id, [])

        assert result["scheduled"] == 1
        rows = await db.execute_fetchall(
            "SELECT id, state, scheduled_at FROM smart_queue_items"
        )
        assert len(rows) == 1, "must promote the existing row, not add a second"
        assert rows[0]["state"] == "scheduled"
        assert rows[0]["scheduled_at"] is not None

    async def test_waiting_items_go_before_a_new_selection(self, accept_env):
        """They have been in the queue longest, so they post first."""
        accept, queue_service, db, queue_id, _scheduled = accept_env
        await db.execute(
            "INSERT INTO smart_queue_items (queue_id, video_id, position, state) "
            "VALUES (?, 'v2', 0, ?)",
            (queue_id, queue_service.ITEM_STATE_QUEUED),
        )
        await db.commit()

        await accept.accept_selection(queue_id, ["v1"])

        rows = await db.execute_fetchall(
            "SELECT video_id FROM smart_queue_items ORDER BY scheduled_at"
        )
        assert [r["video_id"] for r in rows] == ["v2", "v1"]

    async def test_a_waiting_video_named_again_is_not_scheduled_twice(
        self, accept_env
    ):
        accept, queue_service, db, queue_id, _scheduled = accept_env
        await db.execute(
            "INSERT INTO smart_queue_items (queue_id, video_id, position, state) "
            "VALUES (?, 'v1', 0, ?)",
            (queue_id, queue_service.ITEM_STATE_QUEUED),
        )
        await db.commit()

        result = await accept.accept_selection(queue_id, ["v1"])

        assert result["scheduled"] == 1
        rows = await db.execute_fetchall("SELECT COUNT(*) n FROM smart_queue_items")
        assert rows[0]["n"] == 1


class TestAcceptCannotDoubleBook:
    async def test_re_accepting_the_same_batch_schedules_once(self, accept_env):
        """A failed Accept leaves the selection on screen untouched and
        re-enables the button, so the obvious retry submits the same ids."""
        accept, _queue_service, db, queue_id, _scheduled = accept_env

        await accept.accept_selection(queue_id, ["v1", "v2"])
        second = await accept.accept_selection(queue_id, ["v1", "v2"])

        assert second["scheduled"] == 0
        assert {s["reason"] for s in second["skipped"]} == {
            "already scheduled by this queue"
        }
        rows = await db.execute_fetchall("SELECT COUNT(*) n FROM smart_queue_items")
        assert rows[0]["n"] == 2

    async def test_duplicate_ids_in_one_request_are_scheduled_once(self, accept_env):
        accept, _queue_service, db, queue_id, _scheduled = accept_env

        await accept.accept_selection(queue_id, ["v1", "v1", "v1"])

        rows = await db.execute_fetchall("SELECT COUNT(*) n FROM smart_queue_items")
        assert rows[0]["n"] == 1


class TestAcceptWritesWholeVideosOnly:
    async def test_every_approved_post_carries_its_scheduled_at(self, accept_env):
        """A post with a NULL scheduled_at is invisible to restore_scheduled_posts
        AND to missed_items, so a crash before the timer was registered used to
        strand it permanently while its item still read as scheduled."""
        accept, _queue_service, db, queue_id, _scheduled = accept_env

        await accept.accept_selection(queue_id, ["v1", "v2"])

        rows = await db.execute_fetchall(
            "SELECT COUNT(*) n FROM social_posts "
            "WHERE status = 'approved' AND scheduled_at IS NULL"
        )
        assert rows[0]["n"] == 0

    async def test_a_transient_failure_leaves_the_video_untouched(
        self, accept_env, monkeypatch
    ):
        """Not a template error — the API was down. Writing nothing keeps the
        video a candidate so the retry is clean instead of half-scheduled."""
        accept, queue_service, db, queue_id, _scheduled = accept_env
        real_render = accept._render_slot

        async def flaky(db_, video, slot, *, default_ai_system):
            if video["id"] == "v2":
                raise RuntimeError("Anthropic overloaded")
            return await real_render(db_, video, slot,
                                     default_ai_system=default_ai_system)

        monkeypatch.setattr(accept, "_render_slot", flaky)

        result = await accept.accept_selection(queue_id, ["v1", "v2"])

        assert result["scheduled"] == 1
        assert [e["video_id"] for e in result["errors"]] == ["v2"]
        assert "RuntimeError" in result["errors"][0]["error"]
        rows = await db.execute_fetchall(
            "SELECT COUNT(*) n FROM smart_queue_items WHERE video_id = 'v2'"
        )
        assert rows[0]["n"] == 0, "nothing may be written for an abandoned video"
        posts = await db.execute_fetchall(
            "SELECT COUNT(*) n FROM social_posts WHERE video_id = 'v2'"
        )
        assert posts[0]["n"] == 0

    async def test_a_missing_video_consumes_no_posting_time(self, accept_env):
        """An abandoned instant is a posting time at which nothing goes out,
        and nothing ever backfills it."""
        accept, _queue_service, db, queue_id, _scheduled = accept_env

        result = await accept.accept_selection(queue_id, ["ghost", "v1"])

        assert [s["video_id"] for s in result["skipped"]] == ["ghost"]
        rows = await db.execute_fetchall(
            "SELECT position, scheduled_at FROM smart_queue_items"
        )
        assert len(rows) == 1
        assert rows[0]["position"] == 0

    async def test_a_video_from_another_project_is_refused(self, accept_env):
        """It would post another channel's clip on this queue's accounts."""
        accept, _queue_service, db, queue_id, _scheduled = accept_env
        await db.execute(
            "INSERT INTO projects (id, slug, name) VALUES (2, 'other', 'Other')"
        )
        await db.execute(
            "INSERT INTO videos (id, project_id, title, item_type, "
            "duration_seconds, privacy_status, width, height) "
            "VALUES ('alien', 2, 'Not ours', 'hook', 60, 'public', 1080, 1920)"
        )
        await db.commit()

        result = await accept.accept_selection(queue_id, ["alien"])

        assert result["scheduled"] == 0
        assert result["skipped"][0]["reason"] == "belongs to a different project"
        rows = await db.execute_fetchall("SELECT COUNT(*) n FROM smart_queue_items")
        assert rows[0]["n"] == 0
