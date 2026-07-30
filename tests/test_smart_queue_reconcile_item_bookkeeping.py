"""Reconcile must not leave a queue item with no postings still 'scheduled'.

An item's displayed state is derived from its ``social_posts`` rows
(``smart_queue.list_queues``: ``LEFT JOIN social_posts`` … ``ELSE i.state``), so
deleting the last one used to leave the item counted as ``scheduled`` forever
with nothing that could ever send — and because ``PENDING_ITEM_STATES`` covers
``scheduled``, its video was never offered as a candidate again either.

Both handlers that delete postings are covered: ``remove_slots`` (a slot left
the template) and ``drop_excluded_videos`` (the template stopped applying to
that item type).
"""

from __future__ import annotations

import importlib

import pytest


async def _noop_progress(done: int, total: int) -> None:
    return None


#: ``templates.applies_to`` is NOT NULL; this is the schema default.
_ALL_ITEM_TYPES = '["hook","short","segment","video"]'


async def _seed(
    db,
    *,
    applies_to: str = _ALL_ITEM_TYPES,
    item_type: str = "short",
) -> tuple[int, int]:
    """A queue whose template has two slots, one item, one video.

    Returns ``(queue_id, item_id)``. Posts are added per-test so each case can
    choose which slots the item actually has postings for.
    """
    await db.execute(
        "INSERT INTO templates (id, project_id, name, applies_to) VALUES (1,1,'t',?)",
        (applies_to,),
    )
    # Real slot rows: social_posts.slot_id is a live FK. Both slots stay in
    # place, which is what the handler actually sees in production — a DISABLED
    # slot reads as removal (the only option for built-in slots), and it is an
    # UPDATE, so the posts keep their slot_id. A hard-deleted slot NULLs
    # slot_id via ON DELETE SET NULL before the job ever runs.
    for slot_id in (7, 8):
        await db.execute(
            "INSERT INTO template_slots (id, template_id, platform, body) "
            "VALUES (?,1,'bluesky','b')",
            (slot_id,),
        )
    await db.execute(
        "INSERT INTO smart_queues (id, project_id, name, template_id, timezone) "
        "VALUES (1,1,'q',1,'UTC')"
    )
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status, item_type) "
        "VALUES ('vidQ',1,'V','uploaded',?)",
        (item_type,),
    )
    await db.execute(
        "INSERT INTO smart_queue_items (id, queue_id, video_id, position, state, "
        "scheduled_at) VALUES (1,1,'vidQ',0,'scheduled','2030-01-01T00:00:00+00:00')"
    )
    await db.commit()
    return 1, 1


async def _add_post(db, *, item_id: int, slot_id: int, status: str = "approved") -> int:
    cursor = await db.execute(
        "INSERT INTO social_posts (video_id, platform, content, status, slot_id, "
        "smart_queue_item_id) VALUES ('vidQ','bluesky','body',?,?,?)",
        (status, slot_id, item_id),
    )
    await db.commit()
    return int(cursor.lastrowid)


async def _post_exists(db, post_id: int) -> bool:
    rows = await db.execute_fetchall(
        "SELECT 1 FROM social_posts WHERE id = ?", (post_id,)
    )
    return bool(rows)


async def _item(db, item_id: int) -> dict:
    rows = await db.execute_fetchall(
        "SELECT state, reason FROM smart_queue_items WHERE id = ?", (item_id,)
    )
    return dict(rows[0])


@pytest.fixture
def handlers():
    """Resolved lazily — other tests purge sys.modules to re-freeze config."""
    return importlib.import_module(
        "yt_scheduler.services.smart_queue_reconcile_handlers"
    )


async def test_removing_the_last_slot_retires_the_item(isolated_db, handlers):
    queue_id, item_id = await _seed(isolated_db)
    await _add_post(isolated_db, item_id=item_id, slot_id=7)

    result = await handlers.remove_slots(queue_id, [7], _noop_progress)

    item = await _item(isolated_db, item_id)
    assert item["state"] == "removed"
    # The reason is what the queue screen shows; an unexplained 'removed' item
    # is its own small mystery.
    assert "slot" in (item["reason"] or "")
    assert "retired 1 item" in result


async def test_removing_one_of_two_slots_leaves_the_item_scheduled(
    isolated_db, handlers
):
    """The item still has a posting to send, so it is not empty and must stay."""
    queue_id, item_id = await _seed(isolated_db)
    await _add_post(isolated_db, item_id=item_id, slot_id=7)
    await _add_post(isolated_db, item_id=item_id, slot_id=8)

    result = await handlers.remove_slots(queue_id, [7], _noop_progress)

    item = await _item(isolated_db, item_id)
    assert item["state"] == "scheduled"
    assert item["reason"] is None
    assert "retired" not in result


async def test_an_item_that_already_posted_is_not_retired(isolated_db, handlers):
    """A posted row is history: the item is a record of something that went out,
    not an empty shell. Its pending sibling can still go."""
    queue_id, item_id = await _seed(isolated_db)
    await _add_post(isolated_db, item_id=item_id, slot_id=7, status="posted")
    await _add_post(isolated_db, item_id=item_id, slot_id=8)

    await handlers.remove_slots(queue_id, [8], _noop_progress)

    assert (await _item(isolated_db, item_id))["state"] == "scheduled"


async def test_a_queued_item_with_no_posts_is_never_retired(isolated_db, handlers):
    """'queued' means in the queue with no posting time and no posts yet —
    Accept is what creates them. Retiring these would empty the user's queue."""
    queue_id, item_id = await _seed(isolated_db)
    await isolated_db.execute(
        "UPDATE smart_queue_items SET state = 'queued', scheduled_at = NULL "
        "WHERE id = ?",
        (item_id,),
    )
    await isolated_db.commit()

    retired = await handlers._retire_emptied_items([item_id], "should not apply")

    assert retired == 0
    assert (await _item(isolated_db, item_id))["state"] == "queued"


async def test_narrowing_applies_to_retires_the_emptied_item(isolated_db, handlers):
    """Every pending posting for an excluded item goes, so the item is always
    left empty — this is the definite case, not an edge one."""
    queue_id, item_id = await _seed(
        isolated_db, applies_to='["video"]', item_type="short"
    )
    await _add_post(isolated_db, item_id=item_id, slot_id=7)

    result = await handlers.drop_excluded_videos(queue_id, _noop_progress)

    item = await _item(isolated_db, item_id)
    assert item["state"] == "removed"
    assert "applies" in (item["reason"] or "")
    assert "retired 1 item" in result


async def test_an_item_the_template_still_accepts_is_untouched(isolated_db, handlers):
    queue_id, item_id = await _seed(
        isolated_db, applies_to='["short"]', item_type="short"
    )
    post_id = await _add_post(isolated_db, item_id=item_id, slot_id=7)

    result = await handlers.drop_excluded_videos(queue_id, _noop_progress)

    assert (await _item(isolated_db, item_id))["state"] == "scheduled"
    assert "removed 0 posts" in result
    assert await _post_exists(isolated_db, post_id)


async def test_a_video_tier_template_still_accepts_an_episode(isolated_db, handlers):
    """applies_to holds TIERS, videos.item_type holds KINDS, and the full-length
    one is spelled differently in each: tier 'video' covers item_type 'episode'.

    Plain set membership read that as excluded and would delete every episode's
    postings in a queue whose template legitimately accepts them — the same
    mismatch that once made a 'video' template match nothing, but destructive.
    """
    queue_id, item_id = await _seed(
        isolated_db, applies_to='["video"]', item_type="episode"
    )
    post_id = await _add_post(isolated_db, item_id=item_id, slot_id=7)

    result = await handlers.drop_excluded_videos(queue_id, _noop_progress)

    assert await _post_exists(isolated_db, post_id)
    assert (await _item(isolated_db, item_id))["state"] == "scheduled"
    assert "removed 0 posts" in result


async def test_a_narrowed_template_actually_deletes(isolated_db, handlers):
    """The handler loaded applies_to from a projection that never selected it,
    so it always saw None, always took the "applies to everything" exit, and
    every applies_to_removed job reported success having deleted nothing."""
    queue_id, item_id = await _seed(
        isolated_db, applies_to='["video"]', item_type="short"
    )
    post_id = await _add_post(isolated_db, item_id=item_id, slot_id=7)

    result = await handlers.drop_excluded_videos(queue_id, _noop_progress)

    assert not await _post_exists(isolated_db, post_id)
    assert "removed 1 post" in result


async def test_retiring_releases_the_video_for_reuse(isolated_db, handlers):
    """The point of 'removed' over a stuck 'scheduled': PENDING_ITEM_STATES is
    what excludes an already-queued video from being offered again."""
    queue_service = importlib.import_module("yt_scheduler.services.smart_queue")
    queue_id, item_id = await _seed(isolated_db)
    await _add_post(isolated_db, item_id=item_id, slot_id=7)

    assert "scheduled" in queue_service.PENDING_ITEM_STATES
    await handlers.remove_slots(queue_id, [7], _noop_progress)

    assert (await _item(isolated_db, item_id))["state"] not in (
        queue_service.PENDING_ITEM_STATES
    )
