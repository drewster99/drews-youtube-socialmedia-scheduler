"""Missed postings, and what the user can do about one.

"Missed" is a **derived** state, not a stored one: a post whose ``scheduled_at``
has passed and which hasn't been sent is missed, computed when the screen is
read. There is no background sweeper and no flag to keep in sync — recovery is
entirely user-initiated, which is the whole point of the design.

Three dispositions, matching the per-queue missed policy:

* **post now** — send it immediately, late.
* **reschedule to end** — move it behind everything else in the queue.
* **remove** — take it out of the queue, which makes the video eligible to be
  added again later.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from yt_scheduler.database import get_db, write_transaction
from yt_scheduler.services import smart_queue as queue_service

logger = logging.getLogger(__name__)

DISPOSITIONS = ("post_now", "reschedule_end", "remove")


async def missed_items(queue_id: int) -> list[dict]:
    """Posts this queue owns whose time has passed and that never went out.

    Derived from ``scheduled_at < now`` rather than from a stored flag, so it
    is always accurate with no job keeping it up to date. A post that failed
    for its own reason is included too — from the user's point of view both
    are "this didn't go out and needs a decision".
    """
    db = await get_db()
    now = datetime.now(timezone.utc).isoformat()
    rows = await db.execute_fetchall(
        """
        SELECT p.id AS post_id, p.platform, p.status, p.scheduled_at, p.error,
               p.failed_at, p.intended_at,
               i.id AS item_id, i.video_id, v.title,
               -- Carried so the Remove confirmation can state what actually
               -- happens next instead of guessing. Auto-add considers a video
               -- once; whether that has already happened is the difference
               -- between "this will never come back on its own" and "a future
               -- publish could still add it", and only the row knows which.
               v.auto_add_considered_at
          FROM social_posts p
          JOIN smart_queue_items i ON i.id = p.smart_queue_item_id
          JOIN videos v ON v.id = i.video_id
         WHERE i.queue_id = ?
           AND p.status NOT IN ('posted', 'sending', 'skipped')
           AND (
                 (p.scheduled_at IS NOT NULL AND p.scheduled_at < ?)
                 OR p.status = 'failed'
               )
         ORDER BY p.scheduled_at
        """,
        (queue_id, now),
    )
    return [
        {**dict(row), "missed_reason": row["error"] or "its posting time passed"}
        for row in rows
    ]


def within_grace(queue: dict, scheduled_at: str | None) -> bool:
    """Whether a missed post is still inside the queue's post-late window.

    Only meaningful for the ``post_late`` policy; the other two never post
    late at all.

    Pass ``intended_at`` when ``scheduled_at`` is gone: marking a post failed
    clears the scheduling columns (the restore pass would otherwise resurrect
    it), so measuring the window from ``scheduled_at`` alone reported EVERY
    failed post as expired — including one that failed three hours into a
    24-hour window. Callers use ``scheduled_at or intended_at``.
    """
    if queue.get("missed_policy") != "post_late" or not scheduled_at:
        return False
    hours = queue.get("missed_grace_hours")
    if not hours:
        return False
    when = datetime.fromisoformat(str(scheduled_at).replace("Z", "+00:00"))
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    age_hours = (datetime.now(timezone.utc) - when).total_seconds() / 3600
    return age_hours <= float(hours)


async def dispose(queue_id: int, post_id: int, action: str) -> dict:
    """Apply the user's decision to one missed post.

    Per-post, not per-item: one platform failing must not drag the others with
    it, and the user may want a different answer for each.
    """
    if action not in DISPOSITIONS:
        raise queue_service.SmartQueueError(
            f"Unknown action {action!r}; expected one of {', '.join(DISPOSITIONS)}"
        )
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT p.id, p.smart_queue_item_id, i.queue_id "
        "FROM social_posts p "
        "JOIN smart_queue_items i ON i.id = p.smart_queue_item_id "
        "WHERE p.id = ? AND i.queue_id = ?",
        (post_id, queue_id),
    )
    if not rows:
        raise queue_service.SmartQueueError(
            f"Post {post_id} is not part of smart queue {queue_id}"
        )
    item_id = int(rows[0]["smart_queue_item_id"])

    if action == "post_now":
        return await _post_now(post_id)
    if action == "reschedule_end":
        return await _reschedule_to_end(queue_id, item_id, post_id)
    return await _remove(item_id, post_id)


async def _post_now(post_id: int) -> dict:
    """Send immediately, through the ordinary send path.

    Not a special queue-only sender: the same claim, duplicate check, liveness
    check, and media preparation apply as for any other post.
    """
    from yt_scheduler.services.scheduler import _send_scheduled_post

    async with write_transaction() as db:
        await db.execute(
            "UPDATE social_posts SET status = 'approved', error = NULL "
            "WHERE id = ? AND status = 'failed'",
            (post_id,),
        )
    await _send_scheduled_post(post_id)
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT status, error, post_url FROM social_posts WHERE id = ?", (post_id,)
    )
    row = dict(rows[0])
    return {"action": "post_now", "status": row["status"], "error": row["error"],
            "post_url": row["post_url"]}


class PostNoLongerDisposable(RuntimeError):
    """The post moved on before the user's choice reached it.

    The missed-postings screen lists overdue posts, and overdue posts are
    exactly what restart auto-recovery sends. Between render and click, a row
    can go out. Refusing loudly is the only honest answer — silently rewriting
    a published row re-sends it.
    """

    def __init__(self, post_id: int) -> None:
        self.post_id = post_id
        super().__init__(
            f"Post {post_id} is no longer pending — it has already been sent "
            "or removed. Refresh the missed-postings list."
        )


async def _reschedule_to_end(queue_id: int, item_id: int, post_id: int) -> dict:
    """Move this item behind everything else, at the next free posting time."""
    from yt_scheduler.services.scheduler import schedule_social_post

    queue = await queue_service.get_queue(queue_id)
    when = (await queue_service.next_free_posting_times(queue, 1))[0]

    async with write_transaction() as write_db:
        # Position is computed INSIDE the write transaction (BEGIN IMMEDIATE),
        # not read beforehand: reading MAX(position) first let auto-add insert a
        # new tail between the read and the write, so both landed on the same
        # position. Auto-add already computes its position in-statement for the
        # same reason; this matches it, without a UNIQUE constraint that would
        # turn a duplicate into a hard failure.
        await write_db.execute(
            "UPDATE smart_queue_items SET scheduled_at = ?, "
            "position = (SELECT COALESCE(MAX(position), -1) + 1 "
            "            FROM smart_queue_items WHERE queue_id = ?), "
            "state = 'scheduled', reason = NULL WHERE id = ?",
            (when.isoformat(), queue_id, item_id),
        )
        # Status predicate is load-bearing. The missed screen is stale by
        # definition — it lists overdue posts, which is exactly what restart
        # auto-recovery is sending in parallel. Without this, a post that went
        # out a second ago is rewritten to 'approved' with a fresh timer and
        # sent again, and the duplicate guard cannot see it: the only record of
        # the first send IS this row.
        cursor = await write_db.execute(
            "UPDATE social_posts SET status = 'approved', error = NULL "
            "WHERE id = ? AND status IN ('approved', 'failed')",
            (post_id,),
        )
        if not cursor.rowcount:
            raise PostNoLongerDisposable(post_id)
    await schedule_social_post(post_id, when)
    return {"action": "reschedule_end", "scheduled_at": when.isoformat()}


async def _remove(item_id: int, post_id: int) -> dict:
    """Take the item out of the queue.

    ``removed`` is neither ``scheduled`` nor ``posted``, so the video becomes
    eligible to be added again later — by Auto-select + Accept, never
    automatically (the auto-add marker has already been spent).
    """
    from yt_scheduler.services.scheduler import cancel_scheduled_post

    db_read = await get_db()
    # Cancel EVERY pending sibling, not just this one. Retiring the item makes
    # the video immediately re-selectable, so a second Accept builds a second
    # occurrence — and the first item's surviving timers still fire, posting
    # the same clip twice.
    siblings = await db_read.execute_fetchall(
        "SELECT id FROM social_posts "
        "WHERE smart_queue_item_id = ? AND status = 'approved' AND id != ?",
        (item_id, post_id),
    )
    for row in siblings:
        await cancel_scheduled_post(int(row["id"]))
    await cancel_scheduled_post(post_id)
    async with write_transaction() as db:
        await db.execute(
            "UPDATE smart_queue_items SET state = 'removed', "
            "reason = 'removed by the user after a missed posting' WHERE id = ?",
            (item_id,),
        )
        # Same guard: marking a post 'skipped' after it published would make
        # history claim it never went out.
        cursor = await db.execute(
            "UPDATE social_posts SET status = 'skipped', "
            "error = 'removed from the smart queue by the user', "
            "scheduled_at = NULL, scheduler_job_id = NULL "
            "WHERE id = ? AND status IN ('approved', 'failed')",
            (post_id,),
        )
        if not cursor.rowcount:
            raise PostNoLongerDisposable(post_id)
        # Siblings go with it, for the same reason their timers were cancelled.
        await db.execute(
            "UPDATE social_posts SET status = 'skipped', "
            "error = 'removed from the smart queue by the user', "
            "scheduled_at = NULL, scheduler_job_id = NULL "
            "WHERE smart_queue_item_id = ? AND status = 'approved'",
            (item_id,),
        )
    return {"action": "remove"}
