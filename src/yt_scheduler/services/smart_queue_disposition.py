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
               i.id AS item_id, i.video_id, v.title
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


async def _reschedule_to_end(queue_id: int, item_id: int, post_id: int) -> dict:
    """Move this item behind everything else, at the next free posting time."""
    from yt_scheduler.services.scheduler import schedule_social_post

    queue = await queue_service.get_queue(queue_id)
    db = await get_db()
    when = (await queue_service.next_free_posting_times(queue, 1))[0]

    positions = await db.execute_fetchall(
        "SELECT COALESCE(MAX(position), -1) AS last FROM smart_queue_items "
        "WHERE queue_id = ?",
        (queue_id,),
    )
    async with write_transaction() as write_db:
        await write_db.execute(
            "UPDATE smart_queue_items SET scheduled_at = ?, position = ?, "
            "state = 'scheduled', reason = NULL WHERE id = ?",
            (when.isoformat(), int(positions[0]["last"]) + 1, item_id),
        )
        await write_db.execute(
            "UPDATE social_posts SET status = 'approved', error = NULL WHERE id = ?",
            (post_id,),
        )
    await schedule_social_post(post_id, when)
    return {"action": "reschedule_end", "scheduled_at": when.isoformat()}


async def _remove(item_id: int, post_id: int) -> dict:
    """Take the item out of the queue.

    ``removed`` is neither ``scheduled`` nor ``posted``, so the video becomes
    eligible to be added again later — by Auto-select + Accept, never
    automatically (the auto-add marker has already been spent).
    """
    from yt_scheduler.services.scheduler import cancel_scheduled_post

    await cancel_scheduled_post(post_id)
    async with write_transaction() as db:
        await db.execute(
            "UPDATE smart_queue_items SET state = 'removed', "
            "reason = 'removed by the user after a missed posting' WHERE id = ?",
            (item_id,),
        )
        await db.execute(
            "UPDATE social_posts SET status = 'skipped', "
            "error = 'removed from the smart queue by the user', "
            "scheduled_at = NULL, scheduler_job_id = NULL WHERE id = ?",
            (post_id,),
        )
    return {"action": "remove"}
