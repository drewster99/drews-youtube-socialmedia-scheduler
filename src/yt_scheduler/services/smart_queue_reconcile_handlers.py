"""The four things a template edit does to an already-built schedule.

Split from :mod:`smart_queue_reconcile` so the queue/worker machinery stays
readable next to the work itself. Every handler takes a ``progress`` callback
so the app-wide banner can show N of M rather than a spinner.

All of them touch only *pending* posts. A posted row is history: it already
went out, and rewriting or deleting it would misrepresent what was published.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime

from yt_scheduler.database import get_db, write_transaction
from yt_scheduler.services import media_hosting
from yt_scheduler.services import smart_queue as queue_service
from yt_scheduler.services.smart_queue_accept import (
    _DETERMINISTIC_RENDER_FAILURES,
    _render_slot,
    _template_by_id,
    slots_accepting,
)

logger = logging.getLogger(__name__)

Progress = Callable[[int, int], Awaitable[None]]

# A post that has gone out, or is going out right now, is not ours to touch.
_PENDING_STATUSES = ("posted", "sending")


async def _pending_items(queue_id: int) -> list[dict]:
    """Scheduled items with nothing already published.

    A partially-sent item is excluded for the same reason re-flow excludes it:
    some of it is public, so changing the rest is not reconciliation.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        """
        SELECT i.id, i.video_id, i.scheduled_at FROM smart_queue_items i
         WHERE i.queue_id = ? AND i.state = 'scheduled'
           AND NOT EXISTS (
               SELECT 1 FROM social_posts p
                WHERE p.smart_queue_item_id = i.id AND p.status = 'posted'
           )
         ORDER BY i.position
        """,
        (queue_id,),
    )
    return [dict(r) for r in rows]


async def _retire_emptied_items(item_ids: list[int], reason: str) -> int:
    """Take items that have no posting rows left out of ``scheduled``.

    An item's displayed state is DERIVED from its ``social_posts`` rows
    (``smart_queue.list_queues``: ``LEFT JOIN social_posts`` … ``ELSE i.state``),
    so deleting the last one leaves the item reported as ``scheduled`` for good,
    with nothing that can ever send — and because ``PENDING_ITEM_STATES`` covers
    ``scheduled``, its video is never offered as a candidate again either.
    ``removed`` is the state that says the occurrence is over without claiming it
    posted, and it is what releases the video (see
    ``smart_queue_disposition._remove``).

    Both guards live in the statement so the emptiness check can't go stale
    between read and write:

    * ``state = 'scheduled'`` — a ``queued`` item legitimately has no posts yet
      (Accept is what creates them), and marking those removed would quietly
      empty the user's queue.
    * ``NOT EXISTS`` any post — an item that still has a ``posted`` or
      ``sending`` row is history, not an empty shell.
    """
    if not item_ids:
        return 0
    placeholders = ",".join("?" for _ in item_ids)
    async with write_transaction() as db:
        cursor = await db.execute(
            f"""
            UPDATE smart_queue_items
               SET state = 'removed', reason = ?
             WHERE id IN ({placeholders})
               AND state = 'scheduled'
               -- "Emptied" means no post that could EVER send. A 'skipped'
               -- leftover (empty content) blocked retirement, so the item sat
               -- 'scheduled' forever and permanently blocked its video from
               -- being queued again — the exact zombie this function prevents.
               AND NOT EXISTS (
                   SELECT 1 FROM social_posts p
                    WHERE p.smart_queue_item_id = smart_queue_items.id
                      AND p.status != 'skipped'
               )
            """,
            (reason, *item_ids),
        )
        retired = cursor.rowcount or 0
    if retired:
        logger.info(
            "Reconcile: retired %d queue item(s) left with no postings — %s",
            retired, reason,
        )
    return retired


async def _default_ai_system_for_queue(queue_id: int) -> str | None:
    from yt_scheduler.services import prompts as prompt_service

    queue = await queue_service.get_queue(queue_id)
    try:
        resolved = await prompt_service.get_prompt_with_fallback(
            "ai_block_default_system_prompt", project_id=int(queue["project_id"])
        )
        return resolved["system"]
    except prompt_service.RetiredPromptKey:
        raise
    except KeyError:
        # Genuinely absent: no row, no seed. The template's own prompts still
        # render, so this is not a reason to abandon the reconciliation.
        logger.warning("reconcile: no default AI system prompt for queue %s", queue_id)
        return None
    except Exception:
        # Anything else — a blank saved prompt, a DB error — must NOT degrade
        # to None. Re-rendering with a different {{ai:}} system prompt than
        # Accept used produces silently different post text, which is worse
        # than a failed job the user can see and retry.
        logger.exception(
            "reconcile: could not resolve the default AI system prompt for "
            "queue %s; refusing to re-render against a different one", queue_id,
        )
        raise


async def add_slots(queue_id: int, slot_ids: list[int], progress: Progress) -> str:
    """Render and schedule newly-added slots for every pending item.

    New posts inherit their item's ``scheduled_at``, so they go out alongside
    the posts that were already there and nothing on the calendar moves.
    """
    from yt_scheduler.services.scheduler import schedule_social_post

    db = await get_db()
    queue = await queue_service.get_queue(queue_id)
    template = await _template_by_id(int(queue["template_id"]))
    wanted = {
        int(s["id"]): s for s in template["slots"]
        if s.get("id") and int(s["id"]) in set(slot_ids) and not s.get("is_disabled")
    }
    if not wanted:
        return "no matching slots remained by the time this ran"

    items = await _pending_items(queue_id)
    default_ai_system = await _default_ai_system_for_queue(queue_id)
    hosting_ready = await media_hosting.is_configured()

    total = len(items) * len(wanted)
    created = skipped = failed = 0
    done = 0
    await progress(0, total)

    for item in items:
        video_rows = await db.execute_fetchall(
            "SELECT * FROM videos WHERE id = ?", (item["video_id"],)
        )
        if not video_rows:
            done += len(wanted)
            await progress(done, total)
            continue
        video = dict(video_rows[0])

        # Never double-add: the same slot may already have been reconciled by
        # an earlier job, or the item accepted after the slot existed.
        existing = await db.execute_fetchall(
            "SELECT slot_id FROM social_posts WHERE smart_queue_item_id = ?",
            (int(item["id"]),),
        )
        have = {int(r["slot_id"]) for r in existing if r["slot_id"] is not None}

        missing = [wanted[sid] for sid in wanted if sid not in have]
        done += len(wanted) - len(missing)

        for verdict in slots_accepting(video, missing,
                                       media_hosting_configured=hosting_ready):
            slot = verdict.slot
            if not verdict.accepted:
                async with write_transaction() as write_db:
                    await write_db.execute(
                        "INSERT INTO social_posts (video_id, platform, content, "
                        "status, error, slot_id, smart_queue_item_id) "
                        "VALUES (?,?,'','skipped',?,?,?)",
                        (video["id"], slot["platform"], verdict.reason,
                         slot.get("id"), int(item["id"])),
                    )
                skipped += 1
                done += 1
                await progress(done, total)
                continue
            try:
                rendered, media_paths = await _render_slot(
                    db, video, slot, default_ai_system=default_ai_system
                )
            except _DETERMINISTIC_RENDER_FAILURES as exc:
                logger.warning("reconcile add_slots: render failed for %s/%s: %s",
                               video["id"], slot["platform"], exc)
                failed += 1
                done += 1
                await progress(done, total)
                continue

            when_iso = item["scheduled_at"]
            async with write_transaction() as write_db:
                cursor = await write_db.execute(
                    """
                    INSERT INTO social_posts
                        (video_id, platform, content, media_paths, status,
                         social_account_id, max_chars, slot_id,
                         smart_queue_item_id, scheduled_at)
                    VALUES (?,?,?,?,'approved',?,?,?,?,?)
                    """,
                    (video["id"], slot["platform"], rendered,
                     json.dumps(media_paths), slot.get("social_account_id"),
                     slot.get("max_chars"), slot.get("id"), int(item["id"]),
                     when_iso),
                )
                post_id = int(cursor.lastrowid)
            created += 1

            # Outside the transaction: registering a timer writes, and a
            # rollback cannot un-register an APScheduler job.
            if when_iso:
                try:
                    await schedule_social_post(post_id, datetime.fromisoformat(when_iso))
                except Exception:
                    logger.exception(
                        "reconcile add_slots: timer not registered for post %s "
                        "(scheduled_at is committed; restart will restore it)",
                        post_id,
                    )
            done += 1
            await progress(done, total)

    parts = [f"added {created} post{'' if created == 1 else 's'}"]
    if skipped:
        parts.append(f"{skipped} slot(s) couldn't carry the video")
    if failed:
        parts.append(f"{failed} failed to render")
    return ", ".join(parts)


async def remove_slots(queue_id: int, slot_ids: list[int], progress: Progress) -> str:
    """Delete pending posts belonging to slots that no longer exist.

    Without this they keep their timers and go out from a slot the user
    deleted — the most surprising possible outcome of a deletion.

    An item whose LAST posting goes this way is retired too: the queue reads an
    item's state from its posts, so one left with none would sit in the
    ``scheduled`` count forever with nothing to send. Items that still have
    postings from other slots are untouched.
    """
    from yt_scheduler.services.scheduler import cancel_scheduled_post

    db = await get_db()
    placeholders = ",".join("?" * len(slot_ids))
    rows = await db.execute_fetchall(
        f"""
        SELECT p.id, p.smart_queue_item_id FROM social_posts p
          JOIN smart_queue_items i ON i.id = p.smart_queue_item_id
         WHERE i.queue_id = ? AND p.slot_id IN ({placeholders})
           AND p.status NOT IN (?,?)
        """,
        (queue_id, *slot_ids, *_PENDING_STATUSES),
    )
    touched_items = {int(r["smart_queue_item_id"]) for r in rows}
    total = len(rows)
    await progress(0, total)

    for index, row in enumerate(rows, start=1):
        post_id = int(row["id"])
        try:
            await cancel_scheduled_post(post_id)
        except Exception:
            # The row is going regardless; a stale timer would fire on a
            # missing post and log, which is noisier but not harmful.
            logger.exception("reconcile remove_slots: could not cancel timer for %s",
                             post_id)
        async with write_transaction() as write_db:
            # Conditional, like remove_post: a row that turned 'sending' or
            # 'posted' between the SELECT and here is a real public send, and
            # deleting it leaves no DB record for history or duplicate
            # detection while mark_posted silently updates zero rows.
            await write_db.execute(
                "DELETE FROM social_posts WHERE id = ? "
                "AND status NOT IN ('posted', 'sending')",
                (post_id,),
            )
        await progress(index, total)

    retired = await _retire_emptied_items(
        sorted(touched_items),
        "the slot it was scheduled for was deleted from the template",
    )
    summary = f"removed {total} post{'' if total == 1 else 's'}"
    if retired:
        summary += f", retired {retired} item{'' if retired == 1 else 's'}"
    return summary


async def rerender_slots(queue_id: int, slot_ids: list[int], progress: Progress) -> str:
    """Re-render pending posts whose slot body was edited."""
    db = await get_db()
    queue = await queue_service.get_queue(queue_id)
    template = await _template_by_id(int(queue["template_id"]))
    slots_by_id = {int(s["id"]): s for s in template["slots"] if s.get("id")}

    placeholders = ",".join("?" * len(slot_ids))
    rows = await db.execute_fetchall(
        f"""
        SELECT p.id AS post_id, p.slot_id, i.video_id
          FROM social_posts p
          JOIN smart_queue_items i ON i.id = p.smart_queue_item_id
         WHERE i.queue_id = ? AND p.slot_id IN ({placeholders})
           AND p.status NOT IN (?,?)
        """,
        (queue_id, *slot_ids, *_PENDING_STATUSES),
    )
    default_ai_system = await _default_ai_system_for_queue(queue_id)
    total = len(rows)
    updated = failed = 0
    await progress(0, total)

    for index, row in enumerate(rows, start=1):
        slot = slots_by_id.get(int(row["slot_id"] or 0))
        video_rows = await db.execute_fetchall(
            "SELECT * FROM videos WHERE id = ?", (row["video_id"],)
        )
        if slot is None or not video_rows:
            failed += 1
            await progress(index, total)
            continue
        try:
            rendered, media_paths = await _render_slot(
                db, dict(video_rows[0]), slot, default_ai_system=default_ai_system
            )
        except _DETERMINISTIC_RENDER_FAILURES as exc:
            logger.warning("reconcile rerender: %s failed: %s", row["post_id"], exc)
            failed += 1
            await progress(index, total)
            continue
        async with write_transaction() as write_db:
            await write_db.execute(
                "UPDATE social_posts SET content = ?, media_paths = ? WHERE id = ?",
                (rendered, json.dumps(media_paths), int(row["post_id"])),
            )
        updated += 1
        await progress(index, total)

    detail = f"re-rendered {updated} post{'' if updated == 1 else 's'}"
    return detail + (f", {failed} failed" if failed else "")


async def drop_excluded_videos(queue_id: int, progress: Progress) -> str:
    """Delete pending posts for videos the template no longer applies to.

    Narrowing "applies to" is a statement about what this queue should be
    posting, so what is already scheduled has to follow it. The item ROW is left
    in place rather than deleted outright — its history of having been queued is
    still true — but it is retired to ``removed``, because a scheduled item with
    no postings left is a slot in the queue that can never fill: it would keep
    inflating the scheduled count and keep its video from being offered again.
    """
    from yt_scheduler.services.scheduler import cancel_scheduled_post

    db = await get_db()
    queue = await queue_service.get_queue(queue_id)
    # Via template_applies_to, not _template_by_id: that loader selects only
    # id/name/project_id, so `template.get("applies_to")` was always None and
    # this handler always returned early — the job reported success and deleted
    # nothing, for every narrowing edit. template_applies_to is the accessor the
    # eligibility path already uses, and it decodes the JSON.
    applies_to = await queue_service.template_applies_to(int(queue["template_id"]))
    if not applies_to:
        # An empty "applies to" means unrestricted in this codebase; deleting
        # everything on that reading would be catastrophic and wrong.
        return "template applies to everything — nothing to remove"

    items = await _pending_items(queue_id)
    total = len(items)
    removed_posts = 0
    emptied_items: list[int] = []
    await progress(0, total)

    for index, item in enumerate(items, start=1):
        video_rows = await db.execute_fetchall(
            "SELECT item_type FROM videos WHERE id = ?", (item["video_id"],)
        )
        if not video_rows:
            await progress(index, total)
            continue
        # tier_matches_item_type, not `in applies_to`: applies_to holds TIERS
        # (hook|short|segment|video) while videos.item_type holds KINDS
        # (episode|hook|short|segment|standalone), and the full-length one is
        # spelled differently in each. Plain membership would read a template
        # applying to "video" as not covering an 'episode' and delete every
        # episode's postings — the same mismatch that once made such a template
        # match nothing, but destructive instead of inert.
        item_type = str(video_rows[0]["item_type"] or "")
        if any(queue_service.tier_matches_item_type(t, item_type) for t in applies_to):
            await progress(index, total)
            continue

        posts = await db.execute_fetchall(
            "SELECT id FROM social_posts WHERE smart_queue_item_id = ? "
            "AND status NOT IN (?,?)",
            (int(item["id"]), *_PENDING_STATUSES),
        )
        for post in posts:
            post_id = int(post["id"])
            try:
                await cancel_scheduled_post(post_id)
            except Exception:
                logger.exception("reconcile drop_excluded: timer for %s", post_id)
            async with write_transaction() as write_db:
                await write_db.execute(
                    "DELETE FROM social_posts WHERE id = ? "
                    "AND status NOT IN ('posted', 'sending')",
                    (post_id,),
                )
            removed_posts += 1
        if posts:
            emptied_items.append(int(item["id"]))
        await progress(index, total)

    retired = await _retire_emptied_items(
        emptied_items, "the template no longer applies to this item's type"
    )
    summary = f"removed {removed_posts} post{'' if removed_posts == 1 else 's'}"
    if retired:
        summary += f", retired {retired} item{'' if retired == 1 else 's'}"
    return summary
