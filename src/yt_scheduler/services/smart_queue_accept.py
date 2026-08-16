"""Accept — turn a selected batch of videos into a scheduled posting plan.

Each accepted video becomes one queue item stamped with a concrete time, and
one ordinary ``social_posts`` row per template slot that can carry it. Using
the normal posts table rather than a parallel one means send, retry, duplicate
detection, history, and the missed-backlog guard all apply unchanged.

A slot that *cannot* carry a video — a clip longer than the platform's cap, a
platform that can't take an attachment at all — records a ``skipped`` row with
the reason. Skipped means "known in advance, not attempted"; ``failed`` means
"attempted and broke", and the two have to stay tellable apart in history.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime

from yt_scheduler.database import get_db, write_transaction
from yt_scheduler.services import ai
from yt_scheduler.services._keyed_locks import KeyedLocks
from yt_scheduler.services import media_hosting
from yt_scheduler.services import smart_queue as queue_service
from yt_scheduler.services import social, templates as tmpl
from yt_scheduler.services.render_context import (
    RenderContextError,
    build_render_context,
)

# Render failures that repeat identically every time — a template problem,
# not a service blip. Caught per
# slot so one broken body doesn't abort the whole batch: the other
# platforms still go out and the failure is visible with its real reason.
_DETERMINISTIC_RENDER_FAILURES = (
    RenderContextError,
    tmpl.MissingRequiredVariable,
    tmpl.UndefinedTemplateVariables,
    tmpl.SectionTagError,
    tmpl.UnknownImageShortname,
    tmpl.TooManyAIBlocksError,
    tmpl.AIBlockDepthError,
    ValueError,
)

logger = logging.getLogger(__name__)

#: ``(videos_done, videos_total)``. The unit is the VIDEO, not the post: a video
#: is what the user selected and what the count on screen refers to, and the
#: number of posts behind each one varies with how many slots can carry it.
AcceptProgress = Callable[[int, int], Awaitable[None]]

# One Accept per queue at a time. The maxima that decide the next free time
# and the next position are read before a render loop that can run for
# minutes, so two overlapping Accepts would stamp the same instants.
_accept_locks: KeyedLocks[int] = KeyedLocks()


def accept_lock(queue_id: int):
    """The per-queue lock that serializes everything mutating a queue's plan.

    Accept legitimately runs for minutes (one Anthropic call per slot per
    video). The HTTP 409 guard only blocks *inbound* mutations while jobs
    exist — it does nothing to stop the reconcile worker executing a job
    mid-Accept, at which point a slot sweep can delete pending posts that
    Accept then re-creates carrying the deleted slot_id. The worker, reflow
    and the missed-posting dispositions all take this same lock.
    """
    return _accept_locks.get(queue_id)


@dataclass(frozen=True)
class SlotVerdict:
    """Whether one template slot can carry one video."""

    slot: dict
    accepted: bool
    reason: str | None = None


def slots_accepting(video: dict, slots: list[dict], *,
                    media_hosting_configured: bool = True) -> list[SlotVerdict]:
    """Decide, per slot, whether this video can be posted there.

    Checked against the platform's published envelope *before* anything is
    scheduled, so a clip that can never succeed on a platform is recorded as
    skipped up front instead of failing at 9am every time it comes round.

    Only limits no encode can fix are grounds for skipping. Size, resolution,
    and codec are all fixable at send time by ``prepared_media``, so they are
    deliberately not consulted here.
    """
    duration = video.get("duration_seconds")
    verdicts: list[SlotVerdict] = []
    for slot in slots:
        if slot.get("is_disabled"):
            continue
        platform = slot["platform"]

        if platform not in social.ALL_PLATFORMS:
            verdicts.append(SlotVerdict(slot, False, f"unknown platform {platform!r}"))
            continue
        if not social.platform_accepts_attached_media(platform):
            verdicts.append(SlotVerdict(
                slot, False,
                f"{platform} can't take an attached video "
                "(its API fetches media from a public URL)",
            ))
            continue

        # The platform can carry the video, but only via hosting we haven't
        # been given. Caught here so it reads as "not attempted, here's why"
        # rather than failing at the scheduled minute every day.
        if social.platform_requires_hosted_media(platform) and not media_hosting_configured:
            verdicts.append(SlotVerdict(
                slot, False,
                f"{platform} fetches media from a URL and media hosting isn't "
                "configured — set it up under Settings → Media hosting",
            ))
            continue

        limits = social.PLATFORM_MEDIA_LIMITS.get(platform)
        if (
            limits is not None
            and limits.max_duration_seconds is not None
            and duration is not None
            and duration > limits.max_duration_seconds
        ):
            verdicts.append(SlotVerdict(
                slot, False,
                f"{duration:.0f}s is over {platform}'s "
                f"{limits.max_duration_seconds:.0f}s limit",
            ))
            continue

        verdicts.append(SlotVerdict(slot, True))
    return verdicts


@dataclass(frozen=True)
class VideoPlan:
    """Everything one video will write, decided before any lock is taken.

    Rendering fires real Anthropic round-trips, so it finishes before the
    transaction opens: ``write_transaction`` holds a process-wide lock and its
    rules forbid awaiting network work inside it.

    ``transient_error`` means nothing may be written for this video at all.
    The failure was not the template's fault and will not repeat
    deterministically, so leaving the video unqueued keeps it a candidate and
    makes the retry clean rather than half-scheduled.
    """

    posts: list[tuple[dict, str, list[str]]]
    skipped_slots: list[SlotVerdict]
    transient_error: str | None = None


async def _plan_video(
    db, video: dict, slots: list[dict], *, default_ai_system: str | None,
    media_hosting_configured: bool = True,
) -> VideoPlan:
    """Render every slot for one video and classify each outcome.

    Two kinds of render failure, which must not be conflated. A template error
    — undefined variable, malformed section, unknown image — fails identically
    every time, so its slot is recorded as skipped with the reason and the
    other platforms still go out. Anything else (Anthropic overloaded, no API
    key, network down) is transient: it abandons the whole video so a later
    Accept can take it cleanly.
    """
    posts: list[tuple[dict, str, list[str]]] = []
    skipped_slots: list[SlotVerdict] = []
    for verdict in slots_accepting(
        video, slots, media_hosting_configured=media_hosting_configured
    ):
        if not verdict.accepted:
            skipped_slots.append(verdict)
            continue
        try:
            rendered, media_paths = await _render_slot(
                db, video, verdict.slot, default_ai_system=default_ai_system
            )
        except _DETERMINISTIC_RENDER_FAILURES as exc:
            skipped_slots.append(SlotVerdict(verdict.slot, False, str(exc)))
            continue
        except Exception as exc:
            # The type name is part of the message on purpose: "APIStatusError"
            # reads as a service blip and "KeyError" reads as our bug, and the
            # user should be able to tell which they are looking at. The full
            # stack goes to the log either way.
            logger.exception(
                "Accept: unexpected failure rendering %s for video %s",
                verdict.slot["platform"], video["id"],
            )
            return VideoPlan([], [], f"{type(exc).__name__}: {exc}")
        posts.append((verdict.slot, rendered, media_paths))
    return VideoPlan(posts, skipped_slots)


async def check_acceptable(queue_id: int) -> None:
    """Raise ``SmartQueueError`` for anything Accept cannot possibly get past.

    Accept runs on the background worker, where a failure becomes a red banner
    the user has to go and read. The two ways it can be dead on arrival — the
    queue is gone, or it has no posting times at all — are both immediately
    fixable and both knowable without doing any work, so they are answered at
    the button as a 400 instead. Everything else is genuinely per-video and
    belongs in the job's report.

    Deliberately does NOT pre-render or pre-resolve anything: a check that can
    itself be slow or flaky is a second place for Accept to fail, which is what
    moving Accept off the request was meant to stop.
    """
    queue = await queue_service.get_queue(queue_id)
    if not queue["slots"]:
        raise queue_service.SmartQueueError(
            "This queue has no posting times, so nothing can be scheduled."
        )
    await _template_by_id(int(queue["template_id"]))


async def accept_selection(
    queue_id: int,
    video_ids: list[str],
    *,
    default_ai_system: str | None = None,
    progress: AcceptProgress | None = None,
) -> dict:
    """Take the queue's Accept lock and schedule the batch.

    The body is :func:`_accept_selection_locked`, split out for the reconcile
    worker: ``run_job`` already holds this lock for every job kind, so a handler
    that called this wrapper would deadlock against itself. Same shape as
    ``reflow_pending`` / ``_reflow_pending_locked`` below.
    """
    async with _accept_locks.get(queue_id):
        return await _accept_selection_locked(
            queue_id, video_ids,
            default_ai_system=default_ai_system, progress=progress,
        )


async def _accept_selection_locked(
    queue_id: int,
    video_ids: list[str],
    *,
    default_ai_system: str | None = None,
    progress: AcceptProgress | None = None,
) -> dict:
    """Give a posting time to everything waiting in this queue.

    Two sources feed one plan, in this order:

    1. Items already in the queue with no time yet — what auto-add appends
       when a video goes live. They keep their position, so they go out in the
       order they arrived.
    2. ``video_ids``, in the order the caller sends them (shuffled or not).

    Accept is the ONLY thing that assigns a posting time. Auto-add deliberately
    does not, so there is one place where "when does this go out" is decided
    rather than two that could disagree.

    Times are computed by enumerating the queue's recurrence in its own
    timezone, so each stamped instant is right for its own date across a DST
    boundary, and they continue after everything already scheduled so a second
    Accept appends rather than double-booking.

    Returns ``{"scheduled": N, "items": [...], "skipped": [...], "errors": [...]}``.
    It does not raise once the loop has begun: a caller told nothing cannot
    tell a half-landed batch from a normal one, so every outcome comes back in
    the ledger.
    """
    from yt_scheduler.services.scheduler import schedule_social_post

    queue = await queue_service.get_queue(queue_id)
    db = await get_db()

    # The caller holds the queue's Accept lock. The render loop runs for
    # minutes, and the "next free time" and "next position" maxima are read
    # before it — two overlapping Accepts (a double-click is enough) would
    # otherwise read the same maxima and stamp the same instants.
    template = await _template_by_id(int(queue["template_id"]))
    slots = template["slots"]

    # A repeated id would otherwise be scheduled twice in one batch.
    # dict.fromkeys de-dupes while preserving order, so "the order is the
    # caller's" still holds.
    video_ids = list(dict.fromkeys(video_ids))

    skipped: list[dict] = []
    errors: list[dict] = []

    # Waiting items first, oldest position first: auto-add put them here and
    # they have been waiting longest.
    waiting = await db.execute_fetchall(
        "SELECT id, video_id FROM smart_queue_items "
        "WHERE queue_id = ? AND state = ? ORDER BY position",
        (queue_id, queue_service.ITEM_STATE_QUEUED),
    )
    pending: list[tuple[int | None, str]] = [
        (int(row["id"]), row["video_id"]) for row in waiting
    ]
    waiting_ids = {row["video_id"] for row in waiting}

    already = await queue_service.already_scheduled_video_ids(queue_id, video_ids)
    for video_id in video_ids:
        if video_id in waiting_ids:
            # Already picked up above as a waiting item; scheduling it again
            # here would give the same video two rows in one batch.
            continue
        if video_id in already:
            # The screen's selection can be stale — a failed Accept leaves
            # the list untouched, and re-submitting must not append a second
            # copy of what already landed.
            skipped.append({
                "video_id": video_id,
                "reason": "already scheduled by this queue",
            })
            continue
        pending.append((None, video_id))

    if not pending:
        return {"scheduled": 0, "items": [], "skipped": skipped, "errors": errors}

    # Resolve every video BEFORE any posting time is computed. An id that no
    # longer names a video must consume neither an instant nor a position:
    # an abandoned instant is a posting time at which nothing goes out, and
    # nothing ever backfills it.
    batch: list[tuple[int | None, dict]] = []
    for item_id, video_id in pending:
        rows = await db.execute_fetchall(
            "SELECT * FROM videos WHERE id = ?", (video_id,)
        )
        if not rows:
            skipped.append({
                "video_id": video_id, "reason": "video no longer exists",
            })
            continue
        video = dict(rows[0])
        if int(video.get("project_id") or 0) != int(queue["project_id"]):
            # A video id from another project must never be scheduled here:
            # it would post another channel's clip on this queue's accounts.
            skipped.append({
                "video_id": video_id,
                "reason": "belongs to a different project",
            })
            continue
        batch.append((item_id, video))
    if not batch:
        return {"scheduled": 0, "items": [], "skipped": skipped, "errors": errors}

    instants = await queue_service.next_free_posting_times(queue, len(batch))
    instant_index = 0

    position_rows = await db.execute_fetchall(
        "SELECT COALESCE(MAX(position), -1) AS last FROM smart_queue_items "
        "WHERE queue_id = ?",
        (queue_id,),
    )
    position = int(position_rows[0]["last"]) + 1

    created_items: list[dict] = []

    # Read once for the whole batch: it is install-wide config, and every
    # slot of every video would otherwise re-answer the same question.
    media_hosting_configured = await media_hosting.is_configured()

    # Published before the first render so the banner shows "0 of 27"
    # immediately. Without it a long batch reads as 0 of 0 until the first
    # video lands, which is indistinguishable from a job that is wedged.
    if progress is not None:
        await progress(0, len(batch))

    for done, (item_id, video) in enumerate(batch, start=1):
        try:
            video_id = video["id"]

            # Everything that touches the network happens here, before any lock.
            plan = await _plan_video(
                db, video, slots, default_ai_system=default_ai_system,
                media_hosting_configured=media_hosting_configured,
            )
            if plan.transient_error is not None:
                errors.append({"video_id": video_id, "error": plan.transient_error})
                continue

            # An instant IS a posting time, and only a video that will actually
            # post at it may consume one. A video no slot could carry is written
            # with no time at all — stamping it would leave a slot in the plan
            # where nothing goes out and nothing ever backfills it.
            when = instants[instant_index] if plan.posts else None

            try:
                item_id, post_ids = await _write_video_plan(
                    queue_id, video, plan,
                    existing_item_id=item_id, item_position=position, when=when,
                )
            except Exception as exc:
                # The transaction rolled back, so nothing partial survives and
                # the video is still a candidate. Report it and keep going
                # rather than discarding the ledger for what already landed.
                logger.exception(
                    "Accept: could not write the plan for video %s", video_id
                )
                errors.append({
                    "video_id": video_id, "error": f"{type(exc).__name__}: {exc}",
                })
                continue
            # Advanced only after the write lands. Consuming before it would
            # leave a posting time nothing goes out at, which nothing backfills.
            if plan.posts:
                instant_index += 1
            position += 1

            # Outside the transaction: schedule_social_post writes and would
            # otherwise join it, and a rollback cannot un-register an APScheduler
            # job. scheduled_at is already committed, so a failure here is
            # recoverable rather than invisible.
            for post_id in post_ids:
                try:
                    await schedule_social_post(post_id, when)
                except Exception as exc:
                    logger.exception(
                        "Accept: could not register the timer for post %s", post_id
                    )
                    errors.append({
                        "video_id": video_id, "post_id": post_id,
                        "error": (
                            f"scheduled for {when.isoformat()}, but its timer "
                            f"could not be registered ({type(exc).__name__}: "
                            f"{exc}); it will be picked up on the next restart"
                        ),
                    })

            for verdict in plan.skipped_slots:
                skipped.append({
                    "video_id": video_id,
                    "platform": verdict.slot["platform"],
                    "reason": verdict.reason,
                })
            created_items.append({
                "id": item_id, "video_id": video_id,
                "scheduled_at": when.isoformat() if when is not None else None,
                "posted_to_any": bool(plan.posts),
            })
        finally:
            # In a finally so the two `continue` paths above — a transient
            # render failure and a failed write — still advance the count.
            # A batch that stalls at 12 of 27 while it is really working
            # through video 13 reads exactly like a wedged job.
            if progress is not None:
                await progress(done, len(batch))

    return {
        "scheduled": sum(1 for item in created_items if item["posted_to_any"]),
        "items": created_items,
        "skipped": skipped,
        "errors": errors,
    }


async def _write_video_plan(
    queue_id: int, video: dict, plan: VideoPlan, *,
    existing_item_id: int | None, item_position: int, when: datetime | None,
) -> tuple[int, list[int]]:
    """Commit one video's whole plan as a unit. Returns (item id, post ids).

    Everything this video writes lands together or not at all: an item with
    only some of its posts, or a post with no item, is a state nothing
    downstream can reason about.

    ``scheduled_at`` is written in the INSERT rather than left for
    ``schedule_social_post``. A post row with a NULL ``scheduled_at`` is
    invisible to both ``restore_scheduled_posts`` and ``missed_items``, so a
    crash in that window used to strand it permanently while its item still
    read as scheduled.

    Nothing in here does I/O, so the process-wide write lock is held for a
    handful of INSERTs and nothing else.
    """
    # An item no slot can carry would otherwise burn a posting time on a no-op,
    # so it is written in its final state rather than corrected afterwards.
    item_state = "scheduled" if plan.posts else "skipped"
    item_reason = None if plan.posts else "no slot could carry this video"
    # Derived from the same fact that decides the state, so the two cannot
    # drift: an item that posts nothing holds no posting time either.
    when_iso = when.isoformat() if plan.posts and when is not None else None

    async with write_transaction() as db:
        if existing_item_id is None:
            cursor = await db.execute(
                "INSERT INTO smart_queue_items "
                "(queue_id, video_id, position, scheduled_at, state, reason) "
                "VALUES (?,?,?,?,?,?)",
                (queue_id, video["id"], item_position, when_iso,
                 item_state, item_reason),
            )
            item_id = int(cursor.lastrowid)
        else:
            # A waiting item keeps its row and its position — it has been in
            # the queue since it went live; all it was missing was a time.
            item_id = existing_item_id
            await db.execute(
                "UPDATE smart_queue_items SET scheduled_at = ?, state = ?, "
                "reason = ? WHERE id = ?",
                (when_iso, item_state, item_reason, item_id),
            )

        post_ids: list[int] = []
        for slot, rendered, media_paths in plan.posts:
            cursor = await db.execute(
                """
                INSERT INTO social_posts
                    (video_id, platform, content, media_paths, status,
                     social_account_id, max_chars, slot_id,
                     smart_queue_item_id, scheduled_at)
                VALUES (?,?,?,?,'approved',?,?,?,?,?)
                """,
                (
                    video["id"], slot["platform"], rendered,
                    json.dumps(media_paths), slot.get("social_account_id"),
                    slot.get("max_chars"), slot.get("id"), item_id,
                    when_iso,
                ),
            )
            post_ids.append(int(cursor.lastrowid))

        # A row rather than nothing, so history can tell "never attempted" from
        # "posted and later deleted".
        for verdict in plan.skipped_slots:
            await db.execute(
                """
                INSERT INTO social_posts
                    (video_id, platform, content, status, error, slot_id,
                     smart_queue_item_id)
                VALUES (?,?,'','skipped',?,?,?)
                """,
                (video["id"], verdict.slot["platform"], verdict.reason,
                 verdict.slot.get("id"), item_id),
            )
    return item_id, post_ids


async def rerender_pending(queue_id: int, *, default_ai_system: str | None = None) -> dict:
    """Re-render the text of every still-pending post this queue owns.

    Text is rendered at Accept, which means a later template edit doesn't
    reach posts already on the schedule. This is how you push an edit onto
    them without tearing the schedule down and rebuilding it.

    Only posts that haven't been sent are touched; a posted row is history.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        """
        SELECT p.id AS post_id, p.slot_id, i.video_id
          FROM social_posts p
          JOIN smart_queue_items i ON i.id = p.smart_queue_item_id
         WHERE i.queue_id = ? AND p.status NOT IN ('posted', 'sending')
        """,
        (queue_id,),
    )
    queue = await queue_service.get_queue(queue_id)
    template = await _template_by_id(int(queue["template_id"]))
    slots_by_id = {int(s["id"]): s for s in template["slots"] if s.get("id")}

    updated, errors = 0, []
    for row in rows:
        slot = slots_by_id.get(int(row["slot_id"] or 0))
        if slot is None:
            errors.append({
                "post_id": int(row["post_id"]),
                "error": "the slot this post came from no longer exists",
            })
            continue
        video_rows = await db.execute_fetchall(
            "SELECT * FROM videos WHERE id = ?", (row["video_id"],)
        )
        if not video_rows:
            errors.append({
                "post_id": int(row["post_id"]), "error": "video no longer exists"
            })
            continue
        try:
            rendered, media_paths = await _render_slot(
                db, dict(video_rows[0]), slot, default_ai_system=default_ai_system
            )
        except _DETERMINISTIC_RENDER_FAILURES as exc:
            errors.append({"post_id": int(row["post_id"]), "error": str(exc)})
            continue
        async with write_transaction() as write_db:
            await write_db.execute(
                "UPDATE social_posts SET content = ?, media_paths = ? WHERE id = ?",
                (rendered, json.dumps(media_paths), int(row["post_id"])),
            )
        updated += 1
    return {"updated": updated, "errors": errors}


async def _pending_items_missing_slots(queue_id: int) -> tuple[list[dict], dict]:
    """Pending items paired with the enabled template slots they have no row for.

    Slot membership is fixed at Accept, so enabling a slot afterwards leaves
    every already-scheduled item without it — invisibly, because nothing
    compares the two. Returns ``(rows, slots_by_id)`` where each row carries
    ``missing_slot_ids``.
    """
    db = await get_db()
    queue = await queue_service.get_queue(queue_id)
    template = await _template_by_id(int(queue["template_id"]))
    slots_by_id = {
        int(s["id"]): s
        for s in template["slots"]
        if s.get("id") and not s.get("is_disabled")
    }

    items = await db.execute_fetchall(
        """
        SELECT i.id, i.video_id FROM smart_queue_items i
         WHERE i.queue_id = ? AND i.state = 'scheduled'
           AND NOT EXISTS (
               SELECT 1 FROM social_posts p
                WHERE p.smart_queue_item_id = i.id AND p.status = 'posted'
           )
         ORDER BY i.position
        """,
        (queue_id,),
    )

    rows: list[dict] = []
    for item in items:
        # A skipped row still counts as "this slot was considered" — re-adding
        # it would re-litigate a decision already recorded with its reason.
        existing = await db.execute_fetchall(
            "SELECT slot_id FROM social_posts WHERE smart_queue_item_id = ?",
            (int(item["id"]),),
        )
        have = {int(r["slot_id"]) for r in existing if r["slot_id"] is not None}
        missing = [sid for sid in slots_by_id if sid not in have]
        if missing:
            rows.append({
                "item_id": int(item["id"]),
                "video_id": item["video_id"],
                "missing_slot_ids": missing,
            })
    return rows, slots_by_id


async def pending_slot_gap(queue_id: int) -> dict:
    """Report, without writing anything, which pending items lack which slots.

    Lets the queue screen say so up front instead of leaving the mismatch to be
    discovered when a post that was expected never goes out.
    """
    rows, slots_by_id = await _pending_items_missing_slots(queue_id)
    platforms: dict[str, int] = {}
    for row in rows:
        for slot_id in row["missing_slot_ids"]:
            name = slots_by_id[slot_id]["platform"]
            platforms[name] = platforms.get(name, 0) + 1
    return {
        "items_missing_slots": len(rows),
        "missing_posts": sum(len(r["missing_slot_ids"]) for r in rows),
        "by_platform": platforms,
    }


async def backfill_pending_slots(
    queue_id: int, *, default_ai_system: str | None = None
) -> dict:
    """Create the posts pending items would have had, had the slots existed.

    The counterpart to re-render: that one rewrites rows that exist, this one
    adds rows that never did. Times and already-rendered text are left alone —
    a backfilled post inherits its item's ``scheduled_at``, so nothing on the
    calendar moves.

    Every slot goes through the same :func:`slots_accepting` verdicts Accept
    uses, so a slot that cannot carry this video records a ``skipped`` row with
    its reason rather than a post that would fail at send time.
    """
    from datetime import datetime as _datetime

    from yt_scheduler.services.scheduler import schedule_social_post

    db = await get_db()
    rows, slots_by_id = await _pending_items_missing_slots(queue_id)
    if not rows:
        return {"created": 0, "skipped": 0, "errors": []}

    media_hosting_configured = await media_hosting.is_configured()
    created, skipped, errors = 0, 0, []

    for row in rows:
        video_rows = await db.execute_fetchall(
            "SELECT * FROM videos WHERE id = ?", (row["video_id"],)
        )
        if not video_rows:
            errors.append({"item_id": row["item_id"], "error": "video no longer exists"})
            continue
        video = dict(video_rows[0])

        item_rows = await db.execute_fetchall(
            "SELECT scheduled_at FROM smart_queue_items WHERE id = ?",
            (row["item_id"],),
        )
        when_iso = item_rows[0]["scheduled_at"] if item_rows else None

        missing_slots = [slots_by_id[sid] for sid in row["missing_slot_ids"]]
        verdicts = slots_accepting(
            video, missing_slots, media_hosting_configured=media_hosting_configured
        )
        for verdict in verdicts:
            slot = verdict.slot
            if not verdict.accepted:
                async with write_transaction() as write_db:
                    await write_db.execute(
                        """
                        INSERT INTO social_posts
                            (video_id, platform, content, status, error, slot_id,
                             smart_queue_item_id)
                        VALUES (?,?,'','skipped',?,?,?)
                        """,
                        (video["id"], slot["platform"], verdict.reason,
                         slot.get("id"), row["item_id"]),
                    )
                skipped += 1
                continue
            try:
                rendered, media_paths = await _render_slot(
                    db, video, slot, default_ai_system=default_ai_system
                )
            except _DETERMINISTIC_RENDER_FAILURES as exc:
                errors.append({
                    "item_id": row["item_id"],
                    "platform": slot["platform"],
                    "error": str(exc),
                })
                continue
            async with write_transaction() as write_db:
                cursor = await write_db.execute(
                    """
                    INSERT INTO social_posts
                        (video_id, platform, content, media_paths, status,
                         social_account_id, max_chars, slot_id,
                         smart_queue_item_id, scheduled_at)
                    VALUES (?,?,?,?,'approved',?,?,?,?,?)
                    """,
                    (
                        video["id"], slot["platform"], rendered,
                        json.dumps(media_paths), slot.get("social_account_id"),
                        slot.get("max_chars"), slot.get("id"), row["item_id"],
                        when_iso,
                    ),
                )
                post_id = int(cursor.lastrowid)
            created += 1

            # Outside the transaction, as Accept does: registering a timer
            # writes, and a rollback cannot un-register an APScheduler job.
            # scheduled_at is already committed, so a failure here is
            # recoverable on restart rather than a post that silently never
            # goes out.
            if when_iso:
                try:
                    await schedule_social_post(post_id, _datetime.fromisoformat(when_iso))
                except Exception as exc:
                    logger.exception(
                        "backfill: could not register the timer for post %s", post_id
                    )
                    errors.append({
                        "item_id": row["item_id"], "post_id": post_id,
                        "error": (
                            f"created for {when_iso}, but its timer could not be "
                            f"registered ({type(exc).__name__}: {exc}); it will be "
                            "picked up on the next restart"
                        ),
                    })

    logger.info("backfill: queue %s created %s posts, skipped %s, %s errors",
                queue_id, created, skipped, len(errors))
    return {"created": created, "skipped": skipped, "errors": errors}


async def reflow_pending(queue_id: int) -> dict:
    """Re-stamp every pending item onto the queue's current recurrence.

    Used after the posting times change. Existing items keep their order and
    their rendered text — only *when* they go out moves, which is the whole
    point of saying yes to "re-flow existing scheduled postings?".

    Answering no simply doesn't call this: the new times then apply to items
    added from now on, and what is already on the books stays put.
    """
    # Serialized against Accept: reflow rewrites the same posting times.
    async with accept_lock(queue_id):
        return await _reflow_pending_locked(queue_id)


async def _reflow_pending_locked(queue_id: int) -> dict:
    from yt_scheduler.services.scheduler import schedule_social_post

    queue = await queue_service.get_queue(queue_id)
    zone = queue_service.resolve_timezone(queue["timezone"])
    db = await get_db()

    # An item whose posts have gone out is finished, whatever its state column
    # says — sending updates social_posts and never touches the item, so a
    # posted video sits at state='scheduled' forever. Including it here handed
    # it a fresh future occurrence and pushed every remaining video back a
    # week, once per re-flow. A partially-sent item is excluded too: some of it
    # is already public, so moving the rest is not a re-flow, it is a split.
    items = await db.execute_fetchall(
        """
        SELECT i.id FROM smart_queue_items i
         WHERE i.queue_id = ? AND i.state = 'scheduled'
           AND NOT EXISTS (
               SELECT 1 FROM social_posts p
                WHERE p.smart_queue_item_id = i.id AND p.status = 'posted'
           )
         ORDER BY i.position
        """,
        (queue_id,),
    )
    if not items:
        return {"reflowed": 0}

    instants = queue_service.occurrences(queue["slots"], zone, len(items))
    for item, when in zip(items, instants):
        async with write_transaction() as write_db:
            await write_db.execute(
                "UPDATE smart_queue_items SET scheduled_at = ? WHERE id = ?",
                (when.isoformat(), int(item["id"])),
            )
        posts = await db.execute_fetchall(
            # 'approved' ONLY. NOT IN ('posted','sending') also catches
            # 'skipped' (empty content — e.g. Threads with no hosting) and
            # 'failed'. Re-flow would schedule the empty one, and would lift
            # the failed one out of the app-wide failed-sends banner without
            # anything ever retrying it — a surfaced failure disappearing.
            "SELECT id FROM social_posts "
            "WHERE smart_queue_item_id = ? AND status = 'approved'",
            (int(item["id"]),),
        )
        for post in posts:
            await schedule_social_post(int(post["id"]), when)
    return {"reflowed": len(items)}


async def _template_by_id(template_id: int) -> dict:
    """Load a template with its slots, by id.

    ``templates.get_template`` is keyed by name; a queue holds an id, and the
    name can change under it.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, name, project_id FROM templates WHERE id = ?", (template_id,)
    )
    if not rows:
        raise queue_service.SmartQueueError(f"Template {template_id} not found")
    template = dict(rows[0])
    template["slots"] = await tmpl.list_slots(template_id)
    return template


def _render_input_hash(
    *,
    video_id: str,
    slot_id: int,
    cleaned_body: str,
    variables: dict[str, object],
    default_ai_system: str | None,
    media_paths: list[str],
    model: str,
) -> str:
    """Fingerprint everything that decides one slot's rendered output.

    A checkpoint is reused only when the current inputs hash to the stored
    value, so this must cover EVERY input the render depends on — miss on any
    change, never a false hit that ships text rendered from since-edited
    inputs. Variable values are coerced to text exactly as the renderer coerces
    them (``str(value)``; ``None`` distinct from ``""``) so two contexts that
    render identically fingerprint identically. ``media_paths`` is positional,
    so its order is part of the key. ``model`` is the effective Anthropic model
    (see :func:`_render_slot`): a model change alters ``{{ai:}}`` output, so it
    must not read as unchanged.
    """
    payload = {
        "video_id": video_id,
        "slot_id": slot_id,
        "cleaned_body": cleaned_body,
        "variables": {
            key: (None if value is None else str(value))
            for key, value in variables.items()
        },
        "default_ai_system": default_ai_system,
        "media_paths": list(media_paths),
        "model": model,
    }
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


async def _load_render_checkpoint(
    db, video_id: str, slot_id: int
) -> dict | None:
    """The stored render for this (video, slot), or None if none exists yet."""
    rows = await db.execute_fetchall(
        "SELECT input_hash, content, media_paths_json FROM render_checkpoint "
        "WHERE video_id = ? AND slot_id = ?",
        (video_id, slot_id),
    )
    if not rows:
        return None
    row = rows[0]
    return {
        "input_hash": row["input_hash"],
        "content": row["content"],
        "media_paths": json.loads(row["media_paths_json"]),
    }


async def _save_render_checkpoint(
    video_id: str, slot_id: int, input_hash: str, content: str,
    media_paths: list[str],
) -> None:
    """Record (or replace) the render for this (video, slot) as one unit.

    One row per (video, slot): a re-render for changed inputs overwrites the
    prior fingerprint rather than accumulating rows, so the checkpoint can only
    ever hand back the most recent successful render for the current slot.
    """
    async with write_transaction() as db:
        await db.execute(
            "INSERT INTO render_checkpoint "
            "(video_id, slot_id, input_hash, content, media_paths_json) "
            "VALUES (?,?,?,?,?) "
            "ON CONFLICT(video_id, slot_id) DO UPDATE SET "
            "input_hash = excluded.input_hash, content = excluded.content, "
            "media_paths_json = excluded.media_paths_json, "
            "created_at = datetime('now')",
            (video_id, slot_id, input_hash, content, json.dumps(media_paths)),
        )


def _all_media_present(media_paths: list[str]) -> bool:
    """Whether every media file a cached render referenced still exists.

    A render cached with a media path whose file was since cleaned up must miss
    — reusing it would schedule a post pointing at a file that has gone.
    """
    return all(os.path.exists(path) for path in media_paths)


async def _render_slot(
    db, video: dict, slot: dict, *, default_ai_system: str | None
) -> tuple[str, list[str]]:
    """Render one slot's body for one video, returning (text, media paths).

    Directive extraction is a PRE-pass, matching the generate-posts path:
    ``{{video}}`` and friends are media directives, not variables, so they
    have to come out of the body before the strict renderer sees it — left in,
    they read as undefined variables and the render fails.

    A render can fire a paid Anthropic ``{{ai:}}`` round-trip, so a per-slot
    checkpoint lets a retry reuse a prior success instead of re-paying: this is
    the ONE place every render path (Accept, backfill, re-render, and the
    reconcile handlers) flows through, so the checkpoint sits here and covers
    all of them. Reuse is gated on the inputs hashing to the stored value AND
    the referenced media still existing — never a stale or dangling reuse. On a
    miss the render happens normally and its result replaces the checkpoint.
    """
    context = await build_render_context(db, video)
    body = slot.get("body") or ""
    cleaned_body, media_paths, _alts = tmpl.extract_media_directives(
        body,
        video_path=context["video_path"],
        thumbnail_path=context["thumb_path"],
        images=context["images"],
    )

    video_id = video["id"]
    raw_slot_id = slot.get("id")
    # slot_id is half the checkpoint key. A slot with no id can't be cached
    # without two such slots colliding on one row and serving each other's
    # text, so it simply re-renders — costs a call, never wrong.
    slot_id = int(raw_slot_id) if raw_slot_id is not None else None

    input_hash: str | None = None
    if slot_id is not None:
        # The effective model resolved the same way call_ai_block resolves it,
        # so the fingerprint reflects the model the render would actually use
        # rather than a second, possibly-disagreeing source.
        model = await ai._resolve_model()
        input_hash = _render_input_hash(
            video_id=video_id, slot_id=slot_id, cleaned_body=cleaned_body,
            variables=context["variables"], default_ai_system=default_ai_system,
            media_paths=media_paths, model=model,
        )
        cached = await _load_render_checkpoint(db, video_id, slot_id)
        if (
            cached is not None
            and cached["input_hash"] == input_hash
            and _all_media_present(media_paths)
        ):
            return cached["content"], media_paths

    rendered = await tmpl.async_render(
        cleaned_body, context["variables"], default_system_prompt=default_ai_system
    )
    result_text = rendered.strip()

    if slot_id is not None and input_hash is not None:
        await _save_render_checkpoint(
            video_id, slot_id, input_hash, result_text, media_paths
        )
    return result_text, media_paths
