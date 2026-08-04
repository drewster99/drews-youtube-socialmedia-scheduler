"""Smart queue CRUD, candidate preview, and scheduling.

Everything except ``/accept`` and ``/re-render`` is read-only with respect to
the posting schedule — the preview in particular writes nothing, so the config
screen can explore filter changes freely.
"""

from __future__ import annotations

import logging
import random
import sqlite3

from fastapi import APIRouter, HTTPException

from yt_scheduler.database import get_db
from yt_scheduler.services import projects as project_service
from yt_scheduler.services import smart_queue as smart_queue_service
from yt_scheduler.services import smart_queue_accept
from yt_scheduler.services import smart_queue_disposition
from yt_scheduler.services.smart_queue_accept import accept_lock
from yt_scheduler.services import smart_queue_reconcile
from yt_scheduler.services.smart_queue import SmartQueueError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/projects/{slug}/smart-queues", tags=["smart-queues"])


async def _project_or_404(slug: str) -> dict:
    project = await project_service.get_project_by_slug(slug)
    if not project:
        raise HTTPException(404, f"Project {slug!r} not found")
    return project


async def _queue_in_project_or_404(slug: str, queue_id: int) -> dict:
    project = await _project_or_404(slug)
    try:
        queue = await smart_queue_service.get_queue(queue_id)
    except SmartQueueError as exc:
        raise HTTPException(404, str(exc)) from exc
    if queue["project_id"] != project["id"]:
        # Reported as not-found rather than forbidden: a queue id belonging to
        # another project should not be confirmed to exist by the error.
        raise HTTPException(404, f"Smart queue {queue_id} not found in {slug!r}")
    return queue


def _integrity_message(exc: sqlite3.IntegrityError, data: dict) -> str:
    """Turn a constraint violation into the message that actually applies.

    Every IntegrityError used to render as "already has a schedule named X",
    which is wrong for a NOT NULL violation and wrong for a duplicate slot —
    and sends the user to fix a field that isn't the problem.
    """
    text = str(exc)
    if "NOT NULL" in text and "max_duration_seconds" in text:
        return (
            "Maximum clip length is required — enter a number of seconds. "
            "(Leaving it blank to mean \"no maximum\" isn't supported yet.)"
        )
    if "NOT NULL" in text:
        column = text.rsplit(".", 1)[-1].strip()
        return f"{column} is required."
    if "UNIQUE" in text and "smart_queue_slots" in text:
        return "That weekday and time is already a slot on this schedule."
    if "UNIQUE" in text:
        name = (data.get("name") or "").strip()
        return f"This project already has a smart schedule named {name!r}."
    return f"That change conflicts with an existing record: {exc}"


async def _require_not_reconciling(queue_id: int) -> None:
    """Refuse schedule mutations while reconciliation owns this queue.

    Not only PATCH: Accept, re-flow, re-render and backfill all decide what
    posts a pending item should have, which is exactly what the worker is
    deciding. Whichever writes second wins silently, and the losing decision is
    invisible.
    """
    if await smart_queue_reconcile.queue_is_locked(queue_id):
        # Deliberately doesn't say "a template change": the same jobs are
        # queued by Re-render and Add missing slots, and blaming a template
        # edit the user didn't make sends them looking for the wrong thing.
        raise HTTPException(
            409,
            "This schedule's posts are still being updated in the background. "
            "Watch the banner at the top of the page, then try again.",
        )


@router.get("")
async def list_smart_queues(slug: str):
    """Every smart queue in this project, with slots and item-state counts."""
    project = await _project_or_404(slug)
    return {"queues": await smart_queue_service.list_queues(project["id"])}


@router.get("/{queue_id}")
async def get_smart_queue(slug: str, queue_id: int):
    """The queue, plus whether reconciliation currently has it locked.

    Reading is always allowed — refusing to show a queue mid-reconcile would
    hide exactly the thing the user wants to watch. Saving is what's blocked.
    """
    queue = await _queue_in_project_or_404(slug, queue_id)
    queue["reconcile_locked"] = await smart_queue_reconcile.queue_is_locked(queue_id)
    return queue


@router.post("")
async def create_smart_queue(slug: str, data: dict):
    """Create a smart queue.

    Body: ``name``, ``template_id``, ``timezone`` (IANA), ``slots``
    (``[{"weekday": 0-6, "time_of_day": "HH:MM"}]``), and optionally
    ``min_duration_seconds``, ``max_duration_seconds``, ``orientations``,
    ``exclude_already_posted``, ``auto_add_on_live``, ``missed_policy``,
    ``missed_grace_hours``.
    """
    project = await _project_or_404(slug)
    # Only forward keys the client actually sent, so create_queue's documented
    # creation defaults apply to the rest. Passing an explicit None for an
    # absent key would override those defaults and then fail their validation.
    optional: dict = {}
    if "min_duration_seconds" in data:
        optional["min_duration_seconds"] = float(data["min_duration_seconds"] or 0)
    if "max_duration_seconds" in data:
        optional["max_duration_seconds"] = (
            None if data["max_duration_seconds"] is None
            else float(data["max_duration_seconds"])
        )
    if data.get("orientations") is not None:
        optional["orientations"] = data["orientations"]
    # Same coercion PATCH applies, so the two routes cannot disagree about what
    # counts as a boolean.
    if "exclude_already_posted" in data:
        optional["exclude_already_posted"] = smart_queue_service.require_boolean(
            "exclude_already_posted", data["exclude_already_posted"]
        )
    if "auto_add_on_live" in data:
        optional["auto_add_on_live"] = smart_queue_service.require_boolean(
            "auto_add_on_live", data["auto_add_on_live"]
        )
    if data.get("missed_policy"):
        optional["missed_policy"] = data["missed_policy"]
    if data.get("missed_grace_hours") is not None:
        optional["missed_grace_hours"] = int(data["missed_grace_hours"])

    try:
        queue_id = await smart_queue_service.create_queue(
            project_id=project["id"],
            name=data.get("name") or "",
            template_id=int(data["template_id"]),
            timezone_name=data.get("timezone") or "",
            slots=data.get("slots") or [],
            **optional,
        )
    except SmartQueueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except sqlite3.IntegrityError as exc:
        raise HTTPException(400, _integrity_message(exc, data)) from exc
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(400, f"Invalid smart queue payload: {exc}") from exc
    return await smart_queue_service.get_queue(queue_id)


@router.patch("/{queue_id}")
async def update_smart_queue(slug: str, queue_id: int, data: dict):
    """Partial update. ``slots``, when present, replaces the whole set."""
    await _queue_in_project_or_404(slug, queue_id)
    await _require_not_reconciling(queue_id)
    try:
        await smart_queue_service.update_queue(queue_id, data)
    except SmartQueueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except sqlite3.IntegrityError as exc:
        # Previously absent entirely, so clearing "max duration" (NOT NULL in
        # the schema, None everywhere above it) produced a raw 500.
        raise HTTPException(400, _integrity_message(exc, data)) from exc
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(400, f"Invalid smart queue payload: {exc}") from exc
    return await smart_queue_service.get_queue(queue_id)


@router.delete("/{queue_id}")
async def delete_smart_queue(slug: str, queue_id: int):
    """Delete a queue. Cancels everything pending; keeps all posting history."""
    await _queue_in_project_or_404(slug, queue_id)
    await _require_not_reconciling(queue_id)
    cancelled = await smart_queue_service.delete_queue(queue_id)
    return {"deleted": True, "cancelled_posts": cancelled}


@router.post("/{queue_id}/candidates")
async def preview_candidates(slug: str, queue_id: int, data: dict | None = None):
    """Which videos this queue would take, and what it excluded and why.

    Body (all optional) overrides the saved filters for this preview only, so
    the config screen can show the effect of a change before saving:
    ``min_duration_seconds``, ``max_duration_seconds``, ``orientations``,
    ``exclude_already_posted``, and ``shuffle`` (bool).

    Nothing is written. Shuffling here only reorders the proposed batch — it
    never touches items already scheduled by this queue.
    """
    queue = await _queue_in_project_or_404(slug, queue_id)
    overrides = data or {}
    for key in (
        "min_duration_seconds", "max_duration_seconds",
        "orientations", "exclude_already_posted",
    ):
        if key in overrides and overrides[key] is not None:
            queue[key] = overrides[key]

    try:
        result = await smart_queue_service.candidate_videos(queue)
    except SmartQueueError as exc:
        raise HTTPException(400, str(exc)) from exc

    eligible = result["eligible"]
    if overrides.get("shuffle"):
        random.shuffle(eligible)

    forecast = []
    warnings = []
    try:
        # The same helper Accept uses. Computing this from *now* instead made
        # the forecast promise dates Accept would never use as soon as the
        # queue had anything pending.
        instants = await smart_queue_service.next_free_posting_times(
            queue, len(eligible)
        )
        forecast = [dt.isoformat() for dt in instants]
    except SmartQueueError as exc:
        # A queue with no posting times can still show its candidate list; it
        # just can't say when they would go out. Surface it rather than
        # rendering an empty forecast that looks like "nothing scheduled".
        warnings.append(str(exc))

    by_type: dict[str, int] = {}
    for video in eligible:
        by_type[video["item_type"]] = by_type.get(video["item_type"], 0) + 1

    # Items auto-add already put in the queue with no posting time. They are
    # not candidates (they're in the queue already), but Accept schedules them
    # first, so the screen has to be able to say they exist.
    db = await get_db()
    waiting_rows = await db.execute_fetchall(
        "SELECT COUNT(*) AS n FROM smart_queue_items WHERE queue_id = ? AND state = ?",
        (queue_id, smart_queue_service.ITEM_STATE_QUEUED),
    )

    return {
        "waiting": int(waiting_rows[0]["n"]),
        "eligible": eligible,
        "excluded": result["excluded"],
        "unknown_dimensions": result["unknown_dimensions"],
        "summary": {"total": len(eligible), "by_type": by_type},
        "forecast": forecast,
        "ends_at": forecast[-1] if forecast else None,
        "warnings": warnings,
    }


@router.post("/{queue_id}/accept")
async def accept_selection(slug: str, queue_id: int, data: dict):
    """Schedule the given videos, in the order given, onto this queue.

    **Body** — ``video_ids``: the batch to schedule, in the order the user is
    looking at (shuffled or not). The order is the caller's; this endpoint
    does not re-sort it.

    Renders each template slot now and writes ordinary ``social_posts`` rows,
    so a later template edit does not reach them — use ``/re-render`` for
    that. Slots that cannot carry a video record a ``skipped`` row with the
    reason rather than being silently absent.

    Items already scheduled by this queue are untouched; new times continue
    after the last one on the books.
    """
    queue = await _queue_in_project_or_404(slug, queue_id)
    await _require_not_reconciling(queue_id)
    video_ids = data.get("video_ids") or []
    if not isinstance(video_ids, list):
        raise HTTPException(400, "video_ids must be a list.")
    # An empty list is legitimate: it means "schedule whatever is already
    # waiting in this queue", which is what auto-add fills.

    try:
        result = await smart_queue_accept.accept_selection(
            queue_id, [str(v) for v in video_ids],
            default_ai_system=await _default_ai_system(queue["project_id"]),
        )
    except SmartQueueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return result


@router.post("/{queue_id}/re-flow")
async def reflow_pending(slug: str, queue_id: int):
    """Re-stamp every pending item onto the queue's current posting times.

    Call after changing the recurrence, when the user answers yes to "re-flow
    existing scheduled postings?". Answering no means simply not calling this:
    the new times then apply only to items added from now on.

    Order and rendered text are preserved — only *when* each item goes out
    moves.
    """
    await _queue_in_project_or_404(slug, queue_id)
    await _require_not_reconciling(queue_id)
    try:
        return await smart_queue_accept.reflow_pending(queue_id)
    except SmartQueueError as exc:
        raise HTTPException(400, str(exc)) from exc


async def _enabled_slot_ids(queue: dict) -> list[int]:
    """Every enabled slot on this queue's template.

    Manual re-render and backfill are the whole-template cases of the same two
    jobs a template edit queues, so they reuse those handlers rather than
    keeping a second implementation that can drift.
    """
    from yt_scheduler.services.smart_queue_accept import _template_by_id

    template = await _template_by_id(int(queue["template_id"]))
    return [
        int(slot["id"]) for slot in template["slots"]
        if slot.get("id") and not slot.get("is_disabled")
    ]


@router.post("/{queue_id}/re-render")
async def rerender_pending(slug: str, queue_id: int):
    """Queue a re-render of every still-pending post this queue owns.

    Returns as soon as the job is queued — one AI round-trip per post is
    minutes of work that must not hold the request open or die with the
    browser tab. Watch it in the app-wide banner; progress is at
    ``GET /api/reconcile-status``. Posted rows are history and are left alone.

    **Response 200** — ``{"queued": true, "jobs": N}``.
    """
    queue = await _queue_in_project_or_404(slug, queue_id)
    await _require_not_reconciling(queue_id)
    slot_ids = await _enabled_slot_ids(queue)
    if not slot_ids:
        return {"queued": False, "jobs": 0}
    ids = await smart_queue_reconcile.enqueue_jobs(queue_id, [{
        "kind": smart_queue_reconcile.KIND_SLOT_BODY_CHANGED,
        "payload": {"slot_ids": slot_ids},
    }])
    return {"queued": True, "jobs": len(ids)}


@router.post("/{queue_id}/reconcile-jobs/{job_id}/dismiss")
async def dismiss_reconcile_failure(slug: str, queue_id: int, job_id: int):
    """Acknowledge a failed reconciliation so it leaves the app-wide banner.

    **Response 200** — ``{"status": "ok"}``.
    """
    await _queue_in_project_or_404(slug, queue_id)
    dismissed = await smart_queue_reconcile.dismiss_failed(job_id, queue_id)
    if not dismissed:
        raise HTTPException(
            404,
            f"No failed reconcile job {job_id} on this schedule to dismiss.",
        )
    return {"status": "ok"}


@router.get("/{queue_id}/slot-gap")
async def pending_slot_gap(slug: str, queue_id: int):
    """Which pending items lack which of the template's enabled slots.

    Slot membership is fixed at Accept, so enabling a slot afterwards leaves
    already-scheduled items without it. Nothing else compares the two, which
    makes the mismatch invisible until a post you expected never goes out.

    **Response 200** — ``{"items_missing_slots": N, "missing_posts": N,
    "by_platform": {"twitter": N}}``. Writes nothing.
    """
    await _queue_in_project_or_404(slug, queue_id)
    return await smart_queue_accept.pending_slot_gap(queue_id)


@router.post("/{queue_id}/backfill-slots")
async def backfill_pending_slots(slug: str, queue_id: int):
    """Create the posts pending items would have had, had the slots existed.

    Counterpart to re-render: that rewrites rows that exist, this adds rows
    that never did. Existing text and every ``scheduled_at`` are left alone —
    a backfilled post inherits its item's time, so nothing on the calendar
    moves.

    Queued rather than run inline, for the same reason as re-render: it renders
    N posts with an AI round-trip apiece. Watch it in the app-wide banner.

    **Response 200** — ``{"queued": true, "jobs": N}``. The ``slots_added``
    handler skips any slot an item already has, so this is safe to repeat.
    """
    queue = await _queue_in_project_or_404(slug, queue_id)
    await _require_not_reconciling(queue_id)
    slot_ids = await _enabled_slot_ids(queue)
    if not slot_ids:
        return {"queued": False, "jobs": 0}
    ids = await smart_queue_reconcile.enqueue_jobs(queue_id, [{
        "kind": smart_queue_reconcile.KIND_SLOTS_ADDED,
        "payload": {"slot_ids": slot_ids},
    }])
    return {"queued": True, "jobs": len(ids)}


async def _default_ai_system(project_id: int) -> str:
    """The project's editable default system prompt for ``{{ai: …}}`` blocks,
    so a queue render honours the same setting the generate path does."""
    from yt_scheduler.services import prompts as prompt_service

    resolved = await prompt_service.get_prompt_with_fallback(
        "ai_block_default_system_prompt", project_id=project_id
    )
    return resolved["system"]


@router.get("/{queue_id}/missed")
async def list_missed(slug: str, queue_id: int):
    """Posts this queue owns that didn't go out and need a decision.

    Derived at read time from ``scheduled_at`` being in the past — there is no
    stored "missed" flag and no background sweeper, so this is always accurate
    without anything keeping it up to date.

    ``within_grace`` says whether the queue's post-late window still covers it,
    so the UI can present "post now" as the expected action rather than an
    override.
    """
    queue = await _queue_in_project_or_404(slug, queue_id)
    items = await smart_queue_disposition.missed_items(queue_id)
    for item in items:
        # A failed post has had scheduled_at cleared, so fall back to the
        # intent that survives it — otherwise every failure reported as outside
        # the post-late window no matter how recently it failed.
        item["within_grace"] = smart_queue_disposition.within_grace(
            queue, item["scheduled_at"] or item["intended_at"]
        )
    return {
        "missed": items,
        "missed_policy": queue["missed_policy"],
        "missed_grace_hours": queue["missed_grace_hours"],
    }


@router.post("/{queue_id}/missed/{post_id}")
async def dispose_missed(slug: str, queue_id: int, post_id: int, data: dict):
    """Act on one missed post.

    **Body** — ``action``: ``post_now`` | ``reschedule_end`` | ``remove``.

    Per post, not per queue item: one platform failing shouldn't drag the
    others with it, and the right answer can differ per platform.
    """
    await _queue_in_project_or_404(slug, queue_id)
    # Guarded like every other queue mutation: reschedule-to-end reads
    # MAX(position) and the next free slot, so running it beside an Accept or
    # a reconcile sweep double-books.
    await _require_not_reconciling(queue_id)
    try:
        async with accept_lock(queue_id):
            return await smart_queue_disposition.dispose(
                queue_id, post_id, (data or {}).get("action") or ""
            )
    except smart_queue_disposition.PostNoLongerDisposable as exc:
        # 409, not 400: the request was valid when the page rendered.
        raise HTTPException(409, str(exc)) from exc
    except SmartQueueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/{queue_id}/activity")
async def queue_activity(slug: str, queue_id: int, limit: int = 10):
    """What this queue is about to post and what it recently posted.

    Drives the expandable panel on the project dashboard. ``upcoming`` is the
    real posting state — per-platform rows from ``social_posts`` — rather than
    the queue item alone, so a video whose Mastodon slot failed while Bluesky
    succeeded reads correctly instead of as one ambiguous line.
    """
    await _queue_in_project_or_404(slug, queue_id)
    db = await get_db()
    base = """
        SELECT p.id, p.platform, p.status, p.scheduled_at, p.posted_at,
               p.post_url, p.error, v.title, v.id AS video_id
          FROM social_posts p
          JOIN smart_queue_items i ON i.id = p.smart_queue_item_id
          JOIN videos v ON v.id = i.video_id
         WHERE i.queue_id = ?
    """
    upcoming = await db.execute_fetchall(
        # 'failed' is recent activity, not upcoming work — it won't auto-post
        # from here (it lives in the failed-sends banner for manual action), so
        # excluding it stops a failed post showing in BOTH buckets at once.
        base + " AND p.status NOT IN ('posted','skipped','failed')"
        " ORDER BY p.scheduled_at IS NULL, p.scheduled_at LIMIT ?",
        (queue_id, limit),
    )
    recent = await db.execute_fetchall(
        base + " AND p.status IN ('posted','failed','skipped')"
        " ORDER BY COALESCE(p.posted_at, p.scheduled_at) DESC LIMIT ?",
        (queue_id, limit),
    )
    return {
        "upcoming": [dict(r) for r in upcoming],
        "recent": [dict(r) for r in recent],
    }


@router.get("/{queue_id}/items")
async def list_queue_items(slug: str, queue_id: int, state: str | None = None):
    """This queue's items — its full history, newest schedule first.

    ``state`` filters to one of scheduled/posted/failed/skipped/removed.
    """
    await _queue_in_project_or_404(slug, queue_id)
    db = await get_db()
    clause, params = "", [queue_id]
    if state:
        if state not in smart_queue_service.ITEM_STATES:
            raise HTTPException(
                400,
                f"Unknown state {state!r}; expected one of "
                f"{', '.join(smart_queue_service.ITEM_STATES)}",
            )
        clause = " AND i.state = ?"
        params.append(state)
    # has_posted and has_pending are both derived, not read from i.state:
    # sending updates social_posts and never the item, so state alone would
    # present a video that has already gone out as still upcoming.
    rows = await db.execute_fetchall(
        f"""
        SELECT i.id, i.video_id, i.position, i.scheduled_at, i.state,
               i.reason, i.added_at, v.title, v.item_type, v.duration_seconds,
               (i.state = 'posted' OR EXISTS (
                   SELECT 1 FROM social_posts p
                    WHERE p.smart_queue_item_id = i.id AND p.status = 'posted'
               )) AS has_posted,
               -- "Is anything still due?" is a different question from "has
               -- anything gone out?", and only the first one decides whether an
               -- item is upcoming. They agree until a send lands partially --
               -- one platform posts, another fails and is retried -- and then
               -- has_posted alone hides an item that still holds live timers.
               EXISTS (
                   SELECT 1 FROM social_posts p
                    WHERE p.smart_queue_item_id = i.id
                      AND p.status NOT IN ('posted', 'skipped')
               ) AS has_pending
          FROM smart_queue_items i
          JOIN videos v ON v.id = i.video_id
         WHERE i.queue_id = ?{clause}
         ORDER BY i.position
        """,
        params,
    )
    return {"items": [dict(r) for r in rows]}
