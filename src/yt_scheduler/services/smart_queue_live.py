"""The one place a video becoming live reaches the smart queues.

Several code paths make a video live — the publish timer firing, a manual
privacy change, importing something already public. Each calls
:func:`on_video_became_live`, so there is a single funnel rather than one
auto-add implementation per path that could drift.

Eligibility is not decided here either: it comes from
``smart_queue.is_eligible``, the same function the config screen's
Auto-select uses. Two implementations would eventually disagree about which
videos belong in a queue.
"""

from __future__ import annotations

import logging

from yt_scheduler.database import get_db, write_transaction
from yt_scheduler.services import smart_queue as queue_service

logger = logging.getLogger(__name__)


async def on_video_became_live(video_id: str) -> dict:
    """Consider ``video_id`` for every auto-add queue in its project.

    Idempotent by design: a video whose decision has already been taken is
    left alone, so public → unlisted → public does not add it a second time.

    Returns ``{"considered": bool, "added_to": [queue_id, ...],
    "reasons": {queue_id: [...]}}``. ``considered`` is False when the decision
    was already made previously, or when it could not be made at all.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM videos WHERE id = ?", (video_id,)
    )
    if not rows:
        logger.warning("on_video_became_live: video %s not found", video_id)
        return {"considered": False, "added_to": [], "reasons": {}}
    video = dict(rows[0])

    if video.get("auto_add_considered_at"):
        # Already decided once. A later flip back to public is not news.
        return {"considered": False, "added_to": [], "reasons": {}}

    project_id = video.get("project_id")
    if not project_id:
        logger.warning(
            "on_video_became_live: video %s has no project_id; skipping", video_id
        )
        return {"considered": False, "added_to": [], "reasons": {}}

    pairs = await queue_service.auto_add_queues(int(project_id))
    if not pairs:
        # No queue wants it, so no decision was actually taken — leave the
        # marker unset so a queue created later still sees this video the
        # next time it genuinely goes live.
        return {"considered": False, "added_to": [], "reasons": {}}

    # Dimensions drive the orientation filter and may not have been probed
    # yet for a freshly-imported video. Fill them in before deciding, or the
    # answer would be "not eligible" for a reason that isn't true.
    await _ensure_dimensions_best_effort(video)
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    video = dict(rows[0])

    added_to: list[int] = []
    reasons: dict[int, list[str]] = {}
    decidable = True
    for queue, applies_to in pairs:
        verdict = queue_service.is_eligible(video, queue, applies_to)
        if verdict.ok:
            if await _append_to_queue(int(queue["id"]), video_id):
                added_to.append(int(queue["id"]))
            continue
        reasons[int(queue["id"])] = list(verdict.reasons)
        if any(_is_undecidable(reason) for reason in verdict.reasons):
            decidable = False

    if not decidable:
        # Don't burn the marker on an answer we couldn't actually give: the
        # video would never be reconsidered once the missing data arrives.
        logger.info(
            "Video %s not yet decidable for auto-add (%s); leaving it unmarked",
            video_id, reasons,
        )
        return {"considered": False, "added_to": added_to, "reasons": reasons}

    async with write_transaction() as write_db:
        await write_db.execute(
            "UPDATE videos SET auto_add_considered_at = datetime('now') WHERE id = ?",
            (video_id,),
        )
    if added_to:
        logger.info("Video %s auto-added to smart queue(s) %s", video_id, added_to)
    return {"considered": True, "added_to": added_to, "reasons": reasons}


# Reasons that mean "we could not tell", as opposed to "the answer is no".
# Matched on the phrases is_eligible produces for missing data.
_UNDECIDABLE_MARKERS = ("unknown",)


def _is_undecidable(reason: str) -> bool:
    return any(marker in reason for marker in _UNDECIDABLE_MARKERS)


async def _ensure_dimensions_best_effort(video: dict) -> None:
    from yt_scheduler.services.video_dimensions import ensure_dimensions

    if video.get("width") and video.get("height"):
        return
    try:
        await ensure_dimensions(video["id"])
    except Exception:
        # Leaving them unknown is handled above (the video stays unmarked and
        # is reconsidered later); a probe failure must not break publishing.
        logger.exception("Could not probe dimensions for video %s", video["id"])


async def _append_to_queue(queue_id: int, video_id: str) -> bool:
    """Append to the tail as a queued item. Returns False if it's already
    pending there.

    No time is stamped, and the state is `queued` rather than `scheduled` —
    the distinction is load-bearing. `scheduled` means "has a posting time";
    writing it here produced an item Accept could never reach (candidates
    exclude it, and Accept only ever inserted new rows), so an auto-added
    video was queued forever and never posted. Accept promotes `queued` items
    to `scheduled`, which keeps one path for "how does an item get a time".
    """
    db = await get_db()
    placeholders = ",".join("?" for _ in queue_service.PENDING_ITEM_STATES)
    existing = await db.execute_fetchall(
        f"SELECT 1 FROM smart_queue_items "
        f"WHERE queue_id = ? AND video_id = ? "
        f"AND state IN ({placeholders})",
        (queue_id, video_id, *queue_service.PENDING_ITEM_STATES),
    )
    if existing:
        return False
    async with write_transaction() as write_db:
        await write_db.execute(
            """
            INSERT INTO smart_queue_items (queue_id, video_id, position, state)
            SELECT ?, ?, COALESCE(MAX(position), -1) + 1, ?
              FROM smart_queue_items WHERE queue_id = ?
            """,
            (queue_id, video_id, queue_service.ITEM_STATE_QUEUED, queue_id),
        )
    return True
