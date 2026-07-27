"""Smart queue CRUD and candidate preview.

Scheduling itself (Accept) lands in a later phase; everything here is safe to
call without committing anything to a posting schedule.
"""

from __future__ import annotations

import logging
import random
import sqlite3

from fastapi import APIRouter, HTTPException

from yt_scheduler.database import get_db
from yt_scheduler.services import projects as project_service
from yt_scheduler.services import smart_queue as smart_queue_service
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


@router.get("")
async def list_smart_queues(slug: str):
    """Every smart queue in this project, with slots and item-state counts."""
    project = await _project_or_404(slug)
    return {"queues": await smart_queue_service.list_queues(project["id"])}


@router.get("/{queue_id}")
async def get_smart_queue(slug: str, queue_id: int):
    return await _queue_in_project_or_404(slug, queue_id)


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
    if "exclude_already_posted" in data:
        optional["exclude_already_posted"] = bool(data["exclude_already_posted"])
    if "auto_add_on_live" in data:
        optional["auto_add_on_live"] = bool(data["auto_add_on_live"])
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
        # The only uniqueness constraint here is (project_id, name).
        raise HTTPException(
            400,
            f"This project already has a smart schedule named "
            f"{(data.get('name') or '').strip()!r}.",
        ) from exc
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(400, f"Invalid smart queue payload: {exc}") from exc
    return await smart_queue_service.get_queue(queue_id)


@router.patch("/{queue_id}")
async def update_smart_queue(slug: str, queue_id: int, data: dict):
    """Partial update. ``slots``, when present, replaces the whole set."""
    await _queue_in_project_or_404(slug, queue_id)
    try:
        await smart_queue_service.update_queue(queue_id, data)
    except SmartQueueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except (TypeError, ValueError) as exc:
        raise HTTPException(400, f"Invalid smart queue payload: {exc}") from exc
    return await smart_queue_service.get_queue(queue_id)


@router.delete("/{queue_id}")
async def delete_smart_queue(slug: str, queue_id: int):
    """Delete a queue. Cancels everything pending; keeps all posting history."""
    await _queue_in_project_or_404(slug, queue_id)
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
        zone = smart_queue_service.resolve_timezone(queue["timezone"])
        instants = smart_queue_service.occurrences(
            queue["slots"], zone, len(eligible)
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

    return {
        "eligible": eligible,
        "excluded": result["excluded"],
        "unknown_dimensions": result["unknown_dimensions"],
        "summary": {"total": len(eligible), "by_type": by_type},
        "forecast": forecast,
        "ends_at": forecast[-1] if forecast else None,
        "warnings": warnings,
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
    rows = await db.execute_fetchall(
        f"""
        SELECT i.id, i.video_id, i.position, i.scheduled_at, i.state,
               i.reason, i.added_at, v.title, v.item_type, v.duration_seconds
          FROM smart_queue_items i
          JOIN videos v ON v.id = i.video_id
         WHERE i.queue_id = ?{clause}
         ORDER BY i.position
        """,
        params,
    )
    return {"items": [dict(r) for r in rows]}
