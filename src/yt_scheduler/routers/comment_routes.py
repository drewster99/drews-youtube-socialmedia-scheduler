"""Project-wide YouTube comment endpoints.

Reads come from the local mirror (``youtube_comments``), so the dashboard
renders without a YouTube round trip. The one endpoint that does talk to
YouTube is the explicit refresh.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from yt_scheduler.services import comments as comments_service
from yt_scheduler.services import projects as project_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/projects/{slug}/comments", tags=["comments"])


async def _project_or_404(slug: str) -> dict:
    project = await project_service.get_project_by_slug(slug)
    if project is None:
        raise HTTPException(404, f"Project '{slug}' not found")
    return project


@router.get("")
async def list_comment_threads(slug: str, limit: int = 10, offset: int = 0) -> dict:
    """Stored comment threads for a project, most recently active first.

    ``limit`` and ``offset`` count THREADS, not comments: paging by comment
    split a conversation across the page boundary, which is what made a reply
    sort away from the comment it answered.

    Bad paging values are refused rather than clamped: a silently corrected
    limit returns a page the caller did not ask for and looks like the list
    simply ended.
    """
    if limit < 1 or limit > comments_service.MAX_THREADS_PER_PAGE:
        raise HTTPException(
            400,
            f"limit must be between 1 and "
            f"{comments_service.MAX_THREADS_PER_PAGE}, got {limit}",
        )
    if offset < 0:
        raise HTTPException(400, f"offset cannot be negative, got {offset}")

    project = await _project_or_404(slug)
    project_id = int(project["id"])
    return {
        "threads": await comments_service.list_recent_threads(
            project_id, limit=limit, offset=offset
        ),
        "total_threads": await comments_service.count_threads(project_id),
        "last_synced_at": await comments_service.last_synced_at(project_id),
        # The sweep normally runs from a background job, so a failure happens
        # with nobody watching. Returning the recorded outcome is what lets the
        # page say "the last sync had a problem" hours after the fact, instead
        # of rendering a stale mirror under a reassuring "Synced 4 hours ago".
        "last_sweep": await comments_service.last_sweep_run(project_id),
        # For API consumers only. The dashboard does not read it — the whole
        # section is hidden server-side by a Jinja guard when no channel is
        # bound, so its JS never runs in that case.
        "channel_connected": bool(project.get("youtube_channel_id")),
    }


@router.post("/sync")
async def sync_comments(slug: str) -> dict:
    """Sweep this project's channel now and upsert what comes back.

    The periodic job does this on its own schedule; this is the user asking for
    it immediately. Failures surface as the real error — a refresh that quietly
    returned the stale list would be indistinguishable from a working one.
    """
    project = await _project_or_404(slug)
    try:
        return await comments_service.sync_project_comments(project)
    except comments_service.ChannelNotBound as exc:
        raise HTTPException(400, str(exc)) from exc
    except comments_service.SweepAlreadyRunning as exc:
        # Refused rather than queued: two concurrent sweeps corrupt the
        # "gone from YouTube" watermark. Same 409 convention the smart-queue
        # endpoints use for "there is unfinished work on this".
        raise HTTPException(409, str(exc)) from exc
    except Exception as exc:
        logger.exception("Comment sync failed for project %s", slug)
        raise HTTPException(500, f"{type(exc).__name__}: {exc}") from exc
