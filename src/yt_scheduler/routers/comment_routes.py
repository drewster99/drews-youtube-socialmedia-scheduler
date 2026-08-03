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
async def list_comments(slug: str, limit: int = 10, offset: int = 0) -> dict:
    """Newest stored comments for a project, newest first.

    Bad paging values are refused rather than clamped: a silently corrected
    limit returns a page the caller did not ask for and looks like the list
    simply ended.
    """
    if limit < 1 or limit > comments_service.MAX_COMMENTS_PER_PAGE:
        raise HTTPException(
            400,
            f"limit must be between 1 and "
            f"{comments_service.MAX_COMMENTS_PER_PAGE}, got {limit}",
        )
    if offset < 0:
        raise HTTPException(400, f"offset cannot be negative, got {offset}")

    project = await _project_or_404(slug)
    project_id = int(project["id"])
    return {
        "comments": await comments_service.list_recent_comments(
            project_id, limit=limit, offset=offset
        ),
        "total": await comments_service.count_comments(project_id),
        "last_synced_at": await comments_service.last_synced_at(project_id),
        # Lets the UI distinguish "no comments yet" from "this project was
        # never connected to a channel", which need different words.
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
    except Exception as exc:
        logger.exception("Comment sync failed for project %s", slug)
        raise HTTPException(500, f"{type(exc).__name__}: {exc}") from exc
