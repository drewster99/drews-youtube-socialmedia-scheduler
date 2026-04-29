"""Transcript management endpoints."""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, HTTPException

from yt_scheduler.database import get_db
from yt_scheduler.services import events, transcripts, youtube
from yt_scheduler.services.auth import set_active_project
from yt_scheduler.services.projects import get_project_by_id

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/videos/{video_id}/transcripts", tags=["transcripts"])


@router.get("")
async def list_transcripts(video_id: str) -> list[dict]:
    return await transcripts.list_transcripts(video_id)


@router.put("/active")
async def set_active(video_id: str, payload: dict) -> dict:
    """Activate a chosen transcript for a video.

    Body: ``{"transcript_id": int, "text": str, "is_edited": bool}``

    The text is what the user is committing — may differ from the source row
    when the user edited it in the chooser. The diff is recorded as a
    metadata_updated{transcript} event.
    """
    transcript_id = payload.get("transcript_id")
    text = payload.get("text")
    is_edited = bool(payload.get("is_edited"))
    if not isinstance(transcript_id, int):
        raise HTTPException(400, "transcript_id (int) is required")
    if not isinstance(text, str) or not text.strip():
        raise HTTPException(400, "text is required")

    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT transcript, project_id FROM videos WHERE id = ?", (video_id,)
    )
    if not rows:
        raise HTTPException(404, "Video not found")
    old_text = rows[0]["transcript"] or ""

    # Bind the active project so the YouTube caption upload below uses the
    # right OAuth credentials.
    project_id = rows[0]["project_id"]
    if project_id is not None:
        project = await get_project_by_id(int(project_id))
        if project:
            set_active_project(project["slug"])

    try:
        result = await transcripts.set_active_transcript(
            video_id, transcript_id, text=text, is_edited=is_edited
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc

    youtube_result: dict[str, str] = {}
    if old_text != text:
        await events.record_event(
            video_id,
            "metadata_updated",
            {"transcript": {"old": old_text, "new": text}},
        )

        # Push the active transcript to YouTube as a user-uploaded caption
        # track. Treats this as a YouTube metadata update — the user expects
        # transcript edits to round-trip back to YouTube. Failure (auth /
        # quota / non-uploaded video) is non-fatal: we log and surface the
        # reason in the response so the toast can show it.
        try:
            yt_caption = await asyncio.to_thread(youtube.upload_caption, video_id, text)
            youtube_result = {
                "youtube_caption_id": yt_caption.get("id", ""),
                "youtube_status": "uploaded",
            }
        except Exception as exc:
            logger.warning("Caption upload to YouTube failed for %s: %s", video_id, exc)
            youtube_result = {
                "youtube_status": "failed",
                "youtube_error": str(exc),
            }

    return {"status": "ok", **result, **youtube_result}
