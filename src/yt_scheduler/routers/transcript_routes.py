"""Transcript management endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from yt_scheduler.database import get_db
from yt_scheduler.services import events, transcripts

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
        "SELECT transcript FROM videos WHERE id = ?", (video_id,)
    )
    if not rows:
        raise HTTPException(404, "Video not found")
    old_text = rows[0]["transcript"] or ""

    try:
        result = await transcripts.set_active_transcript(
            video_id, transcript_id, text=text, is_edited=is_edited
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc

    if old_text != text:
        await events.record_event(
            video_id,
            "metadata_updated",
            {"transcript": {"old": old_text, "new": text}},
        )

    return {"status": "ok", **result}
