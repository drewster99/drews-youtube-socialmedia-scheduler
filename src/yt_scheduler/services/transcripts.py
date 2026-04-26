"""Transcript CRUD: each video can have multiple transcripts (YouTube,
on-device backends, user_edited). The active transcript text + metadata is
mirrored on the ``videos`` row so existing single-transcript code keeps
working unchanged."""

from __future__ import annotations

import re
from typing import Literal

from yt_scheduler.database import get_db


_SRT_TIMESTAMP_RE = re.compile(
    r"^\d{2}:\d{2}:\d{2}[,.]\d{1,3}\s*-->\s*\d{2}:\d{2}:\d{2}[,.]\d{1,3}"
)


def srt_to_plain_text(srt: str) -> str:
    """Strip SRT/VTT cue numbers and timestamp lines, leaving the spoken text.

    Local transcribers (mlx-whisper, faster-whisper, Apple Speech) all return
    clean plain text. YouTube's caption download returns SRT. This helper
    normalises imported captions so every transcript stored in our DB has the
    same shape — easier on Claude's context budget and easier for the user to
    read/edit in the transcript chooser modal.
    """
    if not srt:
        return ""
    out: list[str] = []
    for block in srt.replace("\r\n", "\n").split("\n\n"):
        lines = [ln for ln in block.split("\n") if ln.strip()]
        if not lines:
            continue
        # Skip a cue number on its own line.
        if lines[0].strip().isdigit():
            lines = lines[1:]
        # Drop any timestamp line.
        lines = [ln for ln in lines if not _SRT_TIMESTAMP_RE.match(ln.strip())]
        # Skip WebVTT header lines.
        lines = [ln for ln in lines if ln.strip().upper() != "WEBVTT"]
        if lines:
            out.append("\n".join(lines))
    return "\n".join(out)

TranscriptSource = Literal[
    "youtube",
    "apple_speech",
    "mlx_whisper",
    "faster_whisper",
    "whispercpp",
    "user_edited",
]

VALID_SOURCES: frozenset[str] = frozenset(
    [
        "youtube",
        "apple_speech",
        "mlx_whisper",
        "faster_whisper",
        "whispercpp",
        "user_edited",
    ]
)


async def list_transcripts(video_id: str) -> list[dict]:
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, video_id, source, source_detail, text, created_at "
        "FROM transcripts WHERE video_id = ? ORDER BY created_at DESC, id DESC",
        (video_id,),
    )
    return [dict(row) for row in rows]


async def get_transcript(transcript_id: int) -> dict | None:
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, video_id, source, source_detail, text, created_at "
        "FROM transcripts WHERE id = ?",
        (transcript_id,),
    )
    return dict(rows[0]) if rows else None


async def add_transcript(
    video_id: str,
    source: TranscriptSource,
    text: str,
    source_detail: str | None = None,
) -> int:
    """Insert a transcript row; returns its id."""
    if source not in VALID_SOURCES:
        raise ValueError(f"Invalid transcript source: {source}")
    if not text.strip():
        raise ValueError("Transcript text is required")
    db = await get_db()
    cursor = await db.execute(
        "INSERT INTO transcripts (video_id, source, source_detail, text) "
        "VALUES (?, ?, ?, ?)",
        (video_id, source, source_detail, text),
    )
    await db.commit()
    return int(cursor.lastrowid)


async def upsert_transcript_for_source(
    video_id: str,
    source: TranscriptSource,
    text: str,
    source_detail: str | None = None,
) -> int:
    """Insert or update the *single* transcript row for a (video, source).

    For sources that are deterministic (YouTube auto-captions, a particular
    Whisper model run), repeating the same generation should not pile up
    duplicate rows. The matching rule is exact ``(video_id, source,
    source_detail)``.
    """
    db = await get_db()
    cursor = await db.execute(
        "SELECT id FROM transcripts "
        "WHERE video_id = ? AND source = ? AND COALESCE(source_detail, '') = COALESCE(?, '')",
        (video_id, source, source_detail),
    )
    row = await cursor.fetchone()
    if row is not None:
        await db.execute(
            "UPDATE transcripts SET text = ?, created_at = datetime('now') WHERE id = ?",
            (text, int(row[0])),
        )
        await db.commit()
        return int(row[0])
    return await add_transcript(video_id, source, text, source_detail=source_detail)


async def set_active_transcript(
    video_id: str,
    transcript_id: int,
    *,
    text: str,
    is_edited: bool,
) -> dict:
    """Mirror the chosen transcript onto the videos row.

    ``text`` is the exact text the user is committing — typically equal to the
    chosen transcripts row, but may differ if the user edited it before saving.
    ``is_edited=True`` indicates the text differs from the source row.

    Updates ``videos.transcript_id``, ``transcript_source``,
    ``transcript_created_at`` (preserved from the source row),
    ``transcript_updated_at`` (now), ``transcript_is_edited``, and the
    legacy ``transcript`` column.
    """
    transcript = await get_transcript(transcript_id)
    if transcript is None:
        raise ValueError(f"Transcript {transcript_id} not found")
    if transcript["video_id"] != video_id:
        raise ValueError("Transcript does not belong to this video")

    db = await get_db()
    await db.execute(
        """
        UPDATE videos SET
            transcript = ?,
            transcript_id = ?,
            transcript_source = ?,
            transcript_created_at = COALESCE(transcript_created_at, ?),
            transcript_updated_at = datetime('now'),
            transcript_is_edited = ?,
            updated_at = datetime('now')
        WHERE id = ?
        """,
        (
            text,
            transcript_id,
            transcript["source"],
            transcript["created_at"],
            1 if is_edited else 0,
            video_id,
        ),
    )
    # When the active transcript text was edited away from any source, also
    # persist that edit as a user_edited transcript row so it survives a
    # subsequent re-selection of a different source.
    if is_edited:
        await db.execute(
            "UPDATE transcripts SET text = ?, created_at = datetime('now') "
            "WHERE id = ? AND source = 'user_edited'",
            (text, transcript_id),
        )
    await db.commit()

    rows = await db.execute_fetchall(
        "SELECT transcript_id, transcript_source, transcript_created_at, "
        "transcript_updated_at, transcript_is_edited "
        "FROM videos WHERE id = ?",
        (video_id,),
    )
    return dict(rows[0]) if rows else {}


async def ensure_user_edited_row(video_id: str, text: str) -> int:
    """Get-or-create the ``user_edited`` transcript row for a video.

    The chooser modal needs to support edits even when no source row exists for
    the current transcript. This row gets created on demand the first time the
    user saves an edit.
    """
    db = await get_db()
    cursor = await db.execute(
        "SELECT id FROM transcripts WHERE video_id = ? AND source = 'user_edited'",
        (video_id,),
    )
    row = await cursor.fetchone()
    if row is not None:
        return int(row[0])
    return await add_transcript(video_id, "user_edited", text)
