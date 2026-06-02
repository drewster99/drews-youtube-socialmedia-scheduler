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
# Same shape, unanchored — used by has_timestamps to scan a whole
# transcript string for any cue line, not just one at the start.
_SRT_TIMESTAMP_SEARCH_RE = re.compile(
    r"\d{2}:\d{2}:\d{2}[,.]\d{1,3}\s*-->\s*\d{2}:\d{2}:\d{2}[,.]\d{1,3}"
)


def has_timestamps(text: str | None) -> bool:
    """True when ``text`` contains at least one SRT-style cue line
    (``HH:MM:SS,mmm --> HH:MM:SS,mmm``).

    The Generate-from-source flow needs timestamped transcripts to give
    Claude meaningful range proposals — a plain-text user-edited
    transcript that lost its timestamps can't be used. This is the
    cheap detector for that pre-flight gate.
    """
    if not text:
        return False
    return bool(_SRT_TIMESTAMP_SEARCH_RE.search(text))


_SRT_TIMESTAMP_PARSE_RE = re.compile(
    r"(\d{1,2}):(\d{2}):(\d{2})[,.](\d{1,3})\s*-->\s*"
    r"(\d{1,2}):(\d{2}):(\d{2})[,.](\d{1,3})"
)


def _parse_srt_cues(srt: str) -> list[tuple[float, float, str]]:
    """Parse an SRT into ``[(start_sec, end_sec, text), ...]``.

    Tolerant of WEBVTT headers, cue-number lines, missing cue numbers
    (some transcribers omit them), and the ``,``/``.`` decimal split.
    Returns an empty list when nothing parses — caller falls back to
    the raw SRT in that case.
    """
    cues: list[tuple[float, float, str]] = []
    blocks = re.split(r"\n\s*\n", (srt or "").replace("\r\n", "\n").strip())
    for blk in blocks:
        lines = [ln for ln in blk.split("\n") if ln.strip()]
        if not lines:
            continue
        if lines[0].strip().upper() == "WEBVTT":
            continue
        # First or second line is the timing; rest is text.
        ts_idx = 1 if lines[0].strip().isdigit() and len(lines) > 1 else 0
        if ts_idx >= len(lines):
            continue
        m = _SRT_TIMESTAMP_PARSE_RE.search(lines[ts_idx])
        if m is None:
            continue
        sh, sm, ss, sms, eh, em, es, ems = m.groups()
        start = int(sh) * 3600 + int(sm) * 60 + int(ss) + int(sms) / 1000.0
        end = int(eh) * 3600 + int(em) * 60 + int(es) + int(ems) / 1000.0
        text = " ".join(ln.strip() for ln in lines[ts_idx + 1:]).strip()
        if not text:
            continue
        cues.append((start, end, text))
    return cues


def srt_to_llm_timeline(srt: str) -> str:
    """Reshape a (possibly dual-speaker / overlapping) SRT into a flat,
    chronological ``[MM:SS] text`` timeline for the LLM.

    The Whisper / mlx-whisper / Apple Speech transcribers on dual-mic
    podcast audio produce SRTs with overlapping cues — two speaker
    channels separately transcribed and interleaved. The cue indices
    don't line up with chronological order, and adjacent cues can
    differ in start time by negative seconds. Claude has trouble
    grounding proposals in an SRT that doesn't flow linearly, so we
    flatten it: sort by start time and present each cue on its own
    line with a ``[H:MM:SS]`` or ``[MM:SS]`` anchor at the front.
    The end timestamp is implicit (the next cue's start), and cue
    numbers are dropped — neither helps the model.

    Falls back to the raw SRT when no cues parse (so a corrupted or
    plain-text transcript still flows through).
    """
    cues = _parse_srt_cues(srt)
    if not cues:
        return srt
    cues.sort(key=lambda c: c[0])

    def stamp(seconds: float) -> str:
        seconds = max(0.0, float(seconds))
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)
        if h:
            return f"{h}:{m:02d}:{s:02d}"
        return f"{m:02d}:{s:02d}"

    return "\n".join(f"[{stamp(start)}] {text}" for start, _end, text in cues)


def srt_to_plain_text(srt: str) -> str:
    """Strip SRT/VTT cue numbers and timestamp lines, leaving the spoken text.

    Local transcribers (mlx-whisper, Apple Speech) all return
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
    "whispercpp",
    "user_edited",
]

VALID_SOURCES: frozenset[str] = frozenset(
    [
        "youtube",
        "apple_speech",
        "mlx_whisper",
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
    await db.commit()
    # When the active transcript text was edited away from any source, also
    # persist that edit as a user_edited transcript row so it survives a
    # subsequent re-selection of a different source. The source row may not
    # itself be a user_edited row, so look up (or create) the per-video
    # user_edited row separately rather than trying to UPDATE by transcript_id.
    if is_edited:
        await ensure_user_edited_row(video_id, text)
        await db.execute(
            "UPDATE transcripts SET text = ?, created_at = datetime('now') "
            "WHERE video_id = ? AND source = 'user_edited'",
            (text, video_id),
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
