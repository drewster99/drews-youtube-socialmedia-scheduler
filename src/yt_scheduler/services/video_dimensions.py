"""Cached frame dimensions for videos, and the orientation derived from them.

Orientation is a *filter* for the smart queue, which selects across every
video in a project — so it has to be answerable in SQL rather than by
shelling out to ffprobe per row.

Only width and height are stored. Orientation is derived on read, so there
is no second column that can disagree with the dimensions it came from.

``videos.youtube_kind`` is not an orientation signal despite the name: it is
a duration-derived guess (``<=60s -> 'short'``) written only on the import
path, and NULL on nearly every row.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Literal

from yt_scheduler.database import get_db, write_transaction
from yt_scheduler.services import media as media_service

logger = logging.getLogger(__name__)

Orientation = Literal["portrait", "landscape", "square"]


def orientation_of(width: int | None, height: int | None) -> Orientation | None:
    """Derive orientation from dimensions. ``None`` means unknown, not square."""
    if not width or not height:
        return None
    if height > width:
        return "portrait"
    if width > height:
        return "landscape"
    return "square"


# SQL for the same derivation, so a query can filter on orientation without
# loading rows into Python. Kept next to orientation_of() so the two can't
# drift apart unnoticed.
ORIENTATION_SQL = """
    CASE
        WHEN width IS NULL OR height IS NULL OR width = 0 OR height = 0 THEN NULL
        WHEN height > width THEN 'portrait'
        WHEN width > height THEN 'landscape'
        ELSE 'square'
    END
"""


async def stamp_dimensions(video_id: str, video_file_path: str | None) -> tuple[int, int] | None:
    """Probe ``video_file_path`` and cache its dimensions on the row.

    Returns the ``(width, height)`` written, or ``None`` when there was
    nothing to probe or ffprobe couldn't determine them. A failure is logged
    and left as NULL — callers must treat unknown as its own state rather
    than assuming a shape.
    """
    if not video_file_path or not Path(video_file_path).exists():
        return None
    probe = await asyncio.to_thread(media_service.probe_video_file, video_file_path)
    if probe is None or not probe.width or not probe.height:
        logger.warning(
            "Could not determine dimensions for video %s (%s)",
            video_id, Path(video_file_path).name,
        )
        return None
    async with write_transaction() as db:
        await db.execute(
            "UPDATE videos SET width = ?, height = ? WHERE id = ?",
            (int(probe.width), int(probe.height), video_id),
        )
    return int(probe.width), int(probe.height)


async def ensure_dimensions(video_id: str) -> tuple[int, int] | None:
    """Return this video's dimensions, probing and caching them if absent.

    Self-healing: a row written by a path that didn't stamp dimensions gets
    them the first time anything asks. That keeps a missed write site from
    silently excluding a video from every orientation filter forever.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT width, height, video_file_path FROM videos WHERE id = ?",
        (video_id,),
    )
    if not rows:
        raise ValueError(f"Video {video_id} not found")
    row = rows[0]
    if row["width"] and row["height"]:
        return int(row["width"]), int(row["height"])
    return await stamp_dimensions(video_id, row["video_file_path"])


async def backfill_video_dimensions() -> int:
    """Stamp dimensions on every row that has a local file but no dimensions.

    Run once at startup. Returns the number of rows updated. Probing is
    sequential on a worker thread: this is a one-time catch-up over a
    few hundred rows at most, and ffprobe on a local file is milliseconds.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, video_file_path FROM videos "
        "WHERE (width IS NULL OR height IS NULL) "
        "AND video_file_path IS NOT NULL AND video_file_path <> ''"
    )
    if not rows:
        return 0
    updated = 0
    for row in rows:
        try:
            if await stamp_dimensions(row["id"], row["video_file_path"]):
                updated += 1
        except Exception:
            # One unreadable file must not abort the catch-up for the rest.
            logger.exception("Dimension backfill failed for video %s", row["id"])
    logger.info(
        "Dimension backfill: stamped %d of %d row(s) missing dimensions",
        updated, len(rows),
    )
    return updated
