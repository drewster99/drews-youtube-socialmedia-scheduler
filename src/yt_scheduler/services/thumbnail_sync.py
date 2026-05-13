"""Keep the local thumbnail in sync with what's on YouTube.

When a user uploads a thumbnail (``thumbnail_source='user'``), the
local file is the source of truth and YouTube has a re-encoded copy
of the same image. When a user (or another tool) changes the
thumbnail on YouTube, the two copies diverge and the detail page
should flag that — but only when the change is *content*, not when
YouTube simply re-compressed the same picture on upload.

The flow:

  1. ``maybe_refresh_youtube_thumbnail`` runs as a background task off
     GET /api/videos/{id}. Skips fast when ``youtube_data`` has the
     same thumbnail URL we last fetched.
  2. New URL → download the JPEG into ``UPLOAD_DIR`` and update
     ``youtube_thumbnail_path`` / ``youtube_thumbnail_url``. Clear
     ``thumbnail_compare_verdict`` so we know to re-ask Claude.
  3. If ``thumbnail_source='user'`` and we have both bytes,
     ``ai.compare_thumbnails`` adjudicates; the verdict lands on the
     row and the detail page picks it up on the next poll tick.

A row with ``thumbnail_source='youtube'`` (the user never overrode)
skips the vision compare entirely — the local copy IS the YouTube
copy, so a "different" verdict would be meaningless.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import httpx

from yt_scheduler.config import UPLOAD_DIR
from yt_scheduler.database import get_db
from yt_scheduler.services import ai

logger = logging.getLogger(__name__)


# Per-video task tracker so a stream of /api/videos/{id} fetches doesn't
# schedule overlapping work for the same row. The dict is keyed by
# video id; the existing entry is honored until it completes.
_in_flight: dict[str, asyncio.Task] = {}


def _extract_youtube_thumbnail_url(yt_data: dict | None) -> str | None:
    """Pull the largest available thumbnail URL out of a youtube videos.
    list snippet (``maxres → high → medium → default``). Returns None
    when the payload has none — i.e. when the video has no thumbnail
    on YouTube at all."""
    if not yt_data:
        return None
    thumbs = (yt_data.get("snippet") or {}).get("thumbnails") or {}
    for size in ("maxres", "high", "medium", "default"):
        url = (thumbs.get(size) or {}).get("url")
        if url:
            return url
    return None


def schedule_refresh(video_id: str, yt_data: dict | None) -> None:
    """Fire-and-forget scheduler invoked from GET /api/videos/{id}.

    Idempotent — a second call while the first is still running is a
    no-op. The caller never awaits the returned task; the detail
    page's polling tick picks up the new ``thumbnail_compare_verdict``
    on its next loadVideo()."""
    existing = _in_flight.get(video_id)
    if existing is not None and not existing.done():
        return
    task = asyncio.create_task(_run(video_id, yt_data))
    _in_flight[video_id] = task
    task.add_done_callback(lambda _t: _in_flight.pop(video_id, None))


async def _run(video_id: str, yt_data: dict | None) -> None:
    try:
        await maybe_refresh_youtube_thumbnail(video_id, yt_data)
    except Exception as exc:
        # The thumbnail compare is a nice-to-have; never let it surface
        # as a 500 or crash the request that scheduled it.
        logger.warning(
            "thumbnail-sync background task failed for %s: %s", video_id, exc
        )


async def maybe_refresh_youtube_thumbnail(
    video_id: str, yt_data: dict | None,
) -> None:
    """Core flow. Public so unit tests can call it directly with a
    crafted yt_data payload — production callers use schedule_refresh."""
    new_url = _extract_youtube_thumbnail_url(yt_data)
    if not new_url:
        return

    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT thumbnail_path, thumbnail_source, youtube_thumbnail_path, "
        "youtube_thumbnail_url, thumbnail_compare_verdict "
        "FROM videos WHERE id = ?",
        (video_id,),
    )
    if not rows:
        return
    row = dict(rows[0])

    url_changed = (row.get("youtube_thumbnail_url") or "") != new_url
    yt_path_missing = not row.get("youtube_thumbnail_path") or not Path(
        row["youtube_thumbnail_path"]
    ).exists()

    if not url_changed and not yt_path_missing:
        # Nothing to refresh. If we never asked Claude (verdict=NULL)
        # AND the user has their own thumbnail, fall through to the
        # compare step so the verdict gets computed eventually.
        if row.get("thumbnail_compare_verdict") is not None:
            return
        if row.get("thumbnail_source") != "user":
            return
    else:
        # Fetch the new YouTube thumbnail to disk. The filename folds in
        # video id so multiple fetches don't collide with each other or
        # with the user's own upload (which uses its own filename).
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        target = UPLOAD_DIR / f"{video_id}_yt_thumb.jpg"
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(new_url)
                resp.raise_for_status()
                target.write_bytes(resp.content)
        except Exception as exc:
            logger.warning(
                "Could not download YouTube thumbnail for %s from %s: %s",
                video_id, new_url, exc,
            )
            return
        await db.execute(
            "UPDATE videos SET youtube_thumbnail_path = ?, youtube_thumbnail_url = ?, "
            "thumbnail_compare_verdict = NULL, thumbnail_compared_at = NULL, "
            "updated_at = datetime('now') WHERE id = ?",
            (str(target), new_url, video_id),
        )
        await db.commit()
        row["youtube_thumbnail_path"] = str(target)
        row["youtube_thumbnail_url"] = new_url

    # Compare step — only meaningful when the user has their own
    # thumbnail AND we have both files on disk. A 'youtube' source
    # already trivially equals the YouTube copy, so we skip the
    # vision call there.
    if row.get("thumbnail_source") != "user":
        return
    local_path = row.get("thumbnail_path")
    yt_local_path = row.get("youtube_thumbnail_path")
    if not local_path or not yt_local_path:
        return
    local = Path(local_path)
    yt_local = Path(yt_local_path)
    if not local.exists() or not yt_local.exists():
        return

    try:
        verdict = await ai.compare_thumbnails(local.read_bytes(), yt_local.read_bytes())
    except Exception as exc:
        logger.warning("Claude thumbnail compare failed for %s: %s", video_id, exc)
        return

    await db.execute(
        "UPDATE videos SET thumbnail_compare_verdict = ?, "
        "thumbnail_compared_at = datetime('now'), "
        "updated_at = datetime('now') WHERE id = ?",
        (verdict, video_id),
    )
    await db.commit()
