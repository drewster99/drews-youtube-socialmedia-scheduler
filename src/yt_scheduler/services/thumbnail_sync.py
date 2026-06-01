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
  2. New URL → optional HEAD precheck (G3): i.ytimg.com sends ``etag``
     on HEAD, so we can short-circuit the GET when the ETag matches
     what we recorded last fetch. Otherwise download the JPEG into
     ``UPLOAD_DIR`` and update ``youtube_thumbnail_path`` /
     ``youtube_thumbnail_url``.
  3. (sha cache, G3) Compute sha256 of both local and youtube bytes.
     If they match the stored ``thumbnail_compare_local_sha`` /
     ``thumbnail_compare_youtube_sha`` AND a non-NULL verdict is on
     file, that verdict still applies — skip the AI call entirely.
  4. Otherwise ``ai.compare_thumbnails`` adjudicates; the verdict +
     both shas land on the row and the detail page picks it up on
     the next poll tick.

A row with ``thumbnail_source='youtube'`` (the user never overrode)
skips the vision compare entirely — the local copy IS the YouTube
copy, so a "different" verdict would be meaningless.
"""

from __future__ import annotations

import asyncio
import hashlib
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
    task = asyncio.create_task(_run(video_id, yt_data), name=f"thumbnail-sync:{video_id}")
    _in_flight[video_id] = task

    def _done(t: asyncio.Task) -> None:
        # Two callbacks merged: drop the dedupe entry, then surface any
        # exception that escaped _run's try/except (programming errors,
        # not the network-blip path _run already swallows).
        _in_flight.pop(video_id, None)
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            logger.error(
                "thumbnail-sync %s raised", video_id, exc_info=exc,
            )

    task.add_done_callback(_done)


async def _run(video_id: str, yt_data: dict | None) -> None:
    try:
        await maybe_refresh_youtube_thumbnail(video_id, yt_data)
    except Exception as exc:
        # The thumbnail compare is a nice-to-have; never let it surface
        # as a 500 or crash the request that scheduled it.
        logger.warning(
            "thumbnail-sync background task failed for %s: %s", video_id, exc
        )


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


async def _youtube_thumbnail_unchanged(
    url: str, etag: str | None,
) -> bool | None:
    """G3 — HEAD precheck against i.ytimg.com.

    Returns ``True`` when the server confirms the bytes haven't
    changed since the recorded ``etag``, ``False`` when they've
    changed (or no etag was stored), and ``None`` when the HEAD
    failed for any reason (treat as "I don't know — go fetch
    normally" rather than holding up the user's polling tick).

    YouTube's thumbnail CDN returns ``etag: "<unix-ish>"`` on every
    response, so an ETag match is a strong signal — same URL, same
    ETag means same bytes (modulo a rare regen with identical
    content, which would only cost us one extra Claude call to
    re-confirm).
    """
    if not etag:
        return False
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.head(url, follow_redirects=True)
        if resp.status_code != 200:
            return None
        new_etag = resp.headers.get("etag") or resp.headers.get("ETag")
        if new_etag is None:
            return None
        return new_etag == etag
    except Exception as exc:
        logger.debug("HEAD precheck failed for %s: %s", url, exc)
        return None


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
        "youtube_thumbnail_url, youtube_thumbnail_etag, "
        "thumbnail_compare_verdict, thumbnail_compare_local_sha, "
        "thumbnail_compare_youtube_sha "
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

    # Decide whether to (re)fetch the YouTube-side thumbnail. Three
    # cases produce a GET:
    #   1) URL changed (new image on the server)
    #   2) We never downloaded one / the file is gone
    #   3) URL is unchanged but the HEAD precheck couldn't confirm
    #      "still the same bytes" via the cached ETag
    # When the URL is unchanged AND HEAD confirms the ETag still
    # matches, the cached file is current and we skip the GET — but
    # we still fall through to the SHA cache below so a CHANGE on
    # the LOCAL side (user uploaded a new thumb) re-triggers Claude.
    must_fetch = url_changed or yt_path_missing
    if not must_fetch:
        unchanged = await _youtube_thumbnail_unchanged(
            new_url, row.get("youtube_thumbnail_etag"),
        )
        if unchanged is False or unchanged is None:
            must_fetch = True

    if must_fetch:
        # Fetch the new YouTube thumbnail to disk. The filename folds in
        # video id so multiple fetches don't collide with each other or
        # with the user's own upload (which uses its own filename).
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        target = UPLOAD_DIR / f"{video_id}_yt_thumb.jpg"
        new_etag: str | None = None
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(new_url, follow_redirects=True)
                resp.raise_for_status()
                target.write_bytes(resp.content)
                new_etag = resp.headers.get("etag") or resp.headers.get("ETag")
        except Exception as exc:
            logger.warning(
                "Could not download YouTube thumbnail for %s from %s: %s",
                video_id, new_url, exc,
            )
            return
        await db.execute(
            "UPDATE videos SET youtube_thumbnail_path = ?, "
            "youtube_thumbnail_url = ?, youtube_thumbnail_etag = ?, "
            "thumbnail_compare_verdict = NULL, "
            "thumbnail_compared_at = NULL, "
            "updated_at = datetime('now') WHERE id = ?",
            (str(target), new_url, new_etag, video_id),
        )
        await db.commit()
        row["youtube_thumbnail_path"] = str(target)
        row["youtube_thumbnail_url"] = new_url
        row["youtube_thumbnail_etag"] = new_etag
        # New bytes → previously-cached verdict is stale.
        row["thumbnail_compare_verdict"] = None

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

    local_bytes = local.read_bytes()
    yt_bytes = yt_local.read_bytes()
    local_sha = _sha256_bytes(local_bytes)
    yt_sha = _sha256_bytes(yt_bytes)

    # G3 — SHA-pair cache. Same bytes on both sides as last time AND
    # we already have a verdict on file → no need to re-ask Claude.
    cached_local_sha = row.get("thumbnail_compare_local_sha")
    cached_yt_sha = row.get("thumbnail_compare_youtube_sha")
    cached_verdict = row.get("thumbnail_compare_verdict")
    if (
        cached_verdict is not None
        and cached_local_sha == local_sha
        and cached_yt_sha == yt_sha
    ):
        return

    try:
        verdict = await ai.compare_thumbnails(local_bytes, yt_bytes)
    except Exception as exc:
        logger.warning("Claude thumbnail compare failed for %s: %s", video_id, exc)
        return

    await db.execute(
        "UPDATE videos SET thumbnail_compare_verdict = ?, "
        "thumbnail_compare_local_sha = ?, "
        "thumbnail_compare_youtube_sha = ?, "
        "thumbnail_compared_at = datetime('now'), "
        "updated_at = datetime('now') WHERE id = ?",
        (verdict, local_sha, yt_sha, video_id),
    )
    await db.commit()


# YouTube serves a predictable thumbnail URL per video id. maxres often
# 404s until processing finishes; hqdefault exists within seconds of
# upload. Try largest-first and take the first that returns content.
_BACKFILL_THUMB_SIZES = ("maxresdefault", "sddefault", "hqdefault")


async def backfill_thumbnail(video_id: str) -> bool:
    """Download YouTube's current thumbnail for a video that has none
    locally, and point thumbnail_path / thumbnail_source at it.

    This produces the same end state as an import (which downloads the
    thumbnail up front). Promo-uploaded videos skip that — the promo
    pipeline has no thumbnail step — so the periodic
    :func:`scheduler.backfill_thumbnails_job` sweep calls this to give
    them YouTube's auto-generated thumbnail. That also lets them pass
    the schedule-readiness check, which requires a thumbnail.

    Returns True when a thumbnail was written, False when the row
    already has one or YouTube served nothing usable.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT thumbnail_path FROM videos WHERE id = ?", (video_id,)
    )
    if not rows or rows[0]["thumbnail_path"]:
        return False

    content: bytes | None = None
    source_url: str | None = None
    async with httpx.AsyncClient(timeout=30) as client:
        for size in _BACKFILL_THUMB_SIZES:
            url = f"https://i.ytimg.com/vi/{video_id}/{size}.jpg"
            try:
                resp = await client.get(url, follow_redirects=True)
            except Exception as exc:
                logger.debug("Thumbnail backfill GET failed (%s): %s", url, exc)
                continue
            if resp.status_code == 200 and resp.content:
                content = resp.content
                source_url = url
                break
    if content is None:
        return False

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    target = UPLOAD_DIR / f"{video_id}_thumb.jpg"
    target.write_bytes(content)
    # Mirror the import path: thumbnail_source='youtube', and seed the
    # dual-thumbnail columns so a later maybe_refresh doesn't re-download
    # an identical image just to find nothing changed.
    await db.execute(
        "UPDATE videos SET thumbnail_path = ?, thumbnail_source = 'youtube', "
        "youtube_thumbnail_path = ?, youtube_thumbnail_url = ?, "
        "updated_at = datetime('now') WHERE id = ?",
        (str(target), str(target), source_url, video_id),
    )
    await db.commit()
    logger.info("Backfilled YouTube thumbnail for %s", video_id)
    return True
