"""YouTube → local import: list candidates, dedup, fetch metadata + thumbnail
+ transcript and stamp the row in the videos table."""

from __future__ import annotations

import json
import logging

import httpx

from yt_scheduler.config import UPLOAD_DIR
from yt_scheduler.database import get_db
from yt_scheduler.services import (
    auto_actions, events, tiers,
    transcripts as transcript_service, youtube,
)
from yt_scheduler.services.auth import set_active_project
from yt_scheduler.services.projects import get_project_by_id

logger = logging.getLogger(__name__)


def _is_deleted_stub(snippet: dict, status: dict) -> bool:
    """A playlistItems entry is a deletion stub when YouTube has stripped
    the title to 'Deleted video' / 'Private video' and the privacyStatus
    is 'privacyStatusUnspecified'. The videoId is still present so we can
    cross-reference against our DB rows."""
    title = (snippet.get("title") or "").strip().lower()
    privacy = (status.get("privacyStatus") or "").strip()
    if title in ("deleted video", "private video"):
        return True
    if privacy == "privacyStatusUnspecified":
        return True
    return False


async def list_available_imports(project_id: int, max_results: int = 50) -> list[dict]:
    """Return YouTube videos on the authenticated channel that aren't already
    in our DB. Each entry includes enough metadata for the user to pick (id,
    title, thumbnail URL, published date, privacy status, embeddable hint,
    duration, youtube_kind).

    Side effect: when the channel's uploads playlist still references a
    video we previously imported, but YouTube has since deleted it (the
    entry is a "Deleted video" stub), the corresponding ``videos`` row's
    ``youtube_deleted`` flag is set to 1 so the dashboard / detail page
    can surface the state.
    """
    db = await get_db()
    rows = await db.execute_fetchall("SELECT id FROM videos")
    known_ids = {r["id"] for r in rows}

    project = await get_project_by_id(project_id)
    if project:
        set_active_project(project["slug"])
    items = youtube.list_channel_videos(max_results=max_results)

    deleted_known: list[str] = []
    out: list[dict] = []
    for item in items:
        snippet = item.get("snippet", {})
        status = item.get("status", {})
        resource_id = snippet.get("resourceId", {})
        video_id = resource_id.get("videoId") or item.get("id")
        if not video_id:
            continue
        if _is_deleted_stub(snippet, status):
            # Stub for a deleted video. If we know about it, mark it
            # deleted on our side; either way, never offer it for import.
            if video_id in known_ids:
                deleted_known.append(video_id)
            continue
        if video_id in known_ids:
            continue
        thumbs = snippet.get("thumbnails", {})
        thumb_url = (
            thumbs.get("maxres") or thumbs.get("high") or thumbs.get("medium")
            or thumbs.get("default") or {}
        ).get("url")
        out.append({
            "video_id": video_id,
            "title": snippet.get("title", ""),
            "description": snippet.get("description", ""),
            "published_at": snippet.get("publishedAt"),
            "thumbnail_url": thumb_url,
            "privacy_status": status.get("privacyStatus"),
            "embeddable": status.get("privacyStatus") == "public",
            "duration_seconds": None,
            "youtube_kind": None,
        })

    if deleted_known:
        # Bulk update the youtube_deleted flag for any DB videos that the
        # channel's uploads playlist now flags as deleted.
        placeholders = ",".join(["?"] * len(deleted_known))
        await db.execute(
            f"UPDATE videos SET youtube_deleted = 1, updated_at = datetime('now') "
            f"WHERE id IN ({placeholders})",
            deleted_known,
        )
        await db.commit()

    # Batch-fetch contentDetails + liveStreamingDetails for the candidate
    # set so each card can show duration + a coarse kind (video / short /
    # live). 1 API unit per chunk of 50 — cheap.
    if out:
        meta = youtube.get_videos_kind_metadata([v["video_id"] for v in out])
        for v in out:
            entry = meta.get(v["video_id"]) or {}
            cd = entry.get("contentDetails") or {}
            v["duration_seconds"] = tiers.parse_iso8601_duration(cd.get("duration"))
            v["youtube_kind"] = youtube.classify_youtube_kind(entry)
    return out


async def import_video(video_id: str, *, project_id: int) -> dict:
    """Pull metadata + thumbnail + transcript from YouTube into our DB.

    Returns the inserted/updated row.
    """
    db = await get_db()
    rows = await db.execute_fetchall("SELECT id FROM videos WHERE id = ?", (video_id,))
    if rows:
        raise ValueError(f"Video {video_id} is already imported")

    project = await get_project_by_id(project_id)
    if project:
        set_active_project(project["slug"])

    full = youtube.get_video(video_id)
    if not full:
        raise ValueError(f"Video {video_id} not found on YouTube")
    snippet = full.get("snippet", {})
    status = full.get("status", {})
    content_details = full.get("contentDetails", {})

    duration = tiers.parse_iso8601_duration(content_details.get("duration"))
    tier = tiers.tier_for_duration(duration)
    youtube_kind = youtube.classify_youtube_kind({
        "contentDetails": content_details,
        "liveStreamingDetails": full.get("liveStreamingDetails"),
    } if full.get("liveStreamingDetails") else {"contentDetails": content_details})

    title = snippet.get("title", "Untitled")
    description = snippet.get("description", "")
    tags_list = snippet.get("tags", []) or []
    privacy = status.get("privacyStatus", "unlisted")

    # Download thumbnail locally so we can re-upload / display offline.
    thumbnail_path: str | None = None
    thumbs = snippet.get("thumbnails", {})
    thumb = (
        thumbs.get("maxres") or thumbs.get("high") or thumbs.get("medium")
        or thumbs.get("default") or {}
    )
    thumb_url = thumb.get("url")
    if thumb_url:
        try:
            UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
            target = UPLOAD_DIR / f"{video_id}_thumb.jpg"
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(thumb_url)
                resp.raise_for_status()
                target.write_bytes(resp.content)
            thumbnail_path = str(target)
        except Exception as exc:
            logger.warning("Could not download thumbnail for %s: %s", video_id, exc)

    await db.execute(
        """INSERT INTO videos (
            id, project_id, title, description, tags, privacy_status,
            thumbnail_path, status, imported_from_youtube,
            duration_seconds, tier, youtube_kind
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'uploaded', 1, ?, ?, ?)""",
        (
            video_id, project_id, title, description, json.dumps(tags_list),
            privacy, thumbnail_path, duration, tier, youtube_kind,
        ),
    )
    await db.commit()

    await events.record_event(
        video_id, "imported", {"source": "youtube", "tier": tier}
    )

    # Try to grab the YouTube transcript on import if one exists. Stored as
    # SRT (the canonical format with timestamps); plain-text consumers strip
    # on read via ``transcripts.srt_to_plain_text``.
    try:
        captions = youtube.list_captions(video_id)
        if captions:
            text = youtube.download_caption(captions[0]["id"], fmt="srt")
            transcript_id = await transcript_service.upsert_transcript_for_source(
                video_id, "youtube", text
            )
            await db.execute(
                """UPDATE videos SET
                    transcript = ?,
                    transcript_id = ?,
                    transcript_source = 'youtube',
                    transcript_created_at = datetime('now'),
                    transcript_updated_at = datetime('now'),
                    status = 'captioned'
                WHERE id = ?""",
                (text, transcript_id, video_id),
            )
            await db.commit()
    except Exception as exc:
        logger.info("No YouTube transcript available for %s: %s", video_id, exc)

    # Run the import-column auto-actions in the background.
    await auto_actions.run_post_create_actions(
        video_id, project_id=project_id, source="import"
    )

    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    return dict(rows[0]) if rows else {}
