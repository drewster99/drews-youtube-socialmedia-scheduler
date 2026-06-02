"""Video management routes."""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import shutil
import subprocess
import sys
import time
import weakref
from pathlib import Path

from fastapi import APIRouter, Body, Form, Header, UploadFile, File, HTTPException, Query

from yt_scheduler.config import (
    UPLOAD_DIR,
    media_filename,
    media_url,
    safe_upload_ext,
    sanitized_original_filename,
)
from yt_scheduler.database import get_db
from yt_scheduler.services import (
    ai, auto_actions, events, media as media_service, tiers,
    transcripts as transcript_service, youtube,
)
from yt_scheduler.services.auth import set_active_project
from yt_scheduler.services.projects import get_project_by_id

logger = logging.getLogger(__name__)


async def _bind_project_for_video(video_id: str) -> None:
    """Look up the video's project and bind it as the active project so any
    ``youtube.*`` calls in this request use the right OAuth credentials.
    Silent no-op if the video or project can't be resolved — the youtube
    wrapper will surface a clearer error than we can here.
    """
    db = await get_db()
    cursor = await db.execute(
        "SELECT project_id FROM videos WHERE id = ?", (video_id,)
    )
    row = await cursor.fetchone()
    if row is None or row["project_id"] is None:
        return
    project = await get_project_by_id(int(row["project_id"]))
    if project:
        set_active_project(project["slug"])


_BACKEND_TO_SOURCE = {
    "mlx-whisper": "mlx_whisper",
    "whisper.cpp": "whispercpp",
    "macos-speech": "apple_speech",
}

router = APIRouter(prefix="/api/videos", tags=["videos"])

_TRACKED_FIELDS_FOR_DIFF = (
    "title",
    "description",
    "tags",
    "privacy_status",
    "publish_at",
    "pinned_links",
)


def _decode_tags(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        decoded = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return []
    if isinstance(decoded, list):
        return [str(t) for t in decoded]
    return []


def _video_public(row: dict) -> dict:
    """Project a ``videos`` row for the API: expose ``/media/...`` URLs and a
    display filename instead of the server's absolute filesystem paths.

    Note: ``source_file_origin`` (migration 026) flows through unchanged
    because the client needs it to render the origin tag on the local-
    file pill and the origin row in the file-info modal. It's a plain
    enum string — no path to strip — so it survives the absolute-path
    cleanup below by virtue of not being on the strip list.
    """
    out = dict(row)
    out["thumbnail_url"] = media_url(out.get("thumbnail_path"))
    out["video_file_url"] = media_url(out.get("video_file_path"))
    out["video_file_name"] = media_filename(out.get("video_file_path"))
    # Dual-thumbnail (migration 018): the YouTube-side copy lives on a
    # separate column so the detail page can render both side-by-side
    # when the Claude-vision compare flagged them as different.
    out["youtube_thumbnail_media_url"] = media_url(out.get("youtube_thumbnail_path"))
    out.pop("thumbnail_path", None)
    out.pop("video_file_path", None)
    out.pop("youtube_thumbnail_path", None)
    return out


@router.get("")
async def list_videos(
    project_slug: str | None = None,
    include_children: bool = False,
):
    """List tracked videos.

    Filters to a single project when ``?project_slug=`` is supplied. Per-
    project dashboards always pass it; the legacy unfiltered call (no
    query string) returns everything across every project so the import
    pages and admin views still work.

    Children (rows where ``parent_item_id`` is set) are hidden by
    default — the Dashboard "Your Videos" list shows only primaries.
    Set ``include_children=true`` for callers that need to see every
    row (e.g. the import-dedup branch, admin views).
    """
    db = await get_db()
    if project_slug:
        cursor = await db.execute(
            "SELECT id FROM projects WHERE slug = ?", (project_slug,)
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, f"Project '{project_slug}' not found")
        if include_children:
            rows = await db.execute_fetchall(
                "SELECT * FROM videos WHERE project_id = ? "
                "ORDER BY created_at DESC",
                (int(row["id"]),),
            )
        else:
            rows = await db.execute_fetchall(
                "SELECT * FROM videos WHERE project_id = ? "
                "AND parent_item_id IS NULL ORDER BY created_at DESC",
                (int(row["id"]),),
            )
    else:
        if include_children:
            rows = await db.execute_fetchall(
                "SELECT * FROM videos ORDER BY created_at DESC"
            )
        else:
            rows = await db.execute_fetchall(
                "SELECT * FROM videos WHERE parent_item_id IS NULL "
                "ORDER BY created_at DESC"
            )
    return [_video_public(dict(r)) for r in rows]


@router.get("/transcription-backends")
async def list_transcription_backends():
    """List available transcription backends."""
    from yt_scheduler.services import transcription
    return transcription.list_available_backends()


@router.get("/transcription-model-cached")
async def is_transcription_model_cached(backend: str, model: str) -> dict:
    """Report whether a transcription model is locally cached so the UI can
    warn the user about a multi-minute first-run download (~1.5 GB for
    medium, ~3 GB for large-v3) before kicking off an MLX transcribe.
    Currently only meaningful for the mlx-whisper backend; other backends
    return ``{"cached": null}`` (we don't know)."""
    from yt_scheduler.services import transcription
    cached = transcription.is_model_cached(backend=backend, model=model)
    return {"backend": backend, "model": model, "cached": cached}


@router.get("/scheduled")
async def list_scheduled():
    """List all videos with scheduled publishes."""
    from yt_scheduler.services.scheduler import get_scheduled_jobs
    return get_scheduled_jobs()


@router.get("/{video_id}/events")
async def list_video_events(video_id: str, limit: int = 200):
    """Per-video activity log (newest first)."""
    return await events.list_events_for_video(video_id, limit=limit)


@router.get("/{video_id}/auto-actions")
async def get_auto_actions_state(video_id: str) -> dict:
    """Per-video Promo-flow progress for the polling UI.

    Returned shape:
        ``{"state": str | None, "last_error": str | None, "updated_at": str}``

    ``state`` is one of the ``PROMO_STATE_*`` strings or
    ``"failed:<step>"``; ``None`` means the row has never been touched
    by the Promo chain. Detail pages poll this every 3s while the state
    is non-terminal and back off to 15s once it lands on ``ready`` or
    ``failed:*``.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT auto_action_state, auto_action_last_error, updated_at "
        "FROM videos WHERE id = ?",
        (video_id,),
    )
    if not rows:
        raise HTTPException(404, "Video not found")
    row = dict(rows[0])
    return {
        "state": row.get("auto_action_state"),
        "last_error": row.get("auto_action_last_error"),
        "updated_at": row.get("updated_at"),
    }


@router.post("/{video_id}/auto-actions/retry")
async def retry_auto_actions_step(video_id: str, step: str) -> dict:
    """Re-run the Promo chain from ``step`` onward. Used by the per-card
    "Retry <step>" button when a step has landed in ``failed:<step>``."""
    from yt_scheduler.services import auto_actions
    try:
        await auto_actions.retry_promo_step(video_id, step)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"status": "ok", "video_id": video_id, "step": step}


@router.get("/{video_id}")
async def get_video(video_id: str):
    """Get a single video's details."""
    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    if not rows:
        raise HTTPException(404, "Video not found")
    row = dict(rows[0])

    await _bind_project_for_video(video_id)
    # Also fetch live YouTube data. ``youtube.get_video`` is the
    # google-api-python-client sync call — wrap it in to_thread so the
    # event loop doesn't stall on the round-trip (the C1 polling tick
    # hits this endpoint every 3s while a download is in progress;
    # blocking inline meant every other concurrent request paused).
    yt_data = None
    yt_error: str | None = None
    try:
        yt_data = await asyncio.to_thread(youtube.get_video, video_id)
    except Exception as e:
        yt_error = str(e)

    # Auto-sync local DB from YouTube. Whenever the canonical fields
    # (title, description, tags, privacy_status) drift on YouTube — the
    # user edited them in Studio, another tool updated them, etc. — we
    # take YouTube's value as truth and overwrite the local row. The
    # user's in-progress unsaved form edits are protected client-side
    # (loadVideo() preserves dirty fields), so this can't clobber typed
    # but unsent text. Only runs when we actually got a yt_data response
    # — a 404 from YouTube (standalone item, deleted video) leaves the
    # local row untouched.
    if yt_data:
        updates = _diff_youtube_metadata(row, yt_data)
        if updates:
            sets = ", ".join(f"{col} = ?" for col in updates)
            params = list(updates.values()) + [video_id]
            await db.execute(
                f"UPDATE videos SET {sets}, updated_at = datetime('now') WHERE id = ?",
                params,
            )
            await db.commit()
            rows = await db.execute_fetchall(
                "SELECT * FROM videos WHERE id = ?", (video_id,)
            )
            if rows:
                row = dict(rows[0])

    # Schedule the dual-thumbnail refresh + Claude compare in the
    # background. The next loadVideo() poll on the detail page picks
    # up thumbnail_compare_verdict / youtube_thumbnail_media_url once
    # the background task lands its UPDATE.
    if yt_data:
        from yt_scheduler.services import thumbnail_sync
        thumbnail_sync.schedule_refresh(video_id, yt_data)

    result = _video_public(row)
    if yt_data:
        result["youtube_data"] = yt_data
    if yt_error is not None:
        result["youtube_data_error"] = yt_error

    # Surface the parent's title so the detail page can render the
    # "Promo of: <parent title>" backlink without a second round-trip.
    parent_item_id = row.get("parent_item_id")
    if parent_item_id:
        parent_rows = await db.execute_fetchall(
            "SELECT title FROM videos WHERE id = ?", (parent_item_id,)
        )
        result["parent_title"] = (
            dict(parent_rows[0]).get("title") if parent_rows else None
        )

    return result


def _diff_youtube_metadata(local_row: dict, yt_data: dict) -> dict[str, object]:
    """Return the subset of (title, description, tags, privacy_status)
    where YouTube's value differs from what we have locally — the
    caller turns this dict into a single UPDATE.

    Only compares the four canonical fields the user can edit. Returns
    {} when everything matches so the caller can skip the DB write."""
    snippet = (yt_data or {}).get("snippet", {}) or {}
    status = (yt_data or {}).get("status", {}) or {}

    yt_title = snippet.get("title")
    yt_description = snippet.get("description")
    yt_tags = snippet.get("tags") or []
    yt_privacy = status.get("privacyStatus")

    out: dict[str, object] = {}
    if yt_title is not None and yt_title != (local_row.get("title") or ""):
        out["title"] = yt_title
    if yt_description is not None and yt_description != (local_row.get("description") or ""):
        out["description"] = yt_description
    # Tags are stored as JSON-encoded array in the DB; compare against
    # the decoded list so a no-op re-serialization doesn't trip the diff.
    local_tags_raw = local_row.get("tags") or "[]"
    try:
        local_tags = json.loads(local_tags_raw) if local_tags_raw else []
        if not isinstance(local_tags, list):
            local_tags = []
    except json.JSONDecodeError:
        local_tags = []
    if list(yt_tags) != list(local_tags):
        out["tags"] = json.dumps(list(yt_tags))
    if yt_privacy and yt_privacy != (local_row.get("privacy_status") or ""):
        out["privacy_status"] = yt_privacy
    return out


_YT_BACKED_ITEM_TYPES = {"episode", "short", "segment", "hook"}


@router.post("/upload")
async def upload_video(
    video_file: UploadFile = File(...),
    thumbnail_file: UploadFile | None = File(None),
    title: str = Form(...),
    description: str = Form(""),
    tags: str = Form(""),  # comma-separated
    pinned_links: str = Form(""),
    privacy_status: str = Form("unlisted"),
    publish_at: str = Form(""),
    project_slug: str = Form("default"),
    item_type: str = Form("episode"),
    parent_item_id: str = Form(""),
):
    """Upload a video to YouTube and track it inside a project as a typed item.

    Form fields:
        item_type: one of ``episode | short | segment | hook``. ``standalone``
            isn't valid here — standalone items don't go through YouTube; use
            the ``POST /api/videos/items`` endpoint instead (Phase D).
        parent_item_id: optional. Required-shape only for ``short``, ``segment``,
            ``hook``; for ``episode`` it must be empty.
    """
    from yt_scheduler.services import projects as project_service

    if item_type not in _YT_BACKED_ITEM_TYPES:
        raise HTTPException(
            400,
            f"item_type must be one of {sorted(_YT_BACKED_ITEM_TYPES)}; got {item_type!r}. "
            "Use POST /api/videos/items for standalone items.",
        )

    project = await project_service.get_project_by_slug(project_slug)
    if project is None:
        raise HTTPException(404, f"Project '{project_slug}' not found")
    if not project.get("youtube_channel_id"):
        raise HTTPException(
            400,
            f"Project '{project_slug}' has no YouTube channel bound; "
            f"cannot upload an {item_type} item. Bind a channel via OAuth, "
            "or create the item as a standalone via POST /api/videos/items.",
        )
    set_active_project(project["slug"])

    parent_item_id = (parent_item_id or "").strip()
    if parent_item_id and item_type == "episode":
        raise HTTPException(400, "An episode cannot have a parent_item_id.")
    if parent_item_id:
        parent_rows = await get_db()
        parent_check = await parent_rows.execute_fetchall(
            "SELECT id FROM videos WHERE id = ?", (parent_item_id,)
        )
        if not parent_check:
            raise HTTPException(400, f"parent_item_id {parent_item_id!r} not found")

    db = await get_db()

    # Save the upload under names we control — never the raw client
    # filename, which can carry path separators (traversal) or collide
    # with an unrelated upload. The video id isn't known until the
    # YouTube upload returns, so write to a temp name and rename after.
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    video_ext = safe_upload_ext(video_file.filename)
    video_path = UPLOAD_DIR / f"upload_{secrets.token_hex(8)}{video_ext}"
    with open(video_path, "wb") as f:
        shutil.copyfileobj(video_file.file, f)
    video_original_name = sanitized_original_filename(video_file.filename)

    # Save thumbnail if provided
    thumbnail_path = None
    if thumbnail_file and thumbnail_file.filename:
        thumb_ext = safe_upload_ext(thumbnail_file.filename, default=".jpg")
        thumbnail_path = UPLOAD_DIR / f"upload_{secrets.token_hex(8)}{thumb_ext}"
        with open(thumbnail_path, "wb") as f:
            shutil.copyfileobj(thumbnail_file.file, f)

    tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []

    # Upload to YouTube
    try:
        result = await asyncio.to_thread(
            youtube.upload_video,
            file_path=video_path,
            title=title,
            description=description,
            tags=tag_list,
            privacy_status=privacy_status,
            publish_at=publish_at or None,
        )
    except Exception as e:
        raise HTTPException(500, f"Upload failed: {e}")

    video_id = result["id"]

    # Rename the local files to canonical <video_id> names now the id is
    # known (matches the import path). Fall back to the temp name on any
    # rename failure rather than losing the file.
    canonical_video = UPLOAD_DIR / f"{video_id}{video_ext}"
    try:
        video_path.rename(canonical_video)
        video_path = canonical_video
    except OSError:
        pass
    if thumbnail_path is not None:
        canonical_thumb = UPLOAD_DIR / f"{video_id}_thumb{thumbnail_path.suffix}"
        try:
            thumbnail_path.rename(canonical_thumb)
            thumbnail_path = canonical_thumb
        except OSError:
            pass

    # Set thumbnail if provided
    thumbnail_error = None
    if thumbnail_path:
        try:
            await asyncio.to_thread(youtube.set_thumbnail, video_id, thumbnail_path)
        except Exception as e:
            thumbnail_error = str(e)

    # Probe duration locally so we can stamp the tier without a YouTube round-trip.
    duration = tiers.probe_local_duration(video_path)
    tier = tiers.tier_for_duration(duration)

    # Track in database. videos.url is set to the canonical YouTube URL
    # right at insert so {{url}} in templates resolves without any
    # at-render derivation. videos.item_type and videos.parent_item_id
    # are set from the form so the type-aware publish flow knows what to
    # do at publish time.
    youtube_url = f"https://youtu.be/{video_id}"
    await db.execute(
        """INSERT INTO videos (id, project_id, title, description, tags, privacy_status, publish_at,
           thumbnail_path, video_file_path, video_file_original_name, pinned_links, status,
           duration_seconds, tier,
           item_type, parent_item_id, url, source_file_origin)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'uploaded', ?, ?, ?, ?, ?, 'uploaded')""",
        (
            video_id,
            project["id"],
            title,
            description,
            json.dumps(tag_list),
            privacy_status,
            publish_at or None,
            str(thumbnail_path) if thumbnail_path else None,
            str(video_path),
            video_original_name,
            pinned_links,
            duration,
            tier,
            item_type,
            parent_item_id or None,
            youtube_url,
        ),
    )
    await db.commit()

    await events.record_event(video_id, "created", {"tier": tier, "item_type": item_type})
    await events.record_event(
        video_id,
        "uploaded",
        {"platform": "youtube", "url": youtube_url},
    )

    # Fire the configured upload-column auto-actions in the background; the
    # HTTP response should not block on transcription / Claude calls.
    await auto_actions.run_post_create_actions(
        video_id, project_id=project["id"], source="upload"
    )

    resp = {"status": "ok", "video_id": video_id, "youtube_url": youtube_url}
    if thumbnail_error:
        resp["thumbnail_error"] = thumbnail_error
    return resp


_NON_YT_ITEM_TYPES = {"hook", "standalone"}


@router.post("/items")
async def create_non_youtube_item(
    title: str = Form(...),
    description: str = Form(""),
    tags: str = Form(""),
    project_slug: str = Form("default"),
    item_type: str = Form("standalone"),
    parent_item_id: str = Form(""),
    url: str = Form(""),
    video_file: UploadFile | None = File(None),
    thumbnail_file: UploadFile | None = File(None),
):
    """Create an item that does NOT go through YouTube.

    Used for ``standalone`` items (text / image / video posts that don't get
    a YouTube counterpart — e.g. a screenshot post about a GitHub repo) and
    ``hook`` items that the user wants to post directly to social without
    also uploading to YouTube. The video file is optional — a standalone
    "post" can be text-only or images-only (use the per-item images endpoint
    to attach images).

    For YouTube-backed items (``episode | short | segment``, plus hooks that
    you do want on YouTube), use ``POST /api/videos/upload`` instead.
    """
    from yt_scheduler.services import projects as project_service

    if item_type not in _NON_YT_ITEM_TYPES:
        raise HTTPException(
            400,
            f"item_type must be one of {sorted(_NON_YT_ITEM_TYPES)}; got {item_type!r}. "
            "Use POST /api/videos/upload for YouTube-backed item types.",
        )

    project = await project_service.get_project_by_slug(project_slug)
    if project is None:
        raise HTTPException(404, f"Project '{project_slug}' not found")

    parent_item_id = (parent_item_id or "").strip() or None
    if parent_item_id:
        if item_type == "standalone":
            raise HTTPException(
                400,
                "standalone items cannot have a parent_item_id. Use item_type=hook "
                "if you want to attach to a parent episode.",
            )
        db_check = await get_db()
        parent_rows = await db_check.execute_fetchall(
            "SELECT id FROM videos WHERE id = ?", (parent_item_id,)
        )
        if not parent_rows:
            raise HTTPException(400, f"parent_item_id {parent_item_id!r} not found")

    # Generate a non-YouTube id. YT video ids are 11 chars; ours are 22 to
    # eliminate any chance of collision with future YT-backed rows.
    video_id = secrets.token_urlsafe(16)[:22]

    # On-disk names are app-chosen (<id>.<ext>) — never the raw client
    # filename. The id is already known here, so write to the canonical
    # name directly.
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    video_path: Path | None = None
    video_original_name: str | None = None
    if video_file is not None and video_file.filename:
        video_path = UPLOAD_DIR / f"{video_id}{safe_upload_ext(video_file.filename)}"
        with open(video_path, "wb") as f:
            shutil.copyfileobj(video_file.file, f)
        video_original_name = sanitized_original_filename(video_file.filename)

    thumbnail_path: Path | None = None
    if thumbnail_file is not None and thumbnail_file.filename:
        thumb_ext = safe_upload_ext(thumbnail_file.filename, default=".jpg")
        thumbnail_path = UPLOAD_DIR / f"{video_id}_thumb{thumb_ext}"
        with open(thumbnail_path, "wb") as f:
            shutil.copyfileobj(thumbnail_file.file, f)

    tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []
    duration = tiers.probe_local_duration(video_path) if video_path else 0.0
    tier = tiers.tier_for_duration(duration) if duration else ""

    db = await get_db()
    await db.execute(
        """INSERT INTO videos (id, project_id, title, description, tags,
           thumbnail_path, video_file_path, video_file_original_name, status,
           duration_seconds, tier,
           item_type, parent_item_id, url, source_file_origin)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ready', ?, ?, ?, ?, ?, ?)""",
        (
            video_id,
            project["id"],
            title,
            description,
            json.dumps(tag_list),
            str(thumbnail_path) if thumbnail_path else None,
            str(video_path) if video_path else None,
            video_original_name,
            duration,
            tier,
            item_type,
            parent_item_id,
            (url or "").strip() or None,
            "uploaded" if video_path else None,
        ),
    )
    await db.commit()
    await events.record_event(
        video_id, "created", {"tier": tier, "item_type": item_type}
    )

    return {
        "status": "ok",
        "video_id": video_id,
        "item_type": item_type,
        "url": (url or "").strip() or None,
    }


@router.put("/{video_id}")
async def update_video(video_id: str, data: dict):
    """Update video metadata."""
    db = await get_db()

    # Snapshot the existing row so we can build a diff payload after the update.
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    if not rows:
        raise HTTPException(404, "Video not found")
    before = dict(rows[0])
    before["tags"] = _decode_tags(before.get("tags"))

    await _bind_project_for_video(video_id)
    # Update on YouTube, then read back from YouTube to confirm. The
    # API can silently coerce values (privacy clamped on managed
    # channels, publish_at adjusted to comply with channel rules,
    # tags trimmed past length limits) so writing what-we-sent to the
    # DB drifts from reality. Always trust YouTube's response.
    try:
        await asyncio.to_thread(
            youtube.update_video_metadata,
            video_id=video_id,
            title=data.get("title"),
            description=data.get("description"),
            tags=data.get("tags"),
            privacy_status=data.get("privacy_status"),
            publish_at=data.get("publish_at"),
        )
    except Exception as e:
        raise HTTPException(500, f"YouTube update failed: {e}")

    confirmed = None
    try:
        fresh = await asyncio.to_thread(youtube.get_video, video_id)
        if fresh:
            snippet = fresh.get("snippet") or {}
            status = fresh.get("status") or {}
            confirmed = {
                "title": snippet.get("title"),
                "description": snippet.get("description"),
                "tags": snippet.get("tags") or [],
                "privacy_status": status.get("privacyStatus"),
                "publish_at": status.get("publishAt"),
            }
    except Exception as e:
        logger.warning("YouTube readback after metadata update failed: %s", e)

    # Update local record. Prefer YouTube's confirmed values for fields
    # that were touched in this request — that way a privacy clamp or
    # silent tag-trim shows up in the DB and event diff. Fall back to
    # the user-supplied values if readback failed.
    #
    # ``publish_at`` is special: writing it directly here would leave
    # the APScheduler job stale (registered to the OLD time). Defer to
    # ``apply_user_reschedule`` below so the job + cascade fire too.
    updates = []
    params = []
    new_privacy: str | None = None
    publish_at_for_reschedule = None
    for field in ["title", "description", "tags", "privacy_status", "publish_at", "pinned_links", "status"]:
        if field in data:
            if field == "publish_at":
                # Capture for the post-update reschedule call; skip the
                # direct write so schedule_publish owns the value.
                publish_at_for_reschedule = data[field]
                continue
            updates.append(f"{field} = ?")
            if confirmed is not None and field in confirmed and confirmed[field] is not None:
                val = confirmed[field]
            else:
                val = data[field]
            if field == "tags" and isinstance(val, list):
                val = json.dumps(val)
            if field == "privacy_status":
                new_privacy = val
            params.append(val)

    # Keep the lifecycle ``status`` column in sync with privacy transitions
    # when the caller did NOT explicitly set it. Without this, flipping the
    # Privacy dropdown to "Public" via the metadata form leaves
    # ``status`` on its pre-publish value, and downstream code that gates
    # on "is this video published?" (project counts, the per-post send
    # precheck) sees contradictory state. The publish_video_job path
    # already moves both columns atomically — this just covers the manual-
    # edit path that historically only touched privacy_status.
    if new_privacy is not None and "status" not in data:
        before_status = before.get("status")
        if new_privacy == "public" and before_status != "published":
            updates.append("status = ?")
            params.append("published")
        elif new_privacy != "public" and before_status == "published":
            updates.append("status = ?")
            params.append("ready")

    # Manual tier override (req: per-spec, user can override the inferred tier
    # on the detail screen). Tracked separately because YouTube doesn't store it.
    if "tier" in data:
        tier_value = data["tier"]
        if tier_value not in (None, "", "hook", "short", "segment", "video"):
            raise HTTPException(400, f"Invalid tier: {tier_value!r}")
        updates.append("tier = ?")
        params.append(tier_value or None)

    # Optional user-supplied episode number — local-only, never sent to
    # YouTube. Empty string / null clears the value (chip disappears).
    if "episode_number" in data:
        raw = data["episode_number"]
        if raw in (None, ""):
            episode_value: int | None = None
        else:
            try:
                episode_value = int(raw)
            except (TypeError, ValueError) as exc:
                raise HTTPException(
                    400, f"episode_number must be an integer or null; got {raw!r}"
                ) from exc
        updates.append("episode_number = ?")
        params.append(episode_value)

    if updates:
        updates.append("updated_at = datetime('now')")
        params.append(video_id)
        await db.execute(
            f"UPDATE videos SET {', '.join(updates)} WHERE id = ?", params
        )
        await db.commit()

    # publish_at changes route through apply_user_reschedule so the
    # APScheduler job actually re-registers, publish_at_manual flips
    # to 1, and the cascade (children-of-parent or same-tier-siblings)
    # fires. Without this dedicated path the metadata-form publish_at
    # edit was a silent scheduler de-sync — a pre-C4 bug.
    if publish_at_for_reschedule is not None:
        from datetime import datetime as _dt, timezone
        from yt_scheduler.services.scheduler import apply_user_reschedule

        raw = publish_at_for_reschedule
        if confirmed is not None and confirmed.get("publish_at"):
            raw = confirmed["publish_at"]
        if raw:
            try:
                normalised = (
                    raw.replace("Z", "+00:00")
                    if isinstance(raw, str) and raw.endswith("Z")
                    else raw
                )
                target = _dt.fromisoformat(normalised)
                if target.tzinfo is None:
                    target = target.replace(tzinfo=timezone.utc)
                await apply_user_reschedule(video_id, target)
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "Could not apply reschedule cascade for %s: %s",
                    video_id, exc,
                )

    if updates or publish_at_for_reschedule is not None:
        # Record per-field diff. Compare against what the user actually sent so
        # tag-list normalisation lines up.
        after = {**before, **{k: data[k] for k in data if k in _TRACKED_FIELDS_FOR_DIFF}}
        if "tags" in data and isinstance(data["tags"], str):
            after["tags"] = _decode_tags(data["tags"])
        diff = events.diff_payload(before, after, _TRACKED_FIELDS_FOR_DIFF)
        if diff:
            await events.record_event(video_id, "metadata_updated", diff)

    return {"status": "ok"}


@router.post("/{video_id}/transcribe")
async def transcribe_video(
    video_id: str,
    data: dict | None = None,
    confirm_unlist: bool = Query(default=False),
):
    """Transcribe a video locally using on-device speech recognition.

    Optional body params:
        model: Whisper model size (tiny, base, small, medium, large-v3). Default: large-v3
        language: Language code (e.g., "en"). Default: auto-detect
        backend: Force specific backend (mlx-whisper, whisper.cpp, macos-speech)

    Imported videos that aren't on disk yet are pulled via pytubefix.
    Private videos can't be downloaded anonymously, so the route returns
    HTTP 409 ``{"private_video": True}`` and the caller should re-issue
    the request with ``?confirm_unlist=true`` after asking the user
    whether to flip the video to unlisted.
    """
    from yt_scheduler.services import transcription

    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    if not rows:
        raise HTTPException(404, "Video not found")

    video = dict(rows[0])
    await _bind_project_for_video(video_id)
    video_file = video.get("video_file_path")
    if not video_file or not Path(video_file).exists():
        if not video.get("imported_from_youtube"):
            raise HTTPException(
                400,
                "Video file not found locally. Re-upload the video to "
                "enable on-device transcription.",
            )

        if confirm_unlist:
            try:
                await asyncio.to_thread(
                    youtube.set_video_privacy, video_id, "unlisted"
                )
            except Exception as exc:
                raise HTTPException(
                    400, f"Could not flip {video_id} to unlisted: {exc}"
                ) from exc
            # Match the metadata-edit path: when privacy drops away from
            # public, the lifecycle ``status`` column must follow or the
            # video lingers in 'published' with mismatched privacy.
            if (video.get("status") or "") == "published":
                await db.execute(
                    "UPDATE videos SET privacy_status = 'unlisted', "
                    "status = 'ready', updated_at = datetime('now') "
                    "WHERE id = ?",
                    (video_id,),
                )
            else:
                await db.execute(
                    "UPDATE videos SET privacy_status = 'unlisted', "
                    "updated_at = datetime('now') WHERE id = ?",
                    (video_id,),
                )
            await db.commit()

        try:
            downloaded = await asyncio.to_thread(
                youtube.download_video_file, video_id, UPLOAD_DIR
            )
        except youtube.PrivateVideoError as exc:
            raise HTTPException(
                409,
                {
                    "private_video": True,
                    "video_id": video_id,
                    "message": (
                        "This video is private on YouTube. To download it for "
                        "transcription, it needs to be flipped to unlisted "
                        "first. It will stay unlisted afterwards."
                    ),
                },
            ) from exc
        except Exception as exc:
            raise HTTPException(
                400, f"Could not download video for re-transcription ({exc})."
            ) from exc

        video_file = str(downloaded)
        await db.execute(
            "UPDATE videos SET video_file_path = ?, updated_at = datetime('now') "
            "WHERE id = ?",
            (video_file, video_id),
        )
        await db.commit()

    opts = data or {}
    try:
        # transcribe() is fully synchronous — runs ffmpeg, downloads HF
        # weights on first use of a model (~1.5 GB for medium), and runs
        # the actual inference. Without to_thread the FastAPI event loop
        # would freeze for the entire duration, dropping TCP keep-alives
        # and stalling concurrent requests; that was the source of the
        # "Network error: Load failed" the client saw on a long
        # first-run download.
        result = await asyncio.to_thread(
            transcription.transcribe,
            video_path=video_file,
            model=opts.get("model", "large-v3"),
            language=opts.get("language"),
            backend=opts.get("backend"),
        )
    except RuntimeError as e:
        raise HTTPException(400, str(e))

    # Save transcript, SRT, and JSON with word-level timestamps
    srt_path = result.save_srt(video_file)
    vtt_path = result.save_vtt(video_file)

    # Save full JSON transcript with word-level timestamps
    json_path = UPLOAD_DIR / f"{Path(video_file).stem}_transcript.json"
    json_path.write_text(json.dumps(result.to_json(), indent=2), encoding="utf-8")

    old_transcript = video.get("transcript") or ""

    # Store SRT canonically — preserves segment timestamps so we can re-upload
    # to YouTube as a caption track or detect chapters later. Plain-text
    # consumers (Claude prompts, template `{{transcript}}`) strip on read.
    canonical_transcript = result.to_srt()

    # Record this transcription as a row in the transcripts table so the user
    # can switch back and forth between alternates on the detail page.
    source = _BACKEND_TO_SOURCE.get(result.backend, "user_edited")
    source_detail = opts.get("model") if source.startswith(("mlx", "faster", "whisp")) else None
    transcript_id = await transcript_service.upsert_transcript_for_source(
        video_id,
        source,
        canonical_transcript,
        source_detail=source_detail,
    )

    await db.execute(
        """UPDATE videos SET
            transcript = ?,
            transcript_id = ?,
            transcript_source = ?,
            transcript_created_at = COALESCE(transcript_created_at, datetime('now')),
            transcript_updated_at = datetime('now'),
            transcript_is_edited = 0,
            status = 'captioned',
            updated_at = datetime('now')
        WHERE id = ?""",
        (canonical_transcript, transcript_id, source, video_id),
    )
    await db.commit()

    if old_transcript != canonical_transcript:
        await events.record_event(
            video_id,
            "metadata_updated",
            {"transcript": {"old": old_transcript, "new": canonical_transcript}},
        )

    return {
        "status": "ok",
        "backend": result.backend,
        "language": result.language,
        "segments": len(result.segments),
        "word_count": len(result.all_words),
        "has_word_timestamps": result.has_word_timestamps,
        "characters": len(result.text),
        "srt_path": str(srt_path),
        "vtt_path": str(vtt_path),
        "json_path": str(json_path),
        "transcript_preview": result.text[:500],
    }


@router.post("/{video_id}/generate-description")
async def generate_description(video_id: str, data: dict | None = None):
    """Generate an SEO description from the video's transcript, or from
    keyframes when no transcript exists.

    Optional body keys:
      * ``extra_instructions``: appended to the prompt verbatim.
      * ``mode``: ``"transcript"`` (default), ``"frames"``, or ``"auto"``.
        ``auto`` uses transcript when present, falls back to frames when
        the video has a local file. ``frames`` forces frame-based even
        if a transcript exists (useful when the transcript is wrong or
        the visuals are the actual content).
    """
    from pathlib import Path as _P

    from yt_scheduler.services import media as media_service

    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    if not rows:
        raise HTTPException(404, "Video not found")

    video = dict(rows[0])
    project_id = int(video.get("project_id") or 1)
    transcript = video.get("transcript", "")
    extra = (data or {}).get("extra_instructions", "")
    mode = ((data or {}).get("mode") or "auto").strip()

    use_frames: bool
    if mode == "transcript":
        if not transcript:
            raise HTTPException(
                400,
                "No transcript available yet. Wait for captions or upload one — "
                "or pass mode='frames' to describe from keyframes instead.",
            )
        use_frames = False
    elif mode == "frames":
        use_frames = True
    else:  # auto
        use_frames = not transcript

    video_file = video.get("video_file_path") or ""
    if use_frames and not (video_file and _P(video_file).exists()):
        raise HTTPException(
            400,
            "No transcript available, and no local video file to extract "
            "keyframes from. Re-import the video or upload a copy.",
        )

    try:
        if use_frames:
            frames = await asyncio.to_thread(
                media_service.extract_keyframes, video_file, 6,
            )
            if not frames:
                raise HTTPException(
                    502,
                    "ffmpeg returned no usable keyframes — the video file may "
                    "be corrupted or in an unsupported format.",
                )
            description = await ai.generate_seo_description_from_frames(
                title=video["title"],
                frames=frames,
                extra_instructions=extra,
                project_id=project_id,
            )
        else:
            description = await ai.generate_seo_description(
                title=video["title"],
                transcript=transcript,
                extra_instructions=extra,
                project_id=project_id,
            )
    except HTTPException:
        raise
    except Exception as e:
        # Surface Anthropic auth / rate-limit / transport failures as a clean 502
        # instead of bubbling up as an uncaught 500 (which breaks the JSON client).
        msg = str(e)
        if "authentication_error" in msg or "invalid x-api-key" in msg or "401" in msg:
            raise HTTPException(
                502,
                "Anthropic API key rejected (401). Open Settings and replace it "
                "with a valid key from console.anthropic.com (starts with sk-ant-).",
            )
        raise HTTPException(502, f"Claude API call failed: {msg}")

    # Compose full description with pinned links appended at the end — keeps
    # the AI-written hook at the top (which is what viewers see in the collapsed
    # description on YouTube) and pushes boilerplate links below.
    pinned = video.get("pinned_links", "")
    full_description = f"{description}\n\n{pinned}" if pinned else description

    # Store generated description
    await db.execute(
        "UPDATE videos SET generated_description = ?, updated_at = datetime('now') WHERE id = ?",
        (full_description, video_id),
    )
    await db.commit()

    return {"description": full_description, "raw_ai_description": description}


@router.post("/{video_id}/generate-tags")
async def generate_tags(video_id: str, data: dict | None = None):
    """Suggest YouTube tags for a video with Claude.

    Optional body keys:
      * ``mode``: ``"metadata"`` (default — title + description + transcript)
        or ``"frames"`` (sample keyframes from the local video file and tag
        from what's visible; useful when there's no transcript).

    Returns ``{"tags": [...]}``. The result is intentionally *not* persisted —
    the caller drops it into the editor for review and saves through the
    normal metadata update, mirroring how ``generate-description`` stages its
    output rather than writing straight to the live field.
    """
    from pathlib import Path as _P

    from yt_scheduler.services import media as media_service

    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    if not rows:
        raise HTTPException(404, "Video not found")

    video = dict(rows[0])
    project_id = int(video.get("project_id") or 1)
    mode = ((data or {}).get("mode") or "metadata").strip()

    try:
        if mode == "frames":
            video_file = video.get("video_file_path") or ""
            if not (video_file and _P(video_file).exists()):
                raise HTTPException(
                    400,
                    "No local video file to extract keyframes from. Re-import "
                    "the video or upload a copy.",
                )
            frames = await asyncio.to_thread(
                media_service.extract_keyframes, video_file, 6,
            )
            if not frames:
                raise HTTPException(
                    502,
                    "ffmpeg returned no usable keyframes — the video file may "
                    "be corrupted or in an unsupported format.",
                )
            tags = await ai.generate_tags_from_frames(
                title=video.get("title", ""),
                description=video.get("description", "") or "",
                frames=frames,
                project_id=project_id,
            )
        else:
            tags = await ai.generate_tags_from_metadata(
                title=video.get("title", ""),
                description=video.get("description", "") or "",
                transcript=video.get("transcript", "") or "",
                project_id=project_id,
            )
    except HTTPException:
        raise
    except Exception as e:
        msg = str(e)
        if "authentication_error" in msg or "invalid x-api-key" in msg or "401" in msg:
            raise HTTPException(
                502,
                "Anthropic API key rejected (401). Open Settings and replace it "
                "with a valid key from console.anthropic.com (starts with sk-ant-).",
            )
        raise HTTPException(502, f"Claude API call failed: {msg}")

    return {"tags": tags}


@router.post("/{video_id}/apply-description")
async def apply_description(video_id: str):
    """Apply the generated description to the YouTube video."""
    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    if not rows:
        raise HTTPException(404, "Video not found")

    video = dict(rows[0])
    desc = video.get("generated_description", "")
    if not desc:
        raise HTTPException(400, "No generated description. Generate one first.")

    await _bind_project_for_video(video_id)
    await asyncio.to_thread(youtube.update_video_metadata, video_id, description=desc)

    old_description = video.get("description") or ""
    await db.execute(
        "UPDATE videos SET description = ?, status = 'ready', "
        "description_generated_at = datetime('now'), updated_at = datetime('now') "
        "WHERE id = ?",
        (desc, video_id),
    )
    await db.commit()

    if old_description != desc:
        await events.record_event(
            video_id,
            "metadata_updated",
            {"description": {"old": old_description, "new": desc}},
        )

    return {"status": "ok"}


@router.post("/{video_id}/publish")
async def publish_video(video_id: str):
    """Publish a video immediately — flips to public and fires all approved social posts."""
    from yt_scheduler.services.scheduler import publish_video_job
    result = await publish_video_job(video_id)
    return result


@router.post("/{video_id}/schedule")
async def schedule_video(video_id: str, data: dict):
    """Schedule a video to go public at a specific time.

    Body: {"publish_at": "2026-04-03T09:00:00-05:00"}

    At the scheduled time, the video flips to public and all
    approved social posts are sent simultaneously.

    User-driven reschedule path: stamps ``publish_at_manual = 1`` on
    the target row, then fires the appropriate cascade — primary
    targets shift their auto-anchored children by the same delta;
    child targets shift later same-tier siblings by the same delta.
    Manually-overridden rows are left in place.
    """
    from datetime import datetime as dt, timezone
    from yt_scheduler.services.scheduler import apply_user_reschedule

    publish_at_str = data.get("publish_at")
    if not publish_at_str:
        raise HTTPException(400, "publish_at is required (ISO 8601 datetime)")

    try:
        publish_at = dt.fromisoformat(publish_at_str)
    except ValueError:
        raise HTTPException(400, "Invalid datetime format. Use ISO 8601 (e.g., 2026-04-03T09:00:00-05:00)")

    if publish_at.tzinfo is None:
        publish_at = publish_at.replace(tzinfo=timezone.utc)

    if publish_at <= dt.now(timezone.utc):
        raise HTTPException(400, "publish_at must be in the future")

    cascade = await apply_user_reschedule(video_id, publish_at)
    await events.record_event(
        video_id,
        "publish_scheduled",
        {
            "platform": "youtube",
            "publish_at": publish_at.isoformat(),
            "url": f"https://youtu.be/{video_id}",
        },
    )
    return {
        "status": "ok",
        "job_id": f"publish_{video_id}",
        "publish_at": publish_at.isoformat(),
        "cascaded_children": cascade["cascaded_children"],
        "cascaded_siblings": cascade["cascaded_siblings"],
        "message": f"Video will go public and social posts will fire at {publish_at.isoformat()}",
    }


@router.delete("/{video_id}/schedule")
async def cancel_schedule(video_id: str):
    """Cancel a scheduled publish."""
    from yt_scheduler.services.scheduler import cancel_scheduled_publish
    cancelled = await cancel_scheduled_publish(video_id)
    if cancelled:
        return {"status": "ok", "message": "Schedule cancelled"}
    raise HTTPException(404, "No scheduled publish found for this video")


@router.post("/{video_id}/schedule-social")
async def schedule_social_only(video_id: str, data: dict):
    """Stagger this video's approved social posts on a chosen timeline,
    without touching the video itself. Use this when the video is
    already public and you just want a custom fan-out for the posts.

    Body:
        ``first_post_at`` (required, ISO 8601) — when the first approved
            post fires; subsequent posts each follow at ``+spacing``.
        ``spacing_minutes`` (optional, int ≥ 0) — per-batch override of
            the project's ``inter_post_spacing_minutes``. Not persisted.

    ``post_video_delay_minutes`` is intentionally ignored — "wait X
    minutes after the video goes live" has no meaning once the video
    is already live, so the picked time IS when the first post fires.
    """
    from datetime import datetime as dt, timezone
    from yt_scheduler.services.scheduler import (
        schedule_approved_social_posts_only,
    )

    raw = data.get("first_post_at")
    if not raw:
        raise HTTPException(400, "first_post_at is required (ISO 8601 datetime)")
    try:
        when = dt.fromisoformat(raw)
    except ValueError as exc:
        raise HTTPException(400, "Invalid datetime format") from exc
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    if when <= dt.now(timezone.utc):
        raise HTTPException(400, "first_post_at must be in the future")

    spacing_minutes = data.get("spacing_minutes")
    if spacing_minutes is not None:
        try:
            spacing_minutes = int(spacing_minutes)
        except (TypeError, ValueError) as exc:
            raise HTTPException(400, "spacing_minutes must be an integer") from exc
        # 0 (or negative) would land every post on the same DateTrigger
        # and fire them all simultaneously — never what the user wanted.
        if spacing_minutes < 1:
            raise HTTPException(400, "spacing_minutes must be ≥ 1")

    # Existence check up-front so we can return a clean 404 here — the
    # service raises ValueError for several reasons (bad project_id,
    # malformed spacing, etc.) and conflating them all as 404 hides bugs.
    db = await get_db()
    cursor = await db.execute("SELECT id FROM videos WHERE id = ?", (video_id,))
    if await cursor.fetchone() is None:
        raise HTTPException(404, f"Video {video_id} not found")

    count, errors = await schedule_approved_social_posts_only(
        video_id, when, spacing_minutes=spacing_minutes,
    )
    return {
        "status": "ok",
        "scheduled": count,
        "errors": errors,
        "first_post_at": when.isoformat(),
        "spacing_minutes": spacing_minutes,
    }


@router.delete("/{video_id}/schedule-social")
async def cancel_schedule_social(video_id: str):
    """Cancel every per-post job for this video, leaving the video's own
    publish_at / status alone. Pairs with the POST sibling above."""
    from yt_scheduler.services.scheduler import (
        cancel_video_social_post_schedule,
    )
    cancelled = await cancel_video_social_post_schedule(video_id)
    return {"status": "ok", "cancelled": cancelled}


@router.get("/{video_id}/captions")
async def list_captions(video_id: str):
    """List available caption tracks."""
    await _bind_project_for_video(video_id)
    try:
        return await asyncio.to_thread(youtube.list_captions, video_id)
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/{video_id}/comments")
async def list_comments(video_id: str, max_results: int = 50):
    """List comments on a video."""
    await _bind_project_for_video(video_id)
    try:
        return await asyncio.to_thread(
            youtube.list_comment_threads, video_id, max_results=max_results
        )
    except Exception as e:
        error_msg = str(e)
        if "disabled" in error_msg.lower() or "commentsDisabled" in error_msg:
            raise HTTPException(403, "Comments are disabled on this video")
        raise HTTPException(500, error_msg)


@router.post("/{video_id}/set-thumbnail")
async def set_thumbnail(video_id: str, file: UploadFile = File(...)):
    """Upload and set a video thumbnail."""
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    thumb_ext = safe_upload_ext(file.filename, default=".jpg")
    path = UPLOAD_DIR / f"{video_id}_thumb{thumb_ext}"
    # Defense in depth: the basename pattern can't escape, but assert it.
    resolved = path.resolve()
    if UPLOAD_DIR.resolve() not in resolved.parents:
        raise HTTPException(400, "Invalid thumbnail path")
    with open(path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    await _bind_project_for_video(video_id)
    try:
        await asyncio.to_thread(youtube.set_thumbnail, video_id, path)
    except Exception as e:
        raise HTTPException(500, f"Failed to set thumbnail: {e}")

    # The user just made our local thumbnail the truth, so it's a
    # 'user' source again, and any stored compare verdict is stale —
    # the next get_video will refresh youtube_thumbnail_url and ask
    # Claude again.
    db = await get_db()
    await db.execute(
        "UPDATE videos SET thumbnail_path = ?, thumbnail_source = 'user', "
        "thumbnail_compare_verdict = NULL, thumbnail_compared_at = NULL, "
        "updated_at = datetime('now') WHERE id = ?",
        (str(path), video_id),
    )
    await db.commit()

    return {"status": "ok"}


@router.post("/{video_id}/thumbnail/use-youtube")
async def use_youtube_thumbnail(video_id: str):
    """Promote the cached YouTube-side thumbnail to be the active local
    thumbnail. Used when the Claude-vision compare flagged the two as
    different and the user prefers what's currently on YouTube. The
    YouTube-side file is left in place (still in UPLOAD_DIR) so future
    fetches have something to diff against."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT youtube_thumbnail_path FROM videos WHERE id = ?", (video_id,)
    )
    if not rows:
        raise HTTPException(404, "Video not found")
    yt_local = rows[0]["youtube_thumbnail_path"]
    if not yt_local or not Path(yt_local).exists():
        raise HTTPException(
            400,
            "No cached YouTube thumbnail to promote. Open the video so the "
            "thumbnail-sync background task can fetch one.",
        )
    await db.execute(
        "UPDATE videos SET thumbnail_path = ?, thumbnail_source = 'youtube', "
        "thumbnail_compare_verdict = 'same', thumbnail_compared_at = datetime('now'), "
        "updated_at = datetime('now') WHERE id = ?",
        (yt_local, video_id),
    )
    await db.commit()
    return {"status": "ok"}


@router.post("/{video_id}/thumbnail/push-to-youtube")
async def push_thumbnail_to_youtube(video_id: str):
    """Upload the current local thumbnail back to YouTube. Used when
    Claude flagged the two as different and the user wants to keep
    what they uploaded. After a successful push, YouTube should match
    the local copy so we mark the verdict 'same' (best-effort — the
    next compare on a fresh open will catch any re-encode that flips
    Claude's mind)."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT thumbnail_path FROM videos WHERE id = ?", (video_id,)
    )
    if not rows:
        raise HTTPException(404, "Video not found")
    local = rows[0]["thumbnail_path"]
    if not local or not Path(local).exists():
        raise HTTPException(400, "No local thumbnail to push.")

    await _bind_project_for_video(video_id)
    try:
        await asyncio.to_thread(youtube.set_thumbnail, video_id, Path(local))
    except Exception as exc:
        raise HTTPException(500, f"Failed to push thumbnail: {exc}") from exc

    await db.execute(
        "UPDATE videos SET thumbnail_compare_verdict = 'same', "
        "thumbnail_compared_at = datetime('now'), "
        "updated_at = datetime('now') WHERE id = ?",
        (video_id,),
    )
    await db.commit()
    return {"status": "ok"}


def _resolve_video_file(video_file_path: str | None) -> Path | None:
    """Resolve a stored ``video_file_path`` and confirm it sits inside
    UPLOAD_DIR. Returns the resolved path, or ``None`` when there's no
    file or it would escape the upload dir — the latter guards the
    reveal action from ever acting on an arbitrary filesystem path.
    """
    if not video_file_path:
        return None
    try:
        resolved = Path(video_file_path).resolve()
        resolved.relative_to(UPLOAD_DIR.resolve())
    except (ValueError, OSError):
        return None
    return resolved


@router.get("/{video_id}/file-info")
async def video_file_info(video_id: str) -> dict:
    """Local-file details for the detail page's file-info popup: the
    name it was uploaded with, the current path on this machine, and
    the technical shape (resolution, duration, bitrate, size) of the
    file on disk if it's still there."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT video_file_path, video_file_original_name, source_file_origin "
        "FROM videos WHERE id = ?",
        (video_id,),
    )
    if not rows:
        raise HTTPException(404, f"Video '{video_id}' not found")
    raw_path = rows[0]["video_file_path"]
    resolved = _resolve_video_file(raw_path)
    exists = bool(resolved and resolved.exists())
    probe: media_service.VideoProbe | None = None
    if exists and resolved is not None:
        probe = await asyncio.to_thread(media_service.probe_video_file, resolved)
    width = probe.width if probe else None
    height = probe.height if probe else None
    codec_name = probe.codec_name if probe else None
    container = probe.container if probe else None
    return {
        "has_file": bool(raw_path),
        "original_name": rows[0]["video_file_original_name"],
        "disk_name": media_filename(raw_path),
        "exists": exists,
        "can_reveal": sys.platform == "darwin",
        "source_origin": rows[0]["source_file_origin"],
        "duration_seconds": probe.duration_seconds if probe else None,
        "width": width,
        "height": height,
        "bitrate_bps": probe.bitrate_bps if probe else None,
        "size_bytes": probe.size_bytes if probe else None,
        "codec_name": codec_name,
        "container": container,
        "browser_playable": media_service.is_browser_playable(codec_name, container),
        "quality_warnings": media_service.source_quality_warnings(
            width=width, height=height,
            source_origin=rows[0]["source_file_origin"],
        ),
    }


_DURATION_TOLERANCE_SECONDS = 2.0
_MAX_SOURCE_FILE_BYTES = 10 * 1024**3  # 10 GiB; defense against pathological uploads.

# Per-video locks: serialize concurrent POST /source-file calls for the
# same video so we can't race the SELECT-row → write-file → UPDATE-row
# sequence into producing orphans. Different videos still proceed in
# parallel.
#
# Stored as ``WeakValueDictionary`` so an idle lock — one nobody is
# currently holding or awaiting — gets garbage-collected on the first
# minor sweep after :func:`_source_file_lock` last returned it. That
# bounds the dict to ~one entry per actively-uploading video instead
# of growing forever (one entry per video_id ever uploaded against).
_source_file_locks: weakref.WeakValueDictionary[str, asyncio.Lock] = (
    weakref.WeakValueDictionary()
)
# Strong refs are taken transiently inside _source_file_lock so the
# lock object survives long enough for the caller's `async with` to
# acquire it. Once the lock is held, the caller's stack frame keeps
# it alive; we drop our strong ref immediately.


def _source_file_lock(video_id: str) -> asyncio.Lock:
    lock = _source_file_locks.get(video_id)
    if lock is None:
        lock = asyncio.Lock()
        _source_file_locks[video_id] = lock
    return lock


# When a Replace-Source upload trips a sanity issue (duration mismatch,
# resolution downgrade) the handler returns 422 with the issue list and
# the user is prompted to confirm. The old flow then re-uploaded the
# entire file with force=1 — brutal on multi-GB sources. We now keep
# the on-disk file and hand the client a one-shot ``pending_token``;
# the confirm step finalizes the already-uploaded file via
# :func:`finalize_pending_source_file`.
#
# Entries live in process memory only — a server restart drops them
# and any in-flight pending upload is then resurfaced as an orphan
# file by ``cleanup_orphan_pending_source_files`` (called from app
# lifespan).
_PENDING_FINALIZE_TTL_SECONDS: float = 30 * 60  # 30 min
_PENDING_FINALIZES: dict[str, dict] = {}
_PENDING_FINALIZES_LOCK: asyncio.Lock = asyncio.Lock()


def _evict_stale_pending_finalizes() -> None:
    """Drop expired pending entries and unlink their files.

    Called under :data:`_PENDING_FINALIZES_LOCK` from the create / pop
    sites — keeps the dict from growing unboundedly when users open the
    confirm prompt and walk away.
    """
    now = time.monotonic()
    for token, entry in list(_PENDING_FINALIZES.items()):
        if entry["expires_at"] < now:
            _PENDING_FINALIZES.pop(token, None)
            Path(entry["path"]).unlink(missing_ok=True)


def cleanup_orphan_pending_source_files() -> int:
    """Delete every ``source_pending_*`` file in UPLOAD_DIR.

    Called on startup so pending-finalize files that survived a previous
    process being killed (the in-memory map doesn't survive a restart)
    don't accumulate on disk. The active source files use the
    ``source_*`` name without the ``pending_`` prefix, so this sweep
    can't touch a file currently referenced by the videos table.

    Returns the number of files removed (for logging).
    """
    removed = 0
    try:
        for path in UPLOAD_DIR.glob("source_pending_*"):
            try:
                path.unlink()
                removed += 1
            except OSError as exc:
                logger.debug(
                    "Could not remove orphan pending source %s: %s",
                    path, exc,
                )
    except OSError as exc:
        logger.debug("Orphan pending-source sweep failed: %s", exc)
    return removed


async def _apply_source_swap(
    *,
    video_id: str,
    row: dict,
    incoming_path: Path,
    incoming_probe: "media_service.VideoProbe | None",
    new_original: str,
    issues: list[dict],
) -> dict:
    """DB-update half of Replace Source.

    Shared by the original POST handler (when the upload passed the
    sanity checks, or force=1 overrode them) and the new
    finalize-pending endpoint (which re-uses a file already on disk).

    ``issues`` is the list returned by :func:`_evaluate_replacement`
    against the incoming file; presence of ``duration_mismatch`` here
    triggers the transcript wipe.
    """
    db = await get_db()
    force_past_duration = any(i["code"] == "duration_mismatch" for i in issues)

    # probe_is_video(None) → True (when ffprobe isn't installed we
    # trust the user's upload as best-effort), so incoming_probe can
    # legitimately be None here. Fall back to existing-row duration
    # when we can't probe.
    if incoming_probe is None or incoming_probe.duration_seconds is None:
        new_duration = row.get("duration_seconds")
        new_width = incoming_probe.width if incoming_probe else None
        new_height = incoming_probe.height if incoming_probe else None
        new_bitrate = incoming_probe.bitrate_bps if incoming_probe else None
        new_size = incoming_probe.size_bytes if incoming_probe else None
    else:
        new_duration = incoming_probe.duration_seconds
        new_width = incoming_probe.width
        new_height = incoming_probe.height
        new_bitrate = incoming_probe.bitrate_bps
        new_size = incoming_probe.size_bytes
    if force_past_duration:
        await db.execute(
            """UPDATE videos SET
                video_file_path = ?,
                video_file_original_name = ?,
                duration_seconds = ?,
                source_file_origin = 'user_attached',
                video_file_download_state = NULL,
                transcript = NULL,
                transcript_id = NULL,
                transcript_source = NULL,
                transcript_created_at = NULL,
                transcript_updated_at = NULL,
                transcript_is_edited = 0,
                updated_at = datetime('now')
            WHERE id = ?""",
            (str(incoming_path), new_original, new_duration, video_id),
        )
    else:
        await db.execute(
            """UPDATE videos SET
                video_file_path = ?,
                video_file_original_name = ?,
                duration_seconds = ?,
                source_file_origin = 'user_attached',
                video_file_download_state = NULL,
                updated_at = datetime('now')
            WHERE id = ?""",
            (str(incoming_path), new_original, new_duration, video_id),
        )
    await db.commit()

    await events.record_event(
        video_id,
        "metadata_updated",
        {
            "source_file_origin": {
                "old": row.get("source_file_origin"), "new": "user_attached",
            },
        },
    )
    if force_past_duration:
        await events.record_event(
            video_id,
            "metadata_updated",
            {"transcript_cleared_reason": "source_file_duration_mismatch_forced"},
        )

    return {
        "status": "ok",
        "original_name": new_original,
        "duration_seconds": new_duration,
        "width": new_width,
        "height": new_height,
        "bitrate_bps": new_bitrate,
        "size_bytes": new_size,
        "source_origin": "user_attached",
        "transcript_cleared": force_past_duration,
    }


def _format_resolution(width: int | None, height: int | None) -> str | None:
    if width is None or height is None:
        return None
    return f"{width}x{height}"


def _evaluate_replacement(
    *,
    incoming: media_service.VideoProbe | None,
    row: dict,
    current: media_service.VideoProbe | None,
) -> list[dict]:
    """Build the list of blocking issues for a source-file replacement.

    Each issue is a structured dict the client renders into a clear
    confirm prompt; the client then resubmits with ``?force=1`` to
    override. We collect every applicable issue rather than failing
    fast so the user sees the full picture in one round trip.

    A ``None`` ``incoming`` means ffprobe wasn't available — the caller
    can't validate and accepts the upload as a best-effort attach
    (consistent with how the rest of the app treats a missing ffprobe).
    """
    if incoming is None:
        return []
    issues: list[dict] = []

    expected_duration = row.get("duration_seconds")
    incoming_duration = incoming.duration_seconds
    if (
        expected_duration is not None
        and incoming_duration is not None
        and abs(float(expected_duration) - float(incoming_duration)) > _DURATION_TOLERANCE_SECONDS
    ):
        issues.append({
            "code": "duration_mismatch",
            "expected_seconds": float(expected_duration),
            "incoming_seconds": float(incoming_duration),
            "tolerance_seconds": _DURATION_TOLERANCE_SECONDS,
        })

    # Resolution downgrade only matters when there's a current file to
    # compare against AND that file isn't itself a known-lossy YouTube
    # re-download (replacing those is always an upgrade in spirit). The
    # same applies to generated_clip outputs: a 9:16 vertical short is
    # always 1080×1920 by construction, and attaching the original
    # landscape master would otherwise trip the height check incorrectly.
    current_origin = row.get("source_file_origin")
    if (
        current is not None
        and current.width is not None
        and current.height is not None
        and incoming.width is not None
        and incoming.height is not None
        and current_origin not in ("youtube_download", "generated_clip")
    ):
        if incoming.width < current.width and incoming.height < current.height:
            issues.append({
                "code": "resolution_downgrade",
                "current": _format_resolution(current.width, current.height),
                "incoming": _format_resolution(incoming.width, incoming.height),
            })

    return issues


@router.post("/{video_id}/source-file")
async def replace_video_source_file(
    video_id: str,
    file: UploadFile = File(...),
    force: int = Query(0),
    content_length: int | None = Header(default=None),
) -> dict:
    """Attach or replace the local high-fidelity source file for a video.

    Used by the "Replace source" / "Attach source" button on the video
    detail page. The YouTube-hosted video is never touched — this only
    swaps the local file the app uses for clip extraction and other
    on-device tasks.

    Wire format: ``multipart/form-data`` with a single ``file`` field.
    A raw octet-stream body would let us stream directly to disk in
    one pass (skipping FastAPI's ``SpooledTemporaryFile``), but
    Safari/WebKit has a bug where ``xhr.send(file)`` with custom
    request headers exhausts the file body stream before the bytes
    hit the wire ("request body stream exhausted"). Until we have a
    chunked-upload protocol, multipart is the compatibility floor.

    Sanity checks (override with ``?force=1``, or with the pending-token
    finalize flow):

    * Duration must be within ``_DURATION_TOLERANCE_SECONDS`` of the row's
      recorded duration. A larger drift almost always means the wrong
      cut was selected. Forcing past this also blanks the stored
      transcript columns, since timestamped captions are no longer
      aligned with the new file's audio.
    * If a current local file exists and isn't itself a youtube_download,
      the incoming file's resolution must not be strictly smaller in both
      width and height.

    Hard rejects (no force override):

    * 400 — no file in the request, or ffprobe ran on the upload and
      found no video stream (the user picked a non-video file).
    * 413 — file larger than ``_MAX_SOURCE_FILE_BYTES``.

    On 422 the body contains ``issues: [{code, ...details}]`` and
    ``pending_token`` so the UI can render a precise confirm prompt
    and call ``/source-file/finalize`` instead of re-uploading.

    The previous file is *not* renamed or deleted — the row is just
    re-pointed to the new file (atomic single UPDATE). The old file
    stays on disk as an orphan; this trades a little disk hygiene for
    crash-safety and avoids needing to coordinate filesystem renames
    with the DB write.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, video_file_path, video_file_original_name, "
        "duration_seconds, source_file_origin FROM videos WHERE id = ?",
        (video_id,),
    )
    if not rows:
        raise HTTPException(404, f"Video '{video_id}' not found")
    row = dict(rows[0])

    if not file.filename:
        raise HTTPException(400, "No file uploaded")
    filename = file.filename

    # Pre-flight size check from the request header before we copy a
    # 10 GB+ body to disk. A lying client can still slip past with a
    # missing/wrong header — _copy_to_disk_capped is the defense in
    # depth that catches that case at write time.
    if content_length is not None and content_length > _MAX_SOURCE_FILE_BYTES:
        raise HTTPException(
            413,
            f"File too large: {content_length} bytes exceeds "
            f"{_MAX_SOURCE_FILE_BYTES} byte cap.",
        )

    # All disk + DB work happens under a per-video lock so two
    # simultaneous replaces for the same video can't see each other's
    # half-written state and produce orphan files / stale rows.
    async with _source_file_lock(video_id):
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        # ``source_pending_*`` while the request is in flight; renamed
        # in place to ``source_*`` once the swap is committed. The
        # ``pending`` prefix lets a startup sweep
        # (cleanup_orphan_pending_source_files) safely delete leftovers
        # from a crashed / killed previous run without risking an
        # in-use source file.
        incoming_name = (
            f"source_pending_{secrets.token_hex(8)}{safe_upload_ext(filename)}"
        )
        incoming_path = UPLOAD_DIR / incoming_name

        def _copy_to_disk_capped(
            src, target: Path, cap: int, expected: int | None,
        ) -> tuple[bool, int]:
            """Stream ``src`` to ``target`` up to ``cap`` bytes.

            Returns ``(ok, bytes_written)``. ``ok=False`` means the cap
            was exceeded — the partial file has already been deleted
            before the return. ``ok=True`` does NOT guarantee the body
            was complete: ``bytes_written`` is the actual count and the
            caller compares it to ``expected`` (Content-Length) to
            detect a client-side abort that produced a short read.
            """
            written = 0
            chunk_size = 1024 * 1024
            with open(target, "wb") as fh:
                while True:
                    chunk = src.read(chunk_size)
                    if not chunk:
                        return True, written
                    written += len(chunk)
                    if written > cap:
                        fh.close()
                        target.unlink(missing_ok=True)
                        return False, written
                    fh.write(chunk)

        expected_bytes = (
            int(content_length)
            if content_length is not None and content_length > 0
            else None
        )
        ok, bytes_written = await asyncio.to_thread(
            _copy_to_disk_capped,
            file.file, incoming_path, _MAX_SOURCE_FILE_BYTES,
            expected_bytes,
        )
        if not ok:
            raise HTTPException(
                413,
                f"File too large: exceeds {_MAX_SOURCE_FILE_BYTES} byte cap.",
            )

        # Client-abort detector. When the browser cancels the XHR mid-
        # upload, Starlette's UploadFile typically returns short on the
        # underlying SpooledTemporaryFile (the body terminated early)
        # but we'd otherwise treat it as a clean close and persist the
        # truncated file. Comparing against Content-Length catches it.
        # Allow a small slack for multipart framing overhead.
        if expected_bytes is not None:
            # multipart-framed Content-Length is larger than the raw
            # file bytes (boundaries, headers); a short read by more
            # than 64KB is unambiguous truncation.
            if expected_bytes - bytes_written > 64 * 1024:
                incoming_path.unlink(missing_ok=True)
                raise HTTPException(
                    400,
                    "Upload was truncated before the full body arrived "
                    f"(got {bytes_written} of {expected_bytes} bytes). "
                    "Try again.",
                )

        incoming_probe = await asyncio.to_thread(
            media_service.probe_video_file, incoming_path
        )
        if not media_service.probe_is_video(incoming_probe):
            # ffprobe ran but the file has no decodable video stream —
            # the user picked the wrong thing. Hard reject, not even
            # force=1 can attach a non-video.
            incoming_path.unlink(missing_ok=True)
            raise HTTPException(
                400,
                "The uploaded file isn't a recognisable video.",
            )

        current_resolved = _resolve_video_file(row.get("video_file_path"))
        current_probe: media_service.VideoProbe | None = None
        if current_resolved is not None and current_resolved.exists():
            current_probe = await asyncio.to_thread(
                media_service.probe_video_file, current_resolved
            )

        issues = _evaluate_replacement(
            incoming=incoming_probe, row=row, current=current_probe
        )
        new_original = sanitized_original_filename(filename)
        if issues and not force:
            # Keep the file on disk and hand the client a token. The
            # confirm path POSTs the token to /source-file/finalize
            # instead of re-uploading the entire body — a brutal
            # round-trip on multi-GB sources.
            token = secrets.token_hex(16)
            entry = {
                "video_id": video_id,
                "path": str(incoming_path),
                "original_name": new_original,
                "probe_width": incoming_probe.width if incoming_probe else None,
                "probe_height": incoming_probe.height if incoming_probe else None,
                "probe_duration_seconds": (
                    incoming_probe.duration_seconds if incoming_probe else None
                ),
                "probe_bitrate_bps": (
                    incoming_probe.bitrate_bps if incoming_probe else None
                ),
                "probe_size_bytes": (
                    incoming_probe.size_bytes if incoming_probe else None
                ),
                "probe_codec_name": (
                    incoming_probe.codec_name if incoming_probe else None
                ),
                "probe_container": (
                    incoming_probe.container if incoming_probe else None
                ),
                "issues": issues,
                "expires_at": time.monotonic() + _PENDING_FINALIZE_TTL_SECONDS,
            }
            async with _PENDING_FINALIZES_LOCK:
                _evict_stale_pending_finalizes()
                _PENDING_FINALIZES[token] = entry
            raise HTTPException(
                422, detail={"issues": issues, "pending_token": token},
            )

        # Promote the temp file to its permanent ``source_*`` name so a
        # startup sweep can't mistake it for an orphan pending upload.
        # Wrap the rename + DB-commit in a try/except so we can log the
        # actual traceback when something explodes after the body is
        # already on disk — FastAPI's default 500 surfaces nothing
        # useful and we end up debugging blind.
        try:
            final_path = _promote_pending_to_source(incoming_path)
            result = await _apply_source_swap(
                video_id=video_id,
                row=row,
                incoming_path=final_path,
                incoming_probe=incoming_probe,
                new_original=new_original,
                issues=issues,
            )
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception(
                "Replace-source: post-upload swap failed for %s (%s)",
                video_id, incoming_path,
            )
            # Drop the on-disk file rather than leave an unreferenced
            # orphan. (rename may have already moved it — try both
            # names.)
            for p in (incoming_path, UPLOAD_DIR / incoming_path.name.replace("source_pending_", "source_", 1)):
                try:
                    p.unlink(missing_ok=True)
                except OSError:
                    pass
            raise HTTPException(
                500,
                f"Replace-source: upload completed but the swap failed: {exc}",
            )
    return result


def _promote_pending_to_source(pending_path: Path) -> Path:
    """Rename ``source_pending_<hex>.<ext>`` → ``source_<hex>.<ext>``.

    Same-filesystem rename, so it's atomic and free. Done right before
    the DB UPDATE so the row never references a path with the
    ``pending_`` prefix; the startup orphan sweep can then safely
    delete any file matching ``source_pending_*`` without risk of
    touching an in-use source.
    """
    name = pending_path.name
    if not name.startswith("source_pending_"):
        # Older code paths that pass an already-promoted path through
        # — accept it and move on instead of failing loudly.
        return pending_path
    new_name = "source_" + name[len("source_pending_"):]
    final_path = pending_path.with_name(new_name)
    pending_path.rename(final_path)
    return final_path


@router.post("/{video_id}/source-file/finalize")
async def finalize_pending_source_file(
    video_id: str, pending_token: str = Body(..., embed=True),
) -> dict:
    """Confirm a pending Replace-Source upload (force=1 on the original).

    The original POST kept the file on disk and returned a token in the
    422 body. The client calls this endpoint with that token after the
    user accepts the warning — no body re-upload needed.

    Errors:

    * 404 — token unknown / expired (the user took longer than the TTL,
      or the server restarted in between). The client should re-upload.
    * 404 — token belongs to a different video (paranoia check).
    """
    async with _PENDING_FINALIZES_LOCK:
        _evict_stale_pending_finalizes()
        entry = _PENDING_FINALIZES.pop(pending_token, None)
    if entry is None or entry["video_id"] != video_id:
        raise HTTPException(
            404,
            "Pending upload not found or expired — please re-upload.",
        )
    incoming_path = Path(entry["path"])
    if not incoming_path.exists():
        raise HTTPException(
            404,
            "Pending upload file missing on disk — please re-upload.",
        )

    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, video_file_path, video_file_original_name, "
        "duration_seconds, source_file_origin FROM videos WHERE id = ?",
        (video_id,),
    )
    if not rows:
        incoming_path.unlink(missing_ok=True)
        raise HTTPException(404, f"Video '{video_id}' not found")
    row = dict(rows[0])

    # Reconstruct a probe-shaped object from the snapshot we took at
    # upload time. We deliberately do NOT re-probe — the file hasn't
    # been touched and re-probing a multi-GB file just to read the
    # same numbers is wasteful.
    incoming_probe = media_service.VideoProbe(
        width=entry["probe_width"],
        height=entry["probe_height"],
        duration_seconds=entry["probe_duration_seconds"],
        bitrate_bps=entry["probe_bitrate_bps"],
        size_bytes=entry["probe_size_bytes"],
        codec_name=entry["probe_codec_name"],
        container=entry["probe_container"],
    )

    async with _source_file_lock(video_id):
        final_path = _promote_pending_to_source(incoming_path)
        return await _apply_source_swap(
            video_id=video_id,
            row=row,
            incoming_path=final_path,
            incoming_probe=incoming_probe,
            new_original=entry["original_name"],
            issues=entry["issues"],
        )


@router.delete("/{video_id}/source-file/pending/{pending_token}")
async def cancel_pending_source_file(
    video_id: str, pending_token: str,
) -> dict:
    """Drop a pending Replace-Source upload without finalizing.

    The user dismissed the confirm prompt — pop the entry and delete
    the on-disk file so we don't carry it until the TTL evicts it.

    Idempotent: a missing or expired token returns ``status=gone``
    rather than 404, because the user's "cancel" intent is satisfied
    either way.
    """
    async with _PENDING_FINALIZES_LOCK:
        _evict_stale_pending_finalizes()
        entry = _PENDING_FINALIZES.pop(pending_token, None)
    if entry is None or entry["video_id"] != video_id:
        return {"status": "gone"}
    Path(entry["path"]).unlink(missing_ok=True)
    return {"status": "cancelled"}


@router.post("/{video_id}/reveal-file")
async def reveal_video_file(video_id: str) -> dict:
    """Reveal the video's local file in Finder (macOS only). The path is
    resolved server-side from the row and confirmed inside UPLOAD_DIR —
    the client never supplies a path."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT video_file_path FROM videos WHERE id = ?", (video_id,)
    )
    if not rows:
        raise HTTPException(404, f"Video '{video_id}' not found")
    path = _resolve_video_file(rows[0]["video_file_path"])
    if path is None or not path.exists():
        raise HTTPException(404, "No local video file for this video.")
    if sys.platform != "darwin":
        raise HTTPException(501, "Reveal in Finder is only supported on macOS.")
    try:
        await asyncio.to_thread(
            subprocess.run, ["open", "-R", str(path)], check=True, timeout=10,
        )
    except Exception as exc:
        raise HTTPException(500, f"Could not reveal file: {exc}") from exc
    return {"revealed": True}
