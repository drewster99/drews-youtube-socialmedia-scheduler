"""Video management routes."""

from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path

from fastapi import APIRouter, Form, UploadFile, File, HTTPException

from yt_scheduler.config import UPLOAD_DIR
from yt_scheduler.database import get_db
from yt_scheduler.services import (
    ai, auto_actions, events, tiers,
    transcripts as transcript_service, youtube,
)


_BACKEND_TO_SOURCE = {
    "mlx-whisper": "mlx_whisper",
    "faster-whisper": "faster_whisper",
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


@router.get("")
async def list_videos():
    """List all tracked videos."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM videos ORDER BY created_at DESC"
    )
    return [dict(r) for r in rows]


@router.get("/transcription-backends")
async def list_transcription_backends():
    """List available transcription backends."""
    from yt_scheduler.services import transcription
    return transcription.list_available_backends()


@router.get("/scheduled")
async def list_scheduled():
    """List all videos with scheduled publishes."""
    from yt_scheduler.services.scheduler import get_scheduled_jobs
    return get_scheduled_jobs()


@router.get("/{video_id}/events")
async def list_video_events(video_id: str, limit: int = 200):
    """Per-video activity log (newest first)."""
    return await events.list_events_for_video(video_id, limit=limit)


@router.get("/{video_id}")
async def get_video(video_id: str):
    """Get a single video's details."""
    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    if not rows:
        raise HTTPException(404, "Video not found")
    result = dict(rows[0])

    # Also fetch live YouTube data
    try:
        yt_data = youtube.get_video(video_id)
        if yt_data:
            result["youtube_data"] = yt_data
    except Exception as e:
        result["youtube_data_error"] = str(e)

    return result


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
):
    """Upload a video to YouTube and track it inside a project."""
    from yt_scheduler.services import projects as project_service

    project = await project_service.get_project_by_slug(project_slug)
    if project is None:
        raise HTTPException(404, f"Project '{project_slug}' not found")

    db = await get_db()

    # Save video file locally
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    video_path = UPLOAD_DIR / video_file.filename
    with open(video_path, "wb") as f:
        shutil.copyfileobj(video_file.file, f)

    # Save thumbnail if provided
    thumbnail_path = None
    if thumbnail_file and thumbnail_file.filename:
        thumbnail_path = UPLOAD_DIR / thumbnail_file.filename
        with open(thumbnail_path, "wb") as f:
            shutil.copyfileobj(thumbnail_file.file, f)

    tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []

    # Upload to YouTube
    try:
        result = youtube.upload_video(
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

    # Set thumbnail if provided
    thumbnail_error = None
    if thumbnail_path:
        try:
            youtube.set_thumbnail(video_id, thumbnail_path)
        except Exception as e:
            thumbnail_error = str(e)

    # Probe duration locally so we can stamp the tier without a YouTube round-trip.
    duration = tiers.probe_local_duration(video_path)
    tier = tiers.tier_for_duration(duration)

    # Track in database
    await db.execute(
        """INSERT INTO videos (id, project_id, title, description, tags, privacy_status, publish_at,
           thumbnail_path, video_file_path, pinned_links, status,
           duration_seconds, tier)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'uploaded', ?, ?)""",
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
            pinned_links,
            duration,
            tier,
        ),
    )
    await db.commit()

    youtube_url = f"https://youtu.be/{video_id}"
    await events.record_event(video_id, "created", {"tier": tier})
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

    # Update on YouTube
    try:
        youtube.update_video_metadata(
            video_id=video_id,
            title=data.get("title"),
            description=data.get("description"),
            tags=data.get("tags"),
            privacy_status=data.get("privacy_status"),
            publish_at=data.get("publish_at"),
        )
    except Exception as e:
        raise HTTPException(500, f"YouTube update failed: {e}")

    # Update local record
    updates = []
    params = []
    for field in ["title", "description", "tags", "privacy_status", "publish_at", "pinned_links", "status"]:
        if field in data:
            updates.append(f"{field} = ?")
            val = data[field]
            if field == "tags" and isinstance(val, list):
                val = json.dumps(val)
            params.append(val)

    # Manual tier override (req: per-spec, user can override the inferred tier
    # on the detail screen). Tracked separately because YouTube doesn't store it.
    if "tier" in data:
        tier_value = data["tier"]
        if tier_value not in (None, "", "hook", "short", "segment", "video"):
            raise HTTPException(400, f"Invalid tier: {tier_value!r}")
        updates.append("tier = ?")
        params.append(tier_value or None)

    if updates:
        updates.append("updated_at = datetime('now')")
        params.append(video_id)
        await db.execute(
            f"UPDATE videos SET {', '.join(updates)} WHERE id = ?", params
        )
        await db.commit()

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
async def transcribe_video(video_id: str, data: dict | None = None):
    """Transcribe a video locally using on-device speech recognition.

    Optional body params:
        model: Whisper model size (tiny, base, small, medium, large-v3). Default: large-v3
        language: Language code (e.g., "en"). Default: auto-detect
        backend: Force specific backend (mlx-whisper, faster-whisper, whisper.cpp, macos-speech)
    """
    from yt_scheduler.services import transcription

    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    if not rows:
        raise HTTPException(404, "Video not found")

    video = dict(rows[0])
    video_file = video.get("video_file_path")
    if not video_file or not Path(video_file).exists():
        # For imported videos we don't keep a local copy by default. Pull it
        # on demand via yt-dlp so Apple Speech / Whisper has a file to read.
        if video.get("imported_from_youtube"):
            try:
                downloaded = await asyncio.to_thread(
                    youtube.download_video_file, video_id, UPLOAD_DIR
                )
            except Exception as exc:
                raise HTTPException(
                    400,
                    "Could not download video for re-transcription "
                    f"({exc}). Install yt-dlp with: "
                    "pip install -e \".[youtube-download]\""
                ) from exc
            video_file = str(downloaded)
            await db.execute(
                "UPDATE videos SET video_file_path = ?, updated_at = datetime('now') "
                "WHERE id = ?",
                (video_file, video_id),
            )
            await db.commit()
        else:
            raise HTTPException(
                400,
                "Video file not found locally. Re-upload the video to "
                "enable on-device transcription.",
            )

    opts = data or {}
    try:
        result = transcription.transcribe(
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
    """Generate an SEO description from the video's transcript."""
    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    if not rows:
        raise HTTPException(404, "Video not found")

    video = dict(rows[0])
    transcript = video.get("transcript", "")
    if not transcript:
        raise HTTPException(400, "No transcript available yet. Wait for captions or upload one.")

    extra = (data or {}).get("extra_instructions", "")
    try:
        description = await ai.generate_seo_description(
            title=video["title"],
            transcript=transcript,
            extra_instructions=extra,
        )
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

    youtube.update_video_metadata(video_id, description=desc)

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
    """
    from datetime import datetime as dt, timezone
    from yt_scheduler.services.scheduler import schedule_publish

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

    job_id = await schedule_publish(video_id, publish_at)
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
        "job_id": job_id,
        "publish_at": publish_at.isoformat(),
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


@router.get("/{video_id}/captions")
async def list_captions(video_id: str):
    """List available caption tracks."""
    try:
        return youtube.list_captions(video_id)
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/{video_id}/comments")
async def list_comments(video_id: str, max_results: int = 50):
    """List comments on a video."""
    try:
        return youtube.list_comment_threads(video_id, max_results=max_results)
    except Exception as e:
        error_msg = str(e)
        if "disabled" in error_msg.lower() or "commentsDisabled" in error_msg:
            raise HTTPException(403, "Comments are disabled on this video")
        raise HTTPException(500, error_msg)


@router.post("/{video_id}/set-thumbnail")
async def set_thumbnail(video_id: str, file: UploadFile = File(...)):
    """Upload and set a video thumbnail."""
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    path = UPLOAD_DIR / file.filename
    with open(path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    try:
        youtube.set_thumbnail(video_id, path)
    except Exception as e:
        raise HTTPException(500, f"Failed to set thumbnail: {e}")

    db = await get_db()
    await db.execute(
        "UPDATE videos SET thumbnail_path = ?, updated_at = datetime('now') WHERE id = ?",
        (str(path), video_id),
    )
    await db.commit()

    return {"status": "ok"}
