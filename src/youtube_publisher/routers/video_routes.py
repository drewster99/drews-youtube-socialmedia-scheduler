"""Video management routes."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from fastapi import APIRouter, Form, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse

from youtube_publisher.config import UPLOAD_DIR
from youtube_publisher.database import get_db
from youtube_publisher.services import youtube, ai

router = APIRouter(prefix="/api/videos", tags=["videos"])


@router.get("")
async def list_videos():
    """List all tracked videos."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM videos ORDER BY created_at DESC"
    )
    return [dict(r) for r in rows]


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
    except Exception:
        pass

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
):
    """Upload a video to YouTube and track it."""
    db = await get_db()

    # Save video file locally
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    video_path = UPLOAD_DIR / video_file.filename
    with open(video_path, "wb") as f:
        shutil.copyfileobj(video_file.file, f)

    # Save thumbnail if provided
    thumbnail_path = None
    if thumbnail_file:
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
    if thumbnail_path:
        try:
            youtube.set_thumbnail(video_id, thumbnail_path)
        except Exception as e:
            pass  # Non-fatal — thumbnail can be set later

    # Track in database
    await db.execute(
        """INSERT INTO videos (id, title, description, tags, privacy_status, publish_at,
           thumbnail_path, video_file_path, pinned_links, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'uploaded')""",
        (
            video_id,
            title,
            description,
            json.dumps(tag_list),
            privacy_status,
            publish_at or None,
            str(thumbnail_path) if thumbnail_path else None,
            str(video_path),
            pinned_links,
        ),
    )
    await db.commit()

    return {"status": "ok", "video_id": video_id, "youtube_url": f"https://youtu.be/{video_id}"}


@router.put("/{video_id}")
async def update_video(video_id: str, data: dict):
    """Update video metadata."""
    db = await get_db()

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

    if updates:
        updates.append("updated_at = datetime('now')")
        params.append(video_id)
        await db.execute(
            f"UPDATE videos SET {', '.join(updates)} WHERE id = ?", params
        )
        await db.commit()

    return {"status": "ok"}


@router.post("/{video_id}/transcribe")
async def transcribe_video(video_id: str, data: dict | None = None):
    """Transcribe a video locally using on-device speech recognition.

    Optional body params:
        model: Whisper model size (tiny, base, small, medium, large-v3). Default: large-v3
        language: Language code (e.g., "en"). Default: auto-detect
        backend: Force specific backend (mlx-whisper, faster-whisper, whisper.cpp, macos-speech)
    """
    from youtube_publisher.services import transcription

    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    if not rows:
        raise HTTPException(404, "Video not found")

    video = dict(rows[0])
    video_file = video.get("video_file_path")
    if not video_file or not Path(video_file).exists():
        raise HTTPException(400, "Video file not found locally. Cannot transcribe.")

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

    # Save transcript and SRT
    srt_path = result.save_srt(video_file)

    await db.execute(
        """UPDATE videos SET transcript = ?, status = 'captioned', updated_at = datetime('now')
        WHERE id = ?""",
        (result.text, video_id),
    )
    await db.commit()

    return {
        "status": "ok",
        "backend": result.backend,
        "language": result.language,
        "segments": len(result.segments),
        "characters": len(result.text),
        "srt_path": str(srt_path),
        "transcript_preview": result.text[:500],
    }


@router.get("/transcription-backends")
async def list_transcription_backends():
    """List available transcription backends."""
    from youtube_publisher.services import transcription
    return transcription.list_available_backends()


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
    description = ai.generate_seo_description(
        title=video["title"],
        transcript=transcript,
        extra_instructions=extra,
    )

    # Compose full description with pinned links
    pinned = video.get("pinned_links", "")
    full_description = f"{pinned}\n\n{description}" if pinned else description

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

    await db.execute(
        "UPDATE videos SET description = ?, status = 'ready', updated_at = datetime('now') WHERE id = ?",
        (desc, video_id),
    )
    await db.commit()

    return {"status": "ok"}


@router.post("/{video_id}/publish")
async def publish_video(video_id: str):
    """Switch a video to public immediately."""
    youtube.update_video_metadata(video_id, privacy_status="public")

    db = await get_db()
    await db.execute(
        "UPDATE videos SET privacy_status = 'public', status = 'published', updated_at = datetime('now') WHERE id = ?",
        (video_id,),
    )
    await db.commit()

    return {"status": "ok", "message": "Video is now public"}


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
        raise HTTPException(500, str(e))


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
