"""Auto-actions runner.

Reads the per-project auto-action settings (saved via Project Settings) and
fires the configured side effects on upload or import. The wiring rules from
the spec:

* ``auto_transcribe`` + ``auto_thumbnail`` run on upload/import as soon as we
  have a local file.
* ``auto_tags`` and ``auto_description`` fire **only when transcription is
  first set** — they don't run on later transcript edits.
* ``auto_gen_socials`` fires only **when description is first set**.

All work is dispatched via ``asyncio.create_task`` so the HTTP handler can
return promptly and side effects continue in the background.
"""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
import subprocess
from pathlib import Path
from typing import Literal

from yt_scheduler.config import UPLOAD_DIR
from yt_scheduler.database import get_db
from yt_scheduler.services import (
    ai,
    events,
    project_settings,
    templates as tmpl,
    transcripts as transcript_service,
    youtube,
)

logger = logging.getLogger(__name__)

Source = Literal["upload", "import"]


# Holding strong references to scheduled tasks so the asyncio loop's GC
# doesn't collect them before they finish — see the warning in
# https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task
_pending_tasks: set[asyncio.Task] = set()


async def run_post_create_actions(video_id: str, project_id: int, source: Source) -> None:
    """Schedule the auto-action chain. Returns immediately; work runs in tasks."""
    task = asyncio.create_task(_run_chain(video_id, project_id, source))
    _pending_tasks.add(task)
    task.add_done_callback(_pending_tasks.discard)


async def _run_chain(video_id: str, project_id: int, source: Source) -> None:
    try:
        actions = await project_settings.get_auto_actions(project_id)
        column = actions.get(source, {})
    except Exception as exc:
        logger.error("Could not load auto-actions for project %s: %s", project_id, exc)
        return

    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    if not rows:
        return
    video = dict(rows[0])

    # 1. Auto-thumbnail — only if no thumbnail is already attached.
    # On upload that means the user didn't provide one; on import it means
    # the YouTube thumbnail download didn't succeed (or YouTube had none).
    # If either source already set thumbnail_path, the keyframe extract is
    # skipped entirely.
    if column.get("auto_thumbnail") and not video.get("thumbnail_path"):
        await _maybe_extract_thumbnail(video_id, video.get("video_file_path"))

    # 2. Auto-transcribe (only if no transcript yet)
    if column.get("auto_transcribe") and not (video.get("transcript") or "").strip():
        backend = column.get("auto_transcribe_backend")
        model = column.get("auto_transcribe_model")
        await _maybe_transcribe(
            video_id, video.get("video_file_path"), backend=backend, model=model
        )
        # Refresh the row so downstream gates see the new transcript.
        rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
        video = dict(rows[0]) if rows else video

    transcript_just_created = bool(video.get("transcript_created_at")) and not video.get(
        "description_generated_at"
    )

    # 3. Auto-tags — only on first transcript set
    if column.get("auto_tags") and transcript_just_created:
        await _maybe_generate_tags(video, project_id, column)

    # 4. Auto-description — only on first transcript set
    if column.get("auto_description") and transcript_just_created and not (
        video.get("description") or ""
    ).strip():
        new_desc = await _maybe_generate_description(video, project_id)
        if new_desc:
            video["description"] = new_desc
            video["description_generated_at"] = "now"  # truthy sentinel for next gate

    # 5. Auto-gen socials — only on first description set
    socials_cfg = column.get("auto_socials") or {}
    if any(socials_cfg.values()) and (video.get("description") or "").strip():
        platforms = [name for name, on in socials_cfg.items() if on]
        await _maybe_generate_socials(video_id, project_id, platforms)


# --- individual side effects ----------------------------------------------


async def _maybe_extract_thumbnail(video_id: str, video_file_path: str | None) -> None:
    if not video_file_path or not Path(video_file_path).exists():
        return
    if not shutil.which("ffprobe") or not shutil.which("ffmpeg"):
        logger.info("ffmpeg/ffprobe not available; skipping auto-thumbnail")
        return
    try:
        probe = await asyncio.to_thread(
            subprocess.run,
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", video_file_path],
            capture_output=True, text=True, timeout=30,
        )
        duration = float(probe.stdout.strip()) if probe.returncode == 0 else None
    except Exception:
        duration = None

    target_seconds = (duration * 0.25) if duration else 1.0
    target = UPLOAD_DIR / f"{video_id}_auto_thumb.jpg"
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    try:
        result = await asyncio.to_thread(
            subprocess.run,
            ["ffmpeg", "-y", "-ss", str(target_seconds), "-i", video_file_path,
             "-frames:v", "1", "-q:v", "2", str(target)],
            capture_output=True, timeout=60,
        )
        if result.returncode != 0:
            return
    except Exception as exc:
        logger.warning("ffmpeg keyframe extract failed: %s", exc)
        return

    db = await get_db()
    cursor = await db.execute(
        "UPDATE videos SET thumbnail_path = ?, updated_at = datetime('now') "
        "WHERE id = ? AND (thumbnail_path IS NULL OR thumbnail_path = '')",
        (str(target), video_id),
    )
    await db.commit()

    # If a concurrent path already set thumbnail_path (e.g. user uploaded one
    # while ffmpeg was running), the WHERE guard above made the update a
    # no-op. Don't push the auto-thumbnail to YouTube in that case — we'd
    # overwrite whatever the user / YouTube provided.
    if (cursor.rowcount or 0) <= 0:
        return

    # Push to YouTube too (best-effort). Failures here don't block the rest of
    # the auto-action chain — quota errors / not-yet-ready videos shouldn't
    # take down transcription or description generation.
    try:
        await asyncio.to_thread(youtube.set_thumbnail, video_id, target)
    except Exception as exc:
        logger.warning("auto-thumbnail YouTube upload failed: %s", exc)


async def _maybe_transcribe(
    video_id: str,
    video_file_path: str | None,
    *,
    backend: str | None,
    model: str | None,
) -> None:
    if not video_file_path or not Path(video_file_path).exists():
        return
    try:
        from yt_scheduler.services import transcription
        result = await asyncio.to_thread(
            transcription.transcribe,
            video_path=video_file_path,
            model=model or "large-v3",
            backend=backend,
        )
    except Exception as exc:
        logger.warning("auto-transcribe failed: %s", exc)
        return

    backend_to_source = {
        "mlx-whisper": "mlx_whisper",
        "faster-whisper": "faster_whisper",
        "whisper.cpp": "whispercpp",
        "macos-speech": "apple_speech",
    }
    source = backend_to_source.get(result.backend, "user_edited")
    canonical = result.to_srt()
    transcript_id = await transcript_service.upsert_transcript_for_source(
        video_id, source, canonical, source_detail=model,
    )

    db = await get_db()
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
        (canonical, transcript_id, source, video_id),
    )
    await db.commit()
    await events.record_event(
        video_id, "metadata_updated",
        {"transcript": {"old": "", "new": canonical}},
    )


async def _maybe_generate_tags(video: dict, project_id: int, column: dict) -> None:
    title = video.get("title", "") if column.get("auto_tags_include_title", True) else ""
    description = video.get("description", "") if column.get(
        "auto_tags_include_description", True
    ) else ""
    transcript = video.get("transcript", "") if column.get(
        "auto_tags_include_transcript", True
    ) else ""

    try:
        new_tags = await ai.generate_tags_from_metadata(
            title=title, description=description, transcript=transcript,
            project_id=project_id,
        )
    except Exception as exc:
        logger.warning("auto-tags generation failed: %s", exc)
        return

    existing = []
    raw_existing = video.get("tags") or "[]"
    try:
        existing = json.loads(raw_existing)
        if not isinstance(existing, list):
            existing = []
    except json.JSONDecodeError:
        existing = []

    mode = column.get("auto_tags_mode", "replace")
    if mode == "add":
        seen = {t.lower(): t for t in existing}
        for tag in new_tags:
            if tag.lower() not in seen:
                seen[tag.lower()] = tag
        merged = list(seen.values())
    else:
        merged = []
        seen: set[str] = set()
        for tag in new_tags:
            if tag.lower() not in seen:
                seen.add(tag.lower())
                merged.append(tag)

    db = await get_db()
    await db.execute(
        "UPDATE videos SET tags = ?, updated_at = datetime('now') WHERE id = ?",
        (json.dumps(merged), video["id"]),
    )
    await db.commit()
    await events.record_event(
        video["id"], "metadata_updated",
        {"tags": {"old": existing, "new": merged}},
    )


async def _maybe_generate_description(video: dict, project_id: int) -> str | None:
    transcript = (video.get("transcript") or "").strip()
    if not transcript:
        return None
    try:
        description = await ai.generate_seo_description(
            title=video.get("title", ""),
            transcript=transcript,
            project_id=project_id,
        )
    except Exception as exc:
        logger.warning("auto-description failed: %s", exc)
        return None

    db = await get_db()
    await db.execute(
        """UPDATE videos SET
            description = ?,
            description_generated_at = datetime('now'),
            status = 'ready',
            updated_at = datetime('now')
        WHERE id = ?""",
        (description, video["id"]),
    )
    await db.commit()
    await events.record_event(
        video["id"], "metadata_updated",
        {"description": {"old": video.get("description") or "", "new": description}},
    )
    return description


async def _maybe_generate_socials(
    video_id: str, project_id: int, platforms: list[str]
) -> None:
    """Use the per-project default template for the video's tier."""
    if not platforms:
        return

    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    if not rows:
        return
    video = dict(rows[0])

    posting = await project_settings.get_posting_settings(project_id)
    tier = video.get("tier") or "video"
    template_name = posting.get(f"default_template_{tier}") or "announce_video"

    template = await tmpl.get_template(template_name, project_id=project_id)
    if not template:
        logger.info("Default template %s not found for tier %s", template_name, tier)
        return

    tags = []
    try:
        tags = json.loads(video.get("tags") or "[]")
    except json.JSONDecodeError:
        pass

    from yt_scheduler.services.transcripts import srt_to_plain_text
    variables = {
        "title": video.get("title", ""),
        "url": f"https://youtu.be/{video_id}",
        "description": video.get("description", "") or "",
        "description_short": (video.get("description") or "")[:150],
        "description_medium": (video.get("description") or "")[:500],
        "tags": ", ".join(tags),
        "hashtags": " ".join(f"#{t.replace(' ', '')}" for t in tags[:5]),
        "thumbnail_path": video.get("thumbnail_path") or "",
        "tier": tier,
        "transcript": srt_to_plain_text(video.get("transcript") or ""),
        "user_message": "",
    }

    for platform in platforms:
        cfg = template["platforms"].get(platform)
        if not cfg or not cfg.get("template"):
            continue
        try:
            rendered = tmpl.render_template(cfg["template"], variables)
        except Exception as exc:
            logger.warning("auto-social render failed for %s: %s", platform, exc)
            continue
        await db.execute(
            """INSERT INTO social_posts (video_id, platform, content, media_type, status)
            VALUES (?, ?, ?, ?, 'draft')""",
            (video_id, platform, rendered, cfg.get("media", "thumbnail")),
        )
    await db.commit()
