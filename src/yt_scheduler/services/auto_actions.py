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

Promo Videos (migration 023) uses a separate orchestrator —
``run_promo_chain`` / ``_run_promo_chain`` — that always runs the full
sequence (title → upload → probe → transcribe → desc → tags →
metadata push) and persists per-step progress in
``videos.auto_action_state``. The Promo flow short-circuits work that's
already done so a Retry resumes from the failing step.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import secrets
import shutil
import subprocess
from pathlib import Path
from collections.abc import Callable
from typing import Literal

from yt_scheduler.config import UPLOAD_DIR, sanitized_original_filename
from yt_scheduler.database import get_db, write_transaction
from yt_scheduler.services import (
    ai,
    events,
    project_settings,
    templates as tmpl,
    tiers,
    transcripts as transcript_service,
    youtube,
)
from yt_scheduler.services.auth import set_active_project
from yt_scheduler.services.background import spawn_background
from yt_scheduler.services.projects import get_project_by_id

logger = logging.getLogger(__name__)

Source = Literal["upload", "import"]


async def run_post_create_actions(video_id: str, project_id: int, source: Source) -> None:
    """Schedule the auto-action chain. Returns immediately; work runs in tasks."""
    spawn_background(
        _run_chain(video_id, project_id, source),
        name=f"auto-actions:{video_id}",
    )


async def _run_chain(video_id: str, project_id: int, source: Source) -> None:
    # Bind the active project so every YouTube wrapper called below picks up
    # the project's OAuth credentials, not the default project's.
    project_row = await get_project_by_id(project_id)
    if project_row:
        set_active_project(project_row["slug"])

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

    # 0. Always-download (import path only). The mp4 unlocks the rest of
    # the auto-action chain (keyframe thumbnail, on-device transcribe,
    # frames-driven description/tags), and a YouTube-imported row that
    # never downloaded its file leaves every later gate as a silent
    # no-op. We surface progress via videos.video_file_download_state
    # so the detail page can show a "Downloading…" indicator and poll
    # for completion without the user reloading.
    if source == "import" and not video.get("video_file_path"):
        await _maybe_download_video_file(video_id)
        rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
        if rows:
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
        async with _get_whisper_semaphore():
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


async def _set_download_state(video_id: str, state: str | None) -> None:
    async with write_transaction() as db:
        await db.execute(
            "UPDATE videos SET video_file_download_state = ?, updated_at = datetime('now') "
            "WHERE id = ?",
            (state, video_id),
        )


async def _maybe_download_video_file(video_id: str) -> None:
    """Fetch the mp4 from YouTube into UPLOAD_DIR. Updates videos.
    video_file_path on success; videos.video_file_download_state tracks
    progress for the detail-page polling loop.

    PrivateVideoError surfaces as 'unavailable' (retrying won't help
    without the user flipping privacy on YouTube); anything else
    surfaces as 'failed' and gets logged so a manual Transcribe-button
    retry will pick up the same code path.

    No-op when the row already has a user_attached master — the YouTube
    re-download is always lossy versus a master, and clobbering the
    user's deliberate Replace-source choice silently would be a real
    surprise.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT source_file_origin, video_file_path FROM videos WHERE id = ?",
        (video_id,),
    )
    if rows:
        existing = dict(rows[0])
        if existing.get("source_file_origin") == "user_attached" and existing.get("video_file_path"):
            logger.info(
                "Skipping YouTube re-download for %s — user-attached master "
                "is already on disk.", video_id,
            )
            return
    await _set_download_state(video_id, "in_progress")
    try:
        downloaded = await asyncio.to_thread(
            youtube.download_video_file, video_id, UPLOAD_DIR
        )
    except youtube.PrivateVideoError:
        logger.info(
            "Skipping video-file download for %s — YouTube reports it as private; "
            "the user can flip it to unlisted and retry from the detail page.",
            video_id,
        )
        await _set_download_state(video_id, "unavailable")
        return
    except Exception as exc:
        logger.warning("Video-file download failed for %s: %s", video_id, exc)
        await _set_download_state(video_id, "failed")
        return

    async with write_transaction() as db:
        await db.execute(
            "UPDATE videos SET video_file_path = ?, video_file_download_state = NULL, "
            "source_file_origin = 'youtube_download', updated_at = datetime('now') "
            "WHERE id = ?",
            (str(downloaded), video_id),
        )


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
    except (subprocess.TimeoutExpired, OSError, ValueError) as exc:
        logger.warning("ffprobe duration probe failed for %s: %s", video_id, exc)
        duration = None

    if duration is None:
        logger.info("Duration unknown for %s; using a 1.0s thumbnail offset", video_id)
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
            logger.warning(
                "ffmpeg auto-thumbnail extract for %s exited %s: %s",
                video_id, result.returncode,
                (result.stderr or b"")[-400:].decode("utf-8", "replace"),
            )
            return
    except Exception as exc:
        logger.warning("ffmpeg keyframe extract failed: %s", exc)
        return

    async with write_transaction() as db:
        cursor = await db.execute(
            "UPDATE videos SET thumbnail_path = ?, updated_at = datetime('now') "
            "WHERE id = ? AND (thumbnail_path IS NULL OR thumbnail_path = '')",
            (str(target), video_id),
        )

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
    """Best-effort transcribe for the fire-and-forget upload/import chain.

    Swallows failures on purpose: this runs unattended behind an upload, and a
    transcription that didn't work should not blow up the rest of the chain.
    The user-initiated path wants the opposite (a visible error), so it calls
    :func:`transcribe_and_store` directly.
    """
    if not video_file_path or not Path(video_file_path).exists():
        return
    try:
        await transcribe_and_store(
            video_id, video_file_path, backend=backend, model=model
        )
    except Exception as exc:
        logger.warning("auto-transcribe failed: %s", exc)


async def transcribe_and_store(
    video_id: str,
    video_file_path: str,
    *,
    backend: str | None,
    model: str | None,
    progress_callback: Callable[[float, float], None] | None = None,
) -> str:
    """Transcribe, persist to the transcript table + videos row, return the SRT.

    Raises on failure — callers that want the errors swallowed wrap it
    themselves. ``progress_callback`` is handed straight to the transcriber;
    only the Apple SpeechAnalyzer backend actually reports progress.
    """
    from yt_scheduler.services import transcription
    # Both may be None: that means "no backend configured for this column",
    # which transcribe() resolves to Apple SpeechAnalyzer. Substituting a
    # model here would silently re-enable the 3 GB large-v3 MLX path.
    result = await asyncio.to_thread(
        transcription.transcribe,
        video_path=video_file_path,
        model=model,
        backend=backend,
        progress_callback=progress_callback,
    )

    backend_to_source = {
        "mlx-whisper": "mlx_whisper",
        "whisper.cpp": "whispercpp",
        "macos-speech": "apple_speech",
    }
    source = backend_to_source.get(result.backend, "user_edited")
    canonical = result.to_srt()
    transcript_id = await transcript_service.upsert_transcript_for_source(
        video_id, source, canonical, source_detail=model,
    )

    async with write_transaction() as db:
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
    await events.record_event(
        video_id, "metadata_updated",
        {"transcript": {"old": "", "new": canonical}},
    )
    return canonical


async def _maybe_generate_tags(video: dict, project_id: int, column: dict) -> None:
    # Skip if tags were already auto-generated (or user-edited). This mirrors
    # the description gate's "skip if description exists" check and prevents
    # a restart from clobbering hand-edited tags: once tags_generated_at is
    # stamped the chain will never re-run this step for the same video.
    if video.get("tags_generated_at"):
        return

    title = video.get("title", "") if column.get("auto_tags_include_title", True) else ""
    description = video.get("description", "") if column.get(
        "auto_tags_include_description", True
    ) else ""
    transcript = video.get("transcript", "") if column.get(
        "auto_tags_include_transcript", True
    ) else ""

    try:
        prompt_variables = await tmpl.build_prompt_variables(video)
        new_tags = await ai.generate_tags_from_metadata(
            title=title, description=description, transcript=transcript,
            project_id=project_id, prompt_variables=prompt_variables,
            is_promo=bool(video.get("parent_item_id")),
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

    async with write_transaction() as db:
        await db.execute(
            "UPDATE videos SET tags = ?, tags_generated_at = datetime('now'), "
            "updated_at = datetime('now') WHERE id = ?",
            (json.dumps(merged), video["id"]),
        )
    await events.record_event(
        video["id"], "metadata_updated",
        {"tags": {"old": existing, "new": merged}},
    )


async def _maybe_generate_description(video: dict, project_id: int) -> str | None:
    transcript = (video.get("transcript") or "").strip()
    if not transcript:
        return None
    try:
        prompt_variables = await tmpl.build_prompt_variables(video)
        description = await ai.generate_seo_description(
            title=video.get("title", ""),
            transcript=transcript,
            project_id=project_id,
            prompt_variables=prompt_variables,
            is_promo=bool(video.get("parent_item_id")),
        )
    except Exception as exc:
        logger.warning("auto-description failed: %s", exc)
        return None

    async with write_transaction() as db:
        await db.execute(
            """UPDATE videos SET
                description = ?,
                description_generated_at = datetime('now'),
                status = 'ready',
                updated_at = datetime('now')
            WHERE id = ?""",
            (description, video["id"]),
        )
    await events.record_event(
        video["id"], "metadata_updated",
        {"description": {"old": video.get("description") or "", "new": description}},
    )
    return description


async def _video_has_social_posts(db, video_id: str) -> bool:
    """Whether any social_posts row already exists for this video.

    The auto chain re-enters on every re-import, restart restore, and manual
    re-trigger, and a description (its only prior gate) is permanent — so
    without this the step inserts a fresh duplicate draft set each time.
    """
    rows = await db.execute_fetchall(
        "SELECT 1 FROM social_posts WHERE video_id = ? LIMIT 1", (video_id,)
    )
    return bool(rows)


async def _maybe_generate_socials(
    video_id: str, project_id: int, platforms: list[str]
) -> None:
    """Use the per-project default template for the video's tier.

    Renders through the unified engine: the same `templates.merge_variables`
    + `templates.extract_media_directives` + `templates.render` pipeline as
    the synchronous `POST /api/social/generate-posts/{video_id}` route.
    There is no second template engine — this auto-path produces identical
    output to the manual path for the same template + item.
    """
    if not platforms:
        return

    # Lazy import to avoid the circular: social_routes imports from this
    # module's neighbours. The shared helper lives in social_routes so both
    # paths render identically.
    from yt_scheduler.routers.social_routes import (
        _build_render_context,
        _legacy_media_for_slot,
    )

    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    if not rows:
        return
    video = dict(rows[0])

    # Cheap pre-check before spending any Claude tokens. The authoritative check
    # runs under the publish lock further down; this one just avoids rendering a
    # whole slot-set we're about to throw away.
    if await _video_has_social_posts(db, video_id):
        logger.info(
            "auto-social: %s already has social_posts; skipping auto-generate "
            "(use the Generate button to regenerate).", video_id,
        )
        return

    posting = await project_settings.get_posting_settings(project_id)
    tier = video.get("tier") or "video"
    template_name = posting.get(f"default_template_{tier}") or "announce_video"

    template = await tmpl.get_template(template_name, project_id=project_id)
    if not template:
        logger.info("Default template %s not found for tier %s", template_name, tier)
        return

    ctx = await _build_render_context(db, video)

    # Same per-project default for ``{{ai: …}}`` blocks as the on-demand
    # generate flow — see social_routes.py for the rationale.
    from yt_scheduler.services import prompts as prompt_service
    default_ai_system = (await prompt_service.get_prompt_with_fallback(
        "ai_block_default_system_prompt", project_id=project_id,
    ))["system"]

    video_directive_re = re.compile(r"\{\{\s*video\s*\}\}", re.IGNORECASE)

    # Rendered rows are collected here and committed as one set below, so a crash
    # mid-loop can't leave a half-generated slot-set that the idempotency gate
    # would then refuse to complete.
    pending_inserts: list[tuple] = []

    for slot in template.get("slots", []):
        if slot.get("is_disabled"):
            continue
        platform = slot["platform"]
        if platform not in platforms:
            continue
        body = slot.get("body") or ""
        if not body:
            continue
        slot_max = slot.get("max_chars")
        if not slot_max:
            logger.warning("auto-social: slot %s (%s) has no max_chars; skipping", slot.get("id"), platform)
            continue
        # Whether the author declared media in the raw body — checked before
        # section resolution so a directive dropped by a false section still
        # counts as "author took manual control" and the legacy media
        # fallback below stays disabled for this slot.
        body_declared_media = tmpl.body_declares_media(body)
        try:
            # user_message is a compose-flow input with no auto-path
            # equivalent; defined-empty (a legitimate optional, unlike a
            # missing required value) so {{user_message}} renders empty and
            # {{#user_message}} sections drop instead of erroring the slot.
            slot_vars = {
                **ctx["variables"],
                "user_message": "",
                "max_chars": str(slot_max),
            }
            # Sections resolve before the media pass so a directive inside
            # a dropped section never attaches media — same order as the
            # manual generate route.
            body = tmpl.resolve_sections(body, slot_vars)
            # Threads can't attach media (text-only API) — skip a {{video}}
            # slot rather than auto-post it without the video. Checked after
            # section resolution so a {{video}} inside a dropped section
            # doesn't skip a slot that would render text-only anyway.
            if platform == "threads" and video_directive_re.search(body):
                logger.info("auto-social: skipping Threads slot for %s — uses {{video}}", video_id)
                continue
            cleaned, media_paths, _alts = tmpl.extract_media_directives(
                body,
                video_path=ctx["video_path"],
                thumbnail_path=ctx["thumb_path"],
                images=ctx["images"],
            )
            rendered = (await tmpl.async_render(
                cleaned, slot_vars, default_system_prompt=default_ai_system,
            )).strip()
        except Exception as exc:
            logger.warning("auto-social render failed for %s: %s", platform, exc)
            continue

        if not media_paths and not body_declared_media:
            fallback = _legacy_media_for_slot(slot, ctx)
            if fallback:
                media_paths = [fallback]

        # Threads can't attach media — drop it (the {{video}} case was already
        # skipped above; this catches {{thumbnail}}/{{image:...}}/fallbacks).
        if platform == "threads" and media_paths:
            logger.info("auto-social: Threads slot for %s — dropping media (Threads is text-only)", video_id)
            media_paths = []

        media_paths_json = json.dumps(media_paths) if media_paths else None
        primary_media = media_paths[0] if media_paths else None
        media_type = slot.get("media", "thumbnail")
        sa_id = slot.get("social_account_id")
        slot_id = slot.get("id")

        # G1 — slot_id pins the post to the exact template_slots row
        # that produced it so a later partial regenerate can target
        # this one specifically (two same-platform multi-account slots
        # are routed independently).
        # async_render (AI) for this slot is already done above; collect the row
        # and commit the whole set together once the lock is held.
        pending_inserts.append((
            video_id, platform, rendered,
            primary_media, media_paths_json,
            media_type, sa_id, int(slot_max),
            int(slot_id) if slot_id is not None else None,
        ))

    if not pending_inserts:
        return

    # Authoritative gate. spawn_background does NOT dedup by name, so two chains
    # for one video really can run concurrently; without the lock both would read
    # "no posts" and both insert a full duplicate set. This is the same per-video
    # lock the manual /generate-posts route takes.
    from yt_scheduler.services.scheduler import get_publish_lock

    async with get_publish_lock(video_id):
        if await _video_has_social_posts(db, video_id):
            logger.info(
                "auto-social: %s gained social_posts while rendering; "
                "skipping insert.", video_id,
            )
            return
        async with write_transaction() as wdb:
            for params in pending_inserts:
                await wdb.execute(
                    """INSERT INTO social_posts
                           (video_id, platform, content, media_path, media_paths,
                            media_type, status, social_account_id, max_chars, slot_id)
                    VALUES (?, ?, ?, ?, ?, ?, 'draft', ?, ?, ?)""",
                    params,
                )


# --- Promo Videos auto-action chain --------------------------------------

# The Promo flow ALWAYS runs the full chain so the user can drop files and
# walk away. The chain skips any step whose output already exists, which
# also makes retry idempotent: a "Retry transcribing" after a real
# transcription failure re-tries; a "Retry transcribing" on a row that
# already has a transcript jumps straight to the next step.

# Step state strings persisted to ``videos.auto_action_state``.
PROMO_STATE_PENDING = "pending"
PROMO_STATE_CUTTING = "cutting"
PROMO_STATE_GENERATING_TITLE = "generating_title"
PROMO_STATE_UPLOADING = "uploading"
PROMO_STATE_PROBING = "probing"
PROMO_STATE_TRANSCRIBING = "transcribing"
PROMO_STATE_GENERATING_DESC = "generating_desc"
PROMO_STATE_GENERATING_TAGS = "generating_tags"
PROMO_STATE_PUSHING_METADATA = "pushing_metadata"
PROMO_STATE_READY = "ready"

# Description-only re-generation, driven by the Promo screen's "Update all
# descriptions" button after a prompt-template edit. Deliberately NOT a member
# of PROMO_STEP_ORDER below: it is a standalone operation, not a chain step, and
# a failed:updating_desc row must not offer the chain's Retry. That retry would
# find a non-empty description, skip generation entirely, and push the OLD text
# to YouTube while reporting success.
PROMO_STATE_UPDATING_DESC = "updating_desc"

# Ordered. ``_resume_from`` walks this list to determine which steps to
# (re-)run on a Retry. Names match the persisted state column. CUTTING
# is the Generate-from-source pre-step (job.has_cut_request) that runs
# before INSERT — it's listed first for symmetry so retry_promo_step
# accepts it. In practice no videos row carries 'cutting' as state
# (the failure path sets state in _UPLOAD_JOBS only), but the symmetry
# means future code that hands a 'cutting' step to the retry endpoint
# won't ValueError.
PROMO_STEP_ORDER: tuple[str, ...] = (
    PROMO_STATE_CUTTING,
    PROMO_STATE_GENERATING_TITLE,
    PROMO_STATE_UPLOADING,
    PROMO_STATE_PROBING,
    PROMO_STATE_TRANSCRIBING,
    PROMO_STATE_GENERATING_DESC,
    PROMO_STATE_GENERATING_TAGS,
    PROMO_STATE_PUSHING_METADATA,
)

# Whisper transcription is CPU-bound and model load is RAM-bound. A process-wide
# semaphore bounds all on-device transcription — both the standard upload/import
# chain and the promo chain — to the CPU count, so boot-time restores and promo
# chains can't fan out unboundedly and OOM the machine. YT upload and Claude
# calls don't take it — they're I/O-bound. Created lazily so it binds to the
# running event loop rather than import time.
_WHISPER_SEMAPHORE: asyncio.Semaphore | None = None


def _get_whisper_semaphore() -> asyncio.Semaphore:
    global _WHISPER_SEMAPHORE
    if _WHISPER_SEMAPHORE is None:
        _WHISPER_SEMAPHORE = asyncio.Semaphore(os.cpu_count() or 1)
    return _WHISPER_SEMAPHORE

# In-flight upload jobs that don't yet have a videos row. The Promo screen
# polls upload-jobs/<id> until ``video_id`` flips to a real id, then
# switches to polling /api/videos/{id}/auto-actions.
#
# Terminal failures (state startswith 'failed:') are popped after
# ``_UPLOAD_JOB_FAILED_TTL_SECONDS`` past their state change so the dict
# doesn't grow unboundedly on a session that hits many cut / upload /
# title-gen failures. Successful jobs are popped at the end of the chain
# already (see _run_promo_chain_inner finally-block).
_UPLOAD_JOBS: dict[str, dict] = {}
_UPLOAD_JOB_FAILED_TTL_SECONDS: float = 10 * 60  # 10 minutes


# Cap on simultaneously-running promo chains. The chain itself fans out
# to ffprobe + whisper + 2-3 Claude calls + a YouTube upload; 16 chains
# in parallel would saturate a Mac (whisper alone is CPU-bound and
# YouTube's upload SDK isn't reentrant per-token). 4 is comfortable on
# wide hardware and keeps individual jobs from starving each other.
#
# Lazily initialised on first use so that the semaphore is always
# created on the running event loop — avoids "bound to a different loop"
# errors when tests spin up a fresh loop per test or a server restart
# creates a new loop in-process.
_PROMO_CHAIN_SEMAPHORE: asyncio.Semaphore | None = None


def _get_promo_chain_semaphore() -> asyncio.Semaphore:
    global _PROMO_CHAIN_SEMAPHORE
    if _PROMO_CHAIN_SEMAPHORE is None:
        _PROMO_CHAIN_SEMAPHORE = asyncio.Semaphore(4)
    return _PROMO_CHAIN_SEMAPHORE


def _evict_stale_upload_jobs() -> None:
    """Drop terminal-failed upload jobs past their TTL.

    Cheap O(N) sweep run on every read/write of the dict. Success-path
    jobs already get popped at chain completion (see
    ``_run_promo_chain_inner``), so this only catches the failure
    paths that previously had no eviction at all.
    """
    import time

    now = time.monotonic()
    stale = [
        job_id for job_id, job in _UPLOAD_JOBS.items()
        if str(job.get("state", "")).startswith("failed:")
        and job.get("_failed_at") is not None
        and (now - float(job["_failed_at"])) > _UPLOAD_JOB_FAILED_TTL_SECONDS
    ]
    for job_id in stale:
        _UPLOAD_JOBS.pop(job_id, None)


def inflight_promo_jobs(parent_id: str, project_id: int) -> list[dict]:
    """Public snapshot of in-flight promo-chain jobs for a parent.

    Returns every upload/promo-chain job for this parent+project that hasn't
    reached ``ready`` (still processing, or recently failed) — so the promos
    page can render a live placeholder card *before* the YouTube upload creates
    a DB row, and keep showing progress through the (slow) post-upload steps.
    ``ready`` jobs are omitted because the DB row already covers them.
    """
    _evict_stale_upload_jobs()
    out: list[dict] = []
    for job in _UPLOAD_JOBS.values():
        if job.get("parent_id") != parent_id:
            continue
        try:
            if int(job.get("project_id")) != int(project_id):
                continue
        except (TypeError, ValueError):
            continue
        state = str(job.get("state") or "")
        if state == PROMO_STATE_READY:
            continue
        out.append({
            "job_id": job.get("job_id"),
            "video_id": job.get("video_id"),
            "item_type": job.get("forced_item_type"),
            "state": state,
            "title": job.get("title") or job.get("pre_supplied_title") or job.get("filename"),
            "last_error": job.get("last_error"),
        })
    return out


def _mark_upload_failed(job: dict, state: str, error: str | None = None) -> None:
    """Stamp the failed-state timestamp + state so eviction can age the job out."""
    import time

    job["state"] = state
    if error is not None:
        job["last_error"] = error
    job["_failed_at"] = time.monotonic()


def get_upload_job(job_id: str) -> dict | None:
    """Public read of an upload-job's current state."""
    _evict_stale_upload_jobs()
    job = _UPLOAD_JOBS.get(job_id)
    if job is None:
        return None
    # Drop internal-only fields before exposing.
    public_keys = {"job_id", "filename", "parent_id", "video_id",
                   "state", "last_error", "title"}
    return {k: v for k, v in job.items() if k in public_keys}


async def _persist_pending_promo_job(job: dict) -> None:
    """Record a pre-INSERT promo job to ``pending_promo_jobs`` so a restart can
    resume it (the in-memory ``_UPLOAD_JOBS`` entry dies with the process).

    Best-effort: a tracking write must never abort the chain it's tracking, so
    failures are logged and swallowed — the job then merely lacks restart
    survival, which is the pre-migration-030 behaviour."""
    try:
        async with write_transaction() as db:
            await db.execute(
                """INSERT INTO pending_promo_jobs (
                    job_id, project_id, parent_id, forced_item_type, original_filename,
                    title, parent_video_path, local_path,
                    cut_start_seconds, cut_end_seconds, vertical_crop,
                    x_shift_normalized, audio_fade_in, audio_fade_out,
                    status, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', datetime('now'))""",
                (
                    job["job_id"], int(job["project_id"]), job.get("parent_id"),
                    job.get("forced_item_type"), job.get("filename"),
                    job.get("pre_supplied_title") or job.get("title"),
                    job.get("parent_video_path"), job.get("local_path"),
                    job.get("cut_start_seconds"), job.get("cut_end_seconds"),
                    1 if job.get("vertical_crop") else 0,
                    float(job.get("x_shift_normalized") or 0.0),
                    float(job.get("audio_fade_in") or 0.0),
                    float(job.get("audio_fade_out") or 0.0),
                ),
            )
    except Exception as exc:
        logger.warning(
            "Could not persist pending promo job %s: %s", job.get("job_id"), exc
        )


async def _mark_pending_promo_job(
    job_id: str, *, status: str | None = None,
    youtube_video_id: str | None = None, last_error: str | None = None,
    critical: bool = False,
) -> None:
    """Update a persisted promo job's progress/terminal fields. Normally
    best-effort (these are bookkeeping writes; never break the chain). When
    ``critical`` is set — used for the post-upload ``youtube_video_id`` stamp,
    which is the load-bearing "don't re-upload on restart" marker — the write is
    retried before giving up, so a transient DB hiccup can't silently drop it and
    cause a duplicate YouTube upload on the next resume."""
    assignments = ["updated_at = datetime('now')"]
    params: list[object] = []
    if status is not None:
        assignments.append("status = ?")
        params.append(status)
    if youtube_video_id is not None:
        assignments.append("youtube_video_id = ?")
        params.append(youtube_video_id)
    if last_error is not None:
        assignments.append("last_error = ?")
        params.append(last_error[:500])
    params.append(job_id)
    attempts = 4 if critical else 1
    for attempt in range(attempts):
        try:
            async with write_transaction() as db:
                await db.execute(
                    f"UPDATE pending_promo_jobs SET {', '.join(assignments)} WHERE job_id = ?",
                    params,
                )
            return
        except Exception as exc:
            if attempt + 1 < attempts:
                await asyncio.sleep(0.25 * (attempt + 1))
                continue
            log = logger.error if critical else logger.warning
            log("Could not update pending promo job %s%s: %s",
                job_id, " (CRITICAL youtube_video_id stamp)" if critical else "", exc)


async def start_promo_upload(
    *,
    local_path: Path,
    original_filename: str,
    parent_id: str,
    project_id: int,
    forced_item_type: str | None = None,
) -> str:
    """Queue a Promo upload chain. Returns a job_id the UI can poll."""
    job_id = "job_" + secrets.token_hex(8)
    _UPLOAD_JOBS[job_id] = {
        "job_id": job_id,
        "filename": original_filename,
        "local_path": str(local_path),
        "parent_id": parent_id,
        "project_id": project_id,
        "forced_item_type": forced_item_type,
        "video_id": None,
        "state": PROMO_STATE_PENDING,
        "last_error": None,
        "title": None,
    }
    await _persist_pending_promo_job(_UPLOAD_JOBS[job_id])
    spawn_background(_run_promo_chain(job_id), name=f"promo-upload:{job_id}")
    return job_id


async def start_promo_from_cut(
    *,
    parent_video_path: Path,
    parent_id: str,
    project_id: int,
    item_type: str,
    title: str,
    cut_start_seconds: float,
    cut_end_seconds: float,
    vertical_crop: bool = False,
    x_shift_normalized: float = 0.0,
    audio_fade_in: float = 0.0,
    audio_fade_out: float = 0.0,
    existing_cut_path: Path | None = None,
) -> str:
    """Queue a Promo chain for a clip cut from a parent video.

    The chain runs an ffmpeg cut as its first step (gated by the cut
    semaphore in ``services/clipper``), then continues into the normal
    promo flow. Differs from :func:`start_promo_upload` in three ways:

    * The local file doesn't exist yet at call time — the chain creates
      it via ``clipper.cut_clip_from_parent`` before the YouTube upload.
    * ``title`` is supplied by the caller (Claude proposed it during the
      Generate-from-source preview) so the chain skips its AI-title step.
    * ``cut_start_seconds`` / ``cut_end_seconds`` are written to the
      videos row at INSERT so the next Generate run on the same parent
      can avoid re-proposing the same ranges.

    ``existing_cut_path``: when the cut already exists (the Generate
    review flow cuts the proposal up-front with the same parameters
    the chain would use), pass the path to skip step 0. The chain
    takes ownership of the file — caller must not delete it.

    Same return contract as :func:`start_promo_upload` — a ``job_id`` the
    UI polls until the YouTube upload step lands and a real video_id is
    available. The job's initial state is ``cutting``.
    """
    job_id = "job_" + secrets.token_hex(8)
    # Derive a sensible client-facing filename from the parent + range so
    # the title/filename pair makes sense in logs.
    pretty_name = (
        f"{Path(parent_video_path).stem}_clip_"
        f"{int(cut_start_seconds)}-{int(cut_end_seconds)}.mp4"
    )
    _UPLOAD_JOBS[job_id] = {
        "job_id": job_id,
        "filename": pretty_name,
        # local_path is set if the caller provided an existing cut;
        # otherwise the chain's step 0 writes it after running ffmpeg.
        "local_path": str(existing_cut_path) if existing_cut_path else None,
        "parent_id": parent_id,
        "project_id": project_id,
        "forced_item_type": item_type,
        "video_id": None,
        "state": PROMO_STATE_CUTTING,
        "last_error": None,
        "title": title,
        "pre_supplied_title": title,
        "cut_start_seconds": float(cut_start_seconds),
        "cut_end_seconds": float(cut_end_seconds),
        "parent_video_path": str(parent_video_path),
        "vertical_crop": bool(vertical_crop),
        "x_shift_normalized": float(x_shift_normalized),
        "audio_fade_in": float(audio_fade_in),
        "audio_fade_out": float(audio_fade_out),
    }
    await _persist_pending_promo_job(_UPLOAD_JOBS[job_id])
    spawn_background(_run_promo_chain(job_id), name=f"promo-from-cut:{job_id}")
    return job_id


async def resume_pending_promo_jobs(*, window_hours: int) -> int:
    """Re-spawn promo chains that were persisted before the last restart but
    never inserted a videos row. Called at startup, after the videos-row-based
    resume (which covers everything from the INSERT onward).

    Safety rules:
      * A job whose upload already finalized (``youtube_video_id`` set) is NOT
        re-uploaded — that would duplicate the YouTube video. It's flagged for
        the user to import from the dashboard instead.
      * A job whose cut file is gone is re-cut from the parent when the cut
        params are present; otherwise it's flagged failed (can't reconstruct).
    Nothing is deleted — terminal jobs are marked ``done``/``failed``.
    """
    db = await get_db()
    rows = await db.execute_fetchall(
        f"""SELECT * FROM pending_promo_jobs
            WHERE status = 'pending'
            AND created_at >= datetime('now', '-{int(window_hours)} hours')
            ORDER BY created_at"""
    )
    resumed = 0
    for row in rows:
        record = dict(row)
        job_id = record["job_id"]
        if job_id in _UPLOAD_JOBS:
            continue  # already live this process — don't double-spawn

        youtube_video_id = record.get("youtube_video_id")
        if youtube_video_id:
            await _mark_pending_promo_job(
                job_id, status="failed",
                last_error=(
                    f"uploaded to YouTube as {youtube_video_id} but the row INSERT "
                    "did not complete before restart; import it from the dashboard"
                ),
            )
            logger.warning(
                "Promo job %s uploaded as %s but never inserted a row; left for "
                "manual import (NOT re-uploaded).", job_id, youtube_video_id,
            )
            continue

        local_path = record.get("local_path")
        if local_path and not Path(local_path).exists():
            local_path = None  # cut file gone — fall back to re-cutting
        can_recut = bool(record.get("parent_video_path")) and (
            record.get("cut_start_seconds") is not None
        )
        if not local_path and not can_recut:
            await _mark_pending_promo_job(
                job_id, status="failed",
                last_error="cut file missing and no parent cut params to re-cut",
            )
            continue

        _UPLOAD_JOBS[job_id] = {
            "job_id": job_id,
            "filename": record.get("original_filename"),
            "local_path": local_path,
            "parent_id": record.get("parent_id"),
            "project_id": int(record["project_id"]),
            "forced_item_type": record.get("forced_item_type"),
            "video_id": None,
            "state": PROMO_STATE_CUTTING if record.get("parent_video_path") else PROMO_STATE_PENDING,
            "last_error": None,
            "title": record.get("title"),
            "pre_supplied_title": record.get("title"),
            "cut_start_seconds": record.get("cut_start_seconds"),
            "cut_end_seconds": record.get("cut_end_seconds"),
            "parent_video_path": record.get("parent_video_path"),
            "vertical_crop": bool(record.get("vertical_crop")),
            "x_shift_normalized": float(record.get("x_shift_normalized") or 0.0),
            "audio_fade_in": float(record.get("audio_fade_in") or 0.0),
            "audio_fade_out": float(record.get("audio_fade_out") or 0.0),
        }
        spawn_background(_run_promo_chain(job_id), name=f"promo-resume:{job_id}")
        resumed += 1

    if resumed:
        logger.info(
            "Re-spawned %d pending Promo chain(s) from before the last restart.",
            resumed,
        )
    return resumed


async def retry_promo_step(video_id: str, step: str) -> None:
    """Re-run the chain starting at ``step``. The chain's idempotency
    gates handle "skip steps we already have" for steps after the failed
    one, so this is the right hook for a per-card retry button."""
    if step not in PROMO_STEP_ORDER:
        raise ValueError(f"Unknown promo step: {step!r}")
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM videos WHERE id = ?", (video_id,)
    )
    if not rows:
        raise ValueError(f"Video {video_id} not found")
    video = dict(rows[0])
    project_id = int(video.get("project_id") or 1)
    await _set_promo_state(video_id, step, error=None)

    async def _gated_resume() -> None:
        # Retries enter the chain mid-stream, but the rest of the chain
        # (Claude calls, ffmpeg keyframe extraction, YouTube metadata)
        # is exactly what _PROMO_CHAIN_SEMAPHORE was sized to throttle.
        # Without this gate a user who mass-retries 12 failed cards
        # fan-outs 12 simultaneous chains and we lose the cap the new-
        # upload entrypoint installed.
        async with _get_promo_chain_semaphore():
            await _resume_promo_chain(video_id, project_id, start_step=step)

    spawn_background(_gated_resume(), name=f"promo-retry:{video_id}:{step}")


async def _set_promo_state(
    video_id: str, state: str | None, *, error: str | None = None
) -> None:
    async with write_transaction() as db:
        await db.execute(
            "UPDATE videos SET auto_action_state = ?, auto_action_last_error = ?, "
            "updated_at = datetime('now') WHERE id = ?",
            (state, error, video_id),
        )


# A transcript this short means the transcriber ran and heard nothing usable —
# a silent or music-only video. Same threshold the promo chain uses.
TRANSCRIPT_MIN_USABLE_CHARS = 10

NO_SPEECH_ERROR = (
    "Transcription finished but produced no usable speech — this video may have "
    "no dialogue. Use “Describe from video frames” if that's expected."
)


async def _set_auto_action_progress(video_id: str, message: str | None) -> None:
    """Update only the human-readable progress line, leaving state/error alone."""
    async with write_transaction() as db:
        await db.execute(
            "UPDATE videos SET auto_action_progress_message = ?, "
            "updated_at = datetime('now') WHERE id = ?",
            (message, video_id),
        )


async def store_generated_description(
    video_id: str, raw_description: str, pinned_links: str
) -> str:
    """Compose the AI description with the pinned links and stage it.

    Staged in ``generated_description`` (not ``description``): the user reviews
    it in the editor and pushes it to YouTube themselves. Shared by the
    synchronous route and the background chain so the two can't drift on how a
    description gets composed.
    """
    full = (
        f"{raw_description}\n\n{pinned_links}" if pinned_links else raw_description
    )
    # raw_description is already clean (ai.py sanitizes what it returns), but
    # pinned_links may be a pre-sanitizer row still holding raw angle brackets.
    # sanitize_youtube_text is idempotent, so re-running it costs nothing.
    full = youtube.sanitize_youtube_text(full)
    async with write_transaction() as db:
        await db.execute(
            "UPDATE videos SET generated_description = ?, "
            "updated_at = datetime('now') WHERE id = ?",
            (full, video_id),
        )
    return full


async def claim_describe_chain(video_id: str) -> bool:
    """Atomically claim this video for a transcribe-then-describe run.

    Returns True if we won the claim, False if a background chain is already
    working on this row. ``spawn_background`` does NOT dedup by task name, so
    without this a double-click, a second browser tab, or any repeated API call
    would start a second on-device transcription of the same file — two chains
    racing on the same transcript, progress line, description, and terminal
    state. The conditional UPDATE is the gate: SQLite applies it atomically, so
    exactly one caller sees rowcount 1.

    The WHERE clause also refuses to trample a chain that is mid-flight for some
    OTHER reason (a Promo chain cutting/uploading/pushing), because every one of
    those states is non-terminal and therefore excluded here.
    """
    async with write_transaction() as db:
        cursor = await db.execute(
            "UPDATE videos SET auto_action_state = ?, auto_action_last_error = NULL, "
            "auto_action_progress_message = ?, updated_at = datetime('now') "
            "WHERE id = ? AND ("
            "  auto_action_state IS NULL"
            "  OR auto_action_state = ?"
            "  OR auto_action_state LIKE 'failed:%'"
            ")",
            (
                PROMO_STATE_TRANSCRIBING,
                "Transcribing on-device…",
                video_id,
                PROMO_STATE_READY,
            ),
        )
    return (cursor.rowcount or 0) > 0


def transcribe_then_describe(
    video_id: str, project_id: int, *, extra_instructions: str = ""
) -> None:
    """Kick off "transcribe, then describe from that transcript" in the background.

    Returns immediately. Progress lands in ``videos.auto_action_state`` +
    ``auto_action_progress_message``, which the detail page polls — so the user
    is free to navigate away and come back to a finished description.

    The caller must have won :func:`claim_describe_chain` first.
    """
    spawn_background(
        _run_transcribe_then_describe(video_id, project_id, extra_instructions),
        name=f"transcribe-then-describe:{video_id}",
    )


async def _run_transcribe_then_describe(
    video_id: str, project_id: int, extra_instructions: str
) -> None:
    # Nothing below may raise past this point. The HTTP request already returned
    # 202 and the row already says "transcribing", so an escaping exception would
    # strand the row in a running state forever with the UI spinning and the real
    # error buried in the log — the exact "misleading fine state" we forbid.
    try:
        await _transcribe_then_describe_steps(
            video_id, project_id, extra_instructions
        )
    except Exception as exc:
        logger.exception("transcribe-then-describe: unexpected failure for %s", video_id)
        await _set_auto_action_progress(video_id, None)
        await _set_promo_state(
            video_id, f"failed:{PROMO_STATE_TRANSCRIBING}",
            error=f"{type(exc).__name__}: {exc}"[:500],
        )


async def _transcribe_then_describe_steps(
    video_id: str, project_id: int, extra_instructions: str
) -> None:
    project_row = await get_project_by_id(project_id)
    if project_row:
        set_active_project(project_row["slug"])

    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    if not rows:
        return
    video = dict(rows[0])
    video_file_path = video.get("video_file_path") or ""

    actions = await project_settings.get_auto_actions(project_id)
    upload_column = actions.get("upload", {})
    backend = upload_column.get("auto_transcribe_backend")
    model = upload_column.get("auto_transcribe_model")

    # --- Step 1: transcribe -------------------------------------------------
    # claim_describe_chain already persisted these; set them again so the chain
    # is still correct when called directly (tests, future callers).
    await _set_promo_state(video_id, PROMO_STATE_TRANSCRIBING)
    await _set_auto_action_progress(video_id, "Transcribing on-device…")

    # transcribe() calls this from its worker thread, so it can't await. Stash
    # the latest percent and let a poller task push it to the DB — writing a row
    # per callback would hammer SQLite for a multi-minute transcription.
    latest_percent: dict[str, int | None] = {"value": None}

    def _on_progress(done_seconds: float, total_seconds: float) -> None:
        if total_seconds > 0:
            pct = max(0, min(100, int(round(done_seconds / total_seconds * 100))))
            latest_percent["value"] = pct

    async def _publish_progress() -> None:
        last_written: int | None = None
        while True:
            await asyncio.sleep(2)
            pct = latest_percent["value"]
            if pct is not None and pct != last_written:
                last_written = pct
                await _set_auto_action_progress(
                    video_id, f"Transcribing on-device… {pct}%"
                )

    progress_task = asyncio.create_task(_publish_progress())
    try:
        async with _get_whisper_semaphore():
            await transcribe_and_store(
                video_id, video_file_path,
                backend=backend, model=model,
                progress_callback=_on_progress,
            )
    except Exception as exc:
        logger.warning("transcribe-then-describe: transcription failed for %s: %s",
                       video_id, exc)
        await _set_auto_action_progress(video_id, None)
        await _set_promo_state(
            video_id, f"failed:{PROMO_STATE_TRANSCRIBING}",
            error=f"Transcription failed: {exc}"[:500],
        )
        return
    finally:
        # Await the cancellation: a bare cancel() leaves the task's own failure
        # (e.g. SQLite blew up inside the publisher) unretrieved, which surfaces
        # later as a stray "Task exception was never retrieved".
        progress_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await progress_task

    # --- Step 2: is there any speech to describe? ---------------------------
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    if not rows:
        return
    video = dict(rows[0])
    transcript = (video.get("transcript") or "").strip()
    plain = (
        transcript_service.srt_to_plain_text(transcript).strip() if transcript else ""
    )
    if len(plain) < TRANSCRIPT_MIN_USABLE_CHARS:
        # Deliberately NOT falling through to the keyframe describer: that would
        # quietly produce a description from a different source than the user
        # asked for, and skip prompts whose required variables are the whole
        # point (e.g. a project whose description prompt demands a show link).
        await _set_auto_action_progress(video_id, None)
        await _set_promo_state(
            video_id, f"failed:{PROMO_STATE_TRANSCRIBING}", error=NO_SPEECH_ERROR,
        )
        return

    # --- Step 3: describe ---------------------------------------------------
    await _set_promo_state(video_id, PROMO_STATE_GENERATING_DESC)
    await _set_auto_action_progress(video_id, "Generating description…")
    try:
        prompt_variables = await tmpl.build_prompt_variables(video)
        description = await ai.generate_seo_description(
            title=video.get("title") or "",
            transcript=transcript,
            extra_instructions=extra_instructions,
            project_id=project_id,
            prompt_variables=prompt_variables,
            is_promo=bool(video.get("parent_item_id")),
        )
        await store_generated_description(
            video_id, description, video.get("pinned_links") or ""
        )
    except Exception as exc:
        logger.warning("transcribe-then-describe: description failed for %s: %s",
                       video_id, exc)
        await _set_auto_action_progress(video_id, None)
        await _set_promo_state(
            video_id, f"failed:{PROMO_STATE_GENERATING_DESC}",
            error=f"{type(exc).__name__}: {exc}"[:500],
        )
        return

    await _set_auto_action_progress(video_id, None)
    await _set_promo_state(video_id, PROMO_STATE_READY)


async def _load_parent_context(parent_id: str | None) -> dict:
    """Fetch parent fields for the title-from-filename prompt. Returns an
    empty dict when there's no parent (caller passes empty strings)."""
    if not parent_id:
        return {}
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT title, description, url, tags FROM videos WHERE id = ?",
        (parent_id,),
    )
    if not rows:
        return {}
    parent = dict(rows[0])
    try:
        parent_tags = ", ".join(json.loads(parent.get("tags") or "[]"))
    except json.JSONDecodeError:
        parent_tags = ""
    return {
        "title": parent.get("title") or "",
        "description": parent.get("description") or "",
        "url": parent.get("url") or "",
        "tags": parent_tags,
    }


async def _run_promo_chain(job_id: str) -> None:
    """Drive a new promo upload from start to finish.

    The first two steps (title generation + YT upload) run before any DB
    row exists; their state lives in ``_UPLOAD_JOBS[job_id]``. Once
    YouTube returns an id we INSERT the row, the upload job hands off to
    the videos row's ``auto_action_state``, and remaining steps update
    that column.

    Wrapped in :data:`_PROMO_CHAIN_SEMAPHORE` so a burst of new jobs (e.g.
    the user accepting 12 generated clips) queues rather than thrashes
    the machine.
    """
    async with _get_promo_chain_semaphore():
        await _run_promo_chain_inner(job_id)


async def _run_promo_chain_inner(job_id: str) -> None:
    job = _UPLOAD_JOBS.get(job_id)
    if job is None:
        return
    project_id = int(job["project_id"])

    # Step 0 — cut from parent (Generate-from-source flow only).
    # ``parent_video_path`` is set by :func:`start_promo_from_cut`. We do
    # this *before* setting the project context because the cut is a
    # pure ffmpeg subprocess call and doesn't need the OAuth token.
    if job.get("parent_video_path") and not job.get("local_path"):
        from yt_scheduler.services import clipper

        try:
            proposal = clipper.ProposedClip(
                kind=job.get("forced_item_type") or "short",  # type: ignore[arg-type]
                start_seconds=float(job["cut_start_seconds"]),
                end_seconds=float(job["cut_end_seconds"]),
                title=job.get("pre_supplied_title") or "",
                reason="",
                audio_fade_in=float(job.get("audio_fade_in", 0.0)),
                audio_fade_out=float(job.get("audio_fade_out", 0.0)),
            )
            cut_path = await clipper.cut_clip_from_parent(
                parent_video_path=Path(job["parent_video_path"]),
                proposal=proposal,
                vertical_crop=bool(job.get("vertical_crop", False)),
                x_shift_normalized=float(job.get("x_shift_normalized", 0.0)),
            )
        except Exception as exc:
            _mark_upload_failed(
                job, f"failed:{PROMO_STATE_CUTTING}",
                error=f"{type(exc).__name__}: {exc}"[:500],
            )
            logger.warning(
                "Promo cut failed for parent=%s range=%s-%s: %r",
                job.get("parent_id"), job.get("cut_start_seconds"),
                job.get("cut_end_seconds"), exc, exc_info=True,
            )
            await _mark_pending_promo_job(
                job_id, status="failed", last_error=f"{type(exc).__name__}: {exc}",
            )
            return
        job["local_path"] = str(cut_path)

    project_row = await get_project_by_id(project_id)
    if project_row:
        set_active_project(project_row["slug"])

    # Step 1 — generate title. Skipped when a title was supplied by the
    # caller (Generate-from-source flow: Claude already proposed one
    # during preview, so re-running title-from-filename would just throw
    # that away in favour of a worse guess). _load_parent_context is
    # only needed for the AI prompt context, so defer the SELECT into
    # the else branch — Generate cuts skip it.
    pre_supplied_title = job.get("pre_supplied_title")
    if pre_supplied_title:
        title = pre_supplied_title
    else:
        parent_ctx = await _load_parent_context(job.get("parent_id"))
        job["state"] = PROMO_STATE_GENERATING_TITLE
        try:
            title = await ai.generate_title_from_filename(
                filename=job["filename"],
                project_id=project_id,
                parent_url=parent_ctx.get("url", ""),
                parent_title=parent_ctx.get("title", ""),
                parent_description=parent_ctx.get("description", ""),
                parent_tags=parent_ctx.get("tags", ""),
            )
        except Exception as exc:
            # ERROR (not warning): if Claude is unreachable here, every
            # downstream AI step (description, tags) will also fail, so this
            # is a "the whole pipeline is degraded" signal, not a routine
            # fallback. The deterministic title is still used so the chain
            # can keep going. local_path is logged alongside the user-facing
            # filename so "file not found" failures can be diagnosed without
            # guessing which path the error refers to (the on-disk path is
            # always app-chosen and has no spaces; the original filename does).
            logger.error(
                "AI title failed for %s (local_path=%s), using deterministic fallback: %r",
                job["filename"], job.get("local_path"), exc,
                exc_info=True,
            )
            title = ai.fallback_title_from_filename(job["filename"])

    # A pre-supplied title (Generate-from-source proposes one, and the user can
    # edit it in the review screen) never passed through the AI generators, so
    # it never got sanitized. Without this, upload_video would sanitize it on the
    # way to YouTube while the INSERT below stored the raw '<'/'>' — exactly the
    # DB-disagrees-with-YouTube split this is all meant to prevent. Idempotent,
    # so the AI-generated branch is unaffected.
    title = youtube.sanitize_youtube_text(title)
    job["title"] = title

    # Step 2 — YouTube upload
    job["state"] = PROMO_STATE_UPLOADING
    local_path = Path(job["local_path"])
    try:
        result = await asyncio.to_thread(
            youtube.upload_video,
            file_path=local_path,
            title=title,
            description="Description pending generation.",
            tags=[],
            privacy_status="unlisted",
            publish_at=None,
        )
    except Exception as exc:
        _mark_upload_failed(
            job, f"failed:{PROMO_STATE_UPLOADING}",
            error=f"{type(exc).__name__}: {exc}"[:500],
        )
        logger.warning(
            "Promo YT upload failed for %s (local_path=%s): %r",
            job["filename"], local_path, exc,
            exc_info=True,
        )
        await _mark_pending_promo_job(
            job_id, status="failed", last_error=f"{type(exc).__name__}: {exc}",
        )
        return

    video_id = result["id"]
    youtube_url = f"https://youtu.be/{video_id}"
    # Stamp the YouTube id BEFORE the row INSERT so a restart in this narrow
    # window recognises the clip already uploaded and does NOT re-upload it
    # (which would create a duplicate — the exact thing we're trying to avoid).
    # critical=True: retry rather than silently swallow — this stamp is the dedup
    # guarantee, not bookkeeping.
    await _mark_pending_promo_job(job_id, youtube_video_id=video_id, critical=True)

    duration = await asyncio.to_thread(tiers.probe_local_duration, local_path)
    derived_tier = tiers.tier_for_duration(duration)
    # ``forced_item_type`` is set when the user adds via a per-section
    # button (Segments / Shorts / Hooks). Top-level "Add" leaves it None
    # so item_type tracks the duration-derived tier.
    item_type = (
        job.get("forced_item_type")
        or derived_tier
        # Final fallback: anything we can't classify lands as short so
        # it shows up on the Promo Videos screen (rather than vanishing).
        or "short"
    )

    # Generate-from-source clips are derived from a parent's local
    # file — they were never directly uploaded by the user. Stamp them
    # 'generated_clip' so the file-info modal can show that honestly
    # and so future Replace-source attempts on a 9:16 generated short
    # don't trip the resolution-downgrade warning against the 1080×1920
    # cut's own dimensions.
    file_origin = "generated_clip" if job.get("parent_video_path") else "uploaded"

    async with write_transaction() as db:
        await db.execute(
            """INSERT INTO videos (
                id, project_id, title, description, tags, privacy_status,
                video_file_path, video_file_original_name, status,
                duration_seconds, tier,
                item_type, parent_item_id, url,
                auto_action_state, source_file_origin,
                cut_start_seconds, cut_end_seconds
            ) VALUES (?, ?, ?, ?, '[]', 'unlisted', ?, ?, 'uploaded',
                      ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                video_id,
                project_id,
                title,
                "Description pending generation.",
                str(local_path),
                sanitized_original_filename(job.get("filename")),
                duration,
                derived_tier,
                item_type,
                job.get("parent_id") or None,
                youtube_url,
                PROMO_STATE_PROBING,
                file_origin,
                job.get("cut_start_seconds"),
                job.get("cut_end_seconds"),
            ),
        )
    job["video_id"] = video_id
    # The videos row now exists; its auto_action_state drives resume from here,
    # so this pre-INSERT record has done its job.
    await _mark_pending_promo_job(job_id, status="done")

    await events.record_event(
        video_id, "created", {"tier": derived_tier, "item_type": item_type}
    )
    await events.record_event(
        video_id, "uploaded", {"platform": "youtube", "url": youtube_url}
    )

    # Hand off: upload-job's state is now mirrored from the column the UI
    # polls; remove it after a short grace so any in-flight polls catch up.
    job["state"] = PROMO_STATE_PROBING

    try:
        await _resume_promo_chain(
            video_id, project_id, start_step=PROMO_STATE_TRANSCRIBING
        )
    finally:
        # Drop the upload-job record — the videos row is the source of
        # truth from here on. Keep a "completed" marker so a slow client
        # polling upload-jobs/<id> can find video_id one last time.
        _UPLOAD_JOBS.pop(job_id, None)


async def _resume_promo_chain(
    video_id: str, project_id: int, *, start_step: str
) -> None:
    """Walk the step list from ``start_step`` onward. Each step is gated
    by an "already done?" check that lets retries skip past completed
    work without redoing it.

    The orchestrator is split out so :func:`retry_promo_step` can hop in
    at any point — the file-system / upload state lives entirely on the
    videos row at this point.
    """
    project_row = await get_project_by_id(project_id)
    if project_row:
        set_active_project(project_row["slug"])

    try:
        start_idx = PROMO_STEP_ORDER.index(start_step)
    except ValueError:
        await _set_promo_state(
            video_id, f"failed:{start_step}",
            error=f"Unknown step: {start_step}",
        )
        return

    # ``probing`` happens at INSERT time in the upload step; if the start
    # point is at or before it, nothing to do — jump to transcribe.
    if start_idx < PROMO_STEP_ORDER.index(PROMO_STATE_TRANSCRIBING):
        start_idx = PROMO_STEP_ORDER.index(PROMO_STATE_TRANSCRIBING)

    for step in PROMO_STEP_ORDER[start_idx:]:
        await _set_promo_state(video_id, step, error=None)
        try:
            if step == PROMO_STATE_TRANSCRIBING:
                await _promo_step_transcribe(video_id)
            elif step == PROMO_STATE_GENERATING_DESC:
                await _promo_step_description(video_id, project_id)
            elif step == PROMO_STATE_GENERATING_TAGS:
                await _promo_step_tags(video_id, project_id)
            elif step == PROMO_STATE_PUSHING_METADATA:
                await _promo_step_push_metadata(video_id)
        except Exception as exc:
            logger.warning("Promo step %s failed for %s: %s", step, video_id, exc)
            await _set_promo_state(
                video_id, f"failed:{step}", error=str(exc)[:500]
            )
            return

    await _set_promo_state(video_id, PROMO_STATE_READY, error=None)
    # Refresh videos.status to 'ready' so the existing schedule paths see
    # the row as eligible (matches what _maybe_generate_description does
    # for the standard upload/import chain).
    async with write_transaction() as db:
        await db.execute(
            "UPDATE videos SET status = 'ready', updated_at = datetime('now') "
            "WHERE id = ? AND status != 'published'",
            (video_id,),
        )


async def _promo_step_transcribe(video_id: str) -> None:
    """Run Whisper if we don't already have a transcript. Concurrency is bounded
    to the CPU count via :data:`_WHISPER_SEMAPHORE` (shared with the standard
    auto-action path)."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT transcript, video_file_path FROM videos WHERE id = ?",
        (video_id,),
    )
    if not rows:
        raise RuntimeError(f"videos row {video_id} missing")
    row = dict(rows[0])
    if (row.get("transcript") or "").strip():
        return  # already done; the chain will move on
    async with _get_whisper_semaphore():
        await _maybe_transcribe(
            video_id, row.get("video_file_path"), backend=None, model=None
        )


async def _promo_step_description(video_id: str, project_id: int) -> None:
    """Generate a description from transcript (≥10 chars after trim) or
    from sampled keyframes."""
    db = await get_db()
    # SELECT * — build_prompt_variables needs project_id, parent_item_id,
    # url, and transcript in addition to the columns this step reads
    # directly. A promo's whole point is referencing its parent, so the
    # parent linkage columns must reach the variable builder.
    rows = await db.execute_fetchall(
        "SELECT * FROM videos WHERE id = ?",
        (video_id,),
    )
    if not rows:
        raise RuntimeError(f"videos row {video_id} missing")
    row = dict(rows[0])
    if (row.get("description") or "").strip() and (
        row.get("description") != "Description pending generation."
    ):
        return
    description = await _generate_promo_description(row, project_id)
    await _store_promo_description(video_id, row.get("description") or "", description)


async def _generate_promo_description(row: dict, project_id: int) -> str:
    """Produce the description text for a promo row. Writes nothing.

    Split out from the chain step so the deliberate re-generation path
    (:func:`_regenerate_and_push_description`) can push the result to YouTube
    *before* persisting it, without duplicating how a promo description is
    composed. ``row`` must be a full ``videos`` row — the prompt-variable
    builder reads the parent linkage columns off it.
    """
    title = row.get("title") or ""
    transcript = (row.get("transcript") or "").strip()
    plain_transcript = transcript
    if transcript:
        plain_transcript = transcript_service.srt_to_plain_text(transcript).strip()

    prompt_variables = await tmpl.build_prompt_variables(row)
    video_is_promo = bool(row.get("parent_item_id"))

    if len(plain_transcript) >= TRANSCRIPT_MIN_USABLE_CHARS:
        return await ai.generate_seo_description(
            title=title, transcript=transcript, project_id=project_id,
            prompt_variables=prompt_variables, is_promo=video_is_promo,
        )
    video_file_path = row.get("video_file_path")
    if not video_file_path or not Path(video_file_path).exists():
        raise RuntimeError(
            "No local file for keyframe-based description fallback"
        )
    from yt_scheduler.services import media as media_service
    frames = await asyncio.to_thread(
        media_service.extract_keyframes, video_file_path, 6,
    )
    return await ai.generate_seo_description_from_frames(
        title=title, frames=frames, project_id=project_id,
        prompt_variables=prompt_variables, is_promo=video_is_promo,
    )


async def _store_promo_description(
    video_id: str, old_description: str, description: str
) -> None:
    """Persist a generated promo description and record the change event."""
    async with write_transaction() as db:
        await db.execute(
            """UPDATE videos SET description = ?, description_generated_at = datetime('now'),
               updated_at = datetime('now') WHERE id = ?""",
            (description, video_id),
        )
    await events.record_event(
        video_id, "metadata_updated",
        {"description": {"old": old_description, "new": description}},
    )


async def _promo_step_tags(video_id: str, project_id: int) -> None:
    db = await get_db()
    rows = await db.execute_fetchall(
        # SELECT * — build_prompt_variables needs project_id,
        # parent_item_id, and url beyond the columns this step reads.
        "SELECT * FROM videos WHERE id = ?",
        (video_id,),
    )
    if not rows:
        raise RuntimeError(f"videos row {video_id} missing")
    row = dict(rows[0])
    # Skip if auto-generation already ran for this video. Using tags_generated_at
    # rather than a tag-count gate prevents re-generation on retry/restart even
    # when the previous run produced an empty or small list.
    if row.get("tags_generated_at"):
        return
    try:
        existing = json.loads(row.get("tags") or "[]")
        if not isinstance(existing, list):
            existing = []
    except json.JSONDecodeError:
        existing = []

    title = row.get("title") or ""
    description = row.get("description") or ""
    transcript = (row.get("transcript") or "").strip()
    plain_transcript = (
        transcript_service.srt_to_plain_text(transcript).strip()
        if transcript else ""
    )

    prompt_variables = await tmpl.build_prompt_variables(row)
    video_is_promo = bool(row.get("parent_item_id"))

    if len(plain_transcript) >= TRANSCRIPT_MIN_USABLE_CHARS:
        new_tags = await ai.generate_tags_from_metadata(
            title=title, description=description, transcript=transcript,
            project_id=project_id, prompt_variables=prompt_variables,
            is_promo=video_is_promo,
        )
    else:
        video_file_path = row.get("video_file_path")
        if not video_file_path or not Path(video_file_path).exists():
            raise RuntimeError(
                "No local file for keyframe-based tags fallback"
            )
        from yt_scheduler.services import media as media_service
        frames = await asyncio.to_thread(
            media_service.extract_keyframes, video_file_path, 6,
        )
        new_tags = await ai.generate_tags_from_frames(
            title=title, description=description, frames=frames,
            project_id=project_id, prompt_variables=prompt_variables,
            is_promo=video_is_promo,
        )

    async with write_transaction() as db:
        await db.execute(
            "UPDATE videos SET tags = ?, tags_generated_at = datetime('now'), "
            "updated_at = datetime('now') WHERE id = ?",
            (json.dumps(new_tags), video_id),
        )
    await events.record_event(
        video_id, "metadata_updated", {"tags": {"old": existing, "new": new_tags}},
    )


async def _promo_step_push_metadata(video_id: str) -> None:
    """Send the newly-generated title / description / tags back to
    YouTube in a single update call (50 quota units)."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT title, description, tags FROM videos WHERE id = ?", (video_id,)
    )
    if not rows:
        raise RuntimeError(f"videos row {video_id} missing")
    row = dict(rows[0])
    try:
        tag_list = json.loads(row.get("tags") or "[]")
    except json.JSONDecodeError:
        tag_list = []
    await asyncio.to_thread(
        youtube.update_video_metadata,
        video_id=video_id,
        title=row.get("title") or "",
        description=row.get("description") or "",
        tags=tag_list,
    )


# --- Bulk description re-generation ---------------------------------------
#
# "Update all descriptions" on the Promo screen: after editing the promo
# description prompt template, re-run generation for every clip under a parent
# and push the new text to YouTube. Per-clip progress rides on the SAME
# ``videos.auto_action_state`` column the promo cards already poll, so a browser
# reload (or a second tab) picks the run up mid-flight.

# videos.list (1) to read the current snippet + videos.update (50) — see
# youtube.update_video_metadata, which merges rather than blind-writes.
DESCRIPTION_UPDATE_QUOTA_UNITS_PER_CLIP = 51

# Reasons Google returns when the DAILY quota is gone. One clip hitting this
# means every remaining clip in the batch would too, so the run stops instead of
# burning through 90 identical failures.
#
# Deliberately excludes rateLimitExceeded / userRateLimitExceeded: those are
# transient "slow down" signals, and treating one as exhaustion would abort a
# batch that was about to succeed while telling the user something untrue about
# their quota. Those fail their own clip, which is retryable.
_QUOTA_EXHAUSTED_MARKERS = (
    "quotaExceeded",
    "dailyLimitExceeded",
)

_QUOTA_ABORT_MESSAGE = (
    "Stopped: YouTube's daily API quota is exhausted. The remaining clips were "
    "not updated — run this again after the quota resets (midnight Pacific)."
)


def _is_quota_exhaustion(exc: BaseException) -> bool:
    text = str(exc)
    return any(marker in text for marker in _QUOTA_EXHAUSTED_MARKERS)


async def fail_interrupted_description_updates() -> int:
    """Boot-time sweep: fail any row left mid description-update by a shutdown.

    Nothing can be re-generating at process start, so every ``updating_desc``
    row is by definition abandoned. Left alone it would spin "Updating
    description…" forever — and worse, be permanently unclaimable, because both
    claim helpers refuse a non-terminal state. There is no repair path from the
    UI for that.

    Failing them (rather than silently resuming) is deliberate: a resume would
    re-spend Claude calls and YouTube quota at boot for work the user never
    re-authorised, bypassing the confirm dialog that exists to show that cost.
    The clip still has its previous, valid description, so nothing is broken by
    waiting — and the card's "Retry description update" button puts the decision
    back in the user's hands.

    Deliberately NOT limited to the resume window the chain uses: a row stranded
    for longer than that window would otherwise stay wedged forever.
    """
    async with write_transaction() as db:
        cursor = await db.execute(
            "UPDATE videos SET auto_action_state = ?, auto_action_last_error = ?, "
            "auto_action_progress_message = NULL, updated_at = datetime('now') "
            "WHERE auto_action_state = ?",
            (
                f"failed:{PROMO_STATE_UPDATING_DESC}",
                "Interrupted by a server restart before this clip was updated. "
                "Its previous description is unchanged — run the update again.",
                PROMO_STATE_UPDATING_DESC,
            ),
        )
    count = cursor.rowcount or 0
    if count:
        logger.warning(
            "Failed %d description update(s) left in flight by a previous shutdown",
            count,
        )
    return count


async def claim_description_update(video_id: str) -> bool:
    """Atomically claim one promo row for a description re-generation.

    Returns True when this caller won the claim. False means the row is already
    mid-chain (uploading, transcribing, a previous update still running) — in
    that case we leave it alone rather than racing two writers on the same
    description. Mirrors :func:`claim_describe_chain`: only terminal states
    (NULL / ready / failed:*) are claimable, and SQLite applies the conditional
    UPDATE atomically so exactly one caller sees rowcount 1.
    """
    async with write_transaction() as db:
        cursor = await db.execute(
            "UPDATE videos SET auto_action_state = ?, auto_action_last_error = NULL, "
            "auto_action_progress_message = ?, updated_at = datetime('now') "
            "WHERE id = ? AND ("
            "  auto_action_state IS NULL"
            "  OR auto_action_state = ?"
            "  OR auto_action_state LIKE 'failed:%'"
            ")",
            (
                PROMO_STATE_UPDATING_DESC,
                "Queued for description update…",
                video_id,
                PROMO_STATE_READY,
            ),
        )
    return (cursor.rowcount or 0) > 0


def start_promo_description_updates(project_id: int, video_ids: list[str]) -> None:
    """Re-generate + push the description for each claimed clip, in the
    background. Returns immediately.

    The caller must have won :func:`claim_description_update` for every id, so
    each row already reads ``updating_desc`` by the time the HTTP response goes
    out and the page shows progress on the very next poll.
    """
    ids = list(video_ids)
    spawn_background(
        _run_promo_description_updates(project_id, ids),
        name=f"promo-desc-update:project-{project_id}:{len(ids)}-clips",
    )


async def _run_promo_description_updates(
    project_id: int, video_ids: list[str]
) -> None:
    """Fan the batch out under the shared promo-chain semaphore.

    Nothing may escape this coroutine: every row is already claimed and showing
    "Updating description…", so an exception here would strand all of them in a
    running state forever.
    """
    project_row = await get_project_by_id(project_id)
    if project_row:
        # Binds this task's YouTube credentials. spawn_background gives the task
        # its own copy of the context, so this cannot leak into other requests.
        set_active_project(project_row["slug"])

    # Set by the first clip to hit a quota wall; every clip that has not started
    # yet then fails fast with the same explanation instead of retrying it.
    quota_abort = False

    async def _fail(video_id: str, error: str) -> None:
        await _set_auto_action_progress(video_id, None)
        await _set_promo_state(
            video_id, f"failed:{PROMO_STATE_UPDATING_DESC}", error=error,
        )

    async def _update_one(video_id: str) -> None:
        nonlocal quota_abort
        # Checked twice on purpose: once before queuing behind the semaphore,
        # and again after acquiring it, since the wall may have been hit while
        # this clip sat in the queue.
        if quota_abort:
            await _fail(video_id, _QUOTA_ABORT_MESSAGE)
            return
        async with _get_promo_chain_semaphore():
            if quota_abort:
                await _fail(video_id, _QUOTA_ABORT_MESSAGE)
                return
            try:
                await _regenerate_and_push_description(video_id, project_id)
            except Exception as exc:
                if _is_quota_exhaustion(exc):
                    quota_abort = True
                logger.warning(
                    "Description update failed for %s: %s", video_id, exc,
                )
                await _fail(video_id, f"{type(exc).__name__}: {exc}"[:500])
                return
        await _set_auto_action_progress(video_id, None)
        await _set_promo_state(video_id, PROMO_STATE_READY, error=None)

    # return_exceptions: _update_one already converts the real work's failures
    # into a per-clip failed state, so anything reaching here is a DB-level
    # fault. Log it against the clip rather than letting one bad row abort the
    # gather and leave the rest of the batch unaccounted for.
    outcomes = await asyncio.gather(
        *(_update_one(vid) for vid in video_ids), return_exceptions=True
    )
    for video_id, outcome in zip(video_ids, outcomes):
        if isinstance(outcome, BaseException):
            logger.error(
                "Description update for %s raised outside its handler",
                video_id, exc_info=outcome,
            )


async def _regenerate_and_push_description(video_id: str, project_id: int) -> None:
    """Re-generate one clip's description, push it, then persist it.

    Push BEFORE the local write, for the reason ``apply_description`` states: if
    the order were reversed, a failed push would leave the row claiming YouTube
    holds a description it never received. These clips are mostly already
    published, so nothing would ever reconcile that — the app would show one
    description and viewers another.

    Refuses rather than falling back when there's no usable transcript. The
    keyframe fallback answers a different question (what is on screen) through a
    different prompt template, so it would quietly produce a description missing
    everything the transcript prompt asks for — including the episode links this
    whole operation exists to add.
    """
    db = await get_db()
    # SELECT * — _generate_promo_description hands the row to the prompt-variable
    # builder, which reads the parent linkage columns off it.
    rows = await db.execute_fetchall("SELECT * FROM videos WHERE id = ?", (video_id,))
    if not rows:
        raise RuntimeError(f"videos row {video_id} missing")
    row = dict(rows[0])
    transcript = (row.get("transcript") or "").strip()
    plain_transcript = (
        transcript_service.srt_to_plain_text(transcript).strip() if transcript else ""
    )
    if len(plain_transcript) < TRANSCRIPT_MIN_USABLE_CHARS:
        raise RuntimeError(
            "No usable transcript, so the description can't be re-generated from "
            "speech. Transcribe this clip first (its detail page), then re-run."
        )

    await _set_auto_action_progress(video_id, "Re-generating description…")
    description = await _generate_promo_description(row, project_id)
    if not description.strip():
        raise RuntimeError(
            "Generation produced an empty description — nothing pushed."
        )
    await _set_auto_action_progress(video_id, "Pushing description to YouTube…")
    await _push_description_to_youtube(video_id, description)
    await _store_promo_description(video_id, row.get("description") or "", description)


async def _push_description_to_youtube(video_id: str, description: str) -> None:
    """Send ONLY the description to YouTube.

    Deliberately narrower than :func:`_promo_step_push_metadata`: a description
    refresh must not overwrite a title or tag list that was edited on YouTube
    since the clip was uploaded. ``update_video_metadata`` merges into the live
    snippet, so the untouched fields keep whatever YouTube currently holds.
    """
    await asyncio.to_thread(
        youtube.update_video_metadata,
        video_id=video_id,
        description=description,
    )
