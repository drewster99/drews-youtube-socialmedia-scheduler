"""Promo Videos API — bulk upload children under a primary, list them,

and (in commit 3) schedule the whole batch.

Endpoints:

* ``GET  /api/projects/{slug}/videos/{parent_id}/promos``
    Counts + per-child summaries, ordered by tier (segment → short →
    hook). Used by the badge on the parent's detail page and by the
    Promo Videos screen itself.

* ``POST /api/projects/{slug}/videos/{parent_id}/promos/upload``
    Multi-file upload. Each file lands in UPLOAD_DIR, gets a job_id,
    and a background ``run_promo_chain`` task is spawned. ``item_type``
    is optional: when set (per-section "Add"), every file gets that
    label; when omitted (top-level "Add"), the chain stamps the tier
    from probed duration.

* ``GET  /api/projects/{slug}/videos/{parent_id}/promos/upload-jobs/{job_id}``
    Poll a single in-flight upload job. Returns the latest state plus
    ``video_id`` once the YouTube upload step succeeds — the UI uses
    that as the cue to start polling ``/api/videos/{id}/auto-actions``
    instead.
"""

from __future__ import annotations

import asyncio
import logging
import math
import secrets
import shutil

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from pathlib import Path

from yt_scheduler.config import UPLOAD_DIR, safe_upload_ext
from yt_scheduler.database import get_db, write_transaction
from yt_scheduler.routers.video_routes import _resolve_video_file, _video_public
from yt_scheduler.services import (
    auto_actions,
    clipper,
    media as media_service,
    projects as project_service,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/projects/{slug}/videos/{parent_id}/promos",
    tags=["promos"],
)


_VALID_FORCED_ITEM_TYPES = {"segment", "short", "hook"}

# Hard parent-length cap for Generate-from-source. Above this, the
# transcript is too long to round-trip into Claude affordably and the
# preview would be unusably slow. The user can still cut clips manually
# from a longer parent via the existing upload path.
_GENERATE_MAX_PARENT_SECONDS: float = 4 * 60 * 60  # 4 hours


async def _existing_promo_dedup_info(
    parent_id: str, project_id: int,
) -> tuple[dict[str, list[tuple[float, float]]], dict[str, list[str]]]:
    """Per-kind cut ranges AND titles of clips already on this parent.

    Ranges feed Generate's overlap filter. Titles feed its duplicate-title
    filter, which exists because imported clips (e.g. re-imported from
    YouTube) have NULL cut ranges — their boundaries within the parent are
    unknowable — making them invisible to the range check. Without the title
    signal, Generate happily re-proposes the exact moment an imported clip
    already covers."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT item_type, cut_start_seconds, cut_end_seconds, title "
        "FROM videos WHERE parent_item_id = ? AND project_id = ?",
        (parent_id, project_id),
    )
    ranges: dict[str, list[tuple[float, float]]] = {"hook": [], "short": [], "segment": []}
    titles: dict[str, list[str]] = {"hook": [], "short": [], "segment": []}
    for r in rows:
        kind = r["item_type"]
        if kind not in ranges:
            continue
        if r["cut_start_seconds"] is not None and r["cut_end_seconds"] is not None:
            ranges[kind].append(
                (float(r["cut_start_seconds"]), float(r["cut_end_seconds"]))
            )
        title = (r["title"] or "").strip()
        if title:
            titles[kind].append(title)
    return ranges, titles


async def _ensure_primary(slug: str, parent_id: str) -> tuple[dict, dict]:
    """Resolve project + parent row; raise 404 / 400 with consistent
    messaging. Returned tuple is (project, parent_row)."""
    project = await project_service.get_project_by_slug(slug)
    if project is None:
        raise HTTPException(404, f"Project '{slug}' not found")
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM videos WHERE id = ? AND project_id = ?",
        (parent_id, project["id"]),
    )
    if not rows:
        raise HTTPException(404, f"Video '{parent_id}' not found in project")
    parent = dict(rows[0])
    if parent.get("parent_item_id"):
        raise HTTPException(
            400,
            f"Video '{parent_id}' is itself a promo child; only one "
            "level of parenting is supported.",
        )
    return project, parent


def _tier_readiness(children: list[dict]) -> dict:
    """One-line readiness summary for a tier's promo children, shown on
    the Promo videos card on the parent's detail page.

    ``children`` are raw ``videos`` rows — not _video_public-processed,
    because is_ready_for_schedule needs the unpopped thumbnail_path.
    Each child falls into exactly one bucket; ``line`` lists the
    non-empty buckets, and ``state`` drives the card's status dot.
    """
    from yt_scheduler.services.scheduler import is_ready_for_schedule

    n = len(children)
    if n == 0:
        return {"count": 0, "line": "", "state": "empty"}

    failed = processing = published = scheduled = ready = 0
    missing_counts: dict[str, int] = {}
    for c in children:
        auto = str(c.get("auto_action_state") or "")
        status = (c.get("status") or "").lower()
        if auto.startswith("failed"):
            failed += 1
        elif auto and auto != "ready":
            processing += 1
        elif status == "published":
            published += 1
        elif c.get("publish_at"):
            scheduled += 1
        else:
            is_ready, missing = is_ready_for_schedule(c)
            if is_ready:
                ready += 1
            else:
                for m in missing:
                    key = "tags" if m.startswith("tags") else m
                    missing_counts[key] = missing_counts.get(key, 0) + 1

    not_ready = n - failed - processing - published - scheduled - ready
    parts: list[str] = []
    if published:
        parts.append(f"{published} published")
    if scheduled:
        parts.append(f"{scheduled} scheduled")
    if ready:
        parts.append(f"{ready} ready")
    if processing:
        parts.append(f"{processing} processing")
    if failed:
        parts.append(f"{failed} failed")
    if not_ready:
        fields = ", ".join(
            sorted(missing_counts, key=lambda k: (-missing_counts[k], k))
        )
        parts.append(
            f"{not_ready} need {fields}" if fields else f"{not_ready} not ready"
        )

    if failed or not_ready:
        state = "attention"
    elif processing:
        state = "working"
    else:
        state = "ready"

    line = "all ready" if ready == n else " · ".join(parts)
    return {"count": n, "line": line, "state": state}


@router.get("")
async def list_promos(
    slug: str, parent_id: str, include_archived: bool = False
) -> dict:
    """Return per-tier counts, the children themselves, and a per-tier
    readiness summary.

    Response:
        {
            "summary": {"segment": int, "short": int, "hook": int},
            "children": {
                "segment": [<_video_public(...)>, ...],
                "short":   [...],
                "hook":    [...],
            },
            "readiness": {
                "segment": {"count": int, "line": str, "state": str},
                "short":   {...},
                "hook":    {...},
            },
        }

    ``children`` is keyed by ``item_type`` (not ``tier``) so a user-
    forced classification surfaces in the bucket the user picked.
    ``readiness.state`` is one of empty | ready | working | attention.
    """
    project, _parent = await _ensure_primary(slug, parent_id)
    # Lazy import, mirroring _tier_readiness's local import of the same
    # symbol below — scheduler is pulled in on demand rather than at router
    # load. is_ready_for_schedule is pure, so call-time import is cheap.
    from yt_scheduler.services.scheduler import is_ready_for_schedule

    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM videos WHERE parent_item_id = ? AND project_id = ? "
        "ORDER BY created_at ASC",
        (parent_id, project["id"]),
    )
    buckets: dict[str, list[dict]] = {"segment": [], "short": [], "hook": []}
    raw_buckets: dict[str, list[dict]] = {"segment": [], "short": [], "hook": []}
    archived_count = 0
    for row in rows:
        raw = dict(row)
        is_archived = bool(raw.get("archived"))
        if is_archived:
            archived_count += 1
            # Hidden from the page unless the user opts to view archived clips.
            if not include_archived:
                continue
        item = _video_public(dict(row))
        # Per-card schedule-readiness, computed from the RAW row (which still
        # carries thumbnail_path — _video_public pops it). This is the SAME
        # is_ready_for_schedule the per-tier summary uses, so a card's "Ready"
        # bar can never disagree with the parent dashboard's "all ready". An
        # imported clip's auto_action_state is NULL, so the card cannot infer
        # readiness from the chain state alone.
        item_ready, item_missing = is_ready_for_schedule(raw)
        item["ready"] = item_ready
        item["missing"] = item_missing
        bucket = item.get("item_type") or item.get("tier") or "short"
        buckets.setdefault(bucket, []).append(item)
        # Summary + readiness reflect ACTIVE clips only, even when archived
        # ones are shown, so an archived dup doesn't inflate the "ready" count.
        if not is_archived:
            raw_buckets.setdefault(bucket, []).append(raw)
    summary = {k: len(v) for k, v in raw_buckets.items() if k in {"segment", "short", "hook"}}
    readiness = {
        tier: _tier_readiness(raw_buckets.get(tier, []))
        for tier in ("segment", "short", "hook")
    }
    # In-flight promo-chain jobs (e.g. just-inserted Generate clips still
    # cutting / uploading / transcribing) so the page can render live
    # placeholder cards before a DB row exists. Survives reloads / new tabs.
    pending_jobs = auto_actions.inflight_promo_jobs(parent_id, int(project["id"]))
    return {
        "summary": summary,
        "children": buckets,
        "readiness": readiness,
        "pending_jobs": pending_jobs,
        "archived_count": archived_count,
    }


@router.post("/{video_id}/archive")
async def archive_promo(slug: str, parent_id: str, video_id: str) -> dict:
    """Archive a promo clip: hide it from the page without deleting it. The
    videos row and the YouTube video both remain; it can be restored via
    :func:`unarchive_promo`."""
    project, _parent = await _ensure_primary(slug, parent_id)
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id FROM videos WHERE id = ? AND parent_item_id = ? AND project_id = ?",
        (video_id, parent_id, project["id"]),
    )
    if not rows:
        raise HTTPException(404, "Promo clip not found under this parent")
    async with write_transaction() as db:
        await db.execute(
            "UPDATE videos SET archived = 1, archived_at = datetime('now'), "
            "updated_at = datetime('now') WHERE id = ?",
            (video_id,),
        )
    return {"archived": True, "video_id": video_id}


@router.post("/{video_id}/unarchive")
async def unarchive_promo(slug: str, parent_id: str, video_id: str) -> dict:
    """Restore an archived promo clip back onto the page."""
    project, _parent = await _ensure_primary(slug, parent_id)
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id FROM videos WHERE id = ? AND parent_item_id = ? AND project_id = ?",
        (video_id, parent_id, project["id"]),
    )
    if not rows:
        raise HTTPException(404, "Promo clip not found under this parent")
    async with write_transaction() as db:
        await db.execute(
            "UPDATE videos SET archived = 0, archived_at = NULL, "
            "updated_at = datetime('now') WHERE id = ?",
            (video_id,),
        )
    return {"archived": False, "video_id": video_id}


@router.post("/upload")
async def upload_promos(
    slug: str,
    parent_id: str,
    files: list[UploadFile] = File(...),
    item_type: str = Form(""),
) -> dict:
    """Queue one or more files into the promo auto-action chain. Returns
    ``{"jobs": [{"job_id": "...", "filename": "..."}, ...]}`` immediately;
    the chain runs in the background, polled via /upload-jobs/{job_id}.
    """
    project, _parent = await _ensure_primary(slug, parent_id)
    forced = (item_type or "").strip().lower() or None
    if forced is not None and forced not in _VALID_FORCED_ITEM_TYPES:
        raise HTTPException(
            400,
            f"item_type must be one of {sorted(_VALID_FORCED_ITEM_TYPES)}; "
            f"got {item_type!r}",
        )

    if not files:
        raise HTTPException(400, "At least one file is required")

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    jobs_out: list[dict] = []

    def _copy_to_disk(src, target) -> None:
        # shutil.copyfileobj is sync I/O; running it directly on the
        # event loop stalls every other coroutine (timing middleware,
        # other API polls, the promo activity refresh) for the duration
        # of the copy — easily seconds for a multi-GB clip. The thread
        # offload keeps the loop responsive.
        with open(target, "wb") as f:
            shutil.copyfileobj(src, f)

    for upload in files:
        if not upload.filename:
            continue
        # The on-disk name is fully app-chosen — never the client
        # filename (path separators, excessive length, or two uploads
        # of the same name clobbering each other on disk). The ORIGINAL
        # filename still flows to start_promo_upload for the
        # title-from-filename seed and is persisted on the row as
        # video_file_original_name.
        disk_name = f"promo_{secrets.token_hex(8)}{safe_upload_ext(upload.filename)}"
        target = UPLOAD_DIR / disk_name
        await asyncio.to_thread(_copy_to_disk, upload.file, target)
        job_id = await auto_actions.start_promo_upload(
            local_path=target,
            original_filename=upload.filename,
            parent_id=parent_id,
            project_id=int(project["id"]),
            forced_item_type=forced,
        )
        jobs_out.append({"job_id": job_id, "filename": upload.filename})

    if not jobs_out:
        raise HTTPException(400, "No usable files in upload")
    return {"jobs": jobs_out}


@router.get("/upload-jobs/{job_id}")
async def get_upload_job(slug: str, parent_id: str, job_id: str) -> dict:
    """Poll a single upload job. Returns the job dict (filename, state,
    last_error, optional video_id) — or 404 once the job has completed
    and been cleared from the in-memory dict (the UI should switch to
    polling /api/videos/{video_id}/auto-actions once ``video_id`` is set,
    so the 404 is a "you're polling the wrong endpoint now" signal).
    """
    # slug / parent_id are part of the URL for API hygiene + future
    # access control; the upload-job lookup itself is by job_id alone.
    job = auto_actions.get_upload_job(job_id)
    if job is None:
        raise HTTPException(404, "Upload job not found or already completed")
    return job


async def _resolve_batch_delays(
    project_id: int, raw_delays: object
) -> tuple[dict, dict | None]:
    """Turn an optional client-supplied promo-delays payload into the
    timedelta shape the batch math consumes.

    Returns ``(timedeltas, validated)``: ``timedeltas`` always feeds the
    batch math; ``validated`` is the normalized ``{value, unit}`` payload
    to persist as the project default, or ``None`` when the caller
    supplied no delays (nothing new to persist).

    ``None`` raw_delays → the project's saved delays; otherwise validate
    the payload (400 on bad input)."""
    from yt_scheduler.services import project_settings, scheduler as _scheduler

    if raw_delays is None:
        stored = await project_settings.get_promo_delays(project_id)
        return _scheduler._promo_delays_to_timedeltas(stored), None
    try:
        validated = project_settings.validate_promo_delays(raw_delays)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return _scheduler._promo_delays_to_timedeltas(validated), validated


async def _sweep_missing_thumbnails(parent_id: str, project_id: int) -> None:
    """Pull YouTube's auto-thumbnail for any eligible promo child that
    doesn't have one yet. Run before computing the schedule preview so
    "Not ready · thumbnail" cases self-heal when the user opens the
    modal, rather than waiting for the 30-min sweep.

    Each child is fetched in parallel — opening the modal shouldn't be
    gated on N sequential YouTube HTTP calls. Failures are logged and
    ignored; the chain just keeps showing as not ready, which the user
    can then act on.
    """
    from yt_scheduler.services import thumbnail_sync

    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id FROM videos "
        "WHERE parent_item_id = ? AND project_id = ? "
        "AND thumbnail_path IS NULL AND LENGTH(id) = 11 "
        "AND COALESCE(youtube_deleted, 0) = 0 "
        "AND COALESCE(LOWER(status), '') != 'published'",
        (parent_id, project_id),
    )
    if not rows:
        return

    async def _try(video_id: str) -> None:
        try:
            await thumbnail_sync.backfill_thumbnail(video_id)
        except Exception as exc:
            logger.warning(
                "Pre-schedule thumbnail backfill failed for %s: %s",
                video_id, exc,
            )

    await asyncio.gather(*(_try(row["id"]) for row in rows))


@router.post("/generate/preview")
async def generate_preview(
    slug: str,
    parent_id: str,
    payload: dict | None = None,
) -> dict:
    """Kick off a Generate-from-source preview job.

    Body:
        ``{"kinds": ["hook", "short", "segment"],
           "crop_vertical": {"hook": true, "short": true, "segment": false}}``

    Returns ``{"job_id": "..."}`` immediately. The client polls
    ``GET /generate/jobs/{job_id}`` until ``state == "done"`` (proposals
    ready) or ``"failed"`` (with ``last_error``).

    Pre-flight refuses (400):

    * Parent has no local video file (the cut step needs the bytes).
    * Parent's duration is unknown or exceeds the 4-hour cap.
    * No kinds requested.
    * None of the requested kinds are eligible at this parent length.
    * Parent has a transcript that lacks SRT timestamps AND
      ``transcript_source == 'user_edited'`` — the user pasted plain
      prose; we refuse rather than silently re-transcribe over their
      edits. Other no-transcript cases fall through and the job will
      transcribe inline as its first state.
    """
    from yt_scheduler.services import transcripts as transcript_service

    project, parent = await _ensure_primary(slug, parent_id)
    payload = payload or {}
    raw_kinds = payload.get("kinds") or []
    requested_kinds = [k for k in raw_kinds if k in _VALID_FORCED_ITEM_TYPES]
    if not requested_kinds:
        raise HTTPException(
            400, "Request must include at least one kind (hook|short|segment).",
        )
    raw_crop = payload.get("crop_vertical") or {}
    crop_vertical = {
        k: bool(raw_crop.get(k, k in ("hook", "short")))
        for k in requested_kinds
    }
    raw_max = payload.get("max_per_kind") or {}
    max_per_kind: dict[str, int] = {}
    for k in requested_kinds:
        raw_val = raw_max.get(k, clipper.DEFAULT_MAX_PROPOSALS_PER_KIND)
        try:
            n = int(raw_val)
        except (TypeError, ValueError):
            raise HTTPException(
                400,
                f"max_per_kind.{k} must be an integer between 1 and "
                f"{clipper.MAX_PROPOSALS_PER_KIND_CAP}.",
            )
        if n < 1 or n > clipper.MAX_PROPOSALS_PER_KIND_CAP:
            raise HTTPException(
                400,
                f"max_per_kind.{k} = {n} out of range "
                f"[1, {clipper.MAX_PROPOSALS_PER_KIND_CAP}].",
            )
        max_per_kind[k] = n

    parent_duration = float(parent.get("duration_seconds") or 0.0)
    if parent_duration <= 0:
        raise HTTPException(
            400,
            "Parent video has no known duration — cannot generate clips. "
            "Probe the local file first.",
        )
    if parent_duration > _GENERATE_MAX_PARENT_SECONDS:
        raise HTTPException(
            400,
            f"Parent is longer than {int(_GENERATE_MAX_PARENT_SECONDS / 3600)} "
            "hours — too large for Generate-from-source. Cut a shorter "
            "section first.",
        )

    eligible_kinds = [
        k for k in requested_kinds
        if clipper.is_parent_eligible_for_kind(parent_duration, k)
    ]
    if not eligible_kinds:
        raise HTTPException(
            400,
            "None of the requested kinds fit this parent's duration "
            "(each kind requires at least kind_max + 15 s of parent length).",
        )

    parent_path = _resolve_video_file(parent.get("video_file_path"))
    if parent_path is None or not parent_path.exists():
        raise HTTPException(
            400,
            "Parent has no local video file. Attach a source on the parent's "
            "detail page first.",
        )

    # User-edited transcript without timestamps → refuse. Other missing-
    # transcript cases will auto-transcribe inside the job.
    transcript = parent.get("transcript") or ""
    transcript_source = parent.get("transcript_source") or ""
    if (
        transcript_source == "user_edited"
        and transcript
        and not transcript_service.has_timestamps(transcript)
    ):
        raise HTTPException(
            400,
            "Active transcript was hand-edited and has no timestamps. "
            "Switch to a source-backed transcript in the chooser, or "
            "re-transcribe, then try again.",
        )

    existing_ranges, existing_titles = await _existing_promo_dedup_info(
        parent_id, int(project["id"]),
    )

    eligible_max_per_kind = {k: max_per_kind[k] for k in eligible_kinds}
    job_id = await clipper.start_generate_job(
        parent_id=parent_id,
        project_id=int(project["id"]),
        parent_video_path=str(parent_path),
        parent_title=parent.get("title") or "",
        parent_duration_seconds=parent_duration,
        kinds=eligible_kinds,
        crop_vertical_for_kind=crop_vertical,
        existing_ranges_per_kind=existing_ranges,
        max_per_kind=eligible_max_per_kind,
        existing_titles_per_kind=existing_titles,
    )

    # Source-quality warnings on the parent are useful to surface here
    # too — the modal renders them above the proposals. Computed from
    # the parent's last-probed width/height; null when we haven't probed.
    parent_probe = await asyncio.to_thread(
        media_service.probe_video_file, parent_path,
    )
    warnings = media_service.source_quality_warnings(
        width=parent_probe.width if parent_probe else None,
        height=parent_probe.height if parent_probe else None,
        source_origin=parent.get("source_file_origin"),
    )

    # Inline-preview metadata: the client uses these to decide whether
    # to render a <video src="/uploads/..."> tag or a YouTube iframe with
    # start/end. ``parent_youtube_id`` is the 11-char id when the parent
    # is YouTube-backed (the normal case for the promos page); falsy
    # otherwise.
    public_parent = _video_public(dict(parent))
    parent_youtube_id = parent_id if len(parent_id) == 11 else ""
    browser_playable = media_service.is_browser_playable(
        parent_probe.codec_name if parent_probe else None,
        parent_probe.container if parent_probe else None,
    )

    return {
        "job_id": job_id,
        "eligible_kinds": eligible_kinds,
        "ineligible_kinds": [k for k in requested_kinds if k not in eligible_kinds],
        "parent_warnings": warnings,
        "parent_video_file_url": public_parent.get("video_file_url"),
        "parent_browser_playable": browser_playable,
        "parent_youtube_id": parent_youtube_id,
    }


@router.get("/generate/jobs/{job_id}")
async def generate_job_status(slug: str, parent_id: str, job_id: str) -> dict:
    """Poll a generate preview job. Returns the latest state + proposals
    when done."""
    job = clipper.get_generate_job(job_id)
    if job is None:
        raise HTTPException(404, "Generate job not found or already discarded")
    return job


@router.post("/generate/confirm")
async def generate_confirm(
    slug: str,
    parent_id: str,
    payload: dict | None = None,
) -> dict:
    """Cut + insert the user-accepted clips.

    Body:
        ``{"accepted": [{"kind": "hook", "start_seconds": 12.3,
                         "end_seconds": 28.5, "title": "..."}]}``

    Returns ``{"jobs": [{"job_id", "kind", "title"}]}`` immediately;
    each new job's state starts at ``cutting`` and the UI polls the
    existing ``/upload-jobs/{job_id}`` endpoint to follow it through
    the regular promo chain.
    """
    project, parent = await _ensure_primary(slug, parent_id)
    payload = payload or {}
    accepted = payload.get("accepted") or []
    if not isinstance(accepted, list) or not accepted:
        raise HTTPException(400, "Request must include an 'accepted' array.")

    parent_path = _resolve_video_file(parent.get("video_file_path"))
    if parent_path is None or not parent_path.exists():
        raise HTTPException(
            400, "Parent has no local video file to cut from.",
        )
    parent_duration = float(parent.get("duration_seconds") or 0.0)

    # Optional cross-check against the generate job's preview-time crop
    # selection. When present, the per-entry vertical_crop is clamped to
    # what the preview said for that kind — a hand-crafted client body
    # can't sneak crop=true on a kind whose preview toggle was off (and
    # therefore had no vision pass). Missing job_id is allowed (a
    # legitimate non-Generate caller wouldn't have one) but logs a
    # debug breadcrumb so the legitimate-path-without-job-id case is
    # observable.
    #
    # Also build an idx lookup so we can hand the existing preview cut
    # straight to the promo chain (the preview IS the final cut now —
    # same params, same output, no re-cut needed). Match on
    # (kind, start_seconds, end_seconds): those round-trip JSON
    # losslessly because they came from the server-side ProposedClip.
    job_crop_snapshot: dict[str, bool] | None = None
    preview_idx_by_range: dict[tuple[str, float, float], int] = {}
    job_id_in = payload.get("job_id")
    if job_id_in:
        job = clipper.get_generate_job(str(job_id_in))
        if job is None:
            raise HTTPException(
                400,
                "Generate job_id supplied but is unknown or expired. "
                "Re-run /generate/preview before confirming.",
            )
        raw = job.get("crop_vertical") or {}
        if isinstance(raw, dict):
            job_crop_snapshot = {
                str(k): bool(v) for k, v in raw.items()
            }
        proposals_by_kind = job.get("proposals") or {}
        if isinstance(proposals_by_kind, dict):
            for k, lst in proposals_by_kind.items():
                if not isinstance(lst, list):
                    continue
                for i, p in enumerate(lst):
                    if not isinstance(p, dict):
                        continue
                    try:
                        s = float(p.get("start_seconds"))
                        e = float(p.get("end_seconds"))
                    except (TypeError, ValueError):
                        continue
                    preview_idx_by_range[(str(k), s, e)] = i

    jobs_out: list[dict] = []
    for entry in accepted:
        if not isinstance(entry, dict):
            continue
        kind = entry.get("kind")
        if kind not in _VALID_FORCED_ITEM_TYPES:
            continue
        try:
            start = float(entry["start_seconds"])
            end = float(entry["end_seconds"])
        except (KeyError, TypeError, ValueError):
            continue
        # Defense against non-finite values (NaN / inf): they compare
        # False to every numeric guard below and would crash the
        # ffmpeg-timestamp formatter in the cut step.
        if not (math.isfinite(start) and math.isfinite(end)):
            continue
        title = str(entry.get("title") or "").strip()
        if not title or end <= start:
            continue
        # Defensive bounds check — the preview already validated, but
        # nothing stops a client from POSTing a hand-crafted body.
        if start < 0 or end > parent_duration + 0.5:
            continue

        # Optional per-clip crop fields from the client. Both default to
        # off / no-shift for backwards compatibility with the 3b API.
        vertical_crop = bool(entry.get("vertical_crop", False))
        try:
            x_shift = float(entry.get("x_shift_normalized") or 0.0)
        except (TypeError, ValueError):
            x_shift = 0.0
        # Clamp to the same range extract_clip clamps to so logs and
        # event records match what actually runs.
        x_shift = max(-1.0, min(1.0, x_shift))
        # Audio edge ramps from the word-stream proposal path (0 on the legacy
        # anchor path). Echoed back by the client; used only by the fallback
        # re-cut — when a preview cut is adopted it already carries the fades.
        try:
            audio_fade_in = max(0.0, float(entry.get("audio_fade_in") or 0.0))
            audio_fade_out = max(0.0, float(entry.get("audio_fade_out") or 0.0))
        except (TypeError, ValueError):
            audio_fade_in = audio_fade_out = 0.0

        # Cross-check against the preview snapshot when we have one. A
        # client-supplied vertical_crop=true on a kind whose preview
        # toggle was off gets forced back to false (and any shift
        # zeroed), because the vision pass that informs a meaningful
        # x_shift never ran for those proposals.
        if job_crop_snapshot is not None:
            preview_crop = job_crop_snapshot.get(kind, False)
            if vertical_crop and not preview_crop:
                logger.info(
                    "Generate confirm: overriding client-supplied "
                    "vertical_crop=true on %s — preview snapshot had "
                    "crop off for this kind.", kind,
                )
                vertical_crop = False
                x_shift = 0.0

        # If the preview cut already exists for this proposal, rename
        # it to a fresh non-preview filename and hand it to the chain
        # so step 0 (the cut) is skipped — the preview file already
        # is the final cut (same params, same encoder, same crop).
        # When the preview is absent (cut failed / job evicted / a
        # non-Generate caller hand-crafted the body) the chain falls
        # back to running ffmpeg itself.
        existing_cut: Path | None = None
        if job_id_in:
            idx = preview_idx_by_range.get((kind, start, end))
            if idx is not None:
                candidate = (
                    UPLOAD_DIR
                    / clipper._preview_filename(str(job_id_in), kind, idx)
                )
                if candidate.exists():
                    # Move to a fresh name so the trailing
                    # cleanup_generate_previews() (rejected-files
                    # sweep) doesn't catch the file we just adopted.
                    final_name = f"clip_{kind}_{secrets.token_hex(6)}.mp4"
                    final_path = UPLOAD_DIR / final_name
                    try:
                        candidate.rename(final_path)
                        existing_cut = final_path
                    except OSError as exc:
                        logger.warning(
                            "Could not adopt preview cut %s: %s; "
                            "chain will re-cut.", candidate, exc,
                        )

        job_id = await auto_actions.start_promo_from_cut(
            parent_video_path=Path(parent_path),
            parent_id=parent_id,
            project_id=int(project["id"]),
            item_type=kind,
            title=title,
            cut_start_seconds=start,
            cut_end_seconds=end,
            vertical_crop=vertical_crop,
            x_shift_normalized=x_shift,
            audio_fade_in=audio_fade_in,
            audio_fade_out=audio_fade_out,
            existing_cut_path=existing_cut,
        )
        jobs_out.append({"job_id": job_id, "kind": kind, "title": title})

    if not jobs_out:
        raise HTTPException(400, "No usable entries in 'accepted'.")

    # Persist any explicitly-rejected proposals so the next visit to
    # the review page can show them in the "Previously dismissed"
    # section. Best-effort: a malformed rejection entry is dropped by
    # store_rejections, not surfaced as an error — the user clicked
    # Cut & insert, they care about the accepts going through.
    rejected_in = payload.get("rejected") or []
    if isinstance(rejected_in, list) and rejected_in:
        try:
            await clipper.store_rejections(
                parent_id=parent_id,
                project_id=int(project["id"]),
                rejected=rejected_in,
            )
        except Exception as exc:
            logger.warning(
                "Persisting Generate rejections for parent %s failed: %r",
                parent_id, exc,
            )

    # Preview files were only useful for the review screen; the final
    # cuts go through the regular promo chain which writes its own
    # files. Drop the previews so they don't linger in UPLOAD_DIR.
    if job_id_in:
        clipper.cleanup_generate_previews(str(job_id_in))

    return {"jobs": jobs_out}


@router.get("/generate/rejections")
async def list_generate_rejections(slug: str, parent_id: str) -> dict:
    """List proposals the user has previously dismissed for this parent.

    Read straight off ``generate_rejections`` — these survive process
    restart, unlike the in-memory ``_GENERATE_JOBS``. The review page
    renders them in the "Previously dismissed" section with a Restore
    button per entry; Restore calls ``DELETE
    /generate/rejections/{id}`` to drop the row.
    """
    project, _parent = await _ensure_primary(slug, parent_id)
    rows = await clipper.list_rejections(
        parent_id=parent_id, project_id=int(project["id"]),
    )
    return {"rejections": rows}


@router.post("/generate/rejections")
async def add_generate_rejections(
    slug: str, parent_id: str, payload: dict | None = None,
) -> dict:
    """Persist one or more dismissed proposals immediately.

    Unlike the rejection-recording side-effect of ``/generate/confirm`` (which
    only runs when the user accepts at least one clip), this records a dismissal
    the moment the user dismisses it — so it's remembered regardless of whether
    anything is ever accepted. Body: ``{"rejected": [<proposal dict>, ...]}``.
    """
    project, _parent = await _ensure_primary(slug, parent_id)
    payload = payload or {}
    rejected = payload.get("rejected")
    if not isinstance(rejected, list) or not rejected:
        raise HTTPException(400, "Body must include a non-empty 'rejected' list.")
    written = await clipper.store_rejections(
        parent_id=parent_id, project_id=int(project["id"]), rejected=rejected,
    )
    return {"stored": written}


@router.delete("/generate/rejections/{rejection_id}")
async def delete_generate_rejection(
    slug: str, parent_id: str, rejection_id: int,
) -> dict:
    """Restore = drop the rejection row. The user can then re-include
    it in their next Cut & insert.

    Returns ``{"deleted": bool}``: ``true`` when this call deleted the
    row, ``false`` when the row was already gone OR didn't belong to
    this parent (the client can treat both as 'the row isn't there
    anymore, refresh the list').

    Ownership is baked into the WHERE clause so the query is atomic —
    a slug-confused / cross-tab DELETE never touches a row it doesn't
    own, and there's no SELECT-then-DELETE window for the row to
    disappear between checks.
    """
    project, _parent = await _ensure_primary(slug, parent_id)
    async with write_transaction() as db:
        cursor = await db.execute(
            "DELETE FROM generate_rejections "
            "WHERE id = ? AND parent_id = ? AND project_id = ?",
            (rejection_id, parent_id, int(project["id"])),
        )
    return {"deleted": bool(cursor.rowcount)}


@router.post("/schedule-all/preview")
async def schedule_all_preview(
    slug: str,
    parent_id: str,
    payload: dict | None = None,
) -> dict:
    """Dry-run for the review modal. Returns the parent row, per-child
    rows grouped by tier with projected publish times and readiness
    chips, total batch span, and warnings.

    Body (all optional): ``parent_publish_at`` (ISO), ``delays``
    (per-tier overrides edited in the modal), ``order`` (explicit
    video-id sequence from drag-reordering)."""
    from yt_scheduler.services import scheduler as _scheduler

    project, _parent = await _ensure_primary(slug, parent_id)
    payload = payload or {}
    parent_dt = _scheduler._parse_iso_datetime(payload.get("parent_publish_at"))
    delays, _validated = await _resolve_batch_delays(
        project["id"], payload.get("delays")
    )
    order = payload.get("order") or None
    # Backfill missing thumbnails BEFORE computing readiness so a freshly
    # uploaded promo doesn't fail readiness purely because the periodic
    # sweep hasn't run yet.
    await _sweep_missing_thumbnails(parent_id, int(project["id"]))
    try:
        preview = await _scheduler.compute_promo_batch_preview(
            parent_id, parent_publish_at=parent_dt, delays=delays, order=order,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return preview


@router.post("/schedule-all")
async def schedule_all(
    slug: str,
    parent_id: str,
    payload: dict | None = None,
) -> dict:
    """Commit the batch from the preview modal. Body (all optional):

    ```
    {"parent_publish_at": "2026-04-01T17:30:00Z",
     "delays": {<per-tier {value,unit} overrides>},
     "order": ["<video_id>", ...]}
    ```

    When the parent already has a publish_at (or is published), omit
    ``parent_publish_at`` — the chain anchors against the existing time.
    A supplied ``delays`` is also saved as the project default so the
    next batch continues at the same pace.
    """
    from yt_scheduler.services import project_settings, scheduler as _scheduler

    project, _parent = await _ensure_primary(slug, parent_id)
    payload = payload or {}
    parent_dt = _scheduler._parse_iso_datetime(payload.get("parent_publish_at"))
    delays, validated_delays = await _resolve_batch_delays(
        project["id"], payload.get("delays")
    )
    order = payload.get("order") or None
    # Mirror the preview's pre-sweep so a child that arrived between
    # modal-open and Confirm doesn't fail readiness purely on thumbnail.
    # backfill_thumbnail is a no-op when one already exists, so paying
    # this once on commit is cheap.
    await _sweep_missing_thumbnails(parent_id, int(project["id"]))
    try:
        result = await _scheduler.schedule_promo_batch(
            parent_id, parent_publish_at=parent_dt, delays=delays, order=order,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    # Persist the (already-validated) delays so the next batch of the
    # same tier defaults to the same pace.
    if validated_delays is not None:
        await project_settings.set_json(
            project["id"], "promo_delays", validated_delays
        )
    return result


