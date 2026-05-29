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
import secrets
import shutil

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from yt_scheduler.config import UPLOAD_DIR, safe_upload_ext
from yt_scheduler.database import get_db
from yt_scheduler.routers.video_routes import _video_public
from yt_scheduler.services import auto_actions, projects as project_service

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/projects/{slug}/videos/{parent_id}/promos",
    tags=["promos"],
)


_VALID_FORCED_ITEM_TYPES = {"segment", "short", "hook"}


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
async def list_promos(slug: str, parent_id: str) -> dict:
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
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM videos WHERE parent_item_id = ? AND project_id = ? "
        "ORDER BY created_at ASC",
        (parent_id, project["id"]),
    )
    buckets: dict[str, list[dict]] = {"segment": [], "short": [], "hook": []}
    raw_buckets: dict[str, list[dict]] = {"segment": [], "short": [], "hook": []}
    for row in rows:
        raw = dict(row)
        item = _video_public(dict(row))
        bucket = item.get("item_type") or item.get("tier") or "short"
        buckets.setdefault(bucket, []).append(item)
        raw_buckets.setdefault(bucket, []).append(raw)
    summary = {k: len(v) for k, v in buckets.items() if k in {"segment", "short", "hook"}}
    readiness = {
        tier: _tier_readiness(raw_buckets.get(tier, []))
        for tier in ("segment", "short", "hook")
    }
    return {"summary": summary, "children": buckets, "readiness": readiness}


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


