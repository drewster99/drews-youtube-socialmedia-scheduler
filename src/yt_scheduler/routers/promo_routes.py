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

import secrets
import shutil

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from yt_scheduler.config import UPLOAD_DIR, safe_upload_ext
from yt_scheduler.database import get_db
from yt_scheduler.routers.video_routes import _video_public
from yt_scheduler.services import auto_actions, projects as project_service

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
        with open(target, "wb") as f:
            shutil.copyfileobj(upload.file, f)
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


@router.get("/schedule-all/preview")
async def schedule_all_preview(
    slug: str,
    parent_id: str,
    parent_publish_at: str | None = None,
) -> dict:
    """Dry-run for the review modal. Returns parent row at the top,
    per-child rows grouped by tier with their projected publish times
    and readiness chips, total batch span, and any warnings (quota +
    parent-not-ready)."""
    from yt_scheduler.services import project_settings, scheduler as _scheduler
    project, _parent = await _ensure_primary(slug, parent_id)
    parent_dt = _scheduler._parse_iso_datetime(parent_publish_at)
    raw_delays = await project_settings.get_promo_delays(project["id"])
    delays = _scheduler._promo_delays_to_timedeltas(raw_delays)
    try:
        preview = await _scheduler.compute_promo_batch_preview(
            parent_id, parent_publish_at=parent_dt, delays=delays,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return preview


@router.post("/schedule-all")
async def schedule_all(
    slug: str,
    parent_id: str,
    payload: dict,
) -> dict:
    """Commit the batch from the preview modal. Body:

    ```
    {"parent_publish_at": "2026-04-01T17:30:00Z"}     # optional
    ```

    When the parent already has a publish_at (or status='published'),
    omit ``parent_publish_at`` — the chain anchors against the
    existing time.
    """
    from yt_scheduler.services import project_settings, scheduler as _scheduler
    project, _parent = await _ensure_primary(slug, parent_id)
    payload = payload or {}
    parent_dt = _scheduler._parse_iso_datetime(payload.get("parent_publish_at"))
    raw_delays = await project_settings.get_promo_delays(project["id"])
    delays = _scheduler._promo_delays_to_timedeltas(raw_delays)
    try:
        result = await _scheduler.schedule_promo_batch(
            parent_id, parent_publish_at=parent_dt, delays=delays,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return result


