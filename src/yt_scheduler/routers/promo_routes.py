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

import shutil

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from yt_scheduler.config import UPLOAD_DIR
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


@router.get("")
async def list_promos(slug: str, parent_id: str) -> dict:
    """Return per-tier counts and the children themselves.

    Response:
        {
            "summary": {"segment": int, "short": int, "hook": int},
            "children": {
                "segment": [<_video_public(...)>, ...],
                "short":   [...],
                "hook":    [...],
            },
        }

    ``children`` is keyed by ``item_type`` (not ``tier``) so a user-
    forced classification surfaces in the bucket the user picked.
    """
    project, _parent = await _ensure_primary(slug, parent_id)
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM videos WHERE parent_item_id = ? AND project_id = ? "
        "ORDER BY created_at ASC",
        (parent_id, project["id"]),
    )
    buckets: dict[str, list[dict]] = {"segment": [], "short": [], "hook": []}
    for row in rows:
        item = _video_public(dict(row))
        bucket = item.get("item_type") or item.get("tier") or "short"
        buckets.setdefault(bucket, []).append(item)
    summary = {k: len(v) for k, v in buckets.items() if k in {"segment", "short", "hook"}}
    return {"summary": summary, "children": buckets}


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
        # Drop the file to UPLOAD_DIR; YT-upload reads from there. Keep
        # the original filename so the title-from-filename prompt sees
        # the user's actual filename (not a server-generated stub).
        target = UPLOAD_DIR / upload.filename
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


