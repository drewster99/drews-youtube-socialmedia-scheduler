"""Project CRUD API and project-scoping helpers."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Path

from yt_scheduler.services import events as events_service
from yt_scheduler.services import project_settings as project_settings_service
from yt_scheduler.services import projects as project_service

router = APIRouter(prefix="/api/projects", tags=["projects"])


@router.get("/__recent-events", include_in_schema=False)
@router.get("/recent-events")
async def recent_events(limit: int = 7) -> list[dict]:
    """Newest activity log entries across all projects, for the Home page feed."""
    return await events_service.list_recent_events(limit=limit)


@router.get("/upcoming")
async def upcoming(limit: int = 7) -> list[dict]:
    """Upcoming scheduled publishes across all projects."""
    from yt_scheduler.database import get_db

    db = await get_db()
    rows = await db.execute_fetchall(
        """
        SELECT v.id AS video_id, v.title, v.publish_at, v.project_id,
               p.name AS project_name, p.slug AS project_slug
        FROM videos v
        JOIN projects p ON p.id = v.project_id
        WHERE v.publish_at IS NOT NULL AND v.status != 'published'
        ORDER BY v.publish_at ASC
        LIMIT ?
        """,
        (limit,),
    )
    return [dict(r) for r in rows]


@router.get("")
async def list_projects() -> list[dict]:
    return await project_service.list_projects()


@router.post("")
async def create_project(payload: dict) -> dict:
    name = payload.get("name", "").strip() if isinstance(payload, dict) else ""
    slug = payload.get("slug") if isinstance(payload, dict) else None
    try:
        return await project_service.create_project(name=name, slug=slug)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{slug}")
async def get_project(slug: str) -> dict:
    project = await project_service.get_project_by_slug(slug)
    if project is None:
        raise HTTPException(status_code=404, detail=f"Project '{slug}' not found")
    return project


@router.patch("/{slug}")
async def rename_project(slug: str, payload: dict) -> dict:
    project = await project_service.get_project_by_slug(slug)
    if project is None:
        raise HTTPException(status_code=404, detail=f"Project '{slug}' not found")
    name = payload.get("name", "").strip() if isinstance(payload, dict) else ""
    try:
        return await project_service.rename_project(project["id"], name=name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/{slug}")
async def delete_project(slug: str) -> dict:
    project = await project_service.get_project_by_slug(slug)
    if project is None:
        return {"status": "ok"}
    try:
        await project_service.delete_project(project["id"])
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "ok"}


# --- Per-project auto-action + posting settings ----------------------------


@router.get("/{slug}/auto-actions")
async def get_auto_actions(slug: str) -> dict:
    project = await project_service.get_project_by_slug(slug)
    if project is None:
        raise HTTPException(404, f"Project '{slug}' not found")
    return await project_settings_service.get_auto_actions(project["id"])


@router.put("/{slug}/auto-actions")
async def put_auto_actions(slug: str, payload: dict) -> dict:
    project = await project_service.get_project_by_slug(slug)
    if project is None:
        raise HTTPException(404, f"Project '{slug}' not found")
    upload = payload.get("upload") or {}
    import_ = payload.get("import") or {}
    if not isinstance(upload, dict) or not isinstance(import_, dict):
        raise HTTPException(400, "upload and import must be objects")
    await project_settings_service.set_json(project["id"], "auto_actions_upload", upload)
    await project_settings_service.set_json(project["id"], "auto_actions_import", import_)
    return await project_settings_service.get_auto_actions(project["id"])


@router.get("/{slug}/posting-settings")
async def get_posting_settings(slug: str) -> dict:
    project = await project_service.get_project_by_slug(slug)
    if project is None:
        raise HTTPException(404, f"Project '{slug}' not found")
    return await project_settings_service.get_posting_settings(project["id"])


@router.put("/{slug}/posting-settings")
async def put_posting_settings(slug: str, payload: dict) -> dict:
    project = await project_service.get_project_by_slug(slug)
    if project is None:
        raise HTTPException(404, f"Project '{slug}' not found")
    if not isinstance(payload, dict):
        raise HTTPException(400, "payload must be an object")
    await project_settings_service.set_json(project["id"], "posting", payload)
    return await project_settings_service.get_posting_settings(project["id"])


# --- Reusable dependency for project-scoped routes ---------------------------


async def require_project_by_slug(
    slug: str = Path(..., description="Project slug from the URL"),
) -> dict:
    """FastAPI dependency that resolves a project by slug or raises 404."""
    project = await project_service.get_project_by_slug(slug)
    if project is None:
        raise HTTPException(status_code=404, detail=f"Project '{slug}' not found")
    return project


CurrentProject = Depends(require_project_by_slug)
