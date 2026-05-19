"""YouTube import endpoints (Phase 8)."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from yt_scheduler.services import imports

router = APIRouter(prefix="/api/projects/{slug}/imports", tags=["imports"])


@router.get("/available")
async def available(slug: str, max_results: int = 50) -> list[dict]:
    """List YouTube videos on the channel that aren't yet in our DB."""
    from yt_scheduler.services import projects as project_service

    project = await project_service.get_project_by_slug(slug)
    if project is None:
        raise HTTPException(404, f"Project '{slug}' not found")
    try:
        return await imports.list_available_imports(
            project_id=project["id"], max_results=max_results
        )
    except Exception as exc:
        raise HTTPException(500, f"Failed to list YouTube videos: {exc}") from exc


@router.post("/import")
async def do_import(slug: str, payload: dict) -> dict:
    """Import a specific YouTube video by id.

    Payload:
        video_id (required): the YouTube id to import.
        parent_item_id (optional): if set, the imported video lands as a
            promo child of that primary (hidden from Dashboard, shown on
            the parent's Promo Videos screen).
    """
    from yt_scheduler.services import projects as project_service

    project = await project_service.get_project_by_slug(slug)
    if project is None:
        raise HTTPException(404, f"Project '{slug}' not found")
    payload = payload or {}
    video_id = payload.get("video_id")
    if not video_id:
        raise HTTPException(400, "video_id is required")
    parent_item_id = (payload.get("parent_item_id") or "").strip() or None
    try:
        return await imports.import_video(
            video_id, project_id=project["id"], parent_item_id=parent_item_id,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(500, f"Import failed: {exc}") from exc
