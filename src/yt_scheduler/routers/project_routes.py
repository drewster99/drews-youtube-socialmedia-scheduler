"""Project CRUD API and project-scoping helpers."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends, HTTPException, Path

from yt_scheduler.services import events as events_service
from yt_scheduler.services import project_settings as project_settings_service
from yt_scheduler.services import projects as project_service
from yt_scheduler.services.auth import (
    get_credentials,
    is_authenticated,
)
from yt_scheduler.services.social import ALL_PLATFORMS
from yt_scheduler.services.social_credentials import (
    format_account_label,
    get_credential_by_id,
)

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


# --- Per-project social defaults --------------------------------------------


@router.get("/{slug}/social-defaults")
async def get_social_defaults(slug: str) -> dict:
    """Return the project's default credential per platform.

    Shape: ``{"twitter": {"social_account_id": 1, "username": "...", "label": "..."}, ...}``
    or ``{"twitter": null}`` when no default is set.
    """
    project = await project_service.get_project_by_slug(slug)
    if project is None:
        raise HTTPException(404, f"Project '{slug}' not found")

    from yt_scheduler.database import get_db

    db = await get_db()
    cursor = await db.execute(
        "SELECT platform, social_account_id FROM project_social_defaults "
        "WHERE project_id = ?",
        (project["id"],),
    )
    rows = await cursor.fetchall()
    by_platform: dict[str, dict | None] = {p: None for p in ALL_PLATFORMS}
    for row in rows:
        platform = row["platform"]
        sa_id = row["social_account_id"]
        if sa_id is None:
            by_platform[platform] = None
            continue
        cred = await get_credential_by_id(int(sa_id))
        if cred is None or cred.get("deleted_at") is not None:
            by_platform[platform] = None
            continue
        by_platform[platform] = {
            "social_account_id": cred["id"],
            "uuid": cred["uuid"],
            "username": cred["username"],
            "label": cred["label"],
        }
    return by_platform


@router.put("/{slug}/social-defaults/{platform}")
async def set_social_default(slug: str, platform: str, payload: dict) -> dict:
    """Set or clear the project's default credential for a platform.

    Body: ``{"social_account_id": int | null}``.
    """
    if platform not in ALL_PLATFORMS:
        raise HTTPException(400, f"Unknown platform: {platform}")
    project = await project_service.get_project_by_slug(slug)
    if project is None:
        raise HTTPException(404, f"Project '{slug}' not found")

    raw = payload.get("social_account_id")
    sa_id: int | None
    if raw is None or raw == "":
        sa_id = None
    else:
        try:
            sa_id = int(raw)
        except (TypeError, ValueError):
            raise HTTPException(400, "social_account_id must be an integer or null")

    if sa_id is not None:
        cred = await get_credential_by_id(sa_id)
        if cred is None or cred.get("deleted_at") is not None:
            raise HTTPException(404, "Credential not found")
        if cred["platform"] != platform:
            raise HTTPException(
                400,
                f"Credential platform '{cred['platform']}' does not match path '{platform}'",
            )

    from yt_scheduler.database import get_db

    db = await get_db()
    if sa_id is None:
        await db.execute(
            "DELETE FROM project_social_defaults "
            "WHERE project_id = ? AND platform = ?",
            (project["id"], platform),
        )
    else:
        await db.execute(
            "INSERT INTO project_social_defaults (project_id, platform, social_account_id) "
            "VALUES (?, ?, ?) "
            "ON CONFLICT(project_id, platform) DO UPDATE SET social_account_id = excluded.social_account_id",
            (project["id"], platform, sa_id),
        )
    await db.commit()
    return await get_social_defaults(slug)


# --- Per-project YouTube info ------------------------------------------------


@router.get("/{slug}/youtube")
async def get_project_youtube(slug: str) -> dict:
    """Return the YouTube channel currently bound to this project.

    Shape: ``{"channel_id": "...", "channel_title": "...", "channel_handle": "...",
                "label": "@title @YouTube", "needs_reauth": bool}``.
    Returns ``channel_id`` from the DB even when the credentials are
    revoked so the UI can still show 'connected to channel <X>'.
    """
    project = await project_service.get_project_by_slug(slug)
    if project is None:
        raise HTTPException(404, f"Project '{slug}' not found")

    channel_id = project.get("youtube_channel_id")
    authenticated = is_authenticated(project["slug"])
    needs_reauth = bool(channel_id) and not authenticated

    title = ""
    handle = ""
    if authenticated:
        try:
            creds = get_credentials(project["slug"])
            if creds is not None:
                from googleapiclient.discovery import build

                def _fetch_channel():
                    service = build("youtube", "v3", credentials=creds)
                    return service.channels().list(
                        part="snippet", mine=True
                    ).execute()

                result = await asyncio.to_thread(_fetch_channel)
                items = result.get("items") or []
                if items:
                    snippet = items[0].get("snippet", {})
                    title = snippet.get("title", "")
                    handle = snippet.get("customUrl", "") or ""
                    if not channel_id:
                        channel_id = items[0].get("id")
        except Exception:
            needs_reauth = True

    label_username = title or channel_id or "(none)"
    label = format_account_label("youtube", label_username) if channel_id else None

    return {
        "channel_id": channel_id,
        "channel_title": title,
        "channel_handle": handle,
        "label": label,
        "authenticated": authenticated,
        "needs_reauth": needs_reauth,
    }


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
