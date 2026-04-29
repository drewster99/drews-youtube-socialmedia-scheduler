"""Template management routes.

Templates are per-project. Every endpoint takes a ``?project_slug=`` query
parameter to resolve which project's template namespace to operate on. When
absent it defaults to the install's default project so legacy callers keep
working.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from yt_scheduler.services import projects as project_service
from yt_scheduler.services import templates as tmpl

router = APIRouter(prefix="/api/templates", tags=["templates"])


async def _resolve_project_id(project_slug: str | None) -> int:
    if not project_slug:
        return 1
    project = await project_service.get_project_by_slug(project_slug)
    if project is None:
        raise HTTPException(404, f"Project '{project_slug}' not found")
    return int(project["id"])


@router.get("")
async def list_templates(project_slug: str | None = None):
    """List all templates for the project."""
    project_id = await _resolve_project_id(project_slug)
    return await tmpl.list_templates(project_id=project_id)


@router.get("/{name}")
async def get_template(name: str, project_slug: str | None = None):
    """Get a template by name."""
    project_id = await _resolve_project_id(project_slug)
    template = await tmpl.get_template(name, project_id=project_id)
    if not template:
        raise HTTPException(404, f"Template '{name}' not found")
    return template


@router.post("")
async def create_template(data: dict, project_slug: str | None = None):
    """Create or update a template."""
    project_id = await _resolve_project_id(project_slug)
    name = data.get("name")
    if not name:
        raise HTTPException(400, "Template name is required")

    try:
        await tmpl.save_template(
            name=name,
            description=data.get("description", ""),
            platforms=data.get("platforms", {}),
            applies_to=data.get("applies_to"),
            project_id=project_id,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"status": "ok"}


@router.put("/{name}")
async def update_template(name: str, data: dict, project_slug: str | None = None):
    """Update an existing template."""
    project_id = await _resolve_project_id(project_slug)
    existing = await tmpl.get_template(name, project_id=project_id)
    if not existing:
        raise HTTPException(404, f"Template '{name}' not found")

    try:
        await tmpl.save_template(
            name=name,
            description=data.get("description", existing["description"]),
            platforms=data.get("platforms", existing["platforms"]),
            applies_to=data.get("applies_to", existing.get("applies_to")),
            project_id=project_id,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"status": "ok"}


@router.delete("/{name}")
async def delete_template(name: str, project_slug: str | None = None):
    """Delete a template."""
    project_id = await _resolve_project_id(project_slug)
    try:
        await tmpl.delete_template(name, project_id=project_id)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"status": "ok"}


# --- Slot CRUD --------------------------------------------------------------


async def _resolve_template_id(name: str, project_id: int) -> int:
    template = await tmpl.get_template(name, project_id=project_id)
    if not template:
        raise HTTPException(404, f"Template '{name}' not found")
    return int(template["id"])


@router.get("/{name}/slots")
async def list_template_slots(name: str, project_slug: str | None = None):
    project_id = await _resolve_project_id(project_slug)
    template_id = await _resolve_template_id(name, project_id)
    return await tmpl.list_slots(template_id)


@router.post("/{name}/slots")
async def add_template_slot(name: str, data: dict, project_slug: str | None = None):
    """Add a non-builtin slot. Body keys: ``platform`` (required), and
    optionally ``body``, ``media``, ``max_chars``, ``social_account_id``,
    ``is_disabled``, ``order_index``."""
    project_id = await _resolve_project_id(project_slug)
    template_id = await _resolve_template_id(name, project_id)
    platform = (data.get("platform") or "").strip()
    if not platform:
        raise HTTPException(400, "platform is required")
    sa_raw = data.get("social_account_id")
    sa_id: int | None
    if sa_raw is None or sa_raw == "":
        sa_id = None
    else:
        try:
            sa_id = int(sa_raw)
        except (TypeError, ValueError) as exc:
            raise HTTPException(400, "social_account_id must be an integer or null") from exc
    try:
        return await tmpl.add_slot(
            template_id,
            platform,
            body=str(data.get("body", "")),
            media=str(data.get("media", "thumbnail")),
            max_chars=int(data.get("max_chars", 500)),
            social_account_id=sa_id,
            is_disabled=bool(data.get("is_disabled", False)),
            order_index=(
                int(data["order_index"]) if "order_index" in data and data["order_index"] is not None else None
            ),
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.patch("/{name}/slots/{slot_id}")
async def update_template_slot(name: str, slot_id: int, data: dict, project_slug: str | None = None):
    project_id = await _resolve_project_id(project_slug)
    template_id = await _resolve_template_id(name, project_id)
    existing = await tmpl.get_slot(slot_id)
    if existing is None or existing["template_id"] != template_id:
        raise HTTPException(404, f"Slot {slot_id} not found in template '{name}'")

    kwargs: dict = {}
    for field in ("body", "media", "is_disabled", "order_index"):
        if field in data:
            kwargs[field] = data[field]
    if "max_chars" in data:
        kwargs["max_chars"] = data["max_chars"]
    if "social_account_id" in data:
        raw = data["social_account_id"]
        if raw is None or raw == "":
            kwargs["social_account_id"] = None
        else:
            try:
                kwargs["social_account_id"] = int(raw)
            except (TypeError, ValueError) as exc:
                raise HTTPException(400, "social_account_id must be an integer or null") from exc

    try:
        return await tmpl.update_slot(slot_id, **kwargs)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.delete("/{name}/slots/{slot_id}")
async def delete_template_slot(name: str, slot_id: int, project_slug: str | None = None):
    project_id = await _resolve_project_id(project_slug)
    template_id = await _resolve_template_id(name, project_id)
    existing = await tmpl.get_slot(slot_id)
    if existing is None or existing["template_id"] != template_id:
        raise HTTPException(404, f"Slot {slot_id} not found in template '{name}'")
    try:
        await tmpl.delete_slot(slot_id)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"status": "ok"}
