"""Template management routes.

Templates are per-project. Every endpoint takes a ``?project_slug=`` query
parameter to resolve which project's template namespace to operate on. When
absent it defaults to the install's default project so legacy callers keep
working.
"""

from __future__ import annotations

import re

from fastapi import APIRouter, HTTPException

from yt_scheduler.services import projects as project_service
from yt_scheduler.services import templates as tmpl

router = APIRouter(prefix="/api/templates", tags=["templates"])

# Template names live in URL paths and get echoed into the edit page's
# inline JS, so keep them to a plain identifier shape.
_TEMPLATE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
_BAD_NAME_MSG = (
    "Template name may contain only letters, digits, '_' and '-' "
    "(and must start with a letter or digit)."
)


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
    name = (data.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "Template name is required")
    if not _TEMPLATE_NAME_RE.match(name):
        raise HTTPException(400, _BAD_NAME_MSG)

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


@router.post("/{name}/duplicate")
async def duplicate_template(name: str, data: dict, project_slug: str | None = None):
    """Create a new template as a deep copy of ``name``.

    Body: ``{"new_name": "..."}``. Returns the newly created template
    (same shape as ``GET /api/templates/{name}``).
    """
    project_id = await _resolve_project_id(project_slug)
    new_name = (data.get("new_name") or "").strip()
    if not new_name:
        raise HTTPException(400, "new_name is required")
    if not _TEMPLATE_NAME_RE.match(new_name):
        raise HTTPException(400, _BAD_NAME_MSG)
    try:
        return await tmpl.duplicate_template(name, new_name, project_id=project_id)
    except ValueError as exc:
        msg = str(exc)
        status = 404 if "not found" in msg.lower() else 409
        raise HTTPException(status, msg) from exc


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
    # The default char limit must match the platform — handing every slot
    # the same 500 fallback meant LinkedIn slots opened at 280 (because
    # the front-end was sending 280 in its add-slot payload) and
    # everything else at 280 too, never getting the platform-appropriate
    # value. Pull from the canonical table when the caller doesn't pin a
    # value.
    max_chars_value: int
    if "max_chars" in data and data["max_chars"] is not None:
        try:
            max_chars_value = int(data["max_chars"])
        except (TypeError, ValueError) as exc:
            raise HTTPException(400, "max_chars must be an integer") from exc
    else:
        max_chars_value = tmpl.default_max_chars(platform)
    try:
        return await tmpl.add_slot(
            template_id,
            platform,
            body=str(data.get("body", "")),
            media=str(data.get("media", "thumbnail")),
            max_chars=max_chars_value,
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
