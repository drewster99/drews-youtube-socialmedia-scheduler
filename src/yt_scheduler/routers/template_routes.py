"""Template management routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from yt_scheduler.services import templates as tmpl

router = APIRouter(prefix="/api/templates", tags=["templates"])


@router.get("")
async def list_templates():
    """List all templates."""
    return await tmpl.list_templates()


@router.get("/{name}")
async def get_template(name: str):
    """Get a template by name."""
    template = await tmpl.get_template(name)
    if not template:
        raise HTTPException(404, f"Template '{name}' not found")
    return template


@router.post("")
async def create_template(data: dict):
    """Create or update a template."""
    name = data.get("name")
    if not name:
        raise HTTPException(400, "Template name is required")

    try:
        await tmpl.save_template(
            name=name,
            description=data.get("description", ""),
            platforms=data.get("platforms", {}),
            applies_to=data.get("applies_to"),
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"status": "ok"}


@router.put("/{name}")
async def update_template(name: str, data: dict):
    """Update an existing template."""
    existing = await tmpl.get_template(name)
    if not existing:
        raise HTTPException(404, f"Template '{name}' not found")

    try:
        await tmpl.save_template(
            name=name,
            description=data.get("description", existing["description"]),
            platforms=data.get("platforms", existing["platforms"]),
            applies_to=data.get("applies_to", existing.get("applies_to")),
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"status": "ok"}


@router.delete("/{name}")
async def delete_template(name: str):
    """Delete a template."""
    try:
        await tmpl.delete_template(name)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"status": "ok"}


# --- Slot CRUD --------------------------------------------------------------


async def _resolve_template_id(name: str) -> int:
    template = await tmpl.get_template(name)
    if not template:
        raise HTTPException(404, f"Template '{name}' not found")
    return int(template["id"])


@router.get("/{name}/slots")
async def list_template_slots(name: str):
    template_id = await _resolve_template_id(name)
    return await tmpl.list_slots(template_id)


@router.post("/{name}/slots")
async def add_template_slot(name: str, data: dict):
    """Add a non-builtin slot. Body keys: ``platform`` (required), and
    optionally ``body``, ``media``, ``max_chars``, ``social_account_id``,
    ``is_disabled``, ``order_index``."""
    template_id = await _resolve_template_id(name)
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
async def update_template_slot(name: str, slot_id: int, data: dict):
    template_id = await _resolve_template_id(name)
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
async def delete_template_slot(name: str, slot_id: int):
    template_id = await _resolve_template_id(name)
    existing = await tmpl.get_slot(slot_id)
    if existing is None or existing["template_id"] != template_id:
        raise HTTPException(404, f"Slot {slot_id} not found in template '{name}'")
    try:
        await tmpl.delete_slot(slot_id)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"status": "ok"}


@router.post("/preview")
async def preview_template(data: dict):
    """Preview a rendered template with variables (without saving or posting)."""
    template_text = data.get("template", "")
    variables = data.get("variables", {})

    if not template_text:
        raise HTTPException(400, "Template text is required")

    try:
        rendered = tmpl.render_template(template_text, variables)
        return {"rendered": rendered}
    except Exception as e:
        return {"rendered": None, "error": str(e)}
