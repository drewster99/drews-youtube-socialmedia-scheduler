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
    await tmpl.delete_template(name)
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
