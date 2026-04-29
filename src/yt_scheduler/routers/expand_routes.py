"""Generic text-expansion endpoint.

Single HTTP surface for the unified template renderer in
``services/templates.render``. Every server-side rendering path (social-
post generation, auto-actions, prompt-template bodies in
``services/ai``) goes through that same function — this endpoint just
exposes it directly so the UI (and any external caller) can render an
arbitrary template string with the same semantics.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from yt_scheduler.services import templates as tmpl

router = APIRouter(prefix="/api/expand_text", tags=["expand"])


@router.post("")
async def expand_text(data: dict) -> dict:
    """Render a template against variables, evaluating any ``{{ai: ...}}``
    blocks via Claude.

    Body shape::

        {
          "template": "<template text>",
          "variables": {"name": "value", ...},
          "default_system_prompt": "<optional system prompt for {{ai:}} blocks>",
          "model": "<optional model override>",
          "max_tokens": 512
        }

    Template syntax:

    - ``{{name}}`` — substitute. Missing keys stay literal so typos surface.
    - ``{{name!}}`` — required. Missing key → 400 with
      ``{"detail": {"missing_required": "<name>"}}``.
    - ``{{name??default text}}`` — fallback. Missing key renders the
      literal default text (no recursive substitution inside it). Empty
      default is allowed: ``{{name??}}``.
    - ``{{ai: prompt}}`` — evaluate against Claude using
      ``default_system_prompt`` (or the built-in social-copywriter default).
    - ``{{ai[system text]: prompt}}`` — per-block system override.
    - AI blocks may be nested. Inner blocks resolve first; their output is
      spliced into the parent's prompt before the parent is sent.
    """
    template_text = data.get("template", "")
    request_variables = data.get("variables", {}) or {}

    if not template_text:
        raise HTTPException(400, "Template text is required")

    # Merge in install-wide globals so {{signoff}} etc. resolve here just
    # like they do in the post-generation paths. The caller's `variables`
    # always win on key collision, since a one-off render is "self-level"
    # in the inheritance hierarchy. Project/parent/item levels need an
    # item context the caller doesn't have, so they're not available here.
    from yt_scheduler.database import get_db

    db = await get_db()
    global_rows = await db.execute_fetchall(
        "SELECT key, value FROM global_variables"
    )
    merged: dict[str, object] = {r["key"]: r["value"] for r in global_rows}
    merged.update(request_variables)

    kwargs: dict[str, object] = {}
    if "default_system_prompt" in data:
        kwargs["default_system_prompt"] = data["default_system_prompt"]
    if data.get("model"):
        kwargs["model"] = str(data["model"])
    if "max_tokens" in data:
        kwargs["max_tokens"] = int(data["max_tokens"])

    try:
        rendered = tmpl.render(template_text, merged, **kwargs)
        return {"rendered": rendered}
    except tmpl.MissingRequiredVariable as exc:
        raise HTTPException(400, {"missing_required": exc.name}) from exc
    except Exception as exc:
        return {"rendered": None, "error": str(exc)}
