"""Template engine with variable substitution and AI generation.

Templates are stored as a parent ``templates`` row plus one or more
``template_slots`` rows. Each slot binds a platform to a credential
(or, for built-in slots, defers to the project's default for that
platform). The ``platforms`` shape returned by :func:`get_template` is
a compatibility view assembled from the built-in slots so callers that
were written against the pre-slot API keep working.
"""

from __future__ import annotations

import json
import re

import aiosqlite

from yt_scheduler.database import get_db
from yt_scheduler.services.ai import render_ai_blocks
from yt_scheduler.services.social import ALL_PLATFORMS

# Default templates shipped with the app
DEFAULT_NEW_MESSAGE_TEMPLATE = {
    "name": "send_message",
    "description": "Plain user-message template — useful for one-off posts",
    "platforms": {
        "twitter":  {"template": "{{user_message}}", "media": "none", "max_chars": 280},
        "bluesky":  {"template": "{{user_message}}", "media": "none", "max_chars": 300},
        "mastodon": {"template": "{{user_message}}", "media": "none", "max_chars": 500},
        "linkedin": {"template": "{{user_message}}", "media": "none", "max_chars": 3000},
        "threads":  {"template": "{{user_message}}", "media": "none", "max_chars": 500},
    },
}


DEFAULT_TEMPLATE = {
    "name": "announce_video",
    "description": "Standard template for announcing a new video upload",
    "platforms": {
        "twitter": {
            "template": '{{ai: Write a punchy tweet announcing a YouTube video titled "{{title}}" about {{tags}}. Include the URL {{url}}. Under 280 chars. 2-3 hashtags.}}',
            "media": "thumbnail",
            "max_chars": 280,
        },
        "bluesky": {
            "template": '{{ai: Write a Bluesky post announcing my new video "{{title}}". Conversational tone, under 300 chars.}}\n\n{{url}}',
            "media": "thumbnail",
            "max_chars": 300,
        },
        "mastodon": {
            "template": 'New video is live!\n\n"{{title}}"\n\n{{url}}\n\n{{ai: Generate 3-5 CamelCase hashtags for: {{tags}}}}',
            "media": "thumbnail",
            "max_chars": 500,
        },
        "linkedin": {
            "template": '{{ai: Write a LinkedIn post (2-3 paragraphs, professional but approachable) about my new video "{{title}}". Description: {{description_short}}. End with a question.}}\n\nWatch here: {{url}}',
            "media": "thumbnail",
            "max_chars": 3000,
        },
        "threads": {
            "template": '{{ai: Write a casual Threads post announcing "{{title}}". Keep it engaging, under 500 chars.}}\n\n{{url}}',
            "media": "thumbnail",
            "max_chars": 500,
        },
    },
}


BUILTIN_TEMPLATE_NAMES = {"announce_video", "send_message"}


def substitute_variables(text: str, variables: dict[str, str]) -> str:
    """Replace {{variable_name}} with values from the variables dict.

    Skips {{ai: ...}} blocks — those are handled separately.
    """

    def replace_var(match: re.Match) -> str:
        key = match.group(1).strip()
        if key.startswith("ai:"):
            return match.group(0)
        return variables.get(key, match.group(0))

    return re.sub(r"\{\{(\w+)\}\}", replace_var, text)


def render_template(template_text: str, variables: dict[str, str]) -> str:
    """Render a template: first substitute variables, then process AI blocks.

    Variables inside AI blocks also get substituted before the AI sees them.
    """
    result = re.sub(
        r"\{\{(?!ai:)(\w+)\}\}",
        lambda m: variables.get(m.group(1).strip(), m.group(0)),
        template_text,
    )

    def sub_vars_in_ai(match: re.Match) -> str:
        ai_content = match.group(1)
        resolved = re.sub(
            r"\{\{(\w+)\}\}",
            lambda m: variables.get(m.group(1).strip(), m.group(0)),
            ai_content,
        )
        return "{{ai: " + resolved + "}}"

    result = re.sub(r"\{\{ai:\s*(.*?)\s*\}\}", sub_vars_in_ai, result, flags=re.DOTALL)
    result = render_ai_blocks(result)
    return result


_DEFAULT_APPLIES_TO = ["hook", "short", "segment", "video"]


def _decode_applies_to(raw: str | None) -> list[str]:
    if not raw:
        return list(_DEFAULT_APPLIES_TO)
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        return list(_DEFAULT_APPLIES_TO)
    if isinstance(decoded, list):
        return [str(t) for t in decoded if t in _DEFAULT_APPLIES_TO]
    return list(_DEFAULT_APPLIES_TO)


# --- Slot helpers ----------------------------------------------------------


def _slot_to_dict(row) -> dict:
    return {
        "id": int(row["id"]),
        "template_id": int(row["template_id"]),
        "platform": row["platform"],
        "social_account_id": (
            int(row["social_account_id"])
            if row["social_account_id"] is not None
            else None
        ),
        "is_builtin": bool(row["is_builtin"]),
        "is_disabled": bool(row["is_disabled"]),
        "order_index": int(row["order_index"]),
        "body": row["body"] or "",
        "media": row["media"] or "thumbnail",
        "max_chars": int(row["max_chars"] or 500),
    }


async def _list_slots(template_id: int) -> list[dict]:
    db = await get_db()
    cursor = await db.execute(
        "SELECT s.*, a.uuid AS account_uuid, a.username AS account_username, "
        "       a.platform AS account_platform, a.deleted_at AS account_deleted_at "
        "FROM template_slots s "
        "LEFT JOIN social_accounts a ON a.id = s.social_account_id "
        "WHERE s.template_id = ? "
        "ORDER BY s.order_index, s.id",
        (template_id,),
    )
    rows = await cursor.fetchall()
    out: list[dict] = []
    for row in rows:
        slot = _slot_to_dict(row)
        if row["account_uuid"] is not None:
            slot["resolved_account"] = {
                "uuid": row["account_uuid"],
                "username": row["account_username"],
                "platform": row["account_platform"],
                "deleted": row["account_deleted_at"] is not None,
            }
        else:
            slot["resolved_account"] = None
        out.append(slot)
    return out


def _platforms_view_from_slots(slots: list[dict]) -> dict:
    """Compatibility view: ``{platform: {template, media, max_chars}}`` from
    the built-in slots only. Used by callers that haven't been updated to
    speak the slot model."""
    view: dict[str, dict] = {}
    for slot in slots:
        if not slot["is_builtin"]:
            continue
        view[slot["platform"]] = {
            "template": slot["body"],
            "media": slot["media"],
            "max_chars": slot["max_chars"],
        }
    return view


# --- Public API ------------------------------------------------------------


async def get_template(name: str, project_id: int = 1) -> dict | None:
    """Get a template by name within a project."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT * FROM templates WHERE project_id = ? AND name = ?",
        (project_id, name),
    )
    row = await cursor.fetchone()
    if row is None:
        return None

    slots = await _list_slots(int(row["id"]))
    return {
        "id": int(row["id"]),
        "name": row["name"],
        "description": row["description"] or "",
        "applies_to": _decode_applies_to(row["applies_to"]),
        "is_builtin": bool(row["is_builtin"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "slots": slots,
        "platforms": _platforms_view_from_slots(slots),
    }


async def list_templates(project_id: int = 1) -> list[dict]:
    """List all templates within a project."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT * FROM templates WHERE project_id = ? ORDER BY name",
        (project_id,),
    )
    rows = await cursor.fetchall()
    out: list[dict] = []
    for row in rows:
        slots = await _list_slots(int(row["id"]))
        out.append({
            "id": int(row["id"]),
            "name": row["name"],
            "description": row["description"] or "",
            "applies_to": _decode_applies_to(row["applies_to"]),
            "is_builtin": bool(row["is_builtin"]),
            "platforms": _platforms_view_from_slots(slots),
            "slot_count": len(slots),
        })
    return out


async def _set_builtin_slot(
    template_id: int,
    platform: str,
    body: str,
    media: str,
    max_chars: int,
) -> None:
    """Upsert a single built-in slot. Built-in slots are unique per
    (template_id, platform)."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT id FROM template_slots "
        "WHERE template_id = ? AND platform = ? AND is_builtin = 1",
        (template_id, platform),
    )
    row = await cursor.fetchone()
    if row is not None:
        await db.execute(
            "UPDATE template_slots "
            "SET body = ?, media = ?, max_chars = ?, updated_at = datetime('now') "
            "WHERE id = ?",
            (body, media, int(max_chars), int(row["id"])),
        )
    else:
        await db.execute(
            "INSERT INTO template_slots "
            "(template_id, platform, social_account_id, is_builtin, is_disabled, "
            " order_index, body, media, max_chars) "
            "VALUES (?, ?, NULL, 1, 0, 0, ?, ?, ?)",
            (template_id, platform, body, media, int(max_chars)),
        )


async def save_template(
    name: str,
    description: str,
    platforms: dict,
    project_id: int = 1,
    applies_to: list[str] | None = None,
) -> dict:
    """Create or update a template within a project (compatibility-shape API).

    Each entry in ``platforms`` becomes a built-in slot (one per platform).
    Non-built-in slots are not touched here — those are managed via the
    slot CRUD endpoints in Phase D's UI.
    """
    tiers = applies_to if applies_to is not None else _DEFAULT_APPLIES_TO
    if not tiers:
        raise ValueError("applies_to must include at least one tier")

    db = await get_db()
    is_builtin_flag = 1 if name in BUILTIN_TEMPLATE_NAMES else 0
    try:
        await db.execute(
            "INSERT INTO templates (project_id, name, description, applies_to, is_builtin) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(project_id, name) DO UPDATE SET "
            "  description = excluded.description, "
            "  applies_to  = excluded.applies_to, "
            "  is_builtin  = excluded.is_builtin OR templates.is_builtin, "
            "  updated_at  = datetime('now')",
            (project_id, name, description, json.dumps(tiers), is_builtin_flag),
        )
    except aiosqlite.IntegrityError as exc:
        raise ValueError(str(exc)) from exc
    await db.commit()

    cursor = await db.execute(
        "SELECT id FROM templates WHERE project_id = ? AND name = ?",
        (project_id, name),
    )
    row = await cursor.fetchone()
    if row is None:
        raise RuntimeError("Failed to read back saved template row")
    template_id = int(row["id"])

    for platform_name, config in (platforms or {}).items():
        if platform_name not in ALL_PLATFORMS:
            continue
        await _set_builtin_slot(
            template_id,
            platform_name,
            body=str(config.get("template", "")),
            media=str(config.get("media", "thumbnail")),
            max_chars=int(config.get("max_chars", 500)),
        )
    await db.commit()

    saved = await get_template(name, project_id=project_id)
    if saved is None:
        raise RuntimeError("Saved template disappeared between write and read")
    return saved


async def delete_template(name: str, project_id: int = 1) -> None:
    """Delete a template within a project. Refuses to delete built-in
    templates by name."""
    if name in BUILTIN_TEMPLATE_NAMES:
        raise ValueError(f"Cannot delete built-in template '{name}'")
    db = await get_db()
    await db.execute(
        "DELETE FROM templates WHERE project_id = ? AND name = ?",
        (project_id, name),
    )
    await db.commit()


async def ensure_default_template(project_id: int = 1) -> None:
    """Create the two built-in templates within a project if they don't
    already exist."""
    for tpl in (DEFAULT_TEMPLATE, DEFAULT_NEW_MESSAGE_TEMPLATE):
        existing = await get_template(tpl["name"], project_id=project_id)
        if existing is not None:
            continue
        await save_template(
            tpl["name"],
            tpl["description"],
            tpl["platforms"],
            project_id=project_id,
        )
