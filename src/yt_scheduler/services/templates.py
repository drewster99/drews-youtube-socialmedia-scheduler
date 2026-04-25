"""Template engine with variable substitution and AI generation."""

from __future__ import annotations

import json
import re

import aiosqlite

from yt_scheduler.database import get_db
from yt_scheduler.services.ai import render_ai_blocks

# Default templates shipped with the app
DEFAULT_NEW_MESSAGE_TEMPLATE = {
    "name": "new_message",
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
    "name": "new_video",
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


def substitute_variables(text: str, variables: dict[str, str]) -> str:
    """Replace {{variable_name}} with values from the variables dict.

    Skips {{ai: ...}} blocks — those are handled separately.
    """

    def replace_var(match: re.Match) -> str:
        key = match.group(1).strip()
        # Don't touch ai blocks
        if key.startswith("ai:"):
            return match.group(0)
        return variables.get(key, match.group(0))

    return re.sub(r"\{\{(\w+)\}\}", replace_var, text)


def render_template(template_text: str, variables: dict[str, str]) -> str:
    """Render a template: first substitute variables, then process AI blocks.

    Variables inside AI blocks also get substituted before the AI sees them.
    """
    # First pass: substitute ALL variables (including those inside ai blocks)
    # We need a smarter regex that handles nested {{ }}
    result = re.sub(
        r"\{\{(?!ai:)(\w+)\}\}",
        lambda m: variables.get(m.group(1).strip(), m.group(0)),
        template_text,
    )

    # Also substitute variables inside ai blocks
    def sub_vars_in_ai(match: re.Match) -> str:
        ai_content = match.group(1)
        resolved = re.sub(
            r"\{\{(\w+)\}\}",
            lambda m: variables.get(m.group(1).strip(), m.group(0)),
            ai_content,
        )
        return "{{ai: " + resolved + "}}"

    result = re.sub(r"\{\{ai:\s*(.*?)\s*\}\}", sub_vars_in_ai, result, flags=re.DOTALL)

    # Second pass: process AI blocks
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


async def get_template(name: str, project_id: int = 1) -> dict | None:
    """Get a template by name within a project."""
    db = await get_db()
    row = await db.execute_fetchall(
        "SELECT * FROM templates WHERE project_id = ? AND name = ?",
        (project_id, name),
    )
    if not row:
        return None
    r = row[0]
    return {
        "id": r["id"],
        "name": r["name"],
        "description": r["description"],
        "platforms": json.loads(r["platforms"]),
        "applies_to": _decode_applies_to(r["applies_to"]),
        "is_builtin": bool(r["is_builtin"]),
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
    }


async def list_templates(project_id: int = 1) -> list[dict]:
    """List all templates within a project."""
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT * FROM templates WHERE project_id = ? ORDER BY name",
        (project_id,),
    )
    return [
        {
            "id": r["id"],
            "name": r["name"],
            "description": r["description"],
            "platforms": json.loads(r["platforms"]),
            "applies_to": _decode_applies_to(r["applies_to"]),
            "is_builtin": bool(r["is_builtin"]),
        }
        for r in rows
    ]


async def save_template(
    name: str,
    description: str,
    platforms: dict,
    project_id: int = 1,
    applies_to: list[str] | None = None,
) -> None:
    """Create or update a template within a project."""
    tiers = applies_to if applies_to is not None else _DEFAULT_APPLIES_TO
    if not tiers:
        raise ValueError("applies_to must include at least one tier")
    db = await get_db()
    await db.execute(
        """INSERT INTO templates (project_id, name, description, platforms, applies_to)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(project_id, name) DO UPDATE SET
            description = excluded.description,
            platforms = excluded.platforms,
            applies_to = excluded.applies_to,
            updated_at = datetime('now')""",
        (project_id, name, description, json.dumps(platforms), json.dumps(tiers)),
    )
    await db.commit()


async def delete_template(name: str, project_id: int = 1) -> None:
    """Delete a template within a project."""
    db = await get_db()
    await db.execute(
        "DELETE FROM templates WHERE project_id = ? AND name = ?",
        (project_id, name),
    )
    await db.commit()


async def ensure_default_template(project_id: int = 1) -> None:
    """Create the default templates within a project if they don't exist."""
    for tpl in (DEFAULT_TEMPLATE, DEFAULT_NEW_MESSAGE_TEMPLATE):
        existing = await get_template(tpl["name"], project_id=project_id)
        if not existing:
            await save_template(
                tpl["name"],
                tpl["description"],
                tpl["platforms"],
                project_id=project_id,
            )
