"""Template engine with variable substitution and AI generation."""

from __future__ import annotations

import json
import re

import aiosqlite

from youtube_publisher.database import get_db
from youtube_publisher.services.ai import render_ai_blocks

# Default template shipped with the app
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


async def get_template(name: str) -> dict | None:
    """Get a template by name from the database."""
    db = await get_db()
    row = await db.execute_fetchall(
        "SELECT * FROM templates WHERE name = ?", (name,)
    )
    if not row:
        return None
    r = row[0]
    return {
        "id": r["id"],
        "name": r["name"],
        "description": r["description"],
        "platforms": json.loads(r["platforms"]),
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
    }


async def list_templates() -> list[dict]:
    """List all templates."""
    db = await get_db()
    rows = await db.execute_fetchall("SELECT * FROM templates ORDER BY name")
    return [
        {
            "id": r["id"],
            "name": r["name"],
            "description": r["description"],
            "platforms": json.loads(r["platforms"]),
        }
        for r in rows
    ]


async def save_template(name: str, description: str, platforms: dict) -> None:
    """Create or update a template."""
    db = await get_db()
    await db.execute(
        """INSERT INTO templates (name, description, platforms)
        VALUES (?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            description = excluded.description,
            platforms = excluded.platforms,
            updated_at = datetime('now')""",
        (name, description, json.dumps(platforms)),
    )
    await db.commit()


async def delete_template(name: str) -> None:
    """Delete a template."""
    db = await get_db()
    await db.execute("DELETE FROM templates WHERE name = ?", (name,))
    await db.commit()


async def ensure_default_template() -> None:
    """Create the default template if it doesn't exist."""
    existing = await get_template("new_video")
    if not existing:
        await save_template(
            DEFAULT_TEMPLATE["name"],
            DEFAULT_TEMPLATE["description"],
            DEFAULT_TEMPLATE["platforms"],
        )
