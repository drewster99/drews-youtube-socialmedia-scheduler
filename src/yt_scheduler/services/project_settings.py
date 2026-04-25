"""Per-project key-value settings.

Used for: auto-action toggles per upload/import column, posting delays/spacings,
default template per tier, etc. Values are stored as TEXT and parsed by the
caller.
"""

from __future__ import annotations

import json
from typing import Any

from yt_scheduler.database import get_db

# Defaults expressed as Python objects; serialised to JSON when stored.
AUTO_ACTION_DEFAULTS_UPLOAD = {
    "auto_transcribe": True,
    "auto_transcribe_backend": None,
    "auto_transcribe_model": None,
    "auto_description": True,
    "auto_tags": False,
    "auto_tags_include_title": True,
    "auto_tags_include_description": True,
    "auto_tags_include_transcript": True,
    "auto_tags_mode": "replace",
    "auto_thumbnail": True,
    "auto_socials": {
        "twitter": False,
        "bluesky": False,
        "mastodon": False,
        "linkedin": False,
        "threads": False,
    },
}

AUTO_ACTION_DEFAULTS_IMPORT = {
    "auto_transcribe": True,
    "auto_transcribe_backend": None,
    "auto_transcribe_model": None,
    "auto_description": False,
    "auto_tags": False,
    "auto_tags_include_title": True,
    "auto_tags_include_description": True,
    "auto_tags_include_transcript": True,
    "auto_tags_mode": "add",
    "auto_thumbnail": False,
    "auto_socials": {
        "twitter": False,
        "bluesky": False,
        "mastodon": False,
        "linkedin": False,
        "threads": False,
    },
}

POSTING_DEFAULTS = {
    "post_video_delay_minutes": 15,
    "inter_post_spacing_minutes": 5,
    "default_template_video": "new_video",
    "default_template_segment": "new_video",
    "default_template_short": "new_video",
    "default_template_hook": "new_video",
}


async def get_setting(project_id: int, key: str) -> str | None:
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT value FROM project_settings WHERE project_id = ? AND key = ?",
        (project_id, key),
    )
    return rows[0]["value"] if rows else None


async def set_setting(project_id: int, key: str, value: str) -> None:
    db = await get_db()
    await db.execute(
        "INSERT INTO project_settings (project_id, key, value) VALUES (?, ?, ?) "
        "ON CONFLICT(project_id, key) DO UPDATE SET value = excluded.value",
        (project_id, key, value),
    )
    await db.commit()


async def get_json(project_id: int, key: str, default: Any = None) -> Any:
    raw = await get_setting(project_id, key)
    if raw is None:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return default


async def set_json(project_id: int, key: str, value: Any) -> None:
    await set_setting(project_id, key, json.dumps(value))


async def get_all(project_id: int) -> dict[str, str]:
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT key, value FROM project_settings WHERE project_id = ?",
        (project_id,),
    )
    return {r["key"]: r["value"] for r in rows}


async def get_auto_actions(project_id: int) -> dict:
    """Return the full auto-actions matrix with defaults filled in."""
    upload = await get_json(project_id, "auto_actions_upload", {})
    import_ = await get_json(project_id, "auto_actions_import", {})
    return {
        "upload": {**AUTO_ACTION_DEFAULTS_UPLOAD, **(upload or {})},
        "import": {**AUTO_ACTION_DEFAULTS_IMPORT, **(import_ or {})},
    }


async def get_posting_settings(project_id: int) -> dict:
    """Return posting delay/spacing settings + per-tier default templates."""
    stored = await get_json(project_id, "posting", {})
    return {**POSTING_DEFAULTS, **(stored or {})}
