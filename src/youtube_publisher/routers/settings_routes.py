"""Settings and moderation routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from youtube_publisher.database import get_db
from youtube_publisher.services import moderation
from youtube_publisher.services.keychain import (
    delete_all_secrets,
    get_storage_type,
    load_all_secrets,
    store_secret,
)
from youtube_publisher.services.social import (
    ALL_PLATFORMS,
    PLATFORM_DESCRIPTIONS,
    PLATFORM_FIELDS,
    get_poster,
)

router = APIRouter(prefix="/api/settings", tags=["settings"])


# --- General Settings ---


@router.get("")
async def get_settings():
    """Get all settings (non-secret only)."""
    db = await get_db()
    rows = await db.execute_fetchall("SELECT key, value FROM settings")
    return {r["key"]: r["value"] for r in rows}


@router.put("")
async def update_settings(data: dict):
    """Update settings (key-value pairs)."""
    db = await get_db()
    for key, value in data.items():
        await db.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = ?",
            (key, str(value), str(value)),
        )
    await db.commit()
    return {"status": "ok"}


# --- Social Media Credentials ---


@router.get("/social")
async def list_social_platforms():
    """List all social platforms with their configuration status and field definitions."""
    result = {}
    for platform in ALL_PLATFORMS:
        poster = get_poster(platform)
        creds = load_all_secrets(platform)

        # Build masked view of stored credentials
        fields = PLATFORM_FIELDS.get(platform, [])
        stored = {}
        for field in fields:
            key = field["key"]
            value = creds.get(key, "")
            if value and field.get("secret"):
                stored[key] = value[:4] + "..." if len(value) > 4 else "***"
            elif value:
                stored[key] = value
            else:
                stored[key] = ""

        result[platform] = {
            "configured": await poster.is_configured(),
            "description": PLATFORM_DESCRIPTIONS.get(platform, ""),
            "fields": fields,
            "stored": stored,
            "storage": get_storage_type(),
        }
    return result


@router.get("/social/{platform}")
async def get_social_config(platform: str):
    """Get social media config for a platform (secrets are masked)."""
    if platform not in ALL_PLATFORMS:
        raise HTTPException(400, f"Unknown platform: {platform}")

    poster = get_poster(platform)
    creds = load_all_secrets(platform)
    fields = PLATFORM_FIELDS.get(platform, [])

    stored = {}
    for field in fields:
        key = field["key"]
        value = creds.get(key, "")
        if value and field.get("secret"):
            stored[key] = value[:4] + "..." if len(value) > 4 else "***"
        elif value:
            stored[key] = value
        else:
            stored[key] = ""

    return {
        "configured": await poster.is_configured(),
        "description": PLATFORM_DESCRIPTIONS.get(platform, ""),
        "fields": fields,
        "stored": stored,
        "storage": get_storage_type(),
    }


@router.put("/social/{platform}")
async def update_social_config(platform: str, data: dict):
    """Update social media credentials for a platform.

    Stores all values in Keychain (macOS) or encrypted secrets file.
    """
    if platform not in ALL_PLATFORMS:
        raise HTTPException(400, f"Unknown platform: {platform}")

    for key, value in data.items():
        if value:  # Don't store empty strings
            store_secret(platform, key, value)

    return {"status": "ok", "storage": get_storage_type()}


@router.delete("/social/{platform}")
async def delete_social_config(platform: str):
    """Remove all credentials for a platform."""
    if platform not in ALL_PLATFORMS:
        raise HTTPException(400, f"Unknown platform: {platform}")

    delete_all_secrets(platform)
    return {"status": "ok"}


# --- Blocklist ---


@router.get("/blocklist")
async def get_blocklist():
    """Get all blocked keywords."""
    return await moderation.get_blocklist()


@router.post("/blocklist")
async def add_keyword(data: dict):
    """Add a keyword to the blocklist."""
    keyword = data.get("keyword", "").strip()
    if not keyword:
        raise HTTPException(400, "Keyword is required")
    await moderation.add_keyword(keyword, is_regex=data.get("is_regex", False))
    return {"status": "ok"}


@router.delete("/blocklist/{keyword_id}")
async def remove_keyword(keyword_id: int):
    """Remove a keyword from the blocklist."""
    await moderation.remove_keyword(keyword_id)
    return {"status": "ok"}


# --- Moderation Log ---


@router.get("/moderation-log")
async def get_moderation_log(limit: int = 50):
    """Get recent moderation actions."""
    return await moderation.get_moderation_log(limit)
