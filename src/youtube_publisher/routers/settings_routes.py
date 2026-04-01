"""Settings and moderation routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from youtube_publisher.database import get_db
from youtube_publisher.services import moderation
from youtube_publisher.services.social import ALL_PLATFORMS

router = APIRouter(prefix="/api/settings", tags=["settings"])


# --- General Settings ---


@router.get("")
async def get_settings():
    """Get all settings."""
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


@router.get("/social/{platform}")
async def get_social_config(platform: str):
    """Get social media config for a platform (credentials are masked)."""
    if platform not in ALL_PLATFORMS:
        raise HTTPException(400, f"Unknown platform: {platform}")

    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT key, value FROM settings WHERE key LIKE ?",
        (f"social_{platform}_%",),
    )
    config = {}
    for r in rows:
        key = r["key"].replace(f"social_{platform}_", "")
        value = r["value"]
        # Mask sensitive values
        if any(s in key for s in ["secret", "token", "password"]):
            config[key] = value[:4] + "..." if len(value) > 4 else "***"
        else:
            config[key] = value

    config["configured"] = bool(rows)
    return config


@router.put("/social/{platform}")
async def update_social_config(platform: str, data: dict):
    """Update social media credentials for a platform."""
    if platform not in ALL_PLATFORMS:
        raise HTTPException(400, f"Unknown platform: {platform}")

    db = await get_db()
    for key, value in data.items():
        full_key = f"social_{platform}_{key}"
        await db.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = ?",
            (full_key, value, value),
        )

    # Mark as configured
    await db.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = ?",
        (f"social_{platform}_configured", "1", "1"),
    )
    await db.commit()
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
