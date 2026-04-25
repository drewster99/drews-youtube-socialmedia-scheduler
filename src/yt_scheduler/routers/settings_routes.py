"""Settings and moderation routes."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from yt_scheduler.config import (
    ANTHROPIC_API_KEY_FIELD,
    ANTHROPIC_NAMESPACE,
    get_anthropic_api_key,
)
from yt_scheduler.database import get_db
from yt_scheduler.services import moderation
from yt_scheduler.services.keychain import (
    delete_all_secrets,
    delete_secret,
    get_storage_type,
    load_all_secrets,
    store_secret,
)
from yt_scheduler.services.social import (
    ALL_PLATFORMS,
    PLATFORM_DESCRIPTIONS,
    PLATFORM_FIELDS,
    PLATFORM_SETUP_GUIDES,
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


# --- Anthropic API Key ---


@router.get("/anthropic")
async def get_anthropic_status():
    """Get Anthropic API key + selected model name (key masked)."""
    from yt_scheduler.config import ANTHROPIC_MODEL
    from yt_scheduler.database import get_db

    key = get_anthropic_api_key()
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT value FROM settings WHERE key = 'anthropic_model'"
    )
    model = rows[0]["value"] if rows else ANTHROPIC_MODEL
    return {
        "configured": bool(key),
        "masked_key": key[:8] + "..." if key and len(key) > 8 else ("***" if key else ""),
        "model": model,
        "storage": get_storage_type(),
    }


@router.put("/anthropic")
async def update_anthropic_key(data: dict):
    """Save Anthropic API key + optional model name."""
    from yt_scheduler.database import get_db

    api_key = (data.get("api_key") or "").strip()
    model = (data.get("model") or "").strip()
    if api_key:
        store_secret(ANTHROPIC_NAMESPACE, ANTHROPIC_API_KEY_FIELD, api_key)
    if model:
        db = await get_db()
        await db.execute(
            "INSERT INTO settings (key, value) VALUES ('anthropic_model', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (model,),
        )
        await db.commit()
        # Bust the in-process cache so existing connections pick up the change
        # without needing a restart.
        from yt_scheduler.services.ai import invalidate_model_cache
        invalidate_model_cache()
    if not api_key and not model:
        raise HTTPException(400, "API key or model is required")
    return {"status": "ok", "storage": get_storage_type()}


@router.delete("/anthropic")
async def delete_anthropic_key():
    """Remove Anthropic API key from Keychain/secrets."""
    delete_secret(ANTHROPIC_NAMESPACE, ANTHROPIC_API_KEY_FIELD)
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
            "setup_guide": PLATFORM_SETUP_GUIDES.get(platform, []),
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
        "setup_guide": PLATFORM_SETUP_GUIDES.get(platform, []),
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


@router.post("/moderation/run")
async def run_moderation_now():
    """Run comment moderation against the default project's videos right now.

    Returns ``{checked, matched, actions_by_video, errors}`` so the toast can
    be informative — fixes the cosmetic-only Run Check Now button (req #3).
    """
    try:
        results = await moderation.check_all_videos(project_id=1)
    except Exception as exc:
        raise HTTPException(500, f"Moderation run failed: {exc}") from exc
    return results
