"""Settings and moderation routes."""

from __future__ import annotations

from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException

from yt_scheduler.config import (
    ANTHROPIC_API_KEY_FIELD,
    ANTHROPIC_NAMESPACE,
    get_anthropic_api_key,
)
from yt_scheduler.database import get_db
from yt_scheduler.services import moderation
from yt_scheduler.services import ngrok, oauth_clients
from yt_scheduler.services.keychain import (
    delete_secret,
    get_storage_type,
    store_secret,
)
from yt_scheduler.services.social import (
    ALL_PLATFORMS,
    PLATFORM_DESCRIPTIONS,
    PLATFORM_FIELDS,
    PLATFORM_SETUP_GUIDES,
    get_poster,
)
from yt_scheduler.services.social_credentials import (
    get_first_active_credential,
    list_credentials,
    load_bundle,
    save_bundle,
    soft_delete_credential,
    upsert_credential,
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


# --- OAuth client credentials (Twitter / LinkedIn / Threads) ---

OAUTH_CLIENT_DISPLAY = {
    "twitter": "X / Twitter",
    "linkedin": "LinkedIn",
    "threads": "Threads / Meta",
}

OAUTH_CLIENT_HELP = {
    "twitter": {
        "console_url": "https://developer.x.com",
        "console_label": "developer.x.com",
        "id_label": "Client ID",
        "secret_label": "Client Secret",
        "instructions": (
            "Register an app, then open your app → Keys and tokens. "
            "OAuth 2.0 with PKCE; the secret is optional (leave blank if you "
            "registered the app as a public client)."
        ),
    },
    "linkedin": {
        "console_url": "https://developer.linkedin.com",
        "console_label": "developer.linkedin.com",
        "id_label": "Client ID",
        "secret_label": "Primary Client Secret",
        "instructions": (
            "Register an app, then open your app → Auth tab → Application "
            "credentials. Both Client ID and Primary Client Secret are required."
        ),
    },
    "threads": {
        "console_url": "https://developers.facebook.com",
        "console_label": "developers.facebook.com",
        "id_label": "Client ID",
        "secret_label": "Client Secret",
        "instructions": (
            "Register a Threads app, then open your app → Use cases → "
            "Threads API → Settings. Both Client ID and Client Secret are "
            "required. Threads OAuth requires HTTPS, so connect the account "
            "with the app open via the ngrok tunnel URL (see the HTTPS "
            "tunnel card)."
        ),
    },
}


def _mask(secret: str) -> str:
    if not secret:
        return ""
    return secret[:4] + "..." if len(secret) > 4 else "***"


@router.get("/oauth-clients")
async def list_oauth_clients():
    """Return the configured OAuth clients for every platform that needs one.

    Shape per platform: ``{configured: bool, client_id, client_secret_set,
    secret_required: bool, display, help}``. ``client_id`` is returned in
    full (not a secret); ``client_secret`` is never returned, just a flag.
    """
    out = {}
    for platform in oauth_clients.SUPPORTED_PLATFORMS:
        cid, csec = oauth_clients.get_oauth_client(platform)
        help_blob = OAUTH_CLIENT_HELP.get(platform, {})
        out[platform] = {
            "configured": bool(cid),
            "client_id": cid,
            "client_secret_set": bool(csec),
            "secret_required": platform != "twitter",
            "display": OAUTH_CLIENT_DISPLAY.get(platform, platform.capitalize()),
            "console_url": help_blob.get("console_url", ""),
            "console_label": help_blob.get("console_label", ""),
            "id_label": help_blob.get("id_label", "Client ID"),
            "secret_label": help_blob.get("secret_label", "Client Secret"),
            "instructions": help_blob.get("instructions", ""),
            "masked_secret": _mask(csec),
        }
    return {"storage": get_storage_type(), "platforms": out}


@router.put("/oauth-clients/{platform}")
async def update_oauth_client(platform: str, data: dict):
    """Save or replace the OAuth client credentials for a platform.

    Body: ``{"client_id": "...", "client_secret": "..."}``. ``client_secret``
    may be omitted or empty for X/Twitter (public client). For LinkedIn /
    Threads it is required.
    """
    if platform not in oauth_clients.SUPPORTED_PLATFORMS:
        raise HTTPException(400, f"Unsupported platform: {platform}")
    client_id = (data.get("client_id") or "").strip()
    client_secret = (data.get("client_secret") or "").strip()
    if not client_id:
        raise HTTPException(400, "client_id is required")
    if platform != "twitter" and not client_secret:
        raise HTTPException(
            400, f"{OAUTH_CLIENT_DISPLAY.get(platform, platform)} requires a client_secret"
        )
    oauth_clients.store_oauth_client(platform, client_id, client_secret)
    return {"status": "ok", "storage": get_storage_type()}


@router.delete("/oauth-clients/{platform}")
async def delete_oauth_client(platform: str):
    """Remove the stored OAuth client credentials for a platform."""
    if platform not in oauth_clients.SUPPORTED_PLATFORMS:
        raise HTTPException(400, f"Unsupported platform: {platform}")
    oauth_clients.clear_oauth_client(platform)
    return {"status": "ok"}


# --- ngrok HTTPS tunnel detection (for OAuth flows that require HTTPS) ---


@router.get("/ngrok")
async def get_ngrok_status():
    """Report whether an ngrok tunnel is forwarding to our local port.

    Used by the Settings UI to surface the ``https://...ngrok-free.app``
    URL the user should open the app at when running an OAuth flow that
    Meta / others reject from plain ``http://`` origins.
    """
    from yt_scheduler.config import PORT

    public_url = await ngrok.detect_https_tunnel(PORT)
    return {
        "detected": public_url is not None,
        "public_url": public_url or "",
        "local_port": PORT,
    }


# --- Social Media Credentials ---


async def _bundle_view_for_platform(platform: str) -> dict:
    """Return the active credential's bundle (or empty dict) for legacy
    settings UI rendering. The Phase A→C transitional view always reflects
    the first active credential."""
    cred = await get_first_active_credential(platform)
    if cred is None:
        return {}
    return load_bundle(platform, cred["uuid"]) or {}


def _stored_view(bundle: dict, fields: list[dict]) -> dict:
    stored = {}
    for field in fields:
        key = field["key"]
        value = bundle.get(key, "") or ""
        if value and field.get("secret"):
            stored[key] = (value[:4] + "...") if len(value) > 4 else "***"
        elif value:
            stored[key] = value
        else:
            stored[key] = ""
    return stored


@router.get("/social")
async def list_social_platforms():
    """List all social platforms with their configuration status and field definitions."""
    result = {}
    for platform in ALL_PLATFORMS:
        poster = get_poster(platform)
        bundle = await _bundle_view_for_platform(platform)
        fields = PLATFORM_FIELDS.get(platform, [])
        result[platform] = {
            "configured": await poster.is_configured(),
            "description": PLATFORM_DESCRIPTIONS.get(platform, ""),
            "setup_guide": PLATFORM_SETUP_GUIDES.get(platform, []),
            "fields": fields,
            "stored": _stored_view(bundle, fields),
            "storage": get_storage_type(),
        }
    return result


@router.get("/social/{platform}")
async def get_social_config(platform: str):
    """Get social media config for a platform (secrets are masked)."""
    if platform not in ALL_PLATFORMS:
        raise HTTPException(400, f"Unknown platform: {platform}")

    poster = get_poster(platform)
    bundle = await _bundle_view_for_platform(platform)
    fields = PLATFORM_FIELDS.get(platform, [])

    return {
        "configured": await poster.is_configured(),
        "description": PLATFORM_DESCRIPTIONS.get(platform, ""),
        "setup_guide": PLATFORM_SETUP_GUIDES.get(platform, []),
        "fields": fields,
        "stored": _stored_view(bundle, fields),
        "storage": get_storage_type(),
    }


def _provider_id_from_paste(platform: str, data: dict) -> tuple[str | None, str | None]:
    """Derive a stable id + display username from a paste-form payload, for
    platforms whose paste form predates OAuth. Bluesky is OAuth-only and
    rejects this path explicitly to avoid resurrecting app-password
    bundles."""
    if platform == "bluesky":
        # OAuth-only. The Settings UI no longer sends paste-form payloads
        # for Bluesky — Connect with Bluesky → /api/oauth/bluesky/start
        # is the only way to create a credential.
        return None, None
    if platform == "threads":
        user_id = (data.get("user_id") or "").strip()
        username = (data.get("username") or "").strip()
        return (user_id or None), (username or user_id or None)
    if platform == "linkedin":
        urn = (data.get("person_urn") or "").strip()
        return (urn or None), (urn or None)
    if platform == "mastodon":
        instance = (data.get("instance_url") or "").strip().rstrip("/")
        token = (data.get("access_token") or "").strip()
        if instance and token:
            host = urlparse(instance).netloc
            return (f"paste:{host or instance}:{token[:8]}", f"mastodon@{host or instance}")
        return None, None
    if platform == "twitter":
        return None, (data.get("username") or "").strip() or None
    return None, None


@router.put("/social/{platform}")
async def update_social_config(platform: str, data: dict):
    """Update social media credentials for a platform.

    When an active credential exists, the paste-form fields are merged
    into its existing bundle. Otherwise (other paste-only platforms) a
    new credential row is created. Bluesky is OAuth-only and rejects this
    path outright so an arbitrary form payload can't pollute the OAuth
    bundle's keys (e.g. resurrect ``app_password`` alongside the access
    token).
    """
    if platform not in ALL_PLATFORMS:
        raise HTTPException(400, f"Unknown platform: {platform}")
    if platform == "bluesky":
        raise HTTPException(
            400,
            "Bluesky is OAuth-only. Use POST /api/oauth/bluesky/start "
            "(Connect with Bluesky in Settings).",
        )

    fresh_values = {k: v for k, v in data.items() if v}
    if not fresh_values:
        return {"status": "ok", "storage": get_storage_type()}

    cred = await get_first_active_credential(platform)
    if cred is not None:
        bundle = load_bundle(platform, cred["uuid"]) or {}
        bundle.update(fresh_values)
        save_bundle(platform, cred["uuid"], bundle)
        return {
            "status": "ok",
            "storage": get_storage_type(),
            "social_account_id": cred["id"],
            "uuid": cred["uuid"],
        }

    provider_id, username = _provider_id_from_paste(platform, data)
    if not provider_id or not username:
        raise HTTPException(
            400,
            f"Cannot create a {platform} credential from this form. "
            "Use the OAuth flow instead.",
        )
    new_cred = await upsert_credential(
        platform=platform,
        provider_account_id=provider_id,
        username=username,
        bundle=fresh_values,
    )
    return {
        "status": "ok",
        "storage": get_storage_type(),
        "social_account_id": new_cred["id"],
        "uuid": new_cred["uuid"],
    }


@router.delete("/social/{platform}")
async def delete_social_config(platform: str):
    """Soft-delete every active credential for a platform.

    The Phase C settings redesign exposes per-credential delete; this
    endpoint is the transitional 'wipe this platform' button kept around
    for the existing legacy UI. Each removal is the same soft-delete used
    elsewhere — bundles purged from Keychain, ``social_accounts`` rows
    keep ``deleted_at`` set so any template slot that pointed at one
    still shows 'Missing credential'.
    """
    if platform not in ALL_PLATFORMS:
        raise HTTPException(400, f"Unknown platform: {platform}")

    creds = await list_credentials(platform=platform, include_deleted=False)
    for cred in creds:
        await soft_delete_credential(cred["uuid"])
    return {"status": "ok", "deleted": len(creds)}


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


@router.get("/moderation-status")
async def get_moderation_status():
    """Return next/last run timestamps for the comment-moderation job.

    APScheduler tracks ``next_run_time`` directly. The job uses a fixed
    interval, so ``last_run = next_run - interval`` gives us "when did
    it last fire" without needing to persist any state. Returns ISO 8601
    UTC strings; the UI formats them via the standard _ensureUtc shim.
    """
    from datetime import datetime, timezone
    from yt_scheduler.config import COMMENT_CHECK_INTERVAL_MINUTES
    from yt_scheduler.services.scheduler import scheduler as _scheduler

    job = _scheduler.get_job("moderate_comments")
    if job is None or job.next_run_time is None:
        return {"next_run": None, "last_run": None, "interval_minutes": COMMENT_CHECK_INTERVAL_MINUTES}
    next_run = job.next_run_time
    if next_run.tzinfo is None:
        next_run = next_run.replace(tzinfo=timezone.utc)
    interval_min = COMMENT_CHECK_INTERVAL_MINUTES
    last_run = next_run.timestamp() - interval_min * 60
    last_run_dt = datetime.fromtimestamp(last_run, tz=timezone.utc)
    return {
        "next_run": next_run.isoformat(),
        "last_run": last_run_dt.isoformat(),
        "interval_minutes": interval_min,
    }


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
