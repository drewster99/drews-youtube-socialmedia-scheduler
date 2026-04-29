"""YouTube OAuth 2.0 authentication.

Storage layout in Keychain (or the encrypted-file fallback):

* ``youtube`` / ``client_secret`` — the JSON downloaded from Google Cloud
  Console (one app-level identity, shared by every project on this install).
* ``youtube`` / ``oauth.<project_slug>`` — per-project authorised user
  credentials. Each project completes its own OAuth flow against a different
  Google account; the resulting refresh+access token bundle lives under that
  project's slug.

Slugs are immutable for the lifetime of a project (see
``services/projects.py``) so they're a stable Keychain account name. When
cloud sync becomes real, swap to a project_uuid column and migrate.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import logging
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from yt_scheduler.config import DATA_DIR, YOUTUBE_SCOPES
from yt_scheduler.services.keychain import (
    delete_secret,
    get_storage_type,
    load_secret,
    store_secret,
)

logger = logging.getLogger(__name__)

YOUTUBE_NAMESPACE = "youtube"
CLIENT_SECRET_KEY = "client_secret"
DEFAULT_PROJECT_SLUG = "default"

# Legacy file paths — only consulted by the one-shot migration below.
_LEGACY_CLIENT_SECRETS_PATHS = (
    DATA_DIR / "client_secret.json",
    Path.home() / ".drews-yt-scheduler" / "client_secret.json",
    Path.home() / ".youtube-publisher" / "client_secret.json",
)
_LEGACY_CREDENTIALS_PATH = DATA_DIR / "credentials.json"
_LEGACY_GLOBAL_OAUTH_KEY = "oauth_credentials"


# --- client_secret -----------------------------------------------------------


def _credentials_key(project_slug: str) -> str:
    return f"oauth.{project_slug}"


def _migrate_client_secret_file_once() -> None:
    """Move ``client_secret.json`` from disk into Keychain on first encounter.

    Idempotent: once Keychain has a value, the file (if any) is removed and
    never read again.
    """
    if load_secret(YOUTUBE_NAMESPACE, CLIENT_SECRET_KEY):
        return
    for path in _LEGACY_CLIENT_SECRETS_PATHS:
        if path.exists():
            try:
                data = path.read_text(encoding="utf-8")
                json.loads(data)  # validate
            except (OSError, json.JSONDecodeError) as exc:
                logger.warning("Could not read %s: %s", path, exc)
                continue
            store_secret(YOUTUBE_NAMESPACE, CLIENT_SECRET_KEY, data)
            try:
                path.unlink()
            except OSError:
                pass
            logger.info("Migrated client_secret.json from %s into Keychain", path)
            return


def get_client_secret_dict() -> dict | None:
    """Return the OAuth client config as a dict, or None if not set up.

    Caller passes this to ``Flow.from_client_config`` instead of pointing at
    a file path on disk.
    """
    _migrate_client_secret_file_once()
    raw = load_secret(YOUTUBE_NAMESPACE, CLIENT_SECRET_KEY)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Stored client_secret is not valid JSON")
        return None


def store_client_secret_from_path(path: str | Path) -> None:
    """Read a client_secret.json file the user uploaded and persist its
    contents into Keychain. Validates that it parses as JSON before writing."""
    text = Path(path).read_text(encoding="utf-8")
    json.loads(text)  # raises if malformed
    store_secret(YOUTUBE_NAMESPACE, CLIENT_SECRET_KEY, text)


def store_client_secret_from_text(text: str) -> None:
    """Validate + persist a client_secret JSON blob (e.g. from a textarea)."""
    json.loads(text)
    store_secret(YOUTUBE_NAMESPACE, CLIENT_SECRET_KEY, text)


def has_client_secret() -> bool:
    return get_client_secret_dict() is not None


# --- per-project credentials ------------------------------------------------


def _migrate_global_credentials_to_default_project_once() -> None:
    """Move pre-multi-project tokens (one global ``oauth_credentials`` blob)
    into ``oauth.default`` so the existing user keeps working."""
    if load_secret(YOUTUBE_NAMESPACE, _credentials_key(DEFAULT_PROJECT_SLUG)):
        return
    legacy = load_secret(YOUTUBE_NAMESPACE, _LEGACY_GLOBAL_OAUTH_KEY)
    if legacy:
        store_secret(
            YOUTUBE_NAMESPACE, _credentials_key(DEFAULT_PROJECT_SLUG), legacy
        )
        delete_secret(YOUTUBE_NAMESPACE, _LEGACY_GLOBAL_OAUTH_KEY)
        logger.info("Migrated global YouTube credentials to oauth.default")
        return
    # File-based legacy
    if _LEGACY_CREDENTIALS_PATH.exists():
        try:
            data = _LEGACY_CREDENTIALS_PATH.read_text(encoding="utf-8")
            json.loads(data)  # validate
            store_secret(
                YOUTUBE_NAMESPACE, _credentials_key(DEFAULT_PROJECT_SLUG), data
            )
            _LEGACY_CREDENTIALS_PATH.unlink()
            logger.info(
                "Migrated file-based YouTube credentials → Keychain oauth.default"
            )
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Could not migrate legacy credentials.json: %s", exc)


def _save_credentials(project_slug: str, creds: Credentials) -> None:
    store_secret(YOUTUBE_NAMESPACE, _credentials_key(project_slug), creds.to_json())


def _load_credentials_json(project_slug: str) -> str | None:
    if project_slug == DEFAULT_PROJECT_SLUG:
        _migrate_global_credentials_to_default_project_once()
    return load_secret(YOUTUBE_NAMESPACE, _credentials_key(project_slug))


def get_credentials(project_slug: str = DEFAULT_PROJECT_SLUG) -> Credentials | None:
    """Load + auto-refresh credentials for the given project."""
    creds_json = _load_credentials_json(project_slug)
    if not creds_json:
        return None
    try:
        creds_data = json.loads(creds_json)
        creds = Credentials.from_authorized_user_info(creds_data, YOUTUBE_SCOPES)
    except (json.JSONDecodeError, ValueError, KeyError):
        logger.warning("Stored credentials for %s are invalid", project_slug)
        return None

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            _save_credentials(project_slug, creds)
        except Exception as exc:
            logger.warning("Failed to refresh credentials for %s: %s", project_slug, exc)
            return None

    if creds and creds.valid:
        return creds
    return None


def run_oauth_flow(
    project_slug: str = DEFAULT_PROJECT_SLUG,
    client_secret_path: str | Path | None = None,
) -> Credentials:
    """Run the OAuth installed-app flow (opens browser).

    ``client_secret_path``: optional one-off override (e.g. CLI ``yt-scheduler
    auth /path/to/client_secret.json``); if provided, the file is loaded,
    validated, and stored into Keychain *before* the flow runs.
    """
    if client_secret_path is not None:
        store_client_secret_from_path(client_secret_path)

    config = get_client_secret_dict()
    if config is None:
        raise RuntimeError(
            "No OAuth client configured. Upload your client_secret.json via "
            "Settings → YouTube authentication, or pass it to ``yt-scheduler "
            "auth /path/to/client_secret.json``."
        )

    flow = InstalledAppFlow.from_client_config(config, YOUTUBE_SCOPES)
    creds = flow.run_local_server(port=8090, prompt="consent", access_type="offline")
    _save_credentials(project_slug, creds)
    return creds


def is_authenticated(project_slug: str = DEFAULT_PROJECT_SLUG) -> bool:
    return get_credentials(project_slug) is not None


_active_project_slug: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "active_project_slug", default=None
)


def set_active_project(slug: str | None) -> None:
    """Bind an active project for callers (background jobs) that don't thread
    a ``project_slug`` argument through every ``youtube.*`` wrapper. Callers
    that pass an explicit ``project_slug`` always win over this binding.
    """
    _active_project_slug.set(slug)


def get_active_project() -> str | None:
    return _active_project_slug.get()


def get_youtube_service(project_slug: str | None = None):
    """Build an authenticated YouTube API service for a project.

    Resolution order: explicit ``project_slug`` arg → ``set_active_project``
    binding. Raises if neither is set — the silent fallback to the default
    project was a footgun: a caller that forgot to bind would unwittingly
    use the default project's credentials and post to the wrong channel.
    """
    slug = project_slug or _active_project_slug.get()
    if slug is None:
        raise RuntimeError(
            "get_youtube_service requires an active project. Either pass "
            "project_slug=... explicitly or call set_active_project(slug) "
            "earlier in the request / job. The default-project fallback "
            "was removed to prevent silent wrong-channel posts."
        )
    creds = get_credentials(slug)
    if not creds:
        raise RuntimeError(
            f"Not authenticated for project '{slug}'. Run the OAuth flow."
        )
    return build("youtube", "v3", credentials=creds)


def get_auth_status(project_slug: str = DEFAULT_PROJECT_SLUG) -> dict:
    """Status payload for the Settings UI."""
    creds = get_credentials(project_slug)
    storage = get_storage_type()
    has_secret = has_client_secret()
    if not creds:
        return {
            "authenticated": False,
            "client_secret_uploaded": has_secret,
            "storage": storage,
            "project_slug": project_slug,
        }
    info = {
        "authenticated": True,
        "valid": creds.valid,
        "client_secret_uploaded": has_secret,
        "storage": storage,
        "project_slug": project_slug,
    }
    creds_json = _load_credentials_json(project_slug)
    if creds_json:
        try:
            data = json.loads(creds_json)
            cid = data.get("client_id", "")
            info["client_id"] = (cid[:20] + "...") if cid else ""
        except json.JSONDecodeError:
            pass
    return info


def clear_credentials(project_slug: str = DEFAULT_PROJECT_SLUG) -> None:
    """Remove a project's stored credentials. Leaves client_secret in place."""
    delete_secret(YOUTUBE_NAMESPACE, _credentials_key(project_slug))


def clear_client_secret() -> None:
    """Remove the install-wide client_secret. Doesn't touch per-project tokens."""
    delete_secret(YOUTUBE_NAMESPACE, CLIENT_SECRET_KEY)


# --- Web OAuth (used by the Phase E project wizard + project re-auth) -------


def store_credentials(project_slug: str, creds: Credentials) -> None:
    """Persist credentials under ``oauth.<project_slug>`` Keychain key.

    Public wrapper around the internal helper so OAuth callbacks (web flow)
    can save tokens once they've been validated against the project's
    bound channel id.
    """
    _save_credentials(project_slug, creds)


def channel_id_from_credentials(creds: Credentials) -> tuple[str | None, str | None, str | None]:
    """Hit ``channels().list(mine=True)`` with the given credentials and
    return ``(channel_id, channel_title, channel_handle)`` or all-None
    when the API call fails."""
    try:
        service = build("youtube", "v3", credentials=creds)
        result = service.channels().list(part="id,snippet", mine=True).execute()
        items = result.get("items") or []
        if not items:
            return None, None, None
        snippet = items[0].get("snippet", {})
        return (
            items[0].get("id"),
            snippet.get("title"),
            snippet.get("customUrl"),
        )
    except Exception as exc:
        logger.warning("Channel lookup failed: %s", exc)
        return None, None, None


# --- Channel id resolution + backfill ---------------------------------------


def resolve_channel_id(project_slug: str = DEFAULT_PROJECT_SLUG) -> str | None:
    """Synchronous helper: return the channel id the credentials authenticate
    as, or ``None`` when no creds or the API call fails."""
    creds = get_credentials(project_slug)
    if creds is None:
        return None
    try:
        service = build("youtube", "v3", credentials=creds)
        result = service.channels().list(part="id", mine=True).execute()
        items = result.get("items") or []
        if not items:
            return None
        return items[0].get("id")
    except Exception as exc:
        logger.warning(
            "Channel id lookup failed for project %s: %s", project_slug, exc
        )
        return None


async def backfill_channel_ids() -> None:
    """For every project missing a ``youtube_channel_id`` but with stored
    credentials, resolve the channel id and stamp it. Refuses to assign a
    channel id that's already claimed by another project — those projects
    are left ``NULL`` and the UI will prompt re-auth."""
    from yt_scheduler.database import get_db

    db = await get_db()
    cursor = await db.execute(
        "SELECT id, slug, youtube_channel_id FROM projects"
    )
    projects = list(await cursor.fetchall())
    if not projects:
        return

    for project in projects:
        if project["youtube_channel_id"]:
            continue
        slug = project["slug"]
        try:
            channel_id = await asyncio.to_thread(resolve_channel_id, slug)
        except Exception as exc:
            logger.info("Skipping channel id backfill for %s: %s", slug, exc)
            continue
        if not channel_id:
            logger.info(
                "Project %s has no usable YouTube credentials; skipping channel backfill",
                slug,
            )
            continue

        cursor = await db.execute(
            "SELECT id, slug FROM projects "
            "WHERE youtube_channel_id = ? AND id != ?",
            (channel_id, project["id"]),
        )
        conflict = await cursor.fetchone()
        if conflict is not None:
            logger.warning(
                "Channel id %s claimed by project %s; cannot assign to project %s. "
                "User must re-authenticate one of them against a different channel.",
                channel_id, conflict["slug"], slug,
            )
            continue

        await db.execute(
            "UPDATE projects SET youtube_channel_id = ?, updated_at = datetime('now') "
            "WHERE id = ?",
            (channel_id, project["id"]),
        )
        await db.commit()
        logger.info("Stamped project %s with YouTube channel %s", slug, channel_id)


async def backfill_channel_assets() -> None:
    """For every project with credentials, fetch the channel's icon and
    banner URLs and cache them on the projects row. We only refetch when
    one of the URL columns is NULL — once cached, there's no automatic
    refresh path; the user can clear and re-auth to refresh, or we can
    add an explicit "refresh assets" button later."""
    from yt_scheduler.database import get_db
    from yt_scheduler.services import youtube as _youtube

    db = await get_db()
    cursor = await db.execute(
        "SELECT id, slug, channel_thumbnail_url, channel_banner_url FROM projects"
    )
    projects = list(await cursor.fetchall())
    for project in projects:
        slug = project["slug"]
        if project["channel_thumbnail_url"] and project["channel_banner_url"]:
            continue
        if not get_credentials(slug):
            continue
        set_active_project(slug)
        try:
            assets = await asyncio.to_thread(_youtube.get_channel_assets)
        except Exception as exc:
            logger.info("Channel asset backfill failed for %s: %s", slug, exc)
            continue
        thumb = assets.get("thumbnail_url") or project["channel_thumbnail_url"]
        banner = assets.get("banner_url") or project["channel_banner_url"]
        if not thumb and not banner:
            continue
        await db.execute(
            "UPDATE projects SET channel_thumbnail_url = ?, "
            "channel_banner_url = ?, updated_at = datetime('now') WHERE id = ?",
            (thumb, banner, project["id"]),
        )
        await db.commit()
        logger.info("Cached channel assets for project %s", slug)
