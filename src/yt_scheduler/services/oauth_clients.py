"""Per-platform OAuth client credentials (Client ID + optional Client Secret).

Mirrors the YouTube ``client_secret.json`` pattern in ``services/auth.py`` —
the install owns one app-level identity per platform, the user enters it
once in Settings, and it persists in Keychain (or the encrypted secrets
file fallback) so the OAuth start endpoints don't have to prompt the user
every time they connect a new account.

Storage layout::

    namespace = platform (e.g. "twitter", "linkedin", "threads")
    key       = "oauth_client_id" | "oauth_client_secret"
    value     = the credential string

Use ``get_oauth_client(platform)`` to read both at once for the start
endpoint; use ``store_oauth_client`` from the Settings PUT route.
"""

from __future__ import annotations

from yt_scheduler.services.keychain import (
    delete_secret,
    load_secret,
    store_secret,
)

CLIENT_ID_KEY = "oauth_client_id"
CLIENT_SECRET_KEY = "oauth_client_secret"

# Platforms that ship without a baked-in app and require the user to
# register their own and paste the credentials into Settings.
SUPPORTED_PLATFORMS = ("twitter", "linkedin", "threads")


def get_oauth_client(platform: str) -> tuple[str, str]:
    """Return ``(client_id, client_secret)`` for the platform; either may
    be empty when the user hasn't configured it yet."""
    cid = load_secret(platform, CLIENT_ID_KEY) or ""
    csec = load_secret(platform, CLIENT_SECRET_KEY) or ""
    return cid, csec


def store_oauth_client(
    platform: str, client_id: str, client_secret: str | None
) -> None:
    """Persist the OAuth client credentials. ``client_secret`` may be
    ``None`` or empty for public-client (PKCE-only) platforms like X."""
    store_secret(platform, CLIENT_ID_KEY, client_id)
    if client_secret:
        store_secret(platform, CLIENT_SECRET_KEY, client_secret)
    else:
        delete_secret(platform, CLIENT_SECRET_KEY)


def clear_oauth_client(platform: str) -> None:
    """Remove both the client_id and client_secret entries."""
    delete_secret(platform, CLIENT_ID_KEY)
    delete_secret(platform, CLIENT_SECRET_KEY)


def has_client_id(platform: str) -> bool:
    return bool(load_secret(platform, CLIENT_ID_KEY))
