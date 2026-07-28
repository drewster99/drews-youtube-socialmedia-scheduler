"""Resolve the authenticated username/handle for each social platform.

Used after credentials are saved (or on demand) to populate
``social_accounts.username`` so the user sees *which* X / Mastodon / LinkedIn
account a project is posting from.

Each helper:

* Reads credentials via the existing keychain layer.
* Calls the platform's ``users/me``-style endpoint.
* Falls back to whatever the user typed in (``handle``, ``username``,
  ``person_urn``) when the API call fails — never raises.
"""

from __future__ import annotations

import logging
from typing import Callable

import httpx

from yt_scheduler.config import (
    THREADS_VERIFY_TIMEOUT_SECONDS,
    USERNAME_RESOLVE_TIMEOUT_SECONDS,
)

from yt_scheduler.services.keychain import load_all_secrets_async

logger = logging.getLogger(__name__)


async def resolve_twitter() -> str | None:
    """OAuth 2.0 user lookup via ``GET /2/users/me``. Returns the @handle
    without the leading @, or None when the API can't tell us."""
    creds = await load_all_secrets_async("twitter")
    bearer = creds.get("bearer_token")
    if not bearer:
        return None
    try:
        async with httpx.AsyncClient(timeout=USERNAME_RESOLVE_TIMEOUT_SECONDS) as client:
            resp = await client.get(
                "https://api.twitter.com/2/users/me",
                headers={"Authorization": f"Bearer {bearer}"},
            )
        if resp.status_code == 200:
            data = resp.json().get("data", {})
            return data.get("username")
    except Exception as exc:
        logger.info("Twitter users/me failed: %s", exc)
    return None


async def resolve_mastodon() -> str | None:
    creds = await load_all_secrets_async("mastodon")
    token = creds.get("access_token")
    instance = creds.get("instance_url")
    if not token or not instance:
        return None
    instance = instance.rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=USERNAME_RESOLVE_TIMEOUT_SECONDS) as client:
            resp = await client.get(
                f"{instance}/api/v1/accounts/verify_credentials",
                headers={"Authorization": f"Bearer {token}"},
            )
        if resp.status_code == 200:
            data = resp.json()
            handle = data.get("acct") or data.get("username")
            if handle and "@" not in handle:
                # Append the instance hostname so the user knows where it lives.
                from urllib.parse import urlparse

                host = urlparse(instance).netloc
                if host:
                    handle = f"{handle}@{host}"
            return handle
    except Exception as exc:
        logger.info("Mastodon verify_credentials failed: %s", exc)
    return None


async def resolve_linkedin() -> str | None:
    creds = await load_all_secrets_async("linkedin")
    token = creds.get("access_token")
    if not token:
        return None
    try:
        async with httpx.AsyncClient(timeout=USERNAME_RESOLVE_TIMEOUT_SECONDS) as client:
            resp = await client.get(
                "https://api.linkedin.com/v2/userinfo",
                headers={"Authorization": f"Bearer {token}"},
            )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("name") or data.get("email")
    except Exception as exc:
        logger.info("LinkedIn userinfo failed: %s", exc)
    return creds.get("person_urn")


async def resolve_threads() -> str | None:
    creds = await load_all_secrets_async("threads")
    return creds.get("username")


async def resolve_bluesky() -> str | None:
    creds = await load_all_secrets_async("bluesky")
    return creds.get("handle")


_RESOLVERS: dict[str, Callable[[], "object"]] = {
    "twitter": resolve_twitter,
    "mastodon": resolve_mastodon,
    "linkedin": resolve_linkedin,
    "threads": resolve_threads,
    "bluesky": resolve_bluesky,
}


async def resolve_username(platform: str) -> str | None:
    resolver = _RESOLVERS.get(platform)
    if resolver is None:
        return None
    return await resolver()


async def upsert_social_account(platform: str, *, project_id: int) -> int | None:
    """Reflect the currently-configured platform credentials into a row in
    ``social_accounts`` (and link it to the given project) so the per-project
    UX can list real accounts. Returns the social_account id, or None if the
    platform has no credentials."""
    from yt_scheduler.database import write_transaction
    from yt_scheduler.services.social import get_poster

    poster = get_poster(platform)
    if not await poster.is_configured():
        return None

    username = await resolve_username(platform) or platform  # last-ditch fallback

    # resolve_username (network) is done above, outside the lock. The whole
    # find-or-create + project-attach is one atomic critical section.
    async with write_transaction() as db:
        cursor = await db.execute(
            "SELECT id FROM social_accounts WHERE platform = ? AND username = ?",
            (platform, username),
        )
        row = await cursor.fetchone()
        if row is not None:
            account_id = int(row[0])
        else:
            cursor = await db.execute(
                "INSERT INTO social_accounts (platform, username, credentials_ref) "
                "VALUES (?, ?, ?)",
                (platform, username, f"{platform}:{username}"),
            )
            account_id = int(cursor.lastrowid)

        # Attach to the requested project (idempotent)
        await db.execute(
            "INSERT OR IGNORE INTO project_social_accounts (project_id, social_account_id) "
            "VALUES (?, ?)",
            (project_id, account_id),
        )
    return account_id


# Platforms with a live credential check implemented below. Exposed through
# /api/platform-capabilities so the UI shows a Verify button only where it
# would work, rather than offering one that always fails.
LIVE_CHECK_PLATFORMS = frozenset({"threads"})


class CredentialCheckUnsupported(RuntimeError):
    """This platform has no cheap identity endpoint to probe."""


async def verify_live(platform: str, bundle: dict) -> dict:
    """Ask the *provider* whether these credentials still work.

    ``resolve_username`` deliberately does not do this — it reads the cached
    username out of the bundle, which is why the Settings ↻ button reported a
    healthy account for a Threads token that had been dead for weeks. This is
    the opposite: a real round-trip whose only purpose is to find out.

    Returns ``{"ok": bool, "detail": str, "username": str|None}``. A network
    failure is reported as such rather than as a dead credential — not
    reaching the provider says nothing about the token.
    """
    import httpx

    if platform not in LIVE_CHECK_PLATFORMS:
        raise CredentialCheckUnsupported(
            f"No live credential check implemented for {platform}."
        )

    token = bundle.get("access_token")
    if not token:
        return {"ok": False, "detail": "No access token stored.", "username": None}

    try:
        async with httpx.AsyncClient(timeout=THREADS_VERIFY_TIMEOUT_SECONDS) as client:
            resp = await client.get(
                "https://graph.threads.net/v1.0/me",
                params={"fields": "id,username", "access_token": token},
            )
    except httpx.HTTPError as exc:
        return {
            "ok": False,
            "unreachable": True,
            "detail": f"Could not reach Threads: {exc}",
            "username": None,
        }

    if resp.status_code == 200:
        data = resp.json()
        return {
            "ok": True,
            "detail": "Token is valid.",
            "username": data.get("username"),
        }

    body = ""
    try:
        parsed = resp.json()
        error = parsed.get("error") if isinstance(parsed, dict) else None
        if isinstance(error, dict):
            body = error.get("message") or ""
    except ValueError:
        body = ""
    if not body:
        body = resp.text[:200]

    # A 5xx is the provider having a bad day, not a verdict on the token.
    # Reporting it as "rejected" would send the user to re-authenticate a
    # perfectly good credential — the same distinction refresh_if_stale makes.
    if resp.status_code >= 500:
        return {
            "ok": False,
            "unreachable": True,
            "detail": f"Threads couldn't answer right now (HTTP {resp.status_code}): "
                      f"{body}. This says nothing about the token — try again.",
            "username": None,
        }
    return {
        "ok": False,
        "detail": f"Threads rejected the token (HTTP {resp.status_code}): {body}",
        "username": None,
    }
