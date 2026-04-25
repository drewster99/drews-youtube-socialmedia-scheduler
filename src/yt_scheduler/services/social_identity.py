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

from yt_scheduler.services.keychain import load_all_secrets

logger = logging.getLogger(__name__)


async def resolve_twitter() -> str | None:
    """OAuth 2.0 user lookup via ``GET /2/users/me``. Returns the @handle
    without the leading @, or None when the API can't tell us."""
    creds = load_all_secrets("twitter")
    bearer = creds.get("bearer_token")
    if not bearer:
        return None
    try:
        async with httpx.AsyncClient(timeout=15) as client:
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
    creds = load_all_secrets("mastodon")
    token = creds.get("access_token")
    instance = creds.get("instance_url")
    if not token or not instance:
        return None
    instance = instance.rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=15) as client:
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
    creds = load_all_secrets("linkedin")
    token = creds.get("access_token")
    if not token:
        return None
    try:
        async with httpx.AsyncClient(timeout=15) as client:
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
    creds = load_all_secrets("threads")
    return creds.get("username")


async def resolve_bluesky() -> str | None:
    creds = load_all_secrets("bluesky")
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


async def upsert_social_account(platform: str, project_id: int = 1) -> int | None:
    """Reflect the currently-configured platform credentials into a row in
    ``social_accounts`` (and link it to the given project) so the per-project
    UX can list real accounts. Returns the social_account id, or None if the
    platform has no credentials."""
    from yt_scheduler.database import get_db
    from yt_scheduler.services.social import get_poster

    poster = get_poster(platform)
    if not await poster.is_configured():
        return None

    username = await resolve_username(platform) or platform  # last-ditch fallback

    db = await get_db()
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
        await db.commit()
        account_id = int(cursor.lastrowid)

    # Attach to the requested project (idempotent)
    await db.execute(
        "INSERT OR IGNORE INTO project_social_accounts (project_id, social_account_id) "
        "VALUES (?, ?)",
        (project_id, account_id),
    )
    await db.commit()
    return account_id
