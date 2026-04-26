"""One-shot Keychain migration to per-credential JSON bundles.

Before this migration each social platform stored its credentials as a flat
set of keys under one Keychain namespace (e.g. ``twitter:bearer_token``,
``twitter:refresh_token``, ``twitter:client_id``). After this migration
each credential is one Keychain entry whose account name is ``cred.<uuid>``
and whose value is a JSON blob containing every field for that credential.

Migration runs at server startup, idempotently, after migration 008 has
created placeholder rows in ``social_accounts``. For each platform we:

1. Read the legacy keys via ``load_all_secrets``.
2. Call the platform's identity endpoint to resolve a stable
   ``provider_account_id`` and a display username.
3. Update the placeholder ``social_accounts`` row (or insert a fresh one)
   with real values.
4. Persist the bundle at ``<platform>:cred.<uuid>``.
5. Delete the legacy per-key entries.
6. Stamp ``<platform>:_migrated_v8`` so subsequent boots are no-ops.
"""

from __future__ import annotations

import json
import logging
import uuid as uuidlib

import httpx

from yt_scheduler.database import get_db
from yt_scheduler.services.keychain import (
    delete_secret,
    load_all_secrets,
    load_secret,
    store_secret,
)

logger = logging.getLogger(__name__)

MIGRATION_MARKER_KEY = "_migrated_v8"
CREDENTIAL_KEY_PREFIX = "cred."
PLATFORMS = ("twitter", "mastodon", "linkedin", "threads", "bluesky")


async def _resolve_twitter(creds: dict[str, str]) -> tuple[str | None, str | None]:
    bearer = creds.get("bearer_token")
    if not bearer:
        return None, None
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://api.twitter.com/2/users/me",
                headers={"Authorization": f"Bearer {bearer}"},
            )
        if resp.status_code == 200:
            data = resp.json().get("data", {})
            return data.get("id"), data.get("username")
    except Exception as exc:
        logger.info("twitter users/me failed during migration: %s", exc)
    return None, None


async def _resolve_mastodon(creds: dict[str, str]) -> tuple[str | None, str | None]:
    token = creds.get("access_token")
    instance = (creds.get("instance_url") or "").rstrip("/")
    if not token or not instance:
        return None, None
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{instance}/api/v1/accounts/verify_credentials",
                headers={"Authorization": f"Bearer {token}"},
            )
        if resp.status_code == 200:
            data = resp.json()
            account_id = data.get("id")
            handle = data.get("acct") or data.get("username")
            if handle and "@" not in handle:
                from urllib.parse import urlparse

                host = urlparse(instance).netloc
                if host:
                    handle = f"{handle}@{host}"
            return (str(account_id) if account_id is not None else None), handle
    except Exception as exc:
        logger.info("mastodon verify_credentials failed during migration: %s", exc)
    return None, None


async def _resolve_linkedin(creds: dict[str, str]) -> tuple[str | None, str | None]:
    token = creds.get("access_token")
    if not token:
        return None, None
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://api.linkedin.com/v2/userinfo",
                headers={"Authorization": f"Bearer {token}"},
            )
        if resp.status_code == 200:
            data = resp.json()
            sub = data.get("sub")
            name = data.get("name") or data.get("email")
            return sub, name
    except Exception as exc:
        logger.info("linkedin userinfo failed during migration: %s", exc)
    return creds.get("person_urn"), creds.get("person_urn")


async def _resolve_threads(creds: dict[str, str]) -> tuple[str | None, str | None]:
    return creds.get("user_id"), creds.get("username")


async def _resolve_bluesky(creds: dict[str, str]) -> tuple[str | None, str | None]:
    handle = creds.get("handle")
    return handle, handle


_RESOLVERS = {
    "twitter": _resolve_twitter,
    "mastodon": _resolve_mastodon,
    "linkedin": _resolve_linkedin,
    "threads": _resolve_threads,
    "bluesky": _resolve_bluesky,
}


def _is_legacy_key(key: str) -> bool:
    """True for old-layout keys: not a bundle entry and not the marker."""
    return not key.startswith(CREDENTIAL_KEY_PREFIX) and key != MIGRATION_MARKER_KEY


async def _migrate_platform(platform: str) -> None:
    """Migrate one platform. Idempotent — re-runs are no-ops."""
    if load_secret(platform, MIGRATION_MARKER_KEY):
        return

    secrets_map = load_all_secrets(platform)
    legacy = {k: v for k, v in secrets_map.items() if _is_legacy_key(k)}

    db = await get_db()

    if not legacy:
        # Nothing in Keychain to migrate. If 008 SQL created a placeholder
        # row for this platform anyway (because there was a 002-era row),
        # drop it -- it can't be reconciled to any real credentials.
        await db.execute(
            "DELETE FROM social_accounts "
            "WHERE platform = ? AND uuid LIKE '__pending__:%'",
            (platform,),
        )
        await db.commit()
        store_secret(platform, MIGRATION_MARKER_KEY, "1")
        return

    resolver = _RESOLVERS.get(platform)
    provider_account_id, username = (None, None)
    if resolver is not None:
        provider_account_id, username = await resolver(legacy)

    is_nickname = 0
    if not username:
        username = legacy.get("username") or legacy.get("handle") or platform
        is_nickname = 1

    new_uuid = uuidlib.uuid4().hex
    final_provider_id = provider_account_id or f"legacy:{new_uuid}"

    cursor = await db.execute(
        "SELECT id FROM social_accounts "
        "WHERE platform = ? AND provider_account_id = ?",
        (platform, final_provider_id),
    )
    if await cursor.fetchone() is not None:
        final_provider_id = f"legacy:{new_uuid}"

    cursor = await db.execute(
        "SELECT id FROM social_accounts "
        "WHERE platform = ? AND uuid LIKE '__pending__:%' ORDER BY id LIMIT 1",
        (platform,),
    )
    placeholder = await cursor.fetchone()

    if placeholder is not None:
        row_id = int(placeholder[0])
        await db.execute(
            "UPDATE social_accounts "
            "SET uuid = ?, provider_account_id = ?, username = ?, "
            "    credentials_ref = ?, is_nickname = ?, deleted_at = NULL "
            "WHERE id = ?",
            (new_uuid, final_provider_id, username,
             f"{CREDENTIAL_KEY_PREFIX}{new_uuid}", is_nickname, row_id),
        )
    else:
        cursor = await db.execute(
            "INSERT INTO social_accounts "
            "(uuid, platform, provider_account_id, username, credentials_ref, is_nickname) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (new_uuid, platform, final_provider_id, username,
             f"{CREDENTIAL_KEY_PREFIX}{new_uuid}", is_nickname),
        )
        row_id = int(cursor.lastrowid)
    await db.commit()

    bundle = dict(legacy)
    bundle["uuid"] = new_uuid
    bundle["provider_account_id"] = final_provider_id
    bundle["username"] = username
    store_secret(platform, f"{CREDENTIAL_KEY_PREFIX}{new_uuid}", json.dumps(bundle))

    for key in legacy:
        delete_secret(platform, key)

    store_secret(platform, MIGRATION_MARKER_KEY, "1")
    logger.info(
        "Keychain v8: %s → cred.%s (id=%s, %s)",
        platform, new_uuid[:8], row_id,
        "resolved" if provider_account_id else "synthetic id",
    )


async def migrate_to_per_credential_bundles() -> None:
    """Run migration for every supported platform. Idempotent."""
    for platform in PLATFORMS:
        try:
            await _migrate_platform(platform)
        except Exception as exc:
            logger.warning(
                "Keychain migration for %s failed (will retry next boot): %s",
                platform, exc,
            )
