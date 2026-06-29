"""Per-credential storage for social platforms.

Each credential is one row in ``social_accounts`` plus a JSON bundle stored
in Keychain at ``<platform>:cred.<uuid>``. Multiple credentials per platform
are allowed; dropping the install-wide single-tenant assumption is the
whole point of this module.

Soft-delete: deleting a credential sets ``deleted_at`` and removes the
Keychain bundle, but leaves the row in place so that template slots
still pointing at the deleted credential render as
``"Missing credential"`` instead of silently changing behaviour.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid as uuidlib

from yt_scheduler.database import get_db, write_transaction
from yt_scheduler.services._keyed_locks import KeyedLocks
from yt_scheduler.services.keychain import (
    delete_secret_async,
    load_secret_async,
    store_secret_async,
)

logger = logging.getLogger(__name__)

CREDENTIAL_KEY_PREFIX = "cred."

# One asyncio.Lock per credential UUID, serialising token refreshes so the
# background refresh job and a post-time refresh never present the same
# (single-use, rotating) refresh token concurrently — which would otherwise
# fail one of them and spuriously flag the credential as needing re-auth.
# Process-wide; the scheduler and the request handlers share the event loop.
# WeakValueDictionary-backed so old credential UUIDs don't pile up forever.
_credential_locks: KeyedLocks[str] = KeyedLocks()


def get_credential_lock(uuid: str) -> asyncio.Lock:
    """Return the shared refresh lock for a credential UUID (created on first use)."""
    return _credential_locks.get(uuid)

PLATFORM_DISPLAY_NAMES: dict[str, str] = {
    "twitter": "X",
    "bluesky": "Bluesky",
    "mastodon": "Mastodon",
    "linkedin": "LinkedIn",
    "threads": "Threads",
    "youtube": "YouTube",
}


def display_name_for(platform: str) -> str:
    """Human-readable platform name (e.g. ``twitter`` → ``X``)."""
    return PLATFORM_DISPLAY_NAMES.get(platform, platform.capitalize())


def format_account_label(platform: str, username: str | None) -> str:
    """Return the canonical label for a credential, e.g. ``@drewbenson @X``.

    Falls back to ``"@<platform>"`` when no username is available.
    """
    handle = (username or "").lstrip("@") or platform
    return f"@{handle} @{display_name_for(platform)}"


def _row_to_dict(row) -> dict:
    return {
        "id": int(row["id"]),
        "uuid": row["uuid"],
        "platform": row["platform"],
        "provider_account_id": row["provider_account_id"],
        "username": row["username"],
        "display_name": row["display_name"],
        "is_nickname": bool(row["is_nickname"]),
        "credentials_ref": row["credentials_ref"],
        "created_at": row["created_at"],
        "deleted_at": row["deleted_at"],
        # ``needs_reauth`` is a flag set by the poster when the platform
        # rejects a request as unauthorised AND a refresh attempt also
        # failed (or no refresh path exists). Cleared on next successful
        # ``upsert_credential`` (i.e. after the user re-OAuths).
        "needs_reauth": bool(row["needs_reauth"]) if "needs_reauth" in row.keys() else False,
        # For X accounts: 'none' | 'blue' | 'business' | 'government' — anything
        # but 'none'/NULL means X Premium (25,000-char posts). NULL elsewhere.
        "verified_type": (row["verified_type"] if "verified_type" in row.keys() else None),
        "label": format_account_label(row["platform"], row["username"]),
    }


async def list_credentials(
    platform: str | None = None, include_deleted: bool = False
) -> list[dict]:
    """List credentials, newest first. Filters by platform when given."""
    db = await get_db()
    sql = "SELECT * FROM social_accounts"
    clauses: list[str] = []
    params: list = []
    if not include_deleted:
        clauses.append("deleted_at IS NULL")
    if platform is not None:
        clauses.append("platform = ?")
        params.append(platform)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY platform, created_at"
    cursor = await db.execute(sql, tuple(params))
    rows = await cursor.fetchall()
    return [_row_to_dict(r) for r in rows]


async def get_credential_by_uuid(uuid: str) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT * FROM social_accounts WHERE uuid = ?", (uuid,)
    )
    row = await cursor.fetchone()
    return _row_to_dict(row) if row else None


async def get_credential_by_id(account_id: int) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT * FROM social_accounts WHERE id = ?", (account_id,)
    )
    row = await cursor.fetchone()
    return _row_to_dict(row) if row else None


async def get_first_active_credential(platform: str) -> dict | None:
    """Return the oldest active credential for a platform, or ``None``.

    Used by transitional code paths in Phase A/B that need a "current
    credential for this platform" without project context. Once routing
    is fully wired, callers will resolve via ``social_account_id``
    instead.
    """
    db = await get_db()
    cursor = await db.execute(
        "SELECT * FROM social_accounts "
        "WHERE platform = ? AND deleted_at IS NULL "
        "ORDER BY id LIMIT 1",
        (platform,),
    )
    row = await cursor.fetchone()
    return _row_to_dict(row) if row else None


async def load_bundle(platform: str, uuid: str) -> dict | None:
    """Load and JSON-parse the Keychain bundle for a credential.

    The bundle's ``uuid`` key is always set from the ``uuid`` argument here,
    not trusted from the stored JSON. Posters surface a credential's UUID on
    auth failure (so the send-path can flag it ``needs_reauth``) by reading
    ``creds["uuid"]``; a bundle written before ``save_bundle`` started
    stamping the UUID — or by any path that didn't — would otherwise lack it,
    silently breaking the reconnect flow. ``load_bundle`` already knows the
    UUID, so it is the single place that can guarantee it.
    """
    raw = await load_secret_async(platform, f"{CREDENTIAL_KEY_PREFIX}{uuid}")
    if not raw:
        return None
    try:
        bundle = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Bundle at %s:cred.%s is not valid JSON", platform, uuid[:8])
        return None
    if not isinstance(bundle, dict):
        logger.warning("Bundle at %s:cred.%s is not a JSON object", platform, uuid[:8])
        return None
    bundle["uuid"] = uuid
    return bundle


async def save_bundle(platform: str, uuid: str, bundle: dict) -> None:
    bundle["uuid"] = uuid
    await store_secret_async(
        platform, f"{CREDENTIAL_KEY_PREFIX}{uuid}", json.dumps(bundle),
    )


async def upsert_credential(
    platform: str,
    provider_account_id: str,
    username: str,
    bundle: dict,
    is_nickname: bool = False,
    display_name: str | None = None,
) -> dict:
    """Insert a fresh credential or update the existing one for this
    ``(platform, provider_account_id)`` pair. Returns the credential row
    (matching ``get_credential_by_uuid``).

    On conflict with a soft-deleted row, the row is undeleted and the
    bundle is replaced — re-authing a previously removed account brings
    it back to life.
    """
    db = await get_db()
    # Captured for X accounts; COALESCE keeps a previously-known value when a
    # re-auth couldn't fetch it (e.g. the /users/me lookup hiccuped).
    verified_type = bundle.get("verified_type")
    cursor = await db.execute(
        "SELECT id, uuid FROM social_accounts "
        "WHERE platform = ? AND provider_account_id = ?",
        (platform, provider_account_id),
    )
    row = await cursor.fetchone()

    if row is not None:
        existing_id = int(row["id"])
        existing_uuid = row["uuid"]
        # Write the Keychain bundle BEFORE committing the DB row so that a
        # crash between the two leaves an orphaned-but-harmless secret rather
        # than a committed row pointing at a missing bundle.
        await save_bundle(
            platform, existing_uuid,
            dict(bundle, provider_account_id=provider_account_id, username=username),
        )
        # Clear needs_reauth: the user just successfully re-OAuthed. The
        # save_bundle (Keychain, above) stays OUTSIDE the transaction — never
        # await a network/to_thread call while holding the write lock.
        async with write_transaction() as db:
            await db.execute(
                "UPDATE social_accounts "
                "SET username = ?, display_name = ?, is_nickname = ?, "
                "    deleted_at = NULL, needs_reauth = 0, "
                "    verified_type = COALESCE(?, verified_type) "
                "WHERE id = ?",
                (username, display_name, 1 if is_nickname else 0, verified_type, existing_id),
            )
        return await get_credential_by_id(existing_id)  # type: ignore[return-value]

    new_uuid = uuidlib.uuid4().hex
    # Write the Keychain bundle BEFORE inserting the DB row for the same
    # reason: a failed bundle write leaves nothing in the DB to dangle.
    await save_bundle(
        platform, new_uuid,
        dict(bundle, provider_account_id=provider_account_id, username=username),
    )
    async with write_transaction() as db:
        cursor = await db.execute(
            "INSERT INTO social_accounts "
            "(uuid, platform, provider_account_id, username, display_name, "
            " is_nickname, credentials_ref, verified_type) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                new_uuid, platform, provider_account_id, username, display_name,
                1 if is_nickname else 0, f"{CREDENTIAL_KEY_PREFIX}{new_uuid}", verified_type,
            ),
        )
    new_id = int(cursor.lastrowid)
    return await get_credential_by_id(new_id)  # type: ignore[return-value]


async def mark_needs_reauth(uuid: str) -> None:
    """Flag a credential as needing the user to re-OAuth.

    Called by send paths after a terminal authentication failure. The
    flag is cleared on the next successful :func:`upsert_credential`
    (re-OAuth) or :func:`clear_needs_reauth` (a successful token refresh).
    """
    async with write_transaction() as db:
        await db.execute(
            "UPDATE social_accounts SET needs_reauth = 1 WHERE uuid = ?", (uuid,)
        )


async def clear_needs_reauth(uuid: str) -> None:
    """Clear a credential's needs-reauth flag — called after a successful
    token refresh, so a credential flagged by a transient blip self-heals
    without forcing the user through the OAuth flow again. No-op if the
    flag wasn't set."""
    async with write_transaction() as db:
        await db.execute(
            "UPDATE social_accounts SET needs_reauth = 0 WHERE uuid = ? AND needs_reauth = 1",
            (uuid,),
        )


async def soft_delete_credential(uuid: str) -> dict | None:
    """Soft-delete a credential: mark deleted_at, remove Keychain bundle.

    Returns the row (with ``deleted_at`` set) or ``None`` if no such
    credential. Project defaults referencing this row become NULL via
    the ``ON DELETE SET NULL`` foreign key on ``project_social_defaults``;
    template slots remain pointing at the row so the UI can render
    "Missing credential".
    """
    cred = await get_credential_by_uuid(uuid)
    if cred is None or cred["deleted_at"] is not None:
        return cred

    # Attempt the Keychain delete BEFORE committing the DB soft-delete so that
    # a crash between the two leaves a harmless orphaned secret rather than a
    # committed "deleted" row whose secret can never be cleaned up. A failed
    # delete is logged but does not block the DB update — the secret can't be
    # loaded via load_bundle once deleted_at is set (the row is excluded from
    # list_credentials and the bundle ref is gone from the UI), so it only
    # wastes Keychain space, not a security concern. The Keychain call stays
    # OUTSIDE the write transaction (never await a network call under the lock).
    try:
        await delete_secret_async(cred["platform"], f"{CREDENTIAL_KEY_PREFIX}{uuid}")
    except Exception:
        logger.warning(
            "Keychain delete failed for %s cred.%s — proceeding with DB soft-delete; "
            "orphaned secret may remain in Keychain",
            cred["platform"], uuid[:8],
        )
    async with write_transaction() as db:
        await db.execute(
            "UPDATE social_accounts SET deleted_at = datetime('now') WHERE uuid = ?",
            (uuid,),
        )
        await db.execute(
            "DELETE FROM project_social_defaults WHERE social_account_id = ?",
            (cred["id"],),
        )
    return await get_credential_by_uuid(uuid)


async def get_dependents(uuid: str) -> dict:
    """Return projects + template slots that reference this credential.

    Shape::

        {"projects": [{"slug": "...", "name": "...", "platform": "..."}],
         "slots":    [{"template_id": 1, "template_name": "...", "project_slug": "...",
                       "slot_id": 7, "platform": "..."}]}
    """
    cred = await get_credential_by_uuid(uuid)
    if cred is None:
        return {"projects": [], "slots": []}

    db = await get_db()

    cursor = await db.execute(
        "SELECT p.slug, p.name, d.platform "
        "FROM project_social_defaults d "
        "JOIN projects p ON p.id = d.project_id "
        "WHERE d.social_account_id = ?",
        (cred["id"],),
    )
    projects = [
        {"slug": r["slug"], "name": r["name"], "platform": r["platform"]}
        for r in await cursor.fetchall()
    ]

    cursor = await db.execute(
        "SELECT s.id AS slot_id, s.platform, t.id AS template_id, t.name AS template_name, "
        "       p.slug AS project_slug, p.name AS project_name "
        "FROM template_slots s "
        "JOIN templates t ON t.id = s.template_id "
        "JOIN projects p ON p.id = t.project_id "
        "WHERE s.social_account_id = ?",
        (cred["id"],),
    )
    slots = [
        {
            "slot_id": int(r["slot_id"]),
            "template_id": int(r["template_id"]),
            "template_name": r["template_name"],
            "project_slug": r["project_slug"],
            "project_name": r["project_name"],
            "platform": r["platform"],
        }
        for r in await cursor.fetchall()
    ]

    return {"projects": projects, "slots": slots}
