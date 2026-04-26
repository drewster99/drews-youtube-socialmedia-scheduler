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

import json
import logging
import uuid as uuidlib

from yt_scheduler.database import get_db
from yt_scheduler.services.keychain import (
    delete_secret,
    load_secret,
    store_secret,
)

logger = logging.getLogger(__name__)

CREDENTIAL_KEY_PREFIX = "cred."

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


def load_bundle(platform: str, uuid: str) -> dict | None:
    """Load and JSON-parse the Keychain bundle for a credential."""
    raw = load_secret(platform, f"{CREDENTIAL_KEY_PREFIX}{uuid}")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Bundle at %s:cred.%s is not valid JSON", platform, uuid[:8])
        return None


def save_bundle(platform: str, uuid: str, bundle: dict) -> None:
    bundle["uuid"] = uuid
    store_secret(platform, f"{CREDENTIAL_KEY_PREFIX}{uuid}", json.dumps(bundle))


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
    cursor = await db.execute(
        "SELECT id, uuid FROM social_accounts "
        "WHERE platform = ? AND provider_account_id = ?",
        (platform, provider_account_id),
    )
    row = await cursor.fetchone()

    if row is not None:
        existing_id = int(row["id"])
        existing_uuid = row["uuid"]
        # Clear needs_reauth: the user just successfully re-OAuthed.
        await db.execute(
            "UPDATE social_accounts "
            "SET username = ?, display_name = ?, is_nickname = ?, "
            "    deleted_at = NULL, needs_reauth = 0 "
            "WHERE id = ?",
            (username, display_name, 1 if is_nickname else 0, existing_id),
        )
        await db.commit()
        save_bundle(platform, existing_uuid, dict(bundle, provider_account_id=provider_account_id, username=username))
        return await get_credential_by_id(existing_id)  # type: ignore[return-value]

    new_uuid = uuidlib.uuid4().hex
    cursor = await db.execute(
        "INSERT INTO social_accounts "
        "(uuid, platform, provider_account_id, username, display_name, "
        " is_nickname, credentials_ref) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            new_uuid, platform, provider_account_id, username, display_name,
            1 if is_nickname else 0, f"{CREDENTIAL_KEY_PREFIX}{new_uuid}",
        ),
    )
    await db.commit()
    new_id = int(cursor.lastrowid)
    save_bundle(
        platform, new_uuid,
        dict(bundle, provider_account_id=provider_account_id, username=username),
    )
    return await get_credential_by_id(new_id)  # type: ignore[return-value]


async def mark_needs_reauth(uuid: str) -> None:
    """Flag a credential as needing the user to re-OAuth.

    Called by send paths after a terminal authentication failure. The
    flag is cleared on the next successful :func:`upsert_credential`
    (which runs as part of the OAuth callback).
    """
    db = await get_db()
    await db.execute(
        "UPDATE social_accounts SET needs_reauth = 1 WHERE uuid = ?", (uuid,)
    )
    await db.commit()


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

    db = await get_db()
    await db.execute(
        "UPDATE social_accounts SET deleted_at = datetime('now') WHERE uuid = ?",
        (uuid,),
    )
    await db.execute(
        "DELETE FROM project_social_defaults WHERE social_account_id = ?",
        (cred["id"],),
    )
    await db.commit()
    delete_secret(cred["platform"], f"{CREDENTIAL_KEY_PREFIX}{uuid}")
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
