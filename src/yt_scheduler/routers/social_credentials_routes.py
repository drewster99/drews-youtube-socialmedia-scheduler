"""HTTP API for managing social credentials independently of any project.

This is the surface the redesigned Settings page (Phase C) and project
settings (Phase E) talk to. Each credential is identified by its UUID;
soft-delete is exposed via ``DELETE`` with an explicit ``confirm`` flag
so the UI can show dependent projects and template slots first.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from yt_scheduler.services.social import ALL_PLATFORMS
from yt_scheduler.services.social_credentials import (
    get_credential_by_uuid,
    get_dependents,
    list_credentials,
    soft_delete_credential,
)
from yt_scheduler.services.social_identity import (
    CredentialCheckUnsupported,
    resolve_username,
    verify_live,
)

router = APIRouter(prefix="/api/social-credentials", tags=["social-credentials"])


@router.get("")
async def list_all_credentials(platform: str | None = Query(default=None)):
    """List active credentials. Optional ``?platform=`` filter."""
    if platform is not None and platform not in ALL_PLATFORMS:
        raise HTTPException(400, f"Unknown platform: {platform}")
    return await list_credentials(platform=platform, include_deleted=False)


@router.get("/{uuid}")
async def get_credential(uuid: str):
    cred = await get_credential_by_uuid(uuid)
    if cred is None:
        raise HTTPException(404, "Credential not found")
    return cred


@router.get("/{uuid}/dependents")
async def get_credential_dependents(uuid: str):
    """Return projects + slots that reference this credential. Used by the
    delete-confirmation dialog to show what will become 'Missing credential'
    after the user confirms."""
    cred = await get_credential_by_uuid(uuid)
    if cred is None:
        raise HTTPException(404, "Credential not found")
    return await get_dependents(uuid)


@router.delete("/{uuid}")
async def delete_credential(uuid: str, confirm: bool = Query(default=False)):
    """Soft-delete a credential.

    Without ``?confirm=1`` returns the dependents payload so the UI can
    show 'this credential is used by X projects and Y template slots'.
    With ``?confirm=1`` performs the soft-delete and returns the (now
    deleted) row.
    """
    cred = await get_credential_by_uuid(uuid)
    if cred is None:
        raise HTTPException(404, "Credential not found")

    if not confirm:
        deps = await get_dependents(uuid)
        return {
            "would_delete": cred,
            "dependents": deps,
            "needs_confirm": True,
        }

    deleted = await soft_delete_credential(uuid)
    return {"deleted": deleted, "needs_confirm": False}


@router.post("/{uuid}/refresh-username")
async def refresh_credential_username(uuid: str):
    """Re-run the platform's identity endpoint and update the row's
    ``username`` if it has changed at the provider."""
    cred = await get_credential_by_uuid(uuid)
    if cred is None:
        raise HTTPException(404, "Credential not found")

    new_username = await resolve_username(cred["platform"])
    if not new_username or new_username == cred["username"]:
        return {"changed": False, "username": cred["username"]}

    from yt_scheduler.database import write_transaction

    async with write_transaction() as db:
        # Re-scope to a live row: the credential could have been soft-deleted
        # during the resolve_username network call above; don't resurrect-write
        # a username onto a deleted row.
        cursor = await db.execute(
            "UPDATE social_accounts SET username = ? WHERE uuid = ? AND deleted_at IS NULL",
            (new_username, uuid),
        )
        if cursor.rowcount == 0:
            raise HTTPException(404, "Credential not found")
    return {"changed": True, "username": new_username}


@router.post("/{uuid}/verify")
async def verify_credential(uuid: str):
    """Ask the provider whether this credential still works.

    Distinct from ``/refresh-username``, which reads the cached username out of
    the stored bundle and therefore reports a healthy account for a token that
    died weeks ago. This one makes a real call.

    **Response 200** — ``{"ok": bool, "detail": "...", "username": "..."}``.
    ``ok: false`` with ``unreachable: true`` means we could not reach the
    provider, which says nothing about the token.

    The verdict is mirrored into ``needs_reauth``: a rejection sets it (so
    Settings flags the row and offers Reconnect), a pass clears a stale flag.
    Unreachable leaves the flag untouched in both directions.
    """
    from yt_scheduler.services.social_credentials import (
        clear_needs_reauth,
        load_bundle,
        mark_needs_reauth,
    )

    cred = await get_credential_by_uuid(uuid)
    if cred is None:
        raise HTTPException(404, "Credential not found")

    bundle = await load_bundle(cred["platform"], uuid)
    if bundle is None:
        raise HTTPException(404, "No stored credentials for this account")

    try:
        result = await verify_live(cred["platform"], bundle)
    except CredentialCheckUnsupported as exc:
        raise HTTPException(501, str(exc)) from exc

    # Without this, Verify could tell the user their token is dead while the
    # row still showed no Reconnect button — a verdict with no way to act on
    # it. The live check is authoritative, so its answer drives the flag.
    if result["ok"]:
        await clear_needs_reauth(uuid)
    elif not result.get("unreachable"):
        await mark_needs_reauth(uuid)
    return result
