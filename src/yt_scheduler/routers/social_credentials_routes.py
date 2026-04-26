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
from yt_scheduler.services.social_identity import resolve_username

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

    from yt_scheduler.database import get_db

    db = await get_db()
    await db.execute(
        "UPDATE social_accounts SET username = ? WHERE uuid = ?",
        (new_username, uuid),
    )
    await db.commit()
    return {"changed": True, "username": new_username}
