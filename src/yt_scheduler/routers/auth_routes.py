"""YouTube authentication routes — per-project."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException, UploadFile

from yt_scheduler.services.auth import (
    DEFAULT_PROJECT_SLUG,
    clear_client_secret,
    clear_credentials,
    get_auth_status,
    has_client_secret,
    run_oauth_flow,
    store_client_secret_from_text,
)

router = APIRouter(prefix="/auth", tags=["auth"])


@router.get("/status")
async def auth_status(project_slug: str = DEFAULT_PROJECT_SLUG):
    """Auth status for the given project.

    get_auth_status does several blocking Keychain reads and may fire a blocking
    Google token refresh, so it must not run on the event loop. One to_thread hop
    covers the whole composition.
    """
    return await asyncio.to_thread(get_auth_status, project_slug)


@router.post("/login")
async def login(project_slug: str = DEFAULT_PROJECT_SLUG):
    """Run the OAuth installed-app flow against ``project_slug``.

    run_local_server blocks for however long the browser dance takes, so we
    offload it to a worker thread so the event loop stays responsive.
    """
    try:
        await asyncio.to_thread(run_oauth_flow, project_slug)
        return {"status": "ok", "message": "Authentication successful"}
    except RuntimeError as e:
        return {"status": "error", "message": str(e)}
    except Exception as e:
        return {"status": "error", "message": f"OAuth flow failed: {e}"}


@router.post("/logout")
async def logout(project_slug: str = DEFAULT_PROJECT_SLUG):
    """Clear stored credentials for one project. Leaves client_secret intact."""
    try:
        await asyncio.to_thread(clear_credentials, project_slug)
        return {"status": "ok", "message": "Credentials removed"}
    except Exception as e:
        return {"status": "error", "message": f"Failed to clear credentials: {e}"}


_MAX_CLIENT_SECRET_BYTES = 64 * 1024  # 64 KiB; client_secret JSON is never larger

@router.post("/upload-client-secret")
async def upload_client_secret(file: UploadFile):
    """Persist the Google Cloud OAuth client JSON to Keychain (no on-disk copy)."""
    content = await file.read(_MAX_CLIENT_SECRET_BYTES + 1)
    if len(content) > _MAX_CLIENT_SECRET_BYTES:
        raise HTTPException(
            413,
            f"File too large: client_secret JSON must be under {_MAX_CLIENT_SECRET_BYTES} bytes",
        )
    try:
        await asyncio.to_thread(store_client_secret_from_text, content.decode("utf-8"))
    except Exception as exc:
        raise HTTPException(400, f"Invalid client_secret JSON: {exc}") from exc
    return {"status": "ok", "message": "Client secret saved to Keychain"}


@router.delete("/client-secret")
async def delete_client_secret():
    """Remove the install-wide OAuth client. All projects' tokens become unusable
    until a new client secret is uploaded and re-auth runs."""
    await asyncio.to_thread(clear_client_secret)
    return {"status": "ok"}


@router.get("/client-secret/status")
async def client_secret_status():
    return {"uploaded": await asyncio.to_thread(has_client_secret)}
