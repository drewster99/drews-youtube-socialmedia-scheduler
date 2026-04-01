"""Authentication routes."""

from __future__ import annotations

from fastapi import APIRouter, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse

from youtube_publisher.config import CLIENT_SECRETS_PATH
from youtube_publisher.services.auth import get_auth_status, is_authenticated, run_oauth_flow

router = APIRouter(prefix="/auth", tags=["auth"])


@router.get("/status")
async def auth_status():
    """Get current authentication status."""
    return get_auth_status()


@router.post("/login")
async def login():
    """Start the OAuth flow."""
    try:
        run_oauth_flow()
        return {"status": "ok", "message": "Authentication successful"}
    except FileNotFoundError as e:
        return {"status": "error", "message": str(e)}
    except Exception as e:
        return {"status": "error", "message": f"OAuth flow failed: {e}"}


@router.post("/upload-client-secret")
async def upload_client_secret(file: UploadFile):
    """Upload the Google Cloud client_secret.json file."""
    content = await file.read()
    CLIENT_SECRETS_PATH.parent.mkdir(parents=True, exist_ok=True)
    CLIENT_SECRETS_PATH.write_bytes(content)
    return {"status": "ok", "message": "Client secret saved"}
