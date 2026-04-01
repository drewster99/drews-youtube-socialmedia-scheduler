"""YouTube OAuth 2.0 authentication."""

from __future__ import annotations

import json
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from youtube_publisher.config import CLIENT_SECRETS_PATH, CREDENTIALS_PATH, YOUTUBE_SCOPES


def get_credentials() -> Credentials | None:
    """Load stored credentials if they exist and are valid."""
    if not CREDENTIALS_PATH.exists():
        return None

    creds = Credentials.from_authorized_user_file(str(CREDENTIALS_PATH), YOUTUBE_SCOPES)

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        _save_credentials(creds)

    if creds and creds.valid:
        return creds

    return None


def run_oauth_flow(client_secrets_path: str | Path | None = None) -> Credentials:
    """Run the OAuth installed app flow (opens browser)."""
    secrets_path = Path(client_secrets_path) if client_secrets_path else CLIENT_SECRETS_PATH

    if not secrets_path.exists():
        raise FileNotFoundError(
            f"Client secrets file not found at {secrets_path}. "
            "Download it from Google Cloud Console → APIs & Services → Credentials."
        )

    flow = InstalledAppFlow.from_client_secrets_file(str(secrets_path), YOUTUBE_SCOPES)
    creds = flow.run_local_server(port=8090, prompt="consent", access_type="offline")

    _save_credentials(creds)
    return creds


def _save_credentials(creds: Credentials) -> None:
    """Save credentials to disk."""
    CREDENTIALS_PATH.parent.mkdir(parents=True, exist_ok=True)
    CREDENTIALS_PATH.write_text(creds.to_json())


def is_authenticated() -> bool:
    """Check if valid credentials exist."""
    return get_credentials() is not None


def get_youtube_service():
    """Build and return an authenticated YouTube API service."""
    creds = get_credentials()
    if not creds:
        raise RuntimeError("Not authenticated. Run the OAuth flow first.")
    return build("youtube", "v3", credentials=creds)


def get_auth_status() -> dict:
    """Get current authentication status info."""
    creds = get_credentials()
    if not creds:
        return {"authenticated": False}

    info = {"authenticated": True, "valid": creds.valid}

    if CREDENTIALS_PATH.exists():
        data = json.loads(CREDENTIALS_PATH.read_text())
        info["client_id"] = data.get("client_id", "")[:20] + "..."

    return info
