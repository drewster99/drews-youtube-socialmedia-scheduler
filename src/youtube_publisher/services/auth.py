"""YouTube OAuth 2.0 authentication using shared Keychain storage."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from youtube_publisher.config import CLIENT_SECRETS_PATH, CREDENTIALS_PATH, YOUTUBE_SCOPES
from youtube_publisher.services.keychain import (
    delete_secret,
    get_storage_type,
    load_secret,
    store_secret,
)

logger = logging.getLogger(__name__)

YOUTUBE_NAMESPACE = "youtube"
YOUTUBE_CREDS_KEY = "oauth_credentials"


def _save_credentials(creds: Credentials) -> None:
    """Save credentials to Keychain (macOS) or secrets file."""
    store_secret(YOUTUBE_NAMESPACE, YOUTUBE_CREDS_KEY, creds.to_json())

    # Remove legacy file-based credentials if they exist
    if CREDENTIALS_PATH.exists():
        CREDENTIALS_PATH.unlink()
        logger.info("Removed legacy file-based credentials")


def _load_credentials_json() -> str | None:
    """Load credentials JSON from best available source."""
    # Try Keychain/secrets file first
    data = load_secret(YOUTUBE_NAMESPACE, YOUTUBE_CREDS_KEY)
    if data:
        return data

    # Check for legacy file
    if CREDENTIALS_PATH.exists():
        data = CREDENTIALS_PATH.read_text().strip()
        if data:
            # Migrate to Keychain
            store_secret(YOUTUBE_NAMESPACE, YOUTUBE_CREDS_KEY, data)
            CREDENTIALS_PATH.unlink()
            logger.info("Migrated YouTube credentials to secure storage")
            return data

    return None


def get_credentials() -> Credentials | None:
    """Load stored credentials if they exist and are valid."""
    creds_json = _load_credentials_json()
    if not creds_json:
        return None

    try:
        creds_data = json.loads(creds_json)
        creds = Credentials.from_authorized_user_info(creds_data, YOUTUBE_SCOPES)
    except (json.JSONDecodeError, ValueError, KeyError):
        logger.warning("Stored credentials are invalid")
        return None

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            _save_credentials(creds)
        except Exception as e:
            logger.warning(f"Failed to refresh credentials: {e}")
            return None

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
    storage = get_storage_type()

    if not creds:
        return {"authenticated": False, "storage": storage}

    info = {
        "authenticated": True,
        "valid": creds.valid,
        "storage": storage,
    }

    creds_json = _load_credentials_json()
    if creds_json:
        try:
            data = json.loads(creds_json)
            info["client_id"] = data.get("client_id", "")[:20] + "..."
        except json.JSONDecodeError:
            pass

    return info


def clear_credentials() -> None:
    """Remove all stored YouTube credentials."""
    delete_secret(YOUTUBE_NAMESPACE, YOUTUBE_CREDS_KEY)
    if CREDENTIALS_PATH.exists():
        CREDENTIALS_PATH.unlink()
