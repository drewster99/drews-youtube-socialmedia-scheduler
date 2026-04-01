"""YouTube OAuth 2.0 authentication with macOS Keychain support."""

from __future__ import annotations

import json
import logging
import platform
import subprocess
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from youtube_publisher.config import CLIENT_SECRETS_PATH, CREDENTIALS_PATH, YOUTUBE_SCOPES

logger = logging.getLogger(__name__)

KEYCHAIN_SERVICE = "com.youtube-publisher.oauth"
KEYCHAIN_ACCOUNT = "youtube-credentials"


# --- Keychain storage (macOS) ---


def _is_macos() -> bool:
    return platform.system() == "Darwin"


def _keychain_store(data: str) -> bool:
    """Store credentials in macOS Keychain. Returns True on success."""
    try:
        # Delete existing entry first (ignore errors if it doesn't exist)
        subprocess.run(
            ["security", "delete-generic-password", "-s", KEYCHAIN_SERVICE, "-a", KEYCHAIN_ACCOUNT],
            capture_output=True,
        )
        # Add new entry
        result = subprocess.run(
            [
                "security", "add-generic-password",
                "-s", KEYCHAIN_SERVICE,
                "-a", KEYCHAIN_ACCOUNT,
                "-w", data,
                "-U",  # update if exists
            ],
            capture_output=True,
            text=True,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


def _keychain_load() -> str | None:
    """Load credentials from macOS Keychain. Returns JSON string or None."""
    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-s", KEYCHAIN_SERVICE, "-a", KEYCHAIN_ACCOUNT, "-w"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return result.stdout.strip()
        return None
    except FileNotFoundError:
        return None


def _keychain_delete() -> bool:
    """Delete credentials from macOS Keychain."""
    try:
        result = subprocess.run(
            ["security", "delete-generic-password", "-s", KEYCHAIN_SERVICE, "-a", KEYCHAIN_ACCOUNT],
            capture_output=True,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


# --- Credential storage (Keychain on macOS, file fallback) ---


def _save_credentials(creds: Credentials) -> None:
    """Save credentials — Keychain on macOS, file on other platforms."""
    creds_json = creds.to_json()

    if _is_macos():
        if _keychain_store(creds_json):
            logger.info("Credentials stored in macOS Keychain")
            # Remove file-based credentials if they exist (migration)
            if CREDENTIALS_PATH.exists():
                CREDENTIALS_PATH.unlink()
                logger.info("Removed legacy file-based credentials")
            return
        logger.warning("Keychain storage failed, falling back to file")

    # Fallback: file storage
    CREDENTIALS_PATH.parent.mkdir(parents=True, exist_ok=True)
    CREDENTIALS_PATH.write_text(creds_json)


def _load_credentials_json() -> str | None:
    """Load credentials JSON from best available source."""
    # Try Keychain first on macOS
    if _is_macos():
        data = _keychain_load()
        if data:
            return data

    # Fallback: file
    if CREDENTIALS_PATH.exists():
        data = CREDENTIALS_PATH.read_text().strip()
        if data:
            # Migrate to Keychain if on macOS
            if _is_macos():
                if _keychain_store(data):
                    CREDENTIALS_PATH.unlink()
                    logger.info("Migrated credentials from file to Keychain")
            return data

    return None


# --- Public API ---


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
    if not creds:
        return {"authenticated": False, "storage": "keychain" if _is_macos() else "file"}

    info = {
        "authenticated": True,
        "valid": creds.valid,
        "storage": "keychain" if _is_macos() else "file",
    }

    # Try to get client_id for display
    creds_json = _load_credentials_json()
    if creds_json:
        try:
            data = json.loads(creds_json)
            info["client_id"] = data.get("client_id", "")[:20] + "..."
        except json.JSONDecodeError:
            pass

    return info


def clear_credentials() -> None:
    """Remove all stored credentials."""
    if _is_macos():
        _keychain_delete()
    if CREDENTIALS_PATH.exists():
        CREDENTIALS_PATH.unlink()
