"""Secure credential storage — macOS Keychain with file fallback.

On macOS: stores secrets in the system Keychain via the `security` CLI.
On other platforms: stores secrets in a JSON file at ~/.youtube-publisher/secrets.json
with restrictive file permissions (600).

Each credential is stored as a separate Keychain item identified by
(service, account) — e.g., ("com.youtube-publisher.twitter", "api_key").
"""

from __future__ import annotations

import json
import logging
import os
import platform
import stat
import subprocess
from pathlib import Path

from youtube_publisher.config import DATA_DIR

logger = logging.getLogger(__name__)

KEYCHAIN_SERVICE_PREFIX = "com.youtube-publisher"
SECRETS_FILE = DATA_DIR / "secrets.json"


# --- macOS Keychain ---


def _is_macos() -> bool:
    return platform.system() == "Darwin"


def _keychain_set(service: str, account: str, value: str) -> bool:
    """Store a value in macOS Keychain."""
    try:
        # Delete existing (ignore errors)
        subprocess.run(
            ["security", "delete-generic-password", "-s", service, "-a", account],
            capture_output=True,
        )
        # -T "" allows any app running as this user to access without prompting.
        # This is required for the launchd background agent, which runs as the user
        # but outside an interactive session — macOS would otherwise block Keychain
        # access with a UI prompt that no one can answer.
        #
        # SECURITY TRADEOFF: this means any process running under this user account
        # can read these credentials without prompting. If tighter access control is
        # needed, replace "" with the full path to the specific binary that should
        # have access (e.g. -T /path/to/youtube-publisher), but note that this will
        # break if the binary location changes (e.g. after venv recreation).
        result = subprocess.run(
            [
                "security", "add-generic-password",
                "-s", service, "-a", account, "-w", value,
                "-U",   # update if exists
                "-T", "",  # allow access from all apps by this user (see above)
            ],
            capture_output=True,
            text=True,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


def _keychain_get(service: str, account: str) -> str | None:
    """Load a value from macOS Keychain."""
    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-s", service, "-a", account, "-w"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return result.stdout.strip()
        return None
    except FileNotFoundError:
        return None


def _keychain_delete(service: str, account: str) -> bool:
    """Delete a value from macOS Keychain."""
    try:
        result = subprocess.run(
            ["security", "delete-generic-password", "-s", service, "-a", account],
            capture_output=True,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


def _keychain_find_all(service: str) -> list[str]:
    """Find all account names for a Keychain service.

    Uses `security dump-keychain` which is slow but there's no better option.
    Falls back to tracking accounts in the secrets file index.
    """
    # We track account names in a local index to avoid parsing dump-keychain
    return _file_list_accounts(service)


# --- File-based storage (fallback + account index) ---


def _load_secrets_file() -> dict:
    """Load the secrets JSON file."""
    if not SECRETS_FILE.exists():
        return {}
    try:
        return json.loads(SECRETS_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _save_secrets_file(data: dict) -> None:
    """Save the secrets JSON file with restrictive permissions."""
    SECRETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    SECRETS_FILE.write_text(json.dumps(data, indent=2))
    # Set file permissions to owner-only read/write
    os.chmod(SECRETS_FILE, stat.S_IRUSR | stat.S_IWUSR)


def _file_set(service: str, account: str, value: str) -> None:
    """Store a value in the secrets file."""
    data = _load_secrets_file()
    if service not in data:
        data[service] = {}
    data[service][account] = value
    _save_secrets_file(data)


def _file_get(service: str, account: str) -> str | None:
    """Load a value from the secrets file."""
    data = _load_secrets_file()
    return data.get(service, {}).get(account)


def _file_delete(service: str, account: str) -> None:
    """Delete a value from the secrets file."""
    data = _load_secrets_file()
    if service in data and account in data[service]:
        del data[service][account]
        if not data[service]:
            del data[service]
        _save_secrets_file(data)


def _file_list_accounts(service: str) -> list[str]:
    """List all account names for a service in the secrets file."""
    data = _load_secrets_file()
    return list(data.get(service, {}).keys())


# --- Public API ---


def _service_name(namespace: str) -> str:
    """Build a Keychain service name from a namespace."""
    return f"{KEYCHAIN_SERVICE_PREFIX}.{namespace}"


def store_secret(namespace: str, key: str, value: str) -> None:
    """Store a secret credential.

    Args:
        namespace: Credential group (e.g., "youtube", "twitter", "bluesky")
        key: Credential key (e.g., "access_token", "api_key")
        value: The secret value
    """
    service = _service_name(namespace)

    if _is_macos():
        if _keychain_set(service, key, value):
            # Also store key name in file index (not the value) for listing
            data = _load_secrets_file()
            if service not in data:
                data[service] = {}
            data[service][key] = "__keychain__"
            _save_secrets_file(data)
            return
        logger.warning(f"Keychain store failed for {namespace}/{key}, using file fallback")

    _file_set(service, key, value)


def load_secret(namespace: str, key: str) -> str | None:
    """Load a secret credential.

    Args:
        namespace: Credential group
        key: Credential key

    Returns:
        The secret value, or None if not found.
    """
    service = _service_name(namespace)

    if _is_macos():
        value = _keychain_get(service, key)
        if value is not None:
            return value

    # Fallback to file
    value = _file_get(service, key)
    if value and value != "__keychain__":
        # Migrate to Keychain if on macOS
        if _is_macos():
            if _keychain_set(service, key, value):
                data = _load_secrets_file()
                if service in data and key in data[service]:
                    data[service][key] = "__keychain__"
                    _save_secrets_file(data)
                logger.info(f"Migrated {namespace}/{key} to Keychain")
        return value

    return None


def delete_secret(namespace: str, key: str) -> None:
    """Delete a secret credential."""
    service = _service_name(namespace)
    if _is_macos():
        _keychain_delete(service, key)
    _file_delete(service, key)


def load_all_secrets(namespace: str) -> dict[str, str]:
    """Load all secrets for a namespace.

    Returns:
        Dict of key -> value for all stored credentials in this namespace.
    """
    service = _service_name(namespace)
    accounts = _file_list_accounts(service)
    result = {}
    for account in accounts:
        value = load_secret(namespace, account)
        if value:
            result[account] = value
    return result


def delete_all_secrets(namespace: str) -> None:
    """Delete all secrets for a namespace."""
    service = _service_name(namespace)
    accounts = _file_list_accounts(service)
    for account in accounts:
        delete_secret(namespace, account)


def get_storage_type() -> str:
    """Return the active storage backend name."""
    return "keychain" if _is_macos() else "file"
