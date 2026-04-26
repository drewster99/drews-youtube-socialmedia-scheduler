"""Secure credential storage — macOS Keychain with file fallback.

On macOS: stores secrets in the system Keychain via the `security` CLI.
On other platforms: stores secrets in a JSON file at ~/.drews-yt-scheduler/secrets.json
with restrictive file permissions (600).

Each credential is stored as a separate Keychain item identified by
(service, account) — e.g.,
("com.nuclearcyborg.drews-socialmedia-scheduler.twitter", "api_key").
The legacy ``com.youtube-publisher.*`` namespace is read on miss and migrated
forward transparently.
"""

from __future__ import annotations

import json
import logging
import os
import platform
import stat
import subprocess

from yt_scheduler.config import DATA_DIR

logger = logging.getLogger(__name__)

KEYCHAIN_SERVICE_PREFIX = "com.nuclearcyborg.drews-socialmedia-scheduler"
LEGACY_KEYCHAIN_SERVICE_PREFIX = "com.youtube-publisher"
SECRETS_FILE = DATA_DIR / "secrets.json"


# --- macOS Keychain ---


def _is_macos() -> bool:
    return platform.system() == "Darwin"


def _keychain_set(service: str, account: str, value: str) -> bool:
    """Store a value in macOS Keychain.

    We delete + re-add (rather than using -U alone) so that stale ACL entries
    from prior runs don't accumulate. The new item is created with
    `-T /usr/bin/security` trusted, which means later reads via
    `security find-generic-password` (the same Apple-signed binary, run as a
    subprocess from Python) don't trigger "app wants to use your confidential
    information" prompts.

    `-T ""` — the previous value — sounds like "allow all apps" but actually
    produces an empty trusted-app ACL, forcing macOS to prompt every read.
    """
    try:
        # Delete existing (ignore errors) so the new ACL replaces any stale one.
        subprocess.run(
            ["security", "delete-generic-password", "-s", service, "-a", account],
            capture_output=True,
        )
        result = subprocess.run(
            [
                "security", "add-generic-password",
                "-s", service, "-a", account, "-w", value,
                "-U",                        # update if exists (belt-and-suspenders after delete)
                "-T", "/usr/bin/security",   # trust the CLI we use to read it back
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


def _legacy_service_name(namespace: str) -> str:
    """Build the pre-rename Keychain service name."""
    return f"{LEGACY_KEYCHAIN_SERVICE_PREFIX}.{namespace}"


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

    Tries the current Keychain service ID first, then the legacy ID
    (`com.youtube-publisher.*`) and the file fallback. When a value is found
    under the legacy ID, it is migrated forward to the new ID transparently.

    Args:
        namespace: Credential group
        key: Credential key

    Returns:
        The secret value, or None if not found.
    """
    service = _service_name(namespace)
    legacy_service = _legacy_service_name(namespace)

    if _is_macos():
        value = _keychain_get(service, key)
        if value is not None:
            return value

        # Read-fallback to legacy Keychain service ID and migrate forward.
        legacy_value = _keychain_get(legacy_service, key)
        if legacy_value is not None:
            if _keychain_set(service, key, legacy_value):
                data = _load_secrets_file()
                data.setdefault(service, {})[key] = "__keychain__"
                _save_secrets_file(data)
                logger.info("Migrated %s/%s from legacy Keychain ID", namespace, key)
            return legacy_value

    # Fallback to file (current then legacy service name)
    value = _file_get(service, key)
    if value is None:
        value = _file_get(legacy_service, key)
        if value is not None and value != "__keychain__":
            # Move legacy file entry forward
            _file_set(service, key, value)
            _file_delete(legacy_service, key)
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
