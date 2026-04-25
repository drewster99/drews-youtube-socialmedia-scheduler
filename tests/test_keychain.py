"""Keychain rename fallback — file backend only (macOS Keychain not exercised in CI)."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
def isolated_keychain(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Return a freshly imported keychain module pointing at tmp_path / secrets.json."""
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    # Reload config + keychain so DATA_DIR + SECRETS_FILE pick up the new env var.
    for mod in ("yt_scheduler.services.keychain", "yt_scheduler.config"):
        sys.modules.pop(mod, None)
    config = importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    # Force the file backend even on macOS so the test doesn't poke the real Keychain.
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    assert config.DATA_DIR == tmp_path
    yield keychain


def test_legacy_file_entry_is_migrated_forward(isolated_keychain) -> None:
    keychain = isolated_keychain

    legacy_service = keychain._legacy_service_name("twitter")
    keychain._file_set(legacy_service, "api_key", "secret-key")

    value = keychain.load_secret("twitter", "api_key")
    assert value == "secret-key"

    # Legacy entry should be gone, new entry present
    new_service = keychain._service_name("twitter")
    assert keychain._file_get(legacy_service, "api_key") is None
    assert keychain._file_get(new_service, "api_key") == "secret-key"


def test_store_uses_new_service_name(isolated_keychain) -> None:
    keychain = isolated_keychain
    keychain.store_secret("bluesky", "handle", "me.bsky.social")
    assert keychain.load_secret("bluesky", "handle") == "me.bsky.social"
    assert keychain._service_name("bluesky").startswith(
        "com.nuclearcyborg.drews-socialmedia-scheduler"
    )


def test_load_returns_none_for_missing(isolated_keychain) -> None:
    keychain = isolated_keychain
    assert keychain.load_secret("twitter", "api_key") is None
