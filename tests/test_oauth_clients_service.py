"""Cover the per-platform OAuth client storage helper."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
def isolated_oauth_clients(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Reload config + keychain + oauth_clients pointed at a fresh tmp dir,
    with the file backend forced so we don't poke the real macOS Keychain."""
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in (
        "yt_scheduler.services.oauth_clients",
        "yt_scheduler.services.keychain",
        "yt_scheduler.config",
    ):
        sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    yield importlib.import_module("yt_scheduler.services.oauth_clients")


def test_get_oauth_client_returns_empty_when_unset(isolated_oauth_clients) -> None:
    cid, csec = isolated_oauth_clients.get_oauth_client("twitter")
    assert cid == ""
    assert csec == ""


def test_store_and_get_round_trip(isolated_oauth_clients) -> None:
    isolated_oauth_clients.store_oauth_client("linkedin", "abc123", "shh-secret")
    cid, csec = isolated_oauth_clients.get_oauth_client("linkedin")
    assert cid == "abc123"
    assert csec == "shh-secret"


def test_store_with_empty_secret_clears_existing(isolated_oauth_clients) -> None:
    """Twitter is a public client — saving without a secret must wipe a
    stale stored secret instead of leaving it dangling."""
    isolated_oauth_clients.store_oauth_client("twitter", "id1", "old-secret")
    isolated_oauth_clients.store_oauth_client("twitter", "id1", "")
    cid, csec = isolated_oauth_clients.get_oauth_client("twitter")
    assert cid == "id1"
    assert csec == ""


def test_clear_removes_both_fields(isolated_oauth_clients) -> None:
    isolated_oauth_clients.store_oauth_client("threads", "thr-id", "thr-secret")
    isolated_oauth_clients.clear_oauth_client("threads")
    cid, csec = isolated_oauth_clients.get_oauth_client("threads")
    assert cid == ""
    assert csec == ""


def test_has_client_id_reflects_state(isolated_oauth_clients) -> None:
    assert isolated_oauth_clients.has_client_id("twitter") is False
    isolated_oauth_clients.store_oauth_client("twitter", "id1", "")
    assert isolated_oauth_clients.has_client_id("twitter") is True
    isolated_oauth_clients.clear_oauth_client("twitter")
    assert isolated_oauth_clients.has_client_id("twitter") is False


def test_supported_platforms_set(isolated_oauth_clients) -> None:
    """Pin SUPPORTED_PLATFORMS so accidental edits get caught."""
    assert set(isolated_oauth_clients.SUPPORTED_PLATFORMS) == {
        "twitter", "linkedin", "threads"
    }
