"""Keychain rename fallback — file backend only (macOS Keychain not exercised in CI)."""

from __future__ import annotations

import importlib
import sys
import threading
import time
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


class _FakeSecLib:
    """Stand-in for the Security framework that records how many threads are
    inside a framework call at once. Apple's real SecKeychain* API deadlocks
    when entered concurrently from one process; the production code guards
    against that with ``_keychain_framework_lock``. This fake lets the
    regression test assert that guard actually serializes callers — without
    touching the real Keychain (no password prompts).
    """

    def __init__(self, hold_secs: float = 0.02) -> None:
        self._hold_secs = hold_secs
        self._counter_lock = threading.Lock()  # guards the counters only
        self._active = 0
        self.max_active = 0

    def _enter_framework(self) -> int:
        with self._counter_lock:
            self._active += 1
            self.max_active = max(self.max_active, self._active)
        # Hold the "inside the framework" window open long enough that an
        # unserialized caller would overlap and push max_active above 1.
        time.sleep(self._hold_secs)
        with self._counter_lock:
            self._active -= 1
        return 0

    def SecKeychainAddGenericPassword(self, *args) -> int:
        return self._enter_framework()  # 0 = success, no duplicate

    def SecKeychainFindGenericPassword(self, *args) -> int:
        return self._enter_framework()  # 0 = found; out-params left null → b""

    def SecKeychainItemModifyAttributesAndData(self, *args) -> int:
        return 0

    def SecKeychainItemFreeContent(self, *args) -> int:
        return 0


def _run_concurrently(target, count: int = 8, join_timeout: float = 10.0) -> list:
    threads = [threading.Thread(target=target) for _ in range(count)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=join_timeout)
    return threads


def test_keychain_set_serializes_framework_calls(isolated_keychain, monkeypatch) -> None:
    """Regression for the 2026-06 deadlock: two threads inside Security.framework
    at once wedged the whole server. ``_keychain_set`` must let only one thread
    into the framework at a time."""
    keychain = isolated_keychain
    fake = _FakeSecLib()
    monkeypatch.setattr(keychain, "_get_sec_lib", lambda: fake)

    threads = _run_concurrently(
        lambda: keychain._keychain_set("svc", "acct", "value"),
    )

    assert all(not t.is_alive() for t in threads), "keychain writes deadlocked"
    assert fake.max_active == 1, (
        f"Security framework entered by {fake.max_active} threads at once; "
        "_keychain_framework_lock is not serializing writes"
    )


def test_keychain_get_serializes_framework_calls(isolated_keychain, monkeypatch) -> None:
    """The read path takes the same lock — a write and a read must not be inside
    the framework simultaneously either."""
    keychain = isolated_keychain
    fake = _FakeSecLib()
    monkeypatch.setattr(keychain, "_get_sec_lib", lambda: fake)

    threads = _run_concurrently(
        lambda: keychain._keychain_get("svc", "acct"),
    )

    assert all(not t.is_alive() for t in threads), "keychain reads deadlocked"
    assert fake.max_active == 1, (
        f"Security framework entered by {fake.max_active} threads at once; "
        "_keychain_framework_lock is not serializing reads"
    )
