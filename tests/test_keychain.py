"""Keychain service-name migration and framework-call serialization.

The real macOS Keychain is never touched: round-trip tests run against the
in-memory primitives, and the serialization tests substitute a fake Security
framework underneath the real ``_keychain_set`` / ``_keychain_get``.
"""

from __future__ import annotations

import importlib
import sys
import threading
import time
from pathlib import Path

import pytest

from tests.conftest import install_in_memory_keychain


def _fresh_keychain_module(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Re-import keychain with DATA_DIR (and so SECRETS_FILE) under tmp_path."""
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in ("yt_scheduler.services.keychain", "yt_scheduler.config"):
        sys.modules.pop(mod, None)
    config = importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    assert config.DATA_DIR == tmp_path
    return keychain


@pytest.fixture
def isolated_keychain(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Keychain module with the system primitives replaced by a dict.

    The default for anything that stores or loads: no real Keychain item is
    read or written, and no secret reaches disk.
    """
    keychain = _fresh_keychain_module(monkeypatch, tmp_path)
    install_in_memory_keychain(monkeypatch, keychain)
    yield keychain


@pytest.fixture
def keychain_with_real_primitives(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Keychain module with ``_keychain_set`` / ``_keychain_get`` INTACT.

    Only for tests that exercise those functions themselves; they must
    substitute a fake Security framework via ``_get_sec_lib`` so the real
    Keychain is still never touched. Never call ``load_secret`` /
    ``store_secret`` from a test using this — that would hit the real Keychain.
    """
    keychain = _fresh_keychain_module(monkeypatch, tmp_path)
    monkeypatch.setattr(keychain, "_is_macos", lambda: True)
    yield keychain


def test_legacy_keychain_service_is_migrated_forward(isolated_keychain) -> None:
    """A secret still filed under the pre-rename service ID is returned and
    copied to the current one, so the next read hits the new ID directly."""
    keychain = isolated_keychain
    legacy_service = keychain._legacy_service_name("twitter")
    new_service = keychain._service_name("twitter")

    # Seed the fake Keychain directly under the legacy service ID.
    keychain._keychain_set(legacy_service, "api_key", "secret-key")

    assert keychain.load_secret("twitter", "api_key") == "secret-key"

    # Migrated forward, and the index now records the new service ID.
    assert keychain._keychain_get(new_service, "api_key") == "secret-key"
    assert keychain._index_list_accounts(new_service) == ["api_key"]


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


def test_index_records_only_the_sentinel_never_the_value(isolated_keychain) -> None:
    """The on-disk index is a key list, not a second copy of the secret."""
    keychain = isolated_keychain
    keychain.store_secret("twitter", "api_key", "super-secret-value")

    raw = keychain.SECRETS_FILE.read_text()
    assert "super-secret-value" not in raw
    service = keychain._service_name("twitter")
    assert keychain._load_secrets_file()[service]["api_key"] == keychain.KEYCHAIN_SENTINEL


def test_off_macos_refuses_rather_than_writing_a_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """The invariant this whole change exists to guarantee: with no Keychain,
    a secret is never written to disk — the call raises instead."""
    keychain = _fresh_keychain_module(monkeypatch, tmp_path)
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)

    with pytest.raises(keychain.UnsupportedPlatform):
        keychain.store_secret("twitter", "api_key", "super-secret-value")
    with pytest.raises(keychain.UnsupportedPlatform):
        keychain.load_secret("twitter", "api_key")
    with pytest.raises(keychain.UnsupportedPlatform):
        keychain.delete_secret("twitter", "api_key")

    # Nothing about the secret reached disk — not even the index sentinel.
    assert not keychain.SECRETS_FILE.exists()


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


def test_keychain_set_serializes_framework_calls(keychain_with_real_primitives, monkeypatch) -> None:
    """Regression for the 2026-06 deadlock: two threads inside Security.framework
    at once wedged the whole server. ``_keychain_set`` must let only one thread
    into the framework at a time."""
    keychain = keychain_with_real_primitives
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


def test_keychain_get_serializes_framework_calls(keychain_with_real_primitives, monkeypatch) -> None:
    """The read path takes the same lock — a write and a read must not be inside
    the framework simultaneously either."""
    keychain = keychain_with_real_primitives
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
