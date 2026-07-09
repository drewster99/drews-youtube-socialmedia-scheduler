"""The keychain module must never leak a secret onto the process argv, and must
never report a corrupt secrets index as "no credentials".

`security add-generic-password -w <value>` puts the secret in the process table
where any local process can read it. There is no safe non-argv CLI write, so a
wedged or missing Security framework has to surface as an error instead.
"""

from __future__ import annotations

import importlib
import json
import sys
import threading
from pathlib import Path

import pytest


@pytest.fixture
def keychain(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Freshly imported keychain module pointing at tmp_path / secrets.json."""
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in ("yt_scheduler.services.keychain", "yt_scheduler.config"):
        sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    module = importlib.import_module("yt_scheduler.services.keychain")
    yield module
    for mod in ("yt_scheduler.services.keychain", "yt_scheduler.config"):
        sys.modules.pop(mod, None)


@pytest.fixture
def no_subprocess(keychain, monkeypatch: pytest.MonkeyPatch):
    """Fail loudly if anything shells out — that is the argv-leak path."""

    def _forbidden(*args, **kwargs):
        raise AssertionError(f"subprocess invoked with secret on argv: {args!r}")

    monkeypatch.setattr(keychain.subprocess, "run", _forbidden)


class _FakeSecLib:
    """Stands in for Security.framework; never actually called in these tests."""

    def SecKeychainAddGenericPassword(self, *args):  # noqa: N802
        return 0


def test_missing_framework_raises_instead_of_argv_cli(keychain, no_subprocess, monkeypatch):
    monkeypatch.setattr(keychain, "_get_sec_lib", lambda: None)

    with pytest.raises(keychain.KeychainWriteError) as excinfo:
        keychain._keychain_set("svc", "acct", "s3cret")

    assert "argv" in str(excinfo.value)


def test_wedged_lock_raises_instead_of_argv_cli(keychain, no_subprocess, monkeypatch):
    """Lock contention is exactly when the old code leaked the secret."""
    monkeypatch.setattr(keychain, "_get_sec_lib", lambda: _FakeSecLib())
    monkeypatch.setattr(keychain, "_KEYCHAIN_FRAMEWORK_LOCK_TIMEOUT_SECS", 0.05)

    holder_has_lock = threading.Event()
    release_holder = threading.Event()

    def _hold() -> None:
        # RLock is reentrant, so the lock must be held from another thread for
        # the acquire timeout to actually fire.
        with keychain._keychain_framework_lock:
            holder_has_lock.set()
            release_holder.wait(timeout=5)

    holder = threading.Thread(target=_hold, daemon=True)
    holder.start()
    assert holder_has_lock.wait(timeout=5)
    try:
        with pytest.raises(keychain.KeychainWriteError) as excinfo:
            keychain._keychain_set("svc", "acct", "s3cret")
    finally:
        release_holder.set()
        holder.join(timeout=5)

    assert "wedged" in str(excinfo.value)


def test_missing_index_is_the_empty_first_run_state(keychain):
    assert not keychain.SECRETS_FILE.exists()
    assert keychain._load_secrets_file() == {}


@pytest.mark.parametrize("payload", ["{not json", "[1, 2, 3]", '"a string"'])
def test_corrupt_index_raises_rather_than_reading_as_empty(keychain, payload):
    keychain.SECRETS_FILE.write_text(payload)

    with pytest.raises(keychain.SecretsIndexError):
        keychain._load_secrets_file()


def test_corrupt_index_surfaces_through_enumerating_helpers(keychain, monkeypatch):
    """A corrupt index must not make export/load-all quietly report zero secrets."""
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    keychain.SECRETS_FILE.write_text("{corrupt")

    with pytest.raises(keychain.SecretsIndexError):
        keychain.load_all_secrets("ns")
    with pytest.raises(keychain.SecretsIndexError):
        keychain.export_all_secrets()
    with pytest.raises(keychain.SecretsIndexError):
        keychain.delete_all_secrets("ns")


def test_valid_index_still_loads(keychain, monkeypatch):
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    keychain.SECRETS_FILE.write_text(json.dumps({"svc": {"k": "v"}}))

    assert keychain._load_secrets_file() == {"svc": {"k": "v"}}


def test_wedged_keychain_does_not_fail_a_read(keychain, monkeypatch):
    """Forward-migration is opportunistic; a write problem must not break load_secret."""
    monkeypatch.setattr(keychain, "_is_macos", lambda: True)

    service = keychain._service_name("ns")
    legacy_service = keychain._legacy_service_name("ns")

    def fake_get(svc: str, key: str):
        return "legacy-value" if svc == legacy_service else None

    def wedged_set(svc: str, key: str, value: str):
        raise keychain.KeychainWriteError("wedged")

    monkeypatch.setattr(keychain, "_keychain_get", fake_get)
    monkeypatch.setattr(keychain, "_keychain_set", wedged_set)

    assert keychain.load_secret("ns", "k") == "legacy-value"
    assert service != legacy_service
