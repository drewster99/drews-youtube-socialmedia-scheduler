"""Regression tests for the one-shot Keychain ACL repair migration.

The real macOS Keychain isn't exercised in CI, so these tests model it with an
in-memory fake that tracks each item's ACL owner (``security`` vs the embedded
``python3.12``). The fake makes the *prompt* condition observable: a framework
read of an item the in-process app doesn't own would have prompted the user, so
the fake raises instead — letting us assert the repair never causes one.
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pytest


class FakeKeychain:
    """In-memory Keychain that models per-item ACL ownership.

    Each item is ``{(service, account): (value, owner)}`` where owner is
    ``"security"`` (old scheme, CLI-readable) or ``"app"`` (framework-created,
    in-process-readable). A framework read/create by the app of an item it
    doesn't own is what triggers the macOS prompt — modeled as a raised
    ``PromptedError`` so a stray prompt fails the test loudly.
    """

    class PromptedError(AssertionError):
        pass

    def __init__(self) -> None:
        self.items: dict[tuple[str, str], tuple[str, str]] = {}
        self.cli_reads: list[tuple[str, str]] = []

    def seed_old(self, service: str, account: str, value: str) -> None:
        self.items[(service, account)] = (value, "security")

    # --- CLI primitives (accessing app == /usr/bin/security) ---

    def cli_get(self, service: str, account: str):
        self.cli_reads.append((service, account))
        item = self.items.get((service, account))
        if item is None:
            return None
        value, owner = item
        if owner != "security":
            # Old binary is no longer in this item's ACL → would prompt.
            raise FakeKeychain.PromptedError(
                f"CLI read of app-owned item {service}/{account} would prompt"
            )
        return value

    def delete(self, service: str, account: str) -> bool:
        return self.items.pop((service, account), None) is not None

    def cli_set_trusted(self, service: str, account: str, value: str) -> bool:
        self.items[(service, account)] = (value, "security")
        return True

    # --- framework primitives (accessing app == python3.12) ---

    def fw_set(self, service: str, account: str, value: str) -> bool:
        # SecKeychainAddGenericPassword on a fresh item → default ACL trusts
        # the creating app. On a duplicate it modifies data but preserves the
        # existing ACL (the macOS behavior this migration works around).
        existing = self.items.get((service, account))
        owner = existing[1] if existing is not None else "app"
        self.items[(service, account)] = (value, owner)
        return True

    def fw_get(self, service: str, account: str):
        item = self.items.get((service, account))
        if item is None:
            return None
        value, owner = item
        if owner != "app":
            raise FakeKeychain.PromptedError(
                f"framework read of security-owned item {service}/{account} would prompt"
            )
        return value


@pytest.fixture
def repair_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Fresh keychain + repair modules pointed at a temp data dir, macOS forced."""
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in (
        "yt_scheduler.services.keychain_acl_repair",
        "yt_scheduler.services.keychain",
        "yt_scheduler.config",
    ):
        sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    repair = importlib.import_module("yt_scheduler.services.keychain_acl_repair")

    monkeypatch.setattr(keychain, "_is_macos", lambda: True)

    fake = FakeKeychain()
    monkeypatch.setattr(keychain, "_keychain_get_cli", fake.cli_get)
    monkeypatch.setattr(keychain, "_keychain_delete", fake.delete)
    monkeypatch.setattr(keychain, "_keychain_set_cli_trusted", fake.cli_set_trusted)
    monkeypatch.setattr(keychain, "_keychain_set", fake.fw_set)
    monkeypatch.setattr(keychain, "_keychain_get", fake.fw_get)

    return keychain, repair, fake, tmp_path


def _seed_index(keychain, mapping: dict[str, dict[str, str]]) -> None:
    keychain._save_secrets_file(mapping)


def test_repairs_old_items_to_app_ownership(repair_env) -> None:
    keychain, repair, fake, _ = repair_env
    svc = keychain._service_name("youtube")
    fake.seed_old(svc, "client_secret", "yt-secret")
    fake.seed_old(svc, "credentials.default", "yt-creds")
    _seed_index(keychain, {svc: {"client_secret": "__keychain__", "credentials.default": "__keychain__"}})

    repaired = repair._repair_all_keychain_acls()

    assert repaired == 2
    assert fake.items[(svc, "client_secret")] == ("yt-secret", "app")
    assert fake.items[(svc, "credentials.default")] == ("yt-creds", "app")
    # After repair, in-process reads succeed without prompting.
    assert keychain._keychain_get(svc, "client_secret") == "yt-secret"


def test_is_idempotent_and_does_not_reread_after_done(repair_env) -> None:
    keychain, repair, fake, _ = repair_env
    svc = keychain._service_name("twitter")
    fake.seed_old(svc, "cred.abc", "bundle")
    _seed_index(keychain, {svc: {"cred.abc": "__keychain__"}})

    assert repair._repair_all_keychain_acls() == 1
    reads_after_first = len(fake.cli_reads)

    # Second run is a no-op: the done flag short-circuits before any CLI read,
    # which is critical because the item is now app-owned and a CLI read of it
    # would prompt.
    assert repair._repair_all_keychain_acls() == 0
    assert len(fake.cli_reads) == reads_after_first


def test_resume_skips_already_completed_items(repair_env) -> None:
    keychain, repair, fake, _ = repair_env
    svc = keychain._service_name("mastodon")
    # Simulate a crashed prior run: cred.done was already repaired (app-owned)
    # and recorded as completed; cred.todo is still old-scheme.
    fake.items[(svc, "cred.done")] = ("done-bundle", "app")
    fake.seed_old(svc, "cred.todo", "todo-bundle")
    _seed_index(keychain, {svc: {"cred.done": "__keychain__", "cred.todo": "__keychain__"}})
    repair._save_state(False, {repair._item_id(svc, "cred.done")})

    repaired = repair._repair_all_keychain_acls()

    assert repaired == 1
    # The already-completed app-owned item was never CLI-read (would prompt).
    assert (svc, "cred.done") not in fake.cli_reads
    assert fake.items[(svc, "cred.todo")] == ("todo-bundle", "app")


def test_failed_readd_restores_old_scheme_and_retries(repair_env, monkeypatch) -> None:
    keychain, repair, fake, _ = repair_env
    svc = keychain._service_name("linkedin")
    fake.seed_old(svc, "cred.x", "li-bundle")
    _seed_index(keychain, {svc: {"cred.x": "__keychain__"}})

    monkeypatch.setattr(keychain, "_keychain_set", lambda *a, **k: False)

    repaired = repair._repair_all_keychain_acls()

    assert repaired == 0
    # Secret is preserved in old-scheme form, still CLI-readable.
    assert fake.items[(svc, "cred.x")] == ("li-bundle", "security")
    # Not marked done, so a later boot retries.
    state = json.loads((repair._state_path()).read_text())
    assert repair._item_id(svc, "cred.x") not in state["completed"]


def test_plaintext_fallback_entries_are_skipped(repair_env) -> None:
    keychain, repair, fake, _ = repair_env
    svc = keychain._service_name("threads")
    _seed_index(keychain, {svc: {"username": "drew", "cred.y": "__keychain__"}})
    fake.seed_old(svc, "cred.y", "th-bundle")

    repair._repair_all_keychain_acls()

    # The plaintext "username" value was never treated as a Keychain item.
    assert (svc, "username") not in fake.cli_reads
    assert fake.items[(svc, "cred.y")] == ("th-bundle", "app")


def test_noop_off_macos(repair_env, monkeypatch) -> None:
    keychain, repair, fake, _ = repair_env
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    svc = keychain._service_name("youtube")
    fake.seed_old(svc, "client_secret", "yt-secret")
    _seed_index(keychain, {svc: {"client_secret": "__keychain__"}})

    assert repair._repair_all_keychain_acls() == 0
    assert fake.cli_reads == []
