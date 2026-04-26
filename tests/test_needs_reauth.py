"""Tests for the needs_reauth flag flow.

A credential is marked ``needs_reauth`` whenever a poster raises
:class:`CredentialAuthError`. The flag is cleared on the next
successful :func:`upsert_credential` (i.e. after the user re-OAuths).
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
async def app_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    creds = importlib.import_module("yt_scheduler.services.social_credentials")

    db = await database.get_db()
    await projects.ensure_default_project()
    yield creds, db
    await database.close_db()


async def test_migration_009_adds_needs_reauth_column(app_db) -> None:
    _creds, db = app_db
    cursor = await db.execute("PRAGMA table_info(social_accounts)")
    cols = {row[1] for row in await cursor.fetchall()}
    assert "needs_reauth" in cols


async def test_fresh_credential_is_not_needs_reauth(app_db) -> None:
    creds, _db = app_db
    cred = await creds.upsert_credential(
        "twitter", "tw:1", "alice", {"bearer_token": "tok"}
    )
    assert cred["needs_reauth"] is False


async def test_mark_needs_reauth_sets_flag(app_db) -> None:
    creds, _db = app_db
    cred = await creds.upsert_credential(
        "twitter", "tw:1", "alice", {"bearer_token": "tok"}
    )
    await creds.mark_needs_reauth(cred["uuid"])
    after = await creds.get_credential_by_uuid(cred["uuid"])
    assert after["needs_reauth"] is True


async def test_upsert_clears_needs_reauth(app_db) -> None:
    """Re-OAuth (which round-trips through upsert_credential for the
    same provider_account_id) must clear the flag automatically."""
    creds, _db = app_db
    cred = await creds.upsert_credential(
        "twitter", "tw:1", "alice", {"bearer_token": "tok"}
    )
    await creds.mark_needs_reauth(cred["uuid"])
    refreshed = await creds.upsert_credential(
        "twitter", "tw:1", "alice",
        {"bearer_token": "newtok", "refresh_token": "rfr"},
    )
    assert refreshed["needs_reauth"] is False


async def test_credential_listing_includes_flag(app_db) -> None:
    creds, _db = app_db
    a = await creds.upsert_credential(
        "twitter", "tw:1", "a", {"bearer_token": "ta"}
    )
    b = await creds.upsert_credential(
        "twitter", "tw:2", "b", {"bearer_token": "tb"}
    )
    await creds.mark_needs_reauth(a["uuid"])
    listed = await creds.list_credentials(platform="twitter")
    by_uuid = {c["uuid"]: c for c in listed}
    assert by_uuid[a["uuid"]]["needs_reauth"] is True
    assert by_uuid[b["uuid"]]["needs_reauth"] is False


async def test_credential_auth_error_carries_uuid(app_db) -> None:
    """The exception class is the contract between posters and
    routes — losing the UUID would mean we can't mark the right
    credential as needs_reauth."""
    from yt_scheduler.services.social import CredentialAuthError

    err = CredentialAuthError("abc-123", "boom")
    assert err.uuid == "abc-123"
    assert "boom" in str(err)


async def test_mark_on_unknown_uuid_is_silent(app_db) -> None:
    """Marking a uuid that doesn't exist must not raise — the route
    handler runs this on a best-effort basis and a missing row
    shouldn't blow up the response."""
    creds, _db = app_db
    await creds.mark_needs_reauth("does-not-exist")
