"""Tests for the Phase F Bluesky-only-OAuth keychain migration.

The previous storage shape (`auth_method` absent, `app_password` set)
must not survive into the OAuth-only world: those bundles are deleted
from Keychain and their ``social_accounts`` rows are soft-deleted so
the Settings UI prompts for re-auth via the new Connect with Bluesky
flow.
"""

from __future__ import annotations

import importlib
import json
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
    km = importlib.import_module("yt_scheduler.services.keychain_migration")

    db = await database.get_db()
    await projects.ensure_default_project()
    yield keychain, creds, km, db
    await database.close_db()


async def test_app_password_bundle_is_wiped(app_db) -> None:
    keychain, creds, km, _db = app_db
    legacy = await creds.upsert_credential(
        "bluesky", "ghost.bsky.social", "ghost.bsky.social",
        {"handle": "ghost.bsky.social", "app_password": "abcd-efgh-ijkl-mnop"},
    )
    keychain.delete_secret("bluesky", km.BLUESKY_OAUTH_MARKER_KEY)
    await km._wipe_bluesky_app_password_bundles()

    after = await creds.get_credential_by_uuid(legacy["uuid"])
    assert after is not None
    assert after["deleted_at"] is not None
    assert keychain.load_secret("bluesky", f"cred.{legacy['uuid']}") is None
    assert keychain.load_secret("bluesky", km.BLUESKY_OAUTH_MARKER_KEY) == "1"


async def test_oauth_bundle_is_kept(app_db) -> None:
    keychain, creds, km, _db = app_db
    oauth = await creds.upsert_credential(
        "bluesky", "did:plc:keep", "alive.bsky.social",
        {
            "auth_method": "oauth",
            "handle": "alive.bsky.social",
            "did": "did:plc:keep",
            "pds": "https://pds.test",
            "private_key_pem": "pem",
            "access_token": "atok",
            "refresh_token": "rtok",
            "token_endpoint": "https://bsky.social/oauth/token",
            "redirect_uri": "http://127.0.0.1:8008/api/oauth/bluesky/callback",
        },
    )
    keychain.delete_secret("bluesky", km.BLUESKY_OAUTH_MARKER_KEY)
    await km._wipe_bluesky_app_password_bundles()

    after = await creds.get_credential_by_uuid(oauth["uuid"])
    assert after is not None
    assert after["deleted_at"] is None
    raw = keychain.load_secret("bluesky", f"cred.{oauth['uuid']}")
    assert raw is not None
    assert json.loads(raw)["auth_method"] == "oauth"


async def test_wipe_is_idempotent(app_db) -> None:
    _keychain, creds, km, _db = app_db
    legacy = await creds.upsert_credential(
        "bluesky", "ghost", "ghost",
        {"handle": "ghost.bsky.social", "app_password": "x"},
    )
    await km._wipe_bluesky_app_password_bundles()
    await km._wipe_bluesky_app_password_bundles()  # second run: no-op
    after = await creds.get_credential_by_uuid(legacy["uuid"])
    assert after["deleted_at"] is not None


async def test_wipe_clears_project_default(app_db) -> None:
    keychain, creds, km, db = app_db
    legacy = await creds.upsert_credential(
        "bluesky", "ghost", "ghost",
        {"handle": "ghost.bsky.social", "app_password": "x"},
    )
    await db.execute(
        "INSERT INTO project_social_defaults (project_id, platform, social_account_id) "
        "VALUES (1, 'bluesky', ?)",
        (legacy["id"],),
    )
    await db.commit()

    keychain.delete_secret("bluesky", km.BLUESKY_OAUTH_MARKER_KEY)
    await km._wipe_bluesky_app_password_bundles()

    cursor = await db.execute(
        "SELECT social_account_id FROM project_social_defaults "
        "WHERE project_id = 1 AND platform = 'bluesky'"
    )
    leftover = await cursor.fetchone()
    assert leftover is None
