"""Tests for ``services/social_credentials.py``.

Covers list/get/upsert/soft-delete and the dependents query feeding the
delete-confirmation dialog. Forces the keychain backend to the file-based
fallback so the tests don't poke the real macOS Keychain.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
async def app_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Reload config + database + keychain pointed at a tmp data dir.

    Returns ``(creds_module, social_module, db)``. Keychain is forced to the
    file backend so credential bundles round-trip through ``secrets.json``
    rather than the real Keychain.
    """
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in (
        "yt_scheduler.services.social_credentials",
        "yt_scheduler.services.social",
        "yt_scheduler.services.keychain",
        "yt_scheduler.services.projects",
        "yt_scheduler.database",
        "yt_scheduler.config",
    ):
        sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")
    creds = importlib.import_module("yt_scheduler.services.social_credentials")
    social = importlib.import_module("yt_scheduler.services.social")

    db = await database.get_db()
    await projects.ensure_default_project()
    yield creds, social, db
    await database.close_db()


async def test_upsert_inserts_fresh_credential(app_db) -> None:
    creds, _social, _db = app_db
    row = await creds.upsert_credential(
        platform="twitter",
        provider_account_id="123456",
        username="someone",
        bundle={"bearer_token": "tok-abc", "refresh_token": "rfr"},
    )
    assert row["platform"] == "twitter"
    assert row["provider_account_id"] == "123456"
    assert row["username"] == "someone"
    assert row["uuid"] and row["uuid"] != "__pending__:1"
    assert row["deleted_at"] is None
    assert row["label"] == "@someone @X"

    bundle = creds.load_bundle("twitter", row["uuid"])
    assert bundle is not None
    assert bundle["bearer_token"] == "tok-abc"
    assert bundle["provider_account_id"] == "123456"


async def test_upsert_updates_existing_provider_pair(app_db) -> None:
    """Same (platform, provider_account_id) → updates row in place."""
    creds, _social, _db = app_db
    first = await creds.upsert_credential(
        platform="twitter",
        provider_account_id="123",
        username="oldname",
        bundle={"bearer_token": "tok1"},
    )
    second = await creds.upsert_credential(
        platform="twitter",
        provider_account_id="123",
        username="newname",
        bundle={"bearer_token": "tok2"},
    )
    assert first["id"] == second["id"]
    assert first["uuid"] == second["uuid"]
    assert second["username"] == "newname"

    bundle = creds.load_bundle("twitter", second["uuid"])
    assert bundle["bearer_token"] == "tok2"


async def test_upsert_undeletes_soft_deleted(app_db) -> None:
    creds, _social, _db = app_db
    row = await creds.upsert_credential(
        platform="bluesky",
        provider_account_id="did:plc:abc",
        username="me.bsky.social",
        bundle={"handle": "me.bsky.social", "app_password": "xxx"},
    )
    await creds.soft_delete_credential(row["uuid"])

    deleted = await creds.get_credential_by_uuid(row["uuid"])
    assert deleted["deleted_at"] is not None

    revived = await creds.upsert_credential(
        platform="bluesky",
        provider_account_id="did:plc:abc",
        username="me.bsky.social",
        bundle={"handle": "me.bsky.social", "app_password": "yyy"},
    )
    assert revived["id"] == row["id"]
    assert revived["deleted_at"] is None
    assert creds.load_bundle("bluesky", revived["uuid"])["app_password"] == "yyy"


async def test_list_filters_deleted_by_default(app_db) -> None:
    creds, _social, _db = app_db
    a = await creds.upsert_credential("twitter", "1", "a", {"bearer_token": "ta"})
    b = await creds.upsert_credential("twitter", "2", "b", {"bearer_token": "tb"})
    await creds.soft_delete_credential(a["uuid"])

    active = await creds.list_credentials(platform="twitter")
    assert [c["uuid"] for c in active] == [b["uuid"]]

    everything = await creds.list_credentials(platform="twitter", include_deleted=True)
    uuids = {c["uuid"] for c in everything}
    assert a["uuid"] in uuids and b["uuid"] in uuids


async def test_soft_delete_removes_bundle_and_clears_default(app_db) -> None:
    creds, _social, db = app_db
    row = await creds.upsert_credential(
        "linkedin", "urn:li:person:1", "me",
        {"access_token": "tok", "person_urn": "urn:li:person:1"},
    )
    await db.execute(
        "INSERT INTO project_social_defaults (project_id, platform, social_account_id) "
        "VALUES (1, 'linkedin', ?)",
        (row["id"],),
    )
    await db.commit()

    deleted = await creds.soft_delete_credential(row["uuid"])
    assert deleted is not None
    assert deleted["deleted_at"] is not None

    assert creds.load_bundle("linkedin", row["uuid"]) is None

    cursor = await db.execute(
        "SELECT social_account_id FROM project_social_defaults "
        "WHERE project_id = 1 AND platform = 'linkedin'"
    )
    leftover = await cursor.fetchone()
    assert leftover is None, (
        "default row should be deleted (not just nulled) by soft_delete_credential"
    )


async def test_get_dependents_lists_projects_and_slots(app_db) -> None:
    creds, _social, db = app_db
    row = await creds.upsert_credential(
        "twitter", "111", "user", {"bearer_token": "tok"}
    )

    # Project default
    await db.execute(
        "INSERT INTO project_social_defaults (project_id, platform, social_account_id) "
        "VALUES (1, 'twitter', ?)",
        (row["id"],),
    )
    # Template + slot
    await db.execute(
        "INSERT INTO templates (project_id, name, applies_to) "
        "VALUES (1, 'my_template', '[\"video\"]')"
    )
    cursor = await db.execute("SELECT id FROM templates WHERE name = 'my_template'")
    tid = (await cursor.fetchone())["id"]
    await db.execute(
        "INSERT INTO template_slots "
        "(template_id, platform, social_account_id, body, max_chars) "
        "VALUES (?, 'twitter', ?, 'body', 280)",
        (tid, row["id"]),
    )
    await db.commit()

    deps = await creds.get_dependents(row["uuid"])
    assert len(deps["projects"]) == 1
    assert deps["projects"][0]["slug"] == "default"
    assert deps["projects"][0]["platform"] == "twitter"
    assert len(deps["slots"]) == 1
    assert deps["slots"][0]["template_name"] == "my_template"
    assert deps["slots"][0]["platform"] == "twitter"


async def test_get_credential_by_uuid_returns_none_when_missing(app_db) -> None:
    creds, _social, _db = app_db
    assert await creds.get_credential_by_uuid("nope") is None


async def test_format_account_label(app_db) -> None:
    creds, _social, _db = app_db
    assert creds.format_account_label("twitter", "alice") == "@alice @X"
    assert creds.format_account_label("bluesky", "@bob.bsky.social") == "@bob.bsky.social @Bluesky"
    assert creds.format_account_label("threads", None) == "@threads @Threads"


async def test_get_first_active_credential(app_db) -> None:
    creds, _social, _db = app_db
    a = await creds.upsert_credential("twitter", "1", "a", {"bearer_token": "ta"})
    b = await creds.upsert_credential("twitter", "2", "b", {"bearer_token": "tb"})
    first = await creds.get_first_active_credential("twitter")
    assert first["id"] == a["id"]

    await creds.soft_delete_credential(a["uuid"])
    first = await creds.get_first_active_credential("twitter")
    assert first["id"] == b["id"]

    await creds.soft_delete_credential(b["uuid"])
    assert await creds.get_first_active_credential("twitter") is None


async def test_get_poster_for_account_round_trip(app_db) -> None:
    creds, social, _db = app_db
    # Bluesky bundles must carry the full OAuth shape post-Phase F. The
    # round-trip here doesn't exercise an actual post — it only verifies
    # that the poster reads its bundle back and reports as configured.
    row = await creds.upsert_credential(
        "bluesky", "did:plc:abc", "me.bsky.social",
        {
            "auth_method": "oauth",
            "handle": "me.bsky.social",
            "did": "did:plc:abc",
            "pds": "https://example.test",
            "private_key_pem": "fake-pem",
            "access_token": "tok",
            "refresh_token": "rfr",
            "token_endpoint": "https://example.test/token",
            "redirect_uri": "http://127.0.0.1:8008/api/oauth/bluesky/callback",
        },
    )
    poster = await social.get_poster_for_account(row["id"])
    assert poster.platform == "bluesky"
    assert await poster.is_configured()


async def test_get_poster_for_account_rejects_deleted(app_db) -> None:
    creds, social, _db = app_db
    row = await creds.upsert_credential(
        "twitter", "9", "x", {"bearer_token": "t"}
    )
    await creds.soft_delete_credential(row["uuid"])
    with pytest.raises(ValueError, match="deleted"):
        await social.get_poster_for_account(row["id"])
