"""Tests for the needs_reauth flag flow.

A credential is marked ``needs_reauth`` whenever a poster raises
:class:`CredentialAuthError`. The flag is cleared on the next
successful :func:`upsert_credential` (i.e. after the user re-OAuths).
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import httpx
import pytest

from tests.conftest import install_in_memory_keychain


@pytest.fixture
async def app_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    install_in_memory_keychain(monkeypatch, keychain)
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


async def test_send_post_precheck_returns_401_for_flagged_credential(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """The route must short-circuit a post against a credential already
    known to be broken — saves a 401 round-trip to the platform."""
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DYS_HOST", "127.0.0.1")
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    (tmp_path / "templates").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)

    keychain = importlib.import_module("yt_scheduler.services.keychain")
    install_in_memory_keychain(monkeypatch, keychain)

    app_module = importlib.import_module("yt_scheduler.app")
    creds_mod = importlib.import_module("yt_scheduler.services.social_credentials")

    from fastapi.testclient import TestClient

    with TestClient(app_module.app) as c:
        # Seed a credential, mark it needs_reauth, then try to send a
        # post bound to it.
        cred = await creds_mod.upsert_credential(
            "twitter", "tw:1", "alice", {"bearer_token": "tok"}
        )
        await creds_mod.mark_needs_reauth(cred["uuid"])

        # Insert a video + a post bound to that credential
        from yt_scheduler.database import get_db
        db = await get_db()
        await db.execute(
            "INSERT INTO videos (id, project_id, title, status) "
            "VALUES ('vidA', 1, 'T', 'uploaded')"
        )
        cursor = await db.execute(
            "INSERT INTO social_posts (video_id, platform, content, status, social_account_id) "
            "VALUES ('vidA', 'twitter', 'hello', 'approved', ?)",
            (cred["id"],),
        )
        post_id = int(cursor.lastrowid)
        await db.commit()

        resp = c.post(f"/api/social/posts/{post_id}/send")
        assert resp.status_code == 401
        assert "needs re-authentication" in resp.json()["detail"]


# --- Verify mirrors its verdict into needs_reauth --------------------------
#
# Without this, Verify could tell the user their token was dead while the row
# still showed no Reconnect button — a verdict with no way to act on it.


def _respond_to_threads_with(monkeypatch: pytest.MonkeyPatch, handler) -> None:
    real_async_client = httpx.AsyncClient

    def patched(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return real_async_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", patched)


async def _seed_threads_credential(creds):
    return await creds.upsert_credential(
        "threads", "th:1", "drew", {"access_token": "tok", "user_id": "42"}
    )


async def test_verify_rejection_marks_needs_reauth(app_db, monkeypatch) -> None:
    creds, _db = app_db
    cred = await _seed_threads_credential(creds)

    def rejected(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": {"message": "token expired"}})

    _respond_to_threads_with(monkeypatch, rejected)
    routes = importlib.import_module(
        "yt_scheduler.routers.social_credentials_routes"
    )

    result = await routes.verify_credential(cred["uuid"])

    assert result["ok"] is False
    after = await creds.get_credential_by_uuid(cred["uuid"])
    assert after["needs_reauth"] is True


async def test_verify_success_clears_a_stale_needs_reauth(app_db, monkeypatch) -> None:
    """After re-auth via paste-token or OAuth, a passing Verify is definitive
    proof the credential works — leaving the flag set would keep blocking
    sends via the pre-check."""
    creds, _db = app_db
    cred = await _seed_threads_credential(creds)
    await creds.mark_needs_reauth(cred["uuid"])

    def valid(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"id": "42", "username": "drew"})

    _respond_to_threads_with(monkeypatch, valid)
    routes = importlib.import_module(
        "yt_scheduler.routers.social_credentials_routes"
    )

    result = await routes.verify_credential(cred["uuid"])

    assert result["ok"] is True
    after = await creds.get_credential_by_uuid(cred["uuid"])
    assert after["needs_reauth"] is False


async def test_verify_unreachable_leaves_the_flag_alone(app_db, monkeypatch) -> None:
    """Not reaching the provider says nothing about the token — the flag must
    survive in both directions."""
    creds, _db = app_db
    cred = await _seed_threads_credential(creds)

    def unreachable(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route to host")

    _respond_to_threads_with(monkeypatch, unreachable)
    routes = importlib.import_module(
        "yt_scheduler.routers.social_credentials_routes"
    )

    result = await routes.verify_credential(cred["uuid"])
    assert result["ok"] is False
    assert result["unreachable"] is True
    after = await creds.get_credential_by_uuid(cred["uuid"])
    assert after["needs_reauth"] is False

    await creds.mark_needs_reauth(cred["uuid"])
    await routes.verify_credential(cred["uuid"])
    after = await creds.get_credential_by_uuid(cred["uuid"])
    assert after["needs_reauth"] is True
