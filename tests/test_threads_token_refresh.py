"""Threads token renewal.

The bug this exists for: a Threads token issued 2026-05-11 lapsed at 60 days
and every post afterwards failed with an opaque ``HTTP 500 code=1`` for weeks.
The periodic refresh sweep ran the whole time and reported success, because
``ThreadsPoster`` inherited a ``refresh_if_stale`` that returns False —
indistinguishable from "nothing due".
"""

from __future__ import annotations

import importlib
import time

import httpx
import pytest

CREDS = {
    "access_token": "old-token",
    "user_id": "42",
    "username": "drewbensonhq",
    "uuid": "cred-uuid",
}


@pytest.fixture
def social(isolated_data_dir):
    return importlib.import_module("yt_scheduler.services.social")


@pytest.fixture
def meta(monkeypatch):
    """Stand in for graph.threads.net, recording what was asked of it."""
    calls: list[httpx.Request] = []
    state = {"status": 200, "payload": {"access_token": "new-token",
                                        "expires_in": 5183944}}

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if state["status"] != 200:
            return httpx.Response(state["status"], json={"error": state["payload"]})
        return httpx.Response(200, json=state["payload"])

    original = httpx.AsyncClient.__init__
    monkeypatch.setattr(
        httpx.AsyncClient, "__init__",
        lambda self, *a, **kw: original(
            self, *a, **{**kw, "transport": httpx.MockTransport(handler)}),
    )
    return calls, state


@pytest.fixture
def stored(monkeypatch, social):
    """Capture what gets written back, without touching Keychain or the DB."""
    saved: dict = {}

    async def fake_load_bundle(platform, uuid):
        return saved.get("bundle")

    async def fake_save_bundle(platform, uuid, bundle):
        saved["bundle"] = dict(bundle)

    async def fake_clear(uuid):
        saved["cleared_reauth"] = True

    async def fake_set_expiry(uuid, expires_at):
        saved["row_expiry"] = expires_at

    class _NullLock:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    creds_mod = importlib.import_module("yt_scheduler.services.social_credentials")
    monkeypatch.setattr(creds_mod, "load_bundle", fake_load_bundle)
    monkeypatch.setattr(creds_mod, "save_bundle", fake_save_bundle)
    monkeypatch.setattr(creds_mod, "clear_needs_reauth", fake_clear)
    monkeypatch.setattr(creds_mod, "set_token_expiry", fake_set_expiry)
    monkeypatch.setattr(creds_mod, "get_credential_lock", lambda uuid: _NullLock())
    return saved


def test_every_poster_declares_refresh_support_matching_reality(social):
    """The flag exists so "can't refresh" stops looking like "nothing due".
    It is only worth anything if it never disagrees with the code."""
    for name in social.ALL_PLATFORMS:
        cls = social._POSTERS[name]
        implements = cls.refresh_if_stale is not social.SocialPoster.refresh_if_stale
        assert cls.supports_token_refresh == implements, (
            f"{name}: supports_token_refresh={cls.supports_token_refresh} but "
            f"{'overrides' if implements else 'inherits'} refresh_if_stale"
        )


def test_threads_declares_and_implements_refresh(social):
    assert social.ThreadsPoster.supports_token_refresh is True


async def test_a_token_near_expiry_is_renewed(social, meta, stored):
    calls, _ = meta
    creds = dict(CREDS, expires_at=int(time.time()) + 60)

    assert await social.ThreadsPoster(bundle=creds).refresh_if_stale(
        window_secs=7 * 24 * 3600) is True

    assert "refresh_access_token" in str(calls[0].url)
    assert dict(calls[0].url.params)["grant_type"] == "th_refresh_token"
    assert dict(calls[0].url.params)["access_token"] == "old-token"
    assert stored["bundle"]["access_token"] == "new-token"
    assert stored["bundle"]["expires_at"] > int(time.time()) + 30 * 24 * 3600
    assert stored["cleared_reauth"] is True
    assert stored["row_expiry"] == stored["bundle"]["expires_at"]


async def test_a_healthy_token_is_left_alone(social, meta, stored):
    """Renewing early wastes nothing, but doing it every sweep would hammer
    Meta and rotate the token needlessly."""
    calls, _ = meta
    creds = dict(CREDS, expires_at=int(time.time()) + 50 * 24 * 3600)

    assert await social.ThreadsPoster(bundle=creds).refresh_if_stale(
        window_secs=7 * 24 * 3600) is False
    assert calls == []


async def test_a_bundle_without_expiry_refreshes_once_to_backfill(social, meta, stored):
    """Not knowing is not the same as being fine — the assumption that let the
    real token die. One refresh records a date every later sweep can use."""
    calls, _ = meta

    assert await social.ThreadsPoster(bundle=dict(CREDS)).refresh_if_stale() is True
    assert len(calls) == 1
    assert "expires_at" in stored["bundle"]


async def test_an_expired_token_demands_reauth_rather_than_failing_silently(
    social, meta, stored
):
    """The whole point: an unrenewable token must say so loudly, not leave
    every post to fail with an opaque provider error."""
    calls, state = meta
    state["status"] = 400
    state["payload"] = {"message": "Invalid OAuth access token"}

    with pytest.raises(social.CredentialAuthError, match="Reconnect Threads"):
        await social.ThreadsPoster(bundle=dict(CREDS)).refresh_if_stale()
    assert "bundle" not in stored, "a failed refresh must not overwrite the token"


async def test_a_token_too_young_to_refresh_is_not_flagged_as_dead(
    social, meta, stored
):
    """Meta refuses tokens under 24h old. That is "come back later" — flagging
    needs_reauth would send the user through OAuth for a healthy credential."""
    calls, state = meta
    state["status"] = 400
    state["payload"] = {"message": "The token must be at least 24 hours old"}

    assert await social.ThreadsPoster(bundle=dict(CREDS)).refresh_if_stale() is False
    assert "bundle" not in stored


async def test_a_network_error_is_retried_not_treated_as_expiry(
    social, monkeypatch, stored
):
    """A refresh that couldn't reach Meta says nothing about the token."""
    async def boom(request):
        raise httpx.ConnectError("dns is down")

    original = httpx.AsyncClient.__init__
    monkeypatch.setattr(
        httpx.AsyncClient, "__init__",
        lambda self, *a, **kw: original(
            self, *a, **{**kw, "transport": httpx.MockTransport(boom)}),
    )

    assert await social.ThreadsPoster(bundle=dict(CREDS)).refresh_if_stale() is False
    assert "bundle" not in stored


async def test_a_meta_outage_is_not_a_verdict_on_the_token(social, meta, stored):
    """A 5xx from the refresh endpoint says Meta is unwell, not that the
    credential is dead. Flagging needs_reauth would send the user through OAuth
    for a working token."""
    calls, state = meta
    state["status"] = 503
    state["payload"] = {"message": "Service temporarily unavailable"}

    assert await social.ThreadsPoster(bundle=dict(CREDS)).refresh_if_stale() is False
    assert "bundle" not in stored


async def test_expiry_reaches_the_api_so_the_ui_can_show_it(isolated_db):
    """The column is useless if the row mapper drops it -- which it did, since
    _row_to_dict whitelists fields rather than passing the row through."""
    creds_mod = importlib.import_module("yt_scheduler.services.social_credentials")
    await isolated_db.execute(
        "INSERT INTO social_accounts "
        "(id, uuid, platform, provider_account_id, username, token_expires_at) "
        "VALUES (1,'u1','threads','pid-1','someone','2026-09-01T00:00:00+00:00')"
    )
    await isolated_db.commit()

    rows = await creds_mod.list_credentials()
    assert rows[0]["token_expires_at"] == "2026-09-01T00:00:00+00:00"


async def test_refresh_needs_a_token_to_refresh(social, meta, stored):
    calls, _ = meta
    assert await social.ThreadsPoster(bundle={"uuid": "u"}).refresh_if_stale() is False
    assert calls == []
