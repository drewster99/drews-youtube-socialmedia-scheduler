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
    """Stand in for graph.threads.net, recording what was asked of it.

    Routed by path: the refresh endpoint and the /me live-check (the refresh
    5xx tiebreaker) answer independently. An unmatched path fails loudly."""
    calls: list[httpx.Request] = []
    state = {
        "status": 200,
        "payload": {"access_token": "new-token", "expires_in": 5183944},
        "me_status": 200,
        "me_payload": {"id": "42", "username": "drewbensonhq"},
        "me_exception": None,
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if "refresh_access_token" in str(request.url):
            if state["status"] != 200:
                return httpx.Response(state["status"], json={"error": state["payload"]})
            return httpx.Response(200, json=state["payload"])
        if request.url.path.endswith("/me"):
            if state["me_exception"] is not None:
                raise state["me_exception"]
            if state["me_status"] != 200:
                return httpx.Response(
                    state["me_status"], json={"error": state["me_payload"]}
                )
            return httpx.Response(200, json=state["me_payload"])
        raise AssertionError(f"unexpected request to {request.url}")

    original = httpx.AsyncClient.__init__
    monkeypatch.setattr(
        httpx.AsyncClient, "__init__",
        lambda self, *a, **kw: original(
            self, *a, **{**kw, "transport": httpx.MockTransport(handler)}),
    )
    return calls, state


@pytest.fixture(autouse=True)
def _reset_dual_failure_counter(social):
    """The dual-failure backstop is process memory; tests must not leak it."""
    social._threads_refresh_dual_failures.clear()
    yield
    social._threads_refresh_dual_failures.clear()


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


    class _NullLock:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

    creds_mod = importlib.import_module("yt_scheduler.services.social_credentials")
    monkeypatch.setattr(creds_mod, "load_bundle", fake_load_bundle)
    monkeypatch.setattr(creds_mod, "save_bundle", fake_save_bundle)
    monkeypatch.setattr(creds_mod, "clear_needs_reauth", fake_clear)
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
    # The row mirror rides on save_bundle now (tested in
    # test_token_metadata_mirror.py), so the bundle carrying both dates is
    # what guarantees the Settings list will show the renewal.
    assert stored["bundle"]["acquired_at"] <= int(time.time())
    assert stored["cleared_reauth"] is True


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
    """A genuine full outage — refresh AND /me both 5xx, no recorded expiry —
    is retried, not flagged. Flagging needs_reauth would send the user through
    OAuth for a working token."""
    calls, state = meta
    state["status"] = 503
    state["payload"] = {"message": "Service temporarily unavailable"}
    state["me_status"] = 503
    state["me_payload"] = {"message": "Service temporarily unavailable"}

    assert await social.ThreadsPoster(bundle=dict(CREDS)).refresh_if_stale() is False
    assert "bundle" not in stored


async def test_a_refresh_500_with_a_dead_token_flags_reauth(social, meta, stored):
    """The production loop this fix exists for: Meta 500s a dead token's
    refresh exactly like an outage, but /me gives a real verdict — a 4xx.
    The old code read the 500 optimistically every 20 minutes for weeks."""
    calls, state = meta
    state["status"] = 500
    state["payload"] = {"message": "An unknown error occurred", "code": 1}
    state["me_status"] = 400
    state["me_payload"] = {"message": "Invalid OAuth access token", "code": 190}

    with pytest.raises(social.CredentialAuthError) as exc_info:
        await social.ThreadsPoster(bundle=dict(CREDS)).refresh_if_stale()
    assert "Reconnect Threads" in str(exc_info.value)
    assert "bundle" not in stored


async def test_a_refresh_500_with_a_still_verifying_token_is_retried(social, meta, stored):
    calls, state = meta
    state["status"] = 500
    state["payload"] = {"message": "An unknown error occurred", "code": 1}

    assert await social.ThreadsPoster(bundle=dict(CREDS)).refresh_if_stale() is False
    assert "bundle" not in stored
    assert any(r.url.path.endswith("/me") for r in calls), "the tiebreaker must ask /me"


async def test_a_full_outage_past_recorded_expiry_is_terminal(social, meta, stored):
    """th_refresh_token cannot renew a token past expiry, so waiting out the
    outage cannot save this credential — flag it now."""
    calls, state = meta
    state["status"] = 500
    state["payload"] = {"message": "An unknown error occurred", "code": 1}
    state["me_exception"] = httpx.ConnectError("no route to host")
    creds = dict(CREDS, expires_at=int(time.time()) - 3600)

    with pytest.raises(social.CredentialAuthError) as exc_info:
        await social.ThreadsPoster(bundle=creds).refresh_if_stale()
    assert "past its recorded expiry" in str(exc_info.value)


async def test_the_dual_failure_backstop_flags_after_bounded_retries(social, meta, stored):
    """No recorded expiry + both endpoints unreadable could loop forever —
    which is Meta's signature for a dead token too. Bounded, then flagged."""
    calls, state = meta
    state["status"] = 500
    state["payload"] = {"message": "An unknown error occurred", "code": 1}
    state["me_status"] = 503
    state["me_payload"] = {"message": "Service temporarily unavailable"}

    poster = social.ThreadsPoster(bundle=dict(CREDS))
    threshold = social._THREADS_DUAL_FAILURE_FLAG_THRESHOLD
    for _ in range(threshold - 1):
        assert await poster.refresh_if_stale() is False
    with pytest.raises(social.CredentialAuthError) as exc_info:
        await poster.refresh_if_stale()
    assert "predates expiry tracking" in str(exc_info.value)


async def test_a_verifying_token_resets_the_dual_failure_count(social, meta, stored):
    calls, state = meta
    state["status"] = 500
    state["payload"] = {"message": "An unknown error occurred", "code": 1}
    state["me_status"] = 503
    state["me_payload"] = {"message": "Service temporarily unavailable"}

    poster = social.ThreadsPoster(bundle=dict(CREDS))
    for _ in range(social._THREADS_DUAL_FAILURE_FLAG_THRESHOLD - 1):
        assert await poster.refresh_if_stale() is False

    state["me_status"] = 200
    assert await poster.refresh_if_stale() is False
    assert social._threads_refresh_dual_failures == {}


async def test_a_4xx_refresh_verdict_never_asks_me(social, meta, stored):
    """The tiebreaker exists for ambiguous 5xx only — it must never dilute a
    real 4xx verdict from the refresh endpoint."""
    calls, state = meta
    state["status"] = 400
    state["payload"] = {"message": "Invalid OAuth access token", "code": 190}

    with pytest.raises(social.CredentialAuthError):
        await social.ThreadsPoster(bundle=dict(CREDS)).refresh_if_stale()
    assert not any(r.url.path.endswith("/me") for r in calls)


def test_threads_live_check_backs_the_refresh_tiebreaker():
    """If threads ever left LIVE_CHECK_PLATFORMS, the tiebreaker would raise
    CredentialCheckUnsupported, which the sweep logs as 'transient' — silently
    reinstating the forever-loop."""
    from yt_scheduler.services.social_identity import LIVE_CHECK_PLATFORMS

    assert "threads" in LIVE_CHECK_PLATFORMS


async def test_expiry_reaches_the_api_so_the_ui_can_show_it(isolated_db):
    """The column is useless if the row mapper drops it -- which it did, since
    _row_to_dict whitelists fields rather than passing the row through."""
    creds_mod = importlib.import_module("yt_scheduler.services.social_credentials")
    await isolated_db.execute(
        "INSERT INTO social_accounts "
        "(id, uuid, platform, provider_account_id, username, credentials_ref, "
        " token_expires_at) "
        "VALUES (1,'u1','threads','pid-1','someone','cred.u1',"
        "'2026-09-01T00:00:00+00:00')"
    )
    await isolated_db.commit()

    rows = await creds_mod.list_credentials()
    assert rows[0]["token_expires_at"] == "2026-09-01T00:00:00+00:00"


async def test_refresh_needs_a_token_to_refresh(social, meta, stored):
    calls, _ = meta
    assert await social.ThreadsPoster(bundle={"uuid": "u"}).refresh_if_stale() is False
    assert calls == []


async def test_a_refresh_without_a_stated_lifetime_records_unknown(social, meta, stored):
    """Meta omitting expires_in on refresh must record NO expiry (unknown),
    not a fabricated 60 days — and the fresh token must not inherit the old
    token's expiry either."""
    calls, state = meta
    state["payload"] = {"access_token": "new-token"}  # no expires_in
    creds = dict(CREDS, expires_at=int(time.time()) + 60)  # a real, soon-to-die expiry

    assert await social.ThreadsPoster(bundle=creds).refresh_if_stale(
        window_secs=7 * 24 * 3600) is True

    assert stored["bundle"]["access_token"] == "new-token"
    assert stored["bundle"]["acquired_at"] <= int(time.time())
    # No fabricated expiry, and the old one is gone — unknown means unknown.
    assert stored["bundle"].get("expires_at") is None
