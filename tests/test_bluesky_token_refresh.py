"""Bluesky token renewal: AS outages and nonce churn are not verdicts.

Any non-200 from the authorization server used to become RuntimeError →
CredentialAuthError, so a 503, a 429, or DPoP nonce churn during the sweep
flagged a healthy credential ``needs_reauth`` — a heavyweight re-OAuth nag
for nothing. Only an actual AS verdict (invalid_grant &co) may flag, and a
local Keychain failure must never be dressed up as one.
"""

from __future__ import annotations

import importlib
import json
import time

import pytest

CREDS = {
    "access_token": "old-access",
    "refresh_token": "old-refresh",
    "uuid": "cred-uuid",
    "private_key_pem": "not-a-real-key",
    "token_endpoint": "https://as.example/token",
    "redirect_uri": "http://127.0.0.1:8008/cb",
    "dpop_nonce_as": "nonce-0",
}


@pytest.fixture
def social(isolated_data_dir):
    return importlib.import_module("yt_scheduler.services.social")


@pytest.fixture
def bluesky_oauth(isolated_data_dir):
    return importlib.import_module("yt_scheduler.services.bluesky_oauth")


@pytest.fixture
def stored(monkeypatch):
    saved: dict = {}

    async def fake_load_bundle(platform, uuid):
        return saved.get("bundle")

    async def fake_save_bundle(platform, uuid, bundle):
        saved["bundle"] = dict(bundle)

    async def fake_clear(uuid):
        saved["cleared_reauth"] = True

    class _NullLock:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    creds_mod = importlib.import_module("yt_scheduler.services.social_credentials")
    monkeypatch.setattr(creds_mod, "load_bundle", fake_load_bundle)
    monkeypatch.setattr(creds_mod, "save_bundle", fake_save_bundle)
    monkeypatch.setattr(creds_mod, "clear_needs_reauth", fake_clear)
    monkeypatch.setattr(creds_mod, "get_credential_lock", lambda uuid: _NullLock())
    return saved


def _due_creds() -> dict:
    # Past expiry so the token is due regardless of the lookahead window.
    return dict(CREDS, expires_at=int(time.time()) - 10)


# --- Poster-level classification -------------------------------------------


async def test_as_unavailable_is_deferred_not_terminal(
    social, bluesky_oauth, stored, monkeypatch,
) -> None:
    async def unavailable(**_kw):
        raise bluesky_oauth.AuthServerUnavailable("HTTP 503")

    monkeypatch.setattr(bluesky_oauth, "refresh_tokens", unavailable)

    assert await social.BlueskyPoster(bundle=_due_creds()).refresh_if_stale() is False
    assert "bundle" not in stored


async def test_an_as_verdict_flags_reauth(social, bluesky_oauth, stored, monkeypatch) -> None:
    async def rejected(**_kw):
        raise RuntimeError("Refresh failed: HTTP 400 invalid_grant")

    monkeypatch.setattr(bluesky_oauth, "refresh_tokens", rejected)

    with pytest.raises(social.CredentialAuthError) as exc_info:
        await social.BlueskyPoster(bundle=_due_creds()).refresh_if_stale()
    assert "invalid_grant" in str(exc_info.value)
    assert exc_info.value.uuid == "cred-uuid"


async def test_a_keychain_failure_is_not_converted_to_reauth(
    social, bluesky_oauth, stored, monkeypatch,
) -> None:
    from yt_scheduler.services.keychain import KeychainWriteError

    async def succeeded(**_kw):
        return {"access_token": "new-access", "refresh_token": "new-refresh",
                "expires_in": 1800, "dpop_nonce_as": "nonce-1"}

    creds_mod = importlib.import_module("yt_scheduler.services.social_credentials")

    async def failing_save(platform, uuid, bundle):
        raise KeychainWriteError("Security framework says no")

    monkeypatch.setattr(bluesky_oauth, "refresh_tokens", succeeded)
    monkeypatch.setattr(creds_mod, "save_bundle", failing_save)

    with pytest.raises(KeychainWriteError):
        await social.BlueskyPoster(bundle=_due_creds()).refresh_if_stale()


async def test_a_successful_refresh_persists_the_rotation(
    social, bluesky_oauth, stored, monkeypatch,
) -> None:
    async def succeeded(**_kw):
        return {"access_token": "new-access", "refresh_token": "new-refresh",
                "expires_in": 1800, "dpop_nonce_as": "nonce-1"}

    monkeypatch.setattr(bluesky_oauth, "refresh_tokens", succeeded)

    assert await social.BlueskyPoster(bundle=_due_creds()).refresh_if_stale() is True
    assert stored["bundle"]["access_token"] == "new-access"
    assert stored["bundle"]["refresh_token"] == "new-refresh"
    assert stored["bundle"]["dpop_nonce_as"] == "nonce-1"
    assert stored["cleared_reauth"] is True


def test_as_unavailable_is_deliberately_not_runtimeerror(bluesky_oauth) -> None:
    assert not issubclass(bluesky_oauth.AuthServerUnavailable, RuntimeError)


# --- refresh_tokens' own classification -------------------------------------


def _response(bluesky_oauth, status: int, body: dict | str, headers: dict | None = None):
    bluesky_http = importlib.import_module("yt_scheduler.services.bluesky_http")
    content = (json.dumps(body) if isinstance(body, dict) else body).encode()
    return bluesky_http.Response(status, content, headers or {})


@pytest.fixture
def as_endpoint(bluesky_oauth, monkeypatch):
    """Queue of canned AS responses; records how many calls were made."""
    bluesky_http = importlib.import_module("yt_scheduler.services.bluesky_http")
    queue: list = []
    calls: list = []

    async def fake_post(url, **kw):
        calls.append((url, kw))
        return queue.pop(0)

    monkeypatch.setattr(bluesky_http, "post", fake_post)
    monkeypatch.setattr(bluesky_oauth, "sign_dpop_proof", lambda *a, **kw: "proof")
    return queue, calls


async def _refresh(bluesky_oauth):
    return await bluesky_oauth.refresh_tokens(
        refresh_token="rt", private_key_pem="pem",
        token_endpoint="https://as.example/token",
        redirect_uri="http://127.0.0.1:8008/cb", nonce="n0",
    )


async def test_a_5xx_from_the_as_is_unavailable(bluesky_oauth, as_endpoint) -> None:
    queue, _calls = as_endpoint
    queue.append(_response(bluesky_oauth, 503, {"error": "overloaded"}))

    with pytest.raises(bluesky_oauth.AuthServerUnavailable):
        await _refresh(bluesky_oauth)


async def test_a_429_from_the_as_is_unavailable(bluesky_oauth, as_endpoint) -> None:
    queue, _calls = as_endpoint
    queue.append(_response(bluesky_oauth, 429, {"error": "rate_limited"}))

    with pytest.raises(bluesky_oauth.AuthServerUnavailable):
        await _refresh(bluesky_oauth)


async def test_a_double_nonce_bounce_is_unavailable_and_unconsumed(
    bluesky_oauth, as_endpoint,
) -> None:
    """Two use_dpop_nonce answers in a row is nonce churn — the refresh token
    was never consumed, so re-OAuth would be pure waste."""
    queue, calls = as_endpoint
    queue.append(_response(bluesky_oauth, 400, {"error": "use_dpop_nonce"},
                           {"DPoP-Nonce": "n1"}))
    queue.append(_response(bluesky_oauth, 400, {"error": "use_dpop_nonce"},
                           {"DPoP-Nonce": "n2"}))

    with pytest.raises(bluesky_oauth.AuthServerUnavailable):
        await _refresh(bluesky_oauth)
    assert len(calls) == 2


async def test_a_nonce_demand_without_a_nonce_is_unavailable(
    bluesky_oauth, as_endpoint,
) -> None:
    queue, calls = as_endpoint
    queue.append(_response(bluesky_oauth, 400, {"error": "use_dpop_nonce"}))

    with pytest.raises(bluesky_oauth.AuthServerUnavailable):
        await _refresh(bluesky_oauth)
    assert len(calls) == 1


async def test_a_4xx_verdict_stays_terminal(bluesky_oauth, as_endpoint) -> None:
    queue, _calls = as_endpoint
    queue.append(_response(bluesky_oauth, 400, {"error": "invalid_grant"}))

    with pytest.raises(RuntimeError) as exc_info:
        await _refresh(bluesky_oauth)
    assert not isinstance(exc_info.value, bluesky_oauth.AuthServerUnavailable)
    assert "invalid_grant" in str(exc_info.value)


async def test_a_successful_refresh_carries_the_next_nonce(
    bluesky_oauth, as_endpoint,
) -> None:
    queue, _calls = as_endpoint
    queue.append(_response(bluesky_oauth, 200,
                           {"access_token": "a", "refresh_token": "r"},
                           {"DPoP-Nonce": "n9"}))

    body = await _refresh(bluesky_oauth)
    assert body["access_token"] == "a"
    assert body["dpop_nonce_as"] == "n9"
