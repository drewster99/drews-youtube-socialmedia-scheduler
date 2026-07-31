"""X token renewal: outages are not verdicts on the credential.

Any non-200 from the token endpoint used to become CredentialAuthError, so a
503 or a 429 during the sweep flagged a healthy credential ``needs_reauth`` —
blocking sends and nagging for a re-OAuth that would change nothing. Worse,
``KeychainWriteError`` subclasses RuntimeError, so a LOCAL storage failure got
the same treatment. Only a provider *verdict* (4xx) may flag.
"""

from __future__ import annotations

import importlib
import time

import httpx
import pytest

CREDS = {
    "bearer_token": "old-bearer",
    "refresh_token": "old-refresh",
    "client_id": "client-1",
    "uuid": "cred-uuid",
    "expires_at": 0,  # overridden per test
}


@pytest.fixture
def social(isolated_data_dir):
    return importlib.import_module("yt_scheduler.services.social")


@pytest.fixture
def token_endpoint(monkeypatch):
    """Stand in for api.x.com's token endpoint."""
    calls: list[httpx.Request] = []
    state = {
        "status": 200,
        "payload": {"access_token": "new-bearer", "refresh_token": "new-refresh",
                    "expires_in": 7200},
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(state["status"], json=state["payload"])

    original = httpx.AsyncClient.__init__
    monkeypatch.setattr(
        httpx.AsyncClient, "__init__",
        lambda self, *a, **kw: original(
            self, *a, **{**kw, "transport": httpx.MockTransport(handler)}),
    )
    return calls, state


@pytest.fixture
def stored(monkeypatch):
    """Capture bundle writes without touching Keychain or the DB."""
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


async def test_a_5xx_is_deferred_not_terminal(social, token_endpoint, stored) -> None:
    calls, state = token_endpoint
    state["status"] = 503
    state["payload"] = {"error": "temporarily_unavailable"}

    assert await social.TwitterPoster(bundle=_due_creds()).refresh_if_stale() is False
    assert "bundle" not in stored, "a failed refresh must not overwrite the bundle"


async def test_a_429_rate_limit_is_deferred_not_terminal(social, token_endpoint, stored) -> None:
    _calls, state = token_endpoint
    state["status"] = 429
    state["payload"] = {"error": "rate_limit_exceeded"}

    assert await social.TwitterPoster(bundle=_due_creds()).refresh_if_stale() is False
    assert "bundle" not in stored


async def test_a_4xx_verdict_flags_reauth_with_the_rfc_code(social, token_endpoint, stored) -> None:
    _calls, state = token_endpoint
    state["status"] = 400
    state["payload"] = {"error": "invalid_grant",
                        "error_description": "refresh token revoked"}

    with pytest.raises(social.CredentialAuthError) as exc_info:
        await social.TwitterPoster(bundle=_due_creds()).refresh_if_stale()
    message = str(exc_info.value)
    assert "invalid_grant" in message
    assert "refresh token revoked" in message
    assert exc_info.value.uuid == "cred-uuid"


async def test_a_transport_error_is_not_a_credential_verdict(social, stored, monkeypatch) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route to host")

    original = httpx.AsyncClient.__init__
    monkeypatch.setattr(
        httpx.AsyncClient, "__init__",
        lambda self, *a, **kw: original(
            self, *a, **{**kw, "transport": httpx.MockTransport(handler)}),
    )

    with pytest.raises(httpx.ConnectError):
        await social.TwitterPoster(bundle=_due_creds()).refresh_if_stale()


async def test_a_successful_refresh_rotates_and_persists(social, token_endpoint, stored) -> None:
    assert await social.TwitterPoster(bundle=_due_creds()).refresh_if_stale() is True
    assert stored["bundle"]["bearer_token"] == "new-bearer"
    assert stored["bundle"]["refresh_token"] == "new-refresh"
    assert stored["cleared_reauth"] is True


async def test_a_keychain_write_failure_is_not_converted_to_reauth(
    social, token_endpoint, stored, monkeypatch,
) -> None:
    """Telling the user to re-OAuth for a broken LOCAL Keychain would run a
    flow whose result the Keychain couldn't even persist."""
    from yt_scheduler.services.keychain import KeychainWriteError

    creds_mod = importlib.import_module("yt_scheduler.services.social_credentials")

    async def failing_save(platform, uuid, bundle):
        raise KeychainWriteError("Security framework says no")

    monkeypatch.setattr(creds_mod, "save_bundle", failing_save)

    with pytest.raises(KeychainWriteError):
        await social.TwitterPoster(bundle=_due_creds()).refresh_if_stale()


def test_token_endpoint_unavailable_is_deliberately_not_runtimeerror(social) -> None:
    """The refresh paths convert RuntimeError to CredentialAuthError; a future
    re-parenting would silently turn outages back into reauth nags."""
    assert not issubclass(social.TokenEndpointUnavailable, RuntimeError)


async def test_a_200_without_a_token_is_terminal(social, token_endpoint, stored) -> None:
    _calls, state = token_endpoint
    state["payload"] = {"token_type": "bearer"}

    with pytest.raises(social.CredentialAuthError):
        await social.TwitterPoster(bundle=_due_creds()).refresh_if_stale()


async def test_error_detail_caps_non_json_bodies(social) -> None:
    resp = httpx.Response(
        502, text="<html>" + "x" * 5000 + "</html>",
        request=httpx.Request("POST", "https://api.x.com/2/oauth2/token"),
    )
    detail = social._http_error_detail(resp)
    assert len(detail) < 600


async def test_error_detail_renders_rfc6749_description(social) -> None:
    resp = httpx.Response(
        400, json={"error": "invalid_grant", "error_description": "revoked"},
        request=httpx.Request("POST", "https://api.x.com/2/oauth2/token"),
    )
    detail = social._http_error_detail(resp)
    assert "invalid_grant" in detail and "revoked" in detail


async def test_a_due_token_with_no_refresh_token_is_surfaced(social, stored) -> None:
    """An OAuth credential that lost its refresh token but has a known, near
    expiry must not silently no-op until every post 401s — it demands reauth."""
    creds = {
        "bearer_token": "old-bearer",
        "uuid": "cred-uuid",
        "expires_at": int(time.time()) - 10,  # known and expired
        # no refresh_token, no client_id
    }
    with pytest.raises(social.CredentialAuthError) as exc_info:
        await social.TwitterPoster(bundle=creds).refresh_if_stale()
    assert exc_info.value.uuid == "cred-uuid"
    assert "reconnect" in str(exc_info.value).lower()


async def test_a_manual_bearer_with_unknown_expiry_stays_silent(social, stored) -> None:
    """A hand-pasted bearer (no expiry, no refresh token) is legitimately
    un-refreshable — nothing is wrong and nothing can be done, so it must not
    raise reauth on every sweep."""
    creds = {"bearer_token": "manual", "uuid": "cred-uuid", "expires_at": 0}
    assert await social.TwitterPoster(bundle=creds).refresh_if_stale() is False
    assert "bundle" not in stored
