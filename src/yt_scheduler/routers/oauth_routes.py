"""Built-in OAuth 2.0 flows for social platforms.

Supports:
- LinkedIn (3-legged Authorization Code flow).
- Threads (3-legged + short-to-long-lived exchange).
- Twitter / X (OAuth 2.0 PKCE — replaces the legacy 1.0a 4-key paste flow).
- Mastodon (per-instance dynamic client registration + Authorization Code).
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import secrets
import time
from urllib.parse import urlencode, urlparse

import httpx
from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse

from yt_scheduler.config import YOUTUBE_SCOPES
from yt_scheduler.database import get_db
from yt_scheduler.services.auth import (
    channel_id_from_credentials,
    get_client_secret_dict,
    has_client_secret,
    store_credentials,
)
from yt_scheduler.services import bluesky_oauth, oauth_clients, youtube as _youtube
from yt_scheduler.services.projects import slugify
from yt_scheduler.services.social_credentials import (
    display_name_for,
    upsert_credential,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/oauth", tags=["oauth"])

LINKEDIN_REDIRECT_PATH = "/api/oauth/linkedin/callback"
LINKEDIN_SCOPES = "openid profile w_member_social"

THREADS_REDIRECT_PATH = "/api/oauth/threads/callback"
THREADS_SCOPES = "threads_basic,threads_content_publish"

TWITTER_REDIRECT_PATH = "/api/oauth/twitter/callback"
# media.write covers v2 simple + chunked media upload (images, GIFs, videos).
TWITTER_SCOPES = "tweet.read tweet.write users.read offline.access media.write"

MASTODON_REDIRECT_PATH = "/api/oauth/mastodon/callback"
MASTODON_SCOPES = "read write"

YOUTUBE_REDIRECT_PATH = "/api/oauth/youtube/callback"

# Pending OAuth starts keyed by state. Held in-process only: if the server
# restarts between start and callback, the user has to re-click Connect.
# Values include client_id, client_secret, redirect_uri, and a timestamp for
# basic TTL cleanup.
_pending: dict[str, dict] = {}
_PENDING_TTL_SECONDS = 600


def _gc_pending() -> None:
    now = time.time()
    stale = [s for s, p in _pending.items() if now - p["ts"] > _PENDING_TTL_SECONDS]
    for s in stale:
        _pending.pop(s, None)


@router.post("/linkedin/start")
async def linkedin_start(data: dict):
    """Begin the LinkedIn OAuth flow.

    Request body: ``{"client_id": "...", "client_secret": "...",
    "origin": "http://127.0.0.1:8008", "project_slug": "..." (optional)}``
    Returns: ``{"auth_url": "https://www.linkedin.com/oauth/v2/authorization?..."}``
    """
    client_id = (data.get("client_id") or "").strip()
    client_secret = (data.get("client_secret") or "").strip()
    origin = (data.get("origin") or "").rstrip("/")
    project_slug = (data.get("project_slug") or "").strip() or None
    if not client_id or not client_secret:
        stored_id, stored_secret = oauth_clients.get_oauth_client("linkedin")
        client_id = client_id or stored_id
        client_secret = client_secret or stored_secret
    if not client_id or not client_secret:
        raise HTTPException(
            400,
            "LinkedIn OAuth client is not configured. Open Settings → "
            "OAuth client credentials and add a Client ID and Client Secret.",
        )
    if not origin:
        raise HTTPException(400, "origin is required")

    _gc_pending()
    state = secrets.token_urlsafe(24)
    redirect_uri = f"{origin}{LINKEDIN_REDIRECT_PATH}"
    _pending[state] = {
        "platform": "linkedin",
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "project_slug": project_slug,
        "ts": time.time(),
    }

    auth_url = (
        "https://www.linkedin.com/oauth/v2/authorization"
        f"?response_type=code&client_id={client_id}"
        f"&redirect_uri={redirect_uri}"
        f"&scope={LINKEDIN_SCOPES.replace(' ', '%20')}"
        f"&state={state}"
    )
    return {"auth_url": auth_url, "redirect_uri": redirect_uri}


@router.get("/linkedin/callback", response_class=HTMLResponse)
async def linkedin_callback(code: str | None = None, state: str | None = None, error: str | None = None, error_description: str | None = None):
    """Receive the LinkedIn authorization code, exchange it, and store credentials."""
    if error:
        return _result_page(False, f"LinkedIn denied authorization: {error} — {error_description or ''}")
    if not code or not state:
        return _result_page(False, "Missing code or state in callback URL.")

    pending = _pending.pop(state, None)
    if pending is None:
        return _result_page(False, "Unknown or expired OAuth state. Click Connect with LinkedIn again.")

    # Exchange code for access token
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            token_resp = await client.post(
                "https://www.linkedin.com/oauth/v2/accessToken",
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": pending["redirect_uri"],
                    "client_id": pending["client_id"],
                    "client_secret": pending["client_secret"],
                },
            )
    except Exception as e:
        return _result_page(False, f"Network error contacting LinkedIn: {e}")

    if token_resp.status_code != 200:
        return _result_page(False, f"Token exchange failed ({token_resp.status_code}): {token_resp.text}")

    token_data = token_resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        return _result_page(False, f"LinkedIn response missing access_token: {token_data}")

    # Look up Person URN via userinfo
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            ui = await client.get(
                "https://api.linkedin.com/v2/userinfo",
                headers={"Authorization": f"Bearer {access_token}"},
            )
    except Exception as e:
        return _result_page(False, f"Network error fetching userinfo: {e}")

    if ui.status_code != 200:
        return _result_page(False, f"/v2/userinfo failed ({ui.status_code}): {ui.text}")

    ui_data = ui.json() or {}
    sub = ui_data.get("sub")
    if not sub:
        return _result_page(False, "userinfo response missing sub claim.")
    person_urn = f"urn:li:person:{sub}"
    display_name = ui_data.get("name") or ui_data.get("email") or person_urn

    bundle = {
        "access_token": access_token,
        "person_urn": person_urn,
        "client_id": pending["client_id"],
        "client_secret": pending["client_secret"],
    }
    cred = await _persist_oauth_credential(
        platform="linkedin",
        provider_account_id=sub,
        username=display_name,
        bundle=bundle,
        project_slug=pending.get("project_slug"),
        display_name=display_name,
    )
    return _result_page(
        True,
        f"LinkedIn connected as {display_name}.",
        platform="linkedin",
        payload=_success_payload(cred, pending.get("project_slug")),
    )


@router.post("/threads/start")
async def threads_start(data: dict):
    """Begin the Threads OAuth flow.

    Request body: {"client_id": "...", "client_secret": "...", "origin": "http://127.0.0.1:8008"}
    Returns: {"auth_url": "https://threads.net/oauth/authorize?...", "redirect_uri": "..."}
    """
    client_id = (data.get("client_id") or "").strip()
    client_secret = (data.get("client_secret") or "").strip()
    origin = (data.get("origin") or "").rstrip("/")
    project_slug = (data.get("project_slug") or "").strip() or None
    if not client_id or not client_secret:
        stored_id, stored_secret = oauth_clients.get_oauth_client("threads")
        client_id = client_id or stored_id
        client_secret = client_secret or stored_secret
    if not client_id or not client_secret:
        raise HTTPException(
            400,
            "Threads OAuth client is not configured. Open Settings → "
            "OAuth client credentials and add a Client ID and Client Secret.",
        )
    if not origin:
        raise HTTPException(400, "origin is required")

    _gc_pending()
    state = secrets.token_urlsafe(24)
    redirect_uri = f"{origin}{THREADS_REDIRECT_PATH}"
    _pending[state] = {
        "platform": "threads",
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "project_slug": project_slug,
        "ts": time.time(),
    }

    auth_url = (
        "https://threads.net/oauth/authorize"
        f"?client_id={client_id}"
        f"&redirect_uri={redirect_uri}"
        f"&scope={THREADS_SCOPES}"
        "&response_type=code"
        f"&state={state}"
    )
    return {"auth_url": auth_url, "redirect_uri": redirect_uri}


@router.get("/threads/callback", response_class=HTMLResponse)
async def threads_callback(code: str | None = None, state: str | None = None, error: str | None = None, error_description: str | None = None):
    """Receive the Threads code, exchange it, upgrade to a long-lived token, and store."""
    if error:
        return _result_page(False, f"Threads denied authorization: {error} — {error_description or ''}", platform="threads")
    if not code or not state:
        return _result_page(False, "Missing code or state in callback URL.", platform="threads")

    pending = _pending.pop(state, None)
    if pending is None or pending.get("platform") != "threads":
        return _result_page(False, "Unknown or expired OAuth state. Click Connect with Threads again.", platform="threads")

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            short_resp = await client.post(
                "https://graph.threads.net/oauth/access_token",
                data={
                    "client_id": pending["client_id"],
                    "client_secret": pending["client_secret"],
                    "grant_type": "authorization_code",
                    "redirect_uri": pending["redirect_uri"],
                    "code": code,
                },
            )
    except Exception as e:
        return _result_page(False, f"Network error contacting Threads: {e}", platform="threads")

    if short_resp.status_code != 200:
        return _result_page(False, f"Token exchange failed ({short_resp.status_code}): {short_resp.text}", platform="threads")

    short_data = short_resp.json()
    short_token = short_data.get("access_token")
    user_id = short_data.get("user_id")
    if not short_token:
        return _result_page(False, f"Threads response missing access_token: {short_data}", platform="threads")

    # Exchange short-lived (~1h) for long-lived (~60d)
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            long_resp = await client.get(
                "https://graph.threads.net/access_token",
                params={
                    "grant_type": "th_exchange_token",
                    "client_secret": pending["client_secret"],
                    "access_token": short_token,
                },
            )
    except Exception as e:
        return _result_page(False, f"Network error upgrading to long-lived token: {e}", platform="threads")

    if long_resp.status_code != 200:
        return _result_page(False, f"Long-lived token exchange failed ({long_resp.status_code}): {long_resp.text}", platform="threads")

    access_token = long_resp.json().get("access_token") or short_token

    # Fetch username (we already have user_id from the initial response)
    username = ""
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            me = await client.get(
                "https://graph.threads.net/v1.0/me",
                params={"fields": "id,username", "access_token": access_token},
            )
        if me.status_code == 200:
            me_data = me.json()
            username = me_data.get("username", "")
            if not user_id:
                user_id = me_data.get("id")
    except Exception:
        pass

    if not user_id:
        return _result_page(False, "Could not resolve Threads user_id.", platform="threads")

    bundle = {
        "access_token": access_token,
        "user_id": str(user_id),
        "client_id": pending["client_id"],
        "client_secret": pending["client_secret"],
    }
    if username:
        bundle["username"] = username

    cred = await _persist_oauth_credential(
        platform="threads",
        provider_account_id=str(user_id),
        username=username or str(user_id),
        bundle=bundle,
        project_slug=pending.get("project_slug"),
        is_nickname=not username,
    )
    return _result_page(
        True,
        f"Threads connected as @{username or user_id}.",
        platform="threads",
        payload=_success_payload(cred, pending.get("project_slug")),
    )


@router.post("/threads/exchange")
async def threads_exchange(data: dict):
    """Given a short-lived Threads token + app_secret, mint a long-lived token,
    resolve user_id + username, and store everything in Keychain.

    This is the Meta-friendly alternative to the 3-legged OAuth flow — Meta
    requires HTTPS redirects, so the popup-based flow only works behind
    HTTPS (ngrok / production). For local use, grab a short-lived token from
    the Graph API Explorer and paste it here.

    Request body: {"app_secret": "...", "short_lived_token": "..."}
    """
    app_secret = (data.get("app_secret") or "").strip()
    short_token = (data.get("short_lived_token") or "").strip()
    if not app_secret:
        _, stored_secret = oauth_clients.get_oauth_client("threads")
        app_secret = stored_secret
    if not short_token:
        raise HTTPException(400, "short_lived_token is required")
    if not app_secret:
        raise HTTPException(
            400,
            "Threads App Secret is not configured. Open Settings → "
            "OAuth client credentials and save the Threads Client ID and Secret first.",
        )

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            long_resp = await client.get(
                "https://graph.threads.net/access_token",
                params={
                    "grant_type": "th_exchange_token",
                    "client_secret": app_secret,
                    "access_token": short_token,
                },
            )
    except Exception as e:
        raise HTTPException(502, f"Network error exchanging token: {e}")

    if long_resp.status_code != 200:
        raise HTTPException(long_resp.status_code, f"Exchange failed: {long_resp.text}")

    long_data = long_resp.json()
    access_token = long_data.get("access_token")
    if not access_token:
        raise HTTPException(502, f"Response missing access_token: {long_data}")

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            me = await client.get(
                "https://graph.threads.net/v1.0/me",
                params={"fields": "id,username", "access_token": access_token},
            )
    except Exception as e:
        raise HTTPException(502, f"Network error fetching userinfo: {e}")

    if me.status_code != 200:
        raise HTTPException(me.status_code, f"/me failed: {me.text}")

    me_data = me.json()
    user_id = me_data.get("id")
    username = me_data.get("username", "")
    if not user_id:
        raise HTTPException(502, "Response missing user id")

    bundle = {
        "access_token": access_token,
        "user_id": str(user_id),
        "client_secret": app_secret,
    }
    if username:
        bundle["username"] = username

    cred = await upsert_credential(
        platform="threads",
        provider_account_id=str(user_id),
        username=username or str(user_id),
        bundle=bundle,
        is_nickname=not username,
    )

    return {
        "ok": True,
        "user_id": str(user_id),
        "username": username,
        "social_account_id": cred["id"],
        "uuid": cred["uuid"],
        "expires_in": long_data.get("expires_in"),
    }


# --- Twitter / X — OAuth 2.0 PKCE -----------------------------------------


def _pkce_pair() -> tuple[str, str]:
    """Generate (verifier, challenge) for PKCE — challenge is the base64url-
    encoded SHA-256 of the verifier."""
    verifier = secrets.token_urlsafe(64)[:128]
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return verifier, challenge


@router.post("/twitter/start")
async def twitter_start(data: dict):
    """Begin Twitter / X OAuth 2.0 PKCE flow.

    Request body: {"client_id": "...", "client_secret": "...", "origin": "http://127.0.0.1:8008"}
    Returns: {"auth_url": "...", "redirect_uri": "..."}
    """
    client_id = (data.get("client_id") or "").strip()
    client_secret = (data.get("client_secret") or "").strip()
    origin = (data.get("origin") or "").rstrip("/")
    project_slug = (data.get("project_slug") or "").strip() or None
    if not client_id:
        stored_id, stored_secret = oauth_clients.get_oauth_client("twitter")
        client_id = stored_id
        if not client_secret:
            client_secret = stored_secret
    if not client_id:
        raise HTTPException(
            400,
            "X / Twitter OAuth client is not configured. Open Settings → "
            "OAuth client credentials and add a Client ID.",
        )
    if not origin:
        raise HTTPException(400, "origin is required")

    _gc_pending()
    state = secrets.token_urlsafe(24)
    verifier, challenge = _pkce_pair()
    redirect_uri = f"{origin}{TWITTER_REDIRECT_PATH}"
    _pending[state] = {
        "platform": "twitter",
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "code_verifier": verifier,
        "project_slug": project_slug,
        "ts": time.time(),
    }

    auth_url = "https://twitter.com/i/oauth2/authorize?" + urlencode({
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": TWITTER_SCOPES,
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    })
    return {"auth_url": auth_url, "redirect_uri": redirect_uri}


@router.get("/twitter/callback", response_class=HTMLResponse)
async def twitter_callback(
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
):
    if error:
        return _result_page(False, f"X denied authorization: {error} — {error_description or ''}", platform="twitter")
    if not code or not state:
        return _result_page(False, "Missing code or state.", platform="twitter")

    pending = _pending.pop(state, None)
    if pending is None or pending.get("platform") != "twitter":
        return _result_page(False, "Unknown or expired OAuth state.", platform="twitter")

    # Token exchange. Confidential clients use Basic auth; public clients send
    # client_id in the body. We try Basic first when client_secret is present.
    auth = None
    body = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": pending["redirect_uri"],
        "code_verifier": pending["code_verifier"],
        "client_id": pending["client_id"],
    }
    if pending.get("client_secret"):
        auth = (pending["client_id"], pending["client_secret"])

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            token_resp = await client.post(
                "https://api.twitter.com/2/oauth2/token",
                data=body,
                auth=auth,
            )
    except Exception as exc:
        return _result_page(False, f"Network error: {exc}", platform="twitter")

    if token_resp.status_code != 200:
        return _result_page(False, f"Token exchange failed ({token_resp.status_code}): {token_resp.text}", platform="twitter")

    token_data = token_resp.json()
    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token", "")
    if not access_token:
        return _result_page(False, f"Missing access_token: {token_data}", platform="twitter")

    # Fetch the @handle + numeric id so we can identify the account stably.
    username = ""
    user_id = ""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            me = await client.get(
                "https://api.twitter.com/2/users/me",
                headers={"Authorization": f"Bearer {access_token}"},
            )
        if me.status_code == 200:
            data = (me.json().get("data") or {})
            username = data.get("username", "")
            user_id = data.get("id", "")
    except Exception:
        pass

    if not user_id:
        return _result_page(
            False,
            "Could not resolve X user id. The provided OAuth scope must include users.read.",
            platform="twitter",
        )

    bundle = {
        "bearer_token": access_token,
        "client_id": pending["client_id"],
    }
    if refresh_token:
        bundle["refresh_token"] = refresh_token
    if pending.get("client_secret"):
        bundle["client_secret"] = pending["client_secret"]
    if username:
        bundle["username"] = username

    cred = await _persist_oauth_credential(
        platform="twitter",
        provider_account_id=str(user_id),
        username=username or str(user_id),
        bundle=bundle,
        project_slug=pending.get("project_slug"),
        is_nickname=not username,
    )
    return _result_page(
        True,
        f"X connected as @{username or user_id}.",
        platform="twitter",
        payload=_success_payload(cred, pending.get("project_slug")),
    )


# --- Mastodon — per-instance dynamic client registration ------------------


@router.post("/mastodon/start")
async def mastodon_start(data: dict):
    """Begin Mastodon OAuth flow.

    Body: {"instance_url": "https://mastodon.social", "origin": "http://127.0.0.1:8008"}

    We dynamically register an OAuth app on the user's chosen instance via
    ``POST /api/v1/apps`` (no admin needed) and then redirect to the auth URL.
    """
    instance_raw = (data.get("instance_url") or "").strip().rstrip("/")
    origin = (data.get("origin") or "").rstrip("/")
    project_slug = (data.get("project_slug") or "").strip() or None
    if not instance_raw or not origin:
        raise HTTPException(400, "instance_url and origin are required")
    parsed = urlparse(instance_raw if "://" in instance_raw else f"https://{instance_raw}")
    instance = f"{parsed.scheme or 'https'}://{parsed.netloc or parsed.path}"

    _gc_pending()
    state = secrets.token_urlsafe(24)
    redirect_uri = f"{origin}{MASTODON_REDIRECT_PATH}"

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            register = await client.post(
                f"{instance}/api/v1/apps",
                data={
                    "client_name": "Drew's YT Scheduler",
                    "redirect_uris": redirect_uri,
                    "scopes": MASTODON_SCOPES,
                    "website": "https://github.com/drewster99/drews-youtube-socialmedia-scheduler",
                },
            )
    except Exception as exc:
        raise HTTPException(502, f"Could not register app on {instance}: {exc}") from exc
    if register.status_code != 200:
        raise HTTPException(register.status_code, f"App registration failed: {register.text}")

    creds = register.json()
    client_id = creds.get("client_id")
    client_secret = creds.get("client_secret")
    if not client_id or not client_secret:
        raise HTTPException(502, f"Mastodon registration response missing client_id/secret: {creds}")

    _pending[state] = {
        "platform": "mastodon",
        "instance": instance,
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "project_slug": project_slug,
        "ts": time.time(),
    }

    auth_url = f"{instance}/oauth/authorize?" + urlencode({
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": MASTODON_SCOPES,
        "state": state,
    })
    return {"auth_url": auth_url, "redirect_uri": redirect_uri}


@router.get("/mastodon/callback", response_class=HTMLResponse)
async def mastodon_callback(
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
):
    if error:
        return _result_page(False, f"Mastodon denied: {error} — {error_description or ''}", platform="mastodon")
    if not code or not state:
        return _result_page(False, "Missing code or state.", platform="mastodon")
    pending = _pending.pop(state, None)
    if pending is None or pending.get("platform") != "mastodon":
        return _result_page(False, "Unknown or expired state.", platform="mastodon")

    instance = pending["instance"]
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            token_resp = await client.post(
                f"{instance}/oauth/token",
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "client_id": pending["client_id"],
                    "client_secret": pending["client_secret"],
                    "redirect_uri": pending["redirect_uri"],
                    "scope": MASTODON_SCOPES,
                },
            )
    except Exception as exc:
        return _result_page(False, f"Network error: {exc}", platform="mastodon")
    if token_resp.status_code != 200:
        return _result_page(False, f"Token exchange failed ({token_resp.status_code}): {token_resp.text}", platform="mastodon")

    access_token = token_resp.json().get("access_token")
    if not access_token:
        return _result_page(False, "Mastodon response missing access_token.", platform="mastodon")

    handle = ""
    account_id = ""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            verify = await client.get(
                f"{instance}/api/v1/accounts/verify_credentials",
                headers={"Authorization": f"Bearer {access_token}"},
            )
        if verify.status_code == 200:
            verify_data = verify.json()
            handle = verify_data.get("acct") or verify_data.get("username") or ""
            account_id = str(verify_data.get("id") or "")
    except Exception:
        pass

    host = urlparse(instance).netloc
    if handle and "@" not in handle and host:
        handle = f"{handle}@{host}"

    if not account_id:
        return _result_page(
            False,
            "Could not resolve Mastodon account id. Check that the access token has the read scope.",
            platform="mastodon",
        )
    provider_account_id = f"{account_id}@{host}" if host else account_id

    bundle = {
        "instance_url": instance,
        "access_token": access_token,
        "client_id": pending["client_id"],
        "client_secret": pending["client_secret"],
    }
    if handle:
        bundle["username"] = handle

    cred = await _persist_oauth_credential(
        platform="mastodon",
        provider_account_id=provider_account_id,
        username=handle or account_id,
        bundle=bundle,
        project_slug=pending.get("project_slug"),
        is_nickname=not handle,
    )
    return _result_page(
        True,
        f"Mastodon connected as @{handle or account_id}.",
        platform="mastodon",
        payload=_success_payload(cred, pending.get("project_slug")),
    )


# --- YouTube web OAuth -------------------------------------------------------
#
# Two start modes:
#   * ``re_auth`` re-authenticates an existing project. Same channel id ⇒
#     tokens are refreshed; different channel id ⇒ rejected (and the old
#     tokens stay in place).
#   * ``pre_create`` is the first step of the new-project wizard. The
#     project row is INSERTed only when the OAuth callback resolves a
#     channel id that isn't already claimed by another project. This keeps
#     half-created projects out of the DB.


@router.post("/youtube/start")
async def youtube_start(data: dict):
    """Begin the YouTube OAuth web flow.

    Body shape (one of two modes):

    * ``{"origin": "http://127.0.0.1:8008", "project_slug": "<existing>"}``
      — re-authenticate the named project.
    * ``{"origin": "...", "pre_create": {"name": "My new project"}}``
      — wizard step 2: name was just typed; project row will be created
      inside the callback if everything checks out.

    Returns ``{"auth_url": "..."}``. Caller should pre-open a popup
    (see ``openOAuthPopup`` helper in app.js) so the browser preserves
    the user gesture.
    """
    origin = (data.get("origin") or "").rstrip("/")
    if not origin:
        raise HTTPException(400, "origin is required")
    if not has_client_secret():
        raise HTTPException(
            400,
            "No OAuth client configured. Upload your client_secret.json from "
            "Settings before starting OAuth.",
        )

    mode_re_auth = (data.get("project_slug") or "").strip() or None
    # Channel-first wizard sends ``pre_create: {}`` (empty dict) before
    # the channel is known. ``data.get("pre_create") or None`` would
    # collapse that to None and 400; check for key PRESENCE instead.
    pre_create_present = "pre_create" in data and data.get("pre_create") is not None
    pre_create_blob = data.get("pre_create") if pre_create_present else None
    if mode_re_auth and pre_create_present:
        raise HTTPException(400, "Provide project_slug OR pre_create, not both")
    if not mode_re_auth and not pre_create_present:
        raise HTTPException(400, "Provide project_slug or pre_create")

    db = await get_db()
    pre_create: dict | None = None

    if mode_re_auth:
        cursor = await db.execute(
            "SELECT id FROM projects WHERE slug = ?", (mode_re_auth,)
        )
        if await cursor.fetchone() is None:
            raise HTTPException(404, f"Project '{mode_re_auth}' not found")
    else:
        name = (pre_create_blob.get("name") or "").strip()
        if name:
            candidate_slug = slugify(name)
            cursor = await db.execute(
                "SELECT id FROM projects WHERE slug = ?", (candidate_slug,)
            )
            if await cursor.fetchone() is not None:
                raise HTTPException(
                    400,
                    f"A project with slug '{candidate_slug}' already exists. "
                    "Pick a different name.",
                )
            pre_create = {"name": name, "slug": candidate_slug}
        else:
            # Channel-first wizard mode: caller doesn't know the project
            # name yet — they'll OAuth first, then we use the resolved
            # YouTube channel title as the default. The callback fills
            # in name + slug once it has the channel.
            pre_create = {"name": None, "slug": None}

    redirect_uri = f"{origin}{YOUTUBE_REDIRECT_PATH}"
    config = get_client_secret_dict()
    if config is None:
        raise HTTPException(400, "Could not load client_secret config")

    from google_auth_oauthlib.flow import Flow

    try:
        flow = Flow.from_client_config(
            config, scopes=YOUTUBE_SCOPES, redirect_uri=redirect_uri,
        )
    except Exception as exc:
        raise HTTPException(400, f"Invalid client_secret config: {exc}") from exc

    # PKCE: generate the code_verifier here so we can persist it across
    # the start→callback boundary. ``flow.authorization_url`` would
    # otherwise auto-generate one and stash it on the Flow object, which
    # we throw away when this request returns — making token exchange in
    # the callback fail with "(invalid_grant) Missing code verifier".
    # 64 random url-safe bytes → ~86 chars, well within the RFC 7636
    # 43–128 char range.
    code_verifier = secrets.token_urlsafe(64)
    flow.code_verifier = code_verifier

    state = secrets.token_urlsafe(24)
    auth_url, _ = flow.authorization_url(
        prompt="consent",
        access_type="offline",
        include_granted_scopes="true",
        state=state,
    )
    _gc_pending()
    _pending[state] = {
        "platform": "youtube",
        "mode": "re_auth" if mode_re_auth else "pre_create",
        "project_slug": mode_re_auth,
        "pre_create": pre_create,
        "redirect_uri": redirect_uri,
        "code_verifier": code_verifier,
        "ts": time.time(),
    }
    return {"auth_url": auth_url}


@router.get("/youtube/callback", response_class=HTMLResponse)
async def youtube_callback(
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
):
    if error:
        return _result_page(
            False,
            f"Google denied authorization: {error} — {error_description or ''}",
            platform="youtube",
        )
    if not code or not state:
        return _result_page(False, "Missing code or state.", platform="youtube")

    pending = _pending.pop(state, None)
    if pending is None or pending.get("platform") != "youtube":
        return _result_page(
            False,
            "Unknown or expired OAuth state. Click Connect again.",
            platform="youtube",
        )

    config = get_client_secret_dict()
    if config is None:
        return _result_page(
            False,
            "client_secret config disappeared between start and callback.",
            platform="youtube",
        )

    from google_auth_oauthlib.flow import Flow

    try:
        flow = Flow.from_client_config(
            config, scopes=YOUTUBE_SCOPES, redirect_uri=pending["redirect_uri"],
        )
        # Replay the PKCE code_verifier we generated at start time —
        # without this, Google rejects the exchange with
        # "(invalid_grant) Missing code verifier" because the auth_url
        # was issued with a code_challenge.
        verifier = pending.get("code_verifier")
        if verifier:
            flow.code_verifier = verifier
        flow.fetch_token(code=code)
        creds = flow.credentials
    except Exception as exc:
        return _result_page(
            False, f"Token exchange failed: {exc}", platform="youtube"
        )

    channel_id, channel_title, channel_handle = channel_id_from_credentials(creds)
    if not channel_id:
        return _result_page(
            False,
            "Could not resolve a YouTube channel for these credentials.",
            platform="youtube",
        )

    db = await get_db()

    if pending["mode"] == "pre_create":
        pre = pending["pre_create"] or {}
        cursor = await db.execute(
            "SELECT slug, name FROM projects WHERE youtube_channel_id = ?",
            (channel_id,),
        )
        existing = await cursor.fetchone()
        if existing is not None:
            return _result_page(
                False,
                f"That YouTube channel ({channel_title or channel_id}) is already "
                f"bound to project '{existing['name']}'.",
                platform="youtube",
            )

        # Channel-first wizard: pre_create.name was None at start time;
        # derive the default name from the resolved channel title now.
        name = pre.get("name")
        slug = pre.get("slug")
        if not name:
            name = (channel_title or channel_handle or channel_id or "Project").strip()
            base_slug = slugify(name) or "project"
            slug = base_slug
            counter = 2
            while True:
                cursor = await db.execute(
                    "SELECT id FROM projects WHERE slug = ?", (slug,)
                )
                if await cursor.fetchone() is None:
                    break
                slug = f"{base_slug}-{counter}"
                counter += 1
        else:
            cursor = await db.execute(
                "SELECT id FROM projects WHERE slug = ?", (slug,)
            )
            if await cursor.fetchone() is not None:
                return _result_page(
                    False,
                    f"Project slug '{slug}' was claimed before this OAuth completed.",
                    platform="youtube",
                )

        # Compose project_url from the channel handle (preferred — the
        # @drewanddanmorning form) or fall back to the channel-id URL when
        # the channel has no published custom URL.
        project_url = _youtube.compose_channel_url(channel_handle, channel_id)

        cursor = await db.execute(
            "INSERT INTO projects (name, slug, youtube_channel_id, project_url) "
            "VALUES (?, ?, ?, ?)",
            (name, slug, channel_id, project_url),
        )
        await db.commit()
        project_id = int(cursor.lastrowid)
        store_credentials(slug, creds)

        from yt_scheduler.services.templates import ensure_default_template

        await ensure_default_template(project_id=project_id)

        return _result_page(
            True,
            f"Project '{name}' created and connected to {channel_title or channel_id}.",
            platform="youtube",
            payload={
                "mode": "pre_create",
                "slug": slug,
                "name": name,
                "channel_id": channel_id,
                "channel_title": channel_title,
                "channel_handle": channel_handle,
                "project_id": project_id,
            },
        )

    project_slug = pending.get("project_slug")
    cursor = await db.execute(
        "SELECT id, name, youtube_channel_id FROM projects WHERE slug = ?",
        (project_slug,),
    )
    project = await cursor.fetchone()
    if project is None:
        return _result_page(
            False,
            f"Project '{project_slug}' was deleted before this OAuth completed.",
            platform="youtube",
        )

    bound = project["youtube_channel_id"]
    if bound and bound != channel_id:
        return _result_page(
            False,
            f"This project is bound to channel {bound}, but you signed in as "
            f"{channel_title or channel_id}. Re-authenticate using the bound channel "
            "or create a new project for the new channel.",
            platform="youtube",
        )

    if not bound:
        # Seed project_url on first bind, AND upgrade migration-backfilled
        # channel-id-form URLs to the prettier @handle form. The CASE
        # detects three states:
        #   - NULL                                       -> set to handle form
        #   - channel-id form for THIS channel (auto)    -> upgrade
        #   - anything else (user-edited / handle form)  -> preserve
        # If a user genuinely wants the channel-id form back, they can
        # PATCH the project; the OAuth flow stops mutating it after that.
        composed_url = _youtube.compose_channel_url(channel_handle, channel_id)
        canonical_channel_id_url = f"https://www.youtube.com/channel/{channel_id}"
        await db.execute(
            "UPDATE projects "
            "SET youtube_channel_id = ?, "
            "    project_url = CASE "
            "        WHEN project_url IS NULL THEN ? "
            "        WHEN project_url = ? THEN ? "
            "        ELSE project_url "
            "    END, "
            "    updated_at = datetime('now') "
            "WHERE id = ?",
            (
                channel_id,
                composed_url,
                canonical_channel_id_url,
                composed_url,
                project["id"],
            ),
        )
        await db.commit()

    store_credentials(project_slug, creds)
    return _result_page(
        True,
        f"{project['name']} re-connected to {channel_title or channel_id}.",
        platform="youtube",
        payload={
            "mode": "re_auth",
            "slug": project_slug,
            "channel_id": channel_id,
            "channel_title": channel_title,
            "channel_handle": channel_handle,
        },
    )


def _result_page(
    ok: bool,
    message: str,
    platform: str = "linkedin",
    payload: dict | None = None,
) -> HTMLResponse:
    """Small self-contained HTML page shown in the popup/tab after callback.

    ``payload`` is merged into the postMessage body so the opener (settings
    page or new-project wizard) can refresh its dropdowns from the
    credential id without a separate fetch.
    """
    color = "#3fb950" if ok else "#f85149"
    icon = "✓" if ok else "✕"
    title = display_name_for(platform) + " OAuth"
    message_payload = {"source": "oauth", "platform": platform, "ok": ok}
    if payload:
        message_payload.update(payload)
    payload_json = json.dumps(message_payload)
    body = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>{title}</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; background: #0d1117; color: #e6edf3;
         display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; }}
  .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 2rem 2.5rem; max-width: 520px; text-align: center; }}
  .icon {{ font-size: 3rem; color: {color}; }}
  h1 {{ margin: 0.5rem 0; font-size: 1.2rem; }}
  p {{ color: #8b949e; font-size: 0.9rem; }}
  button {{ margin-top: 1rem; padding: 0.5rem 1rem; background: #238636; color: white; border: 0; border-radius: 4px; cursor: pointer; }}
</style></head>
<body><div class="card">
  <div class="icon">{icon}</div>
  <h1>{"Success" if ok else "Failed"}</h1>
  <p>{message}</p>
  <button onclick="window.close()">Close</button>
</div>
<script>
  if (window.opener) {{
    try {{ window.opener.postMessage({payload_json}, '*'); }} catch (_) {{}}
  }}
</script>
</body></html>"""
    return HTMLResponse(body)


async def _bind_project_default(
    project_slug: str | None, platform: str, social_account_id: int
) -> None:
    """Set this credential as the project's default for the platform when
    the start endpoint was given a ``project_slug``."""
    if not project_slug:
        return
    db = await get_db()
    cursor = await db.execute(
        "SELECT id FROM projects WHERE slug = ?", (project_slug,)
    )
    row = await cursor.fetchone()
    if row is None:
        logger.info("project default binding skipped: unknown slug %s", project_slug)
        return
    project_id = int(row["id"])
    await db.execute(
        "INSERT INTO project_social_defaults (project_id, platform, social_account_id) "
        "VALUES (?, ?, ?) "
        "ON CONFLICT(project_id, platform) DO UPDATE SET social_account_id = excluded.social_account_id",
        (project_id, platform, social_account_id),
    )
    await db.commit()


async def _persist_oauth_credential(
    platform: str,
    provider_account_id: str,
    username: str,
    bundle: dict,
    project_slug: str | None,
    is_nickname: bool = False,
    display_name: str | None = None,
) -> dict:
    """Upsert the credential into ``social_accounts`` + Keychain bundle, and
    bind to a project default if ``project_slug`` is set. Returns the
    credential row dict (matching ``get_credential_by_uuid``)."""
    cred = await upsert_credential(
        platform=platform,
        provider_account_id=provider_account_id,
        username=username,
        bundle=bundle,
        is_nickname=is_nickname,
        display_name=display_name,
    )
    await _bind_project_default(project_slug, platform, cred["id"])
    return cred


def _success_payload(cred: dict, project_slug: str | None) -> dict:
    return {
        "social_account_id": cred["id"],
        "uuid": cred["uuid"],
        "username": cred["username"],
        "label": cred["label"],
        "project_slug": project_slug,
    }


# --- Bluesky AT-proto OAuth ------------------------------------------------

BLUESKY_REDIRECT_PATH = "/api/oauth/bluesky/callback"

# Pending Bluesky auth state, keyed by ``state``. Held in-process only;
# server restarts between start and callback force a re-auth.
_bluesky_pending: dict[str, bluesky_oauth.PendingAuth] = {}


@router.post("/bluesky/start")
async def bluesky_start(data: dict):
    """Begin a Bluesky AT-proto OAuth flow.

    Body shape::

        {
          "handle": "alice.bsky.social",
          "origin": "http://127.0.0.1:8008",
          "project_slug": "<optional, binds default on success>"
        }

    Returns ``{"auth_url": "https://bsky.social/oauth/authorize?..."}``.
    Caller pre-opens a popup and redirects it to ``auth_url``.
    """
    handle = bluesky_oauth.normalise_handle(data.get("handle") or "")
    origin = (data.get("origin") or "").rstrip("/")
    project_slug = (data.get("project_slug") or "").strip() or None
    if not handle:
        raise HTTPException(400, "A valid Bluesky handle is required (e.g. alice.bsky.social)")
    if not origin:
        raise HTTPException(400, "origin is required")

    redirect_uri = f"{origin}{BLUESKY_REDIRECT_PATH}"
    code_verifier, _challenge = bluesky_oauth.make_pkce_pair()
    state = secrets.token_urlsafe(24)
    private_key_pem = bluesky_oauth.generate_keypair_pem()

    async with httpx.AsyncClient(follow_redirects=True) as client:
        try:
            identity = await bluesky_oauth.resolve_identity(handle, client)
        except (ValueError, httpx.HTTPError) as exc:
            raise HTTPException(400, f"Could not resolve {handle}: {exc}") from exc
        try:
            auth_meta = await bluesky_oauth.discover_auth_server_for_pds(
                identity.pds, client
            )
        except (ValueError, httpx.HTTPError) as exc:
            raise HTTPException(
                400,
                f"Could not discover authorization server for PDS {identity.pds}: {exc}",
            ) from exc

        pending = bluesky_oauth.PendingAuth(
            handle=identity.handle,
            did=identity.did,
            pds=identity.pds,
            auth_server=auth_meta,
            redirect_uri=redirect_uri,
            code_verifier=code_verifier,
            state=state,
            private_key_pem=private_key_pem,
            project_slug=project_slug,
        )
        try:
            request_uri = await bluesky_oauth.push_authorization_request(pending, client)
        except (RuntimeError, httpx.HTTPError) as exc:
            raise HTTPException(400, f"PAR failed: {exc}") from exc

    _gc_pending()
    _bluesky_pending[state] = pending

    auth_url = bluesky_oauth.authorization_redirect_url(pending, request_uri)
    return {"auth_url": auth_url}


@router.get("/bluesky/callback", response_class=HTMLResponse)
async def bluesky_callback(
    code: str | None = None,
    state: str | None = None,
    iss: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
):
    if error:
        return _result_page(
            False,
            f"Bluesky denied authorization: {error} — {error_description or ''}",
            platform="bluesky",
        )
    if not code or not state:
        return _result_page(False, "Missing code or state.", platform="bluesky")

    pending = _bluesky_pending.pop(state, None)
    if pending is None:
        return _result_page(
            False,
            "Unknown or expired OAuth state. Click Connect with Bluesky again.",
            platform="bluesky",
        )

    if not iss:
        return _result_page(
            False,
            "Callback is missing the required 'iss' parameter — refusing as a "
            "defense against OAuth mix-up attacks. Click Connect with Bluesky "
            "again.",
            platform="bluesky",
        )
    if iss.rstrip("/") != pending.auth_server.issuer.rstrip("/"):
        return _result_page(
            False,
            f"Issuer mismatch: callback claimed {iss}, expected {pending.auth_server.issuer}.",
            platform="bluesky",
        )

    async with httpx.AsyncClient(follow_redirects=True) as client:
        try:
            tokens = await bluesky_oauth.exchange_code_for_tokens(pending, code, client)
        except (RuntimeError, httpx.HTTPError) as exc:
            return _result_page(
                False, f"Token exchange failed: {exc}", platform="bluesky"
            )

    sub = tokens.get("sub") or pending.did
    if sub != pending.did:
        logger.warning(
            "Bluesky OAuth: token sub %s does not match pre-resolved DID %s; trusting sub",
            sub, pending.did,
        )

    bundle = bluesky_oauth.credentialed_bundle(
        handle=pending.handle,
        did=sub,
        pds=pending.pds,
        auth_server_issuer=pending.auth_server.issuer,
        token_endpoint=pending.auth_server.token_endpoint,
        redirect_uri=pending.redirect_uri,
        private_key_pem=pending.private_key_pem,
        access_token=tokens["access_token"],
        refresh_token=tokens.get("refresh_token", ""),
        expires_in=int(tokens.get("expires_in") or 7200),
        dpop_nonce_as=pending.dpop_nonce_as,
    )
    cred = await _persist_oauth_credential(
        platform="bluesky",
        provider_account_id=sub,
        username=pending.handle,
        bundle=bundle,
        project_slug=pending.project_slug,
    )
    return _result_page(
        True,
        f"Connected as @{pending.handle}.",
        platform="bluesky",
        payload=_success_payload(cred, pending.project_slug),
    )
