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
import secrets
import time
from urllib.parse import urlencode, urlparse

import httpx
from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse

from yt_scheduler.services.keychain import load_secret, store_secret

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

    Request body: {"client_id": "...", "client_secret": "...", "origin": "http://127.0.0.1:8008"}
    Returns: {"auth_url": "https://www.linkedin.com/oauth/v2/authorization?..."}
    """
    client_id = (data.get("client_id") or "").strip()
    client_secret = (data.get("client_secret") or "").strip()
    origin = (data.get("origin") or "").rstrip("/")
    if not client_id or not client_secret or not origin:
        raise HTTPException(400, "client_id, client_secret, and origin are required")

    _gc_pending()
    state = secrets.token_urlsafe(24)
    redirect_uri = f"{origin}{LINKEDIN_REDIRECT_PATH}"
    _pending[state] = {
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
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

    sub = (ui.json() or {}).get("sub")
    if not sub:
        return _result_page(False, "userinfo response missing sub claim.")
    person_urn = f"urn:li:person:{sub}"

    # Persist to Keychain (or encrypted file on non-mac). We also stash
    # client_id + client_secret so a future "Refresh" button could re-run the
    # flow without the user pasting credentials again.
    store_secret("linkedin", "access_token", access_token)
    store_secret("linkedin", "person_urn", person_urn)
    store_secret("linkedin", "client_id", pending["client_id"])
    store_secret("linkedin", "client_secret", pending["client_secret"])

    return _result_page(True, "LinkedIn connected. You can close this tab.", platform="linkedin")


@router.post("/threads/start")
async def threads_start(data: dict):
    """Begin the Threads OAuth flow.

    Request body: {"client_id": "...", "client_secret": "...", "origin": "http://127.0.0.1:8008"}
    Returns: {"auth_url": "https://threads.net/oauth/authorize?...", "redirect_uri": "..."}
    """
    client_id = (data.get("client_id") or "").strip()
    client_secret = (data.get("client_secret") or "").strip()
    origin = (data.get("origin") or "").rstrip("/")
    if not client_id or not client_secret or not origin:
        raise HTTPException(400, "client_id, client_secret, and origin are required")

    _gc_pending()
    state = secrets.token_urlsafe(24)
    redirect_uri = f"{origin}{THREADS_REDIRECT_PATH}"
    _pending[state] = {
        "platform": "threads",
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
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

    store_secret("threads", "access_token", access_token)
    store_secret("threads", "user_id", str(user_id))
    if username:
        store_secret("threads", "username", username)
    store_secret("threads", "client_id", pending["client_id"])
    store_secret("threads", "client_secret", pending["client_secret"])

    return _result_page(True, f"Threads connected as @{username or user_id}. You can close this tab.", platform="threads")


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
    if not app_secret or not short_token:
        raise HTTPException(400, "app_secret and short_lived_token are required")

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

    store_secret("threads", "access_token", access_token)
    store_secret("threads", "user_id", str(user_id))
    if username:
        store_secret("threads", "username", username)
    store_secret("threads", "client_secret", app_secret)

    return {"ok": True, "user_id": str(user_id), "username": username, "expires_in": long_data.get("expires_in")}


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
    if not client_id or not origin:
        raise HTTPException(400, "client_id and origin are required")

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

    # Fetch the @handle so the user can see which account they connected.
    username = ""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            me = await client.get(
                "https://api.twitter.com/2/users/me",
                headers={"Authorization": f"Bearer {access_token}"},
            )
        if me.status_code == 200:
            username = (me.json().get("data") or {}).get("username", "")
    except Exception:
        pass

    store_secret("twitter", "bearer_token", access_token)
    if refresh_token:
        store_secret("twitter", "refresh_token", refresh_token)
    if pending.get("client_secret"):
        store_secret("twitter", "client_secret", pending["client_secret"])
    store_secret("twitter", "client_id", pending["client_id"])
    if username:
        store_secret("twitter", "username", username)

    return _result_page(True, f"X connected as @{username or '?'}.", platform="twitter")


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
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            verify = await client.get(
                f"{instance}/api/v1/accounts/verify_credentials",
                headers={"Authorization": f"Bearer {access_token}"},
            )
        if verify.status_code == 200:
            handle = verify.json().get("acct") or verify.json().get("username") or ""
    except Exception:
        pass

    store_secret("mastodon", "instance_url", instance)
    store_secret("mastodon", "access_token", access_token)
    store_secret("mastodon", "client_id", pending["client_id"])
    store_secret("mastodon", "client_secret", pending["client_secret"])

    return _result_page(True, f"Mastodon connected as {handle or 'user'}.", platform="mastodon")


def _result_page(ok: bool, message: str, platform: str = "linkedin") -> HTMLResponse:
    """Small self-contained HTML page shown in the popup/tab after callback."""
    color = "#3fb950" if ok else "#f85149"
    icon = "✓" if ok else "✕"
    title = platform.capitalize() + " OAuth"
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
  // Notify the opener (the Settings page) so it can refresh status.
  if (window.opener) {{
    try {{ window.opener.postMessage({{source: 'oauth', platform: '{platform}', ok: {str(ok).lower()} }}, '*'); }} catch (_) {{}}
  }}
</script>
</body></html>"""
    return HTMLResponse(body)
