"""AT-proto OAuth 2.1 client for Bluesky / arbitrary PDSes.

The flow:

* Resolve the user's handle (e.g. ``alice.bsky.social``) to a DID via
  DNS or ``/.well-known/atproto-did``.
* Resolve the DID to a DID document (``plc.directory`` or ``did:web:``).
* Pull the PDS endpoint out of the DID document.
* Hit the PDS's ``/.well-known/oauth-protected-resource`` to find which
  authorization server it delegates to.
* Hit the AS's ``/.well-known/oauth-authorization-server`` to discover
  the PAR / auth / token endpoints.
* Generate a per-credential ES256 keypair. The JWK thumbprint is bound
  to every access token issued during this OAuth session, and we sign
  one DPoP proof JWT per outgoing request.
* Push the authorization request to the PAR endpoint with a DPoP proof,
  receive a ``request_uri`` URN, redirect the user to the AS with that
  URI plus our ``client_id``.
* In the callback, exchange the code for tokens (again with DPoP).
  Bluesky enforces DPoP nonces — the first request comes back with a
  ``use_dpop_nonce`` error and a ``DPoP-Nonce`` header which we replay
  on the retry.

We never store an app password. The only secret in the credential bundle
is the per-credential ES256 private key (PEM-serialised). On
``BlueskyPoster.post`` we use the same key to sign DPoP proofs against
``com.atproto.repo.uploadBlob`` and ``createRecord`` on the user's PDS.

Localhost ``client_id`` shortcut:

The AT-proto OAuth spec lets clients with ``client_id`` starting with
``http://localhost`` skip the metadata-fetch step. The AS synthesises a
``client_metadata`` from query parameters appended to the URL. We use
this to avoid hosting any public file. The ``redirect_uri`` the user's
browser actually lands on can still be ``http://127.0.0.1:8008/...`` —
loopback addresses are interchangeable per OAuth's loopback rule.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import secrets
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote, urlparse

import httpx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import (
    decode_dss_signature,
)
from cryptography.hazmat.primitives.hashes import SHA256

logger = logging.getLogger(__name__)

# Public Bluesky entryway. Used for handle resolution as a fallback when
# the user types just a handle without enough info to find their PDS.
BSKY_API_FALLBACK = "https://bsky.social"

# Localhost shortcut: per the AT-proto OAuth spec, an AS that sees a
# client_id starting with this string skips the metadata fetch and
# synthesises client metadata from the URL's query string instead. We
# rely on this so the user never has to host a public client_metadata.json.
LOCALHOST_CLIENT_BASE = "http://localhost"

# Scopes we request. ``atproto`` is the base scope; ``transition:generic``
# grants access to most lexicons including ``com.atproto.repo.*``.
DEFAULT_SCOPES = "atproto transition:generic"


# --- Base64url helpers -----------------------------------------------------


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(text: str) -> bytes:
    padding = "=" * ((4 - len(text) % 4) % 4)
    return base64.urlsafe_b64decode(text + padding)


# --- ES256 keypair ---------------------------------------------------------


def generate_keypair_pem() -> str:
    """Generate a fresh ES256 (P-256) private key, PEM-serialised so it
    round-trips through the JSON Keychain bundle."""
    key = ec.generate_private_key(ec.SECP256R1())
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return pem.decode("ascii")


def load_private_key(pem: str) -> ec.EllipticCurvePrivateKey:
    key = serialization.load_pem_private_key(pem.encode("ascii"), password=None)
    if not isinstance(key, ec.EllipticCurvePrivateKey):
        raise ValueError("Stored key is not an EC private key")
    return key


def public_jwk(private_key: ec.EllipticCurvePrivateKey) -> dict[str, str]:
    """Return the public-key JWK (RFC 7517) for the given P-256 private key."""
    numbers = private_key.public_key().public_numbers()
    x = numbers.x.to_bytes(32, "big")
    y = numbers.y.to_bytes(32, "big")
    return {
        "kty": "EC",
        "crv": "P-256",
        "x": _b64url_encode(x),
        "y": _b64url_encode(y),
    }


def jwk_thumbprint(jwk: dict[str, str]) -> str:
    """RFC 7638 JWK thumbprint of an EC public-key JWK."""
    canonical = json.dumps(
        {"crv": jwk["crv"], "kty": jwk["kty"], "x": jwk["x"], "y": jwk["y"]},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return _b64url_encode(hashlib.sha256(canonical).digest())


# --- DPoP proof JWT --------------------------------------------------------


def sign_dpop_proof(
    private_key_pem: str,
    htm: str,
    htu: str,
    *,
    nonce: str | None = None,
    access_token: str | None = None,
) -> str:
    """Build and sign a DPoP proof JWT.

    ``htm`` is the HTTP method (e.g. ``POST``); ``htu`` is the request
    URL without query string and fragment. ``nonce`` is the value the
    server gave us in a previous response's ``DPoP-Nonce`` header (None
    for the very first request). ``access_token`` is included as the
    ``ath`` claim once we have one — the AS doesn't require it on PAR
    or token requests, but the resource server does on every API call.
    """
    private_key = load_private_key(private_key_pem)
    jwk = public_jwk(private_key)

    header = {"typ": "dpop+jwt", "alg": "ES256", "jwk": jwk}
    payload: dict[str, Any] = {
        "jti": _b64url_encode(secrets.token_bytes(16)),
        "htm": htm.upper(),
        "htu": htu.split("?")[0].split("#")[0],
        "iat": int(time.time()),
    }
    if nonce:
        payload["nonce"] = nonce
    if access_token:
        payload["ath"] = _b64url_encode(
            hashlib.sha256(access_token.encode("ascii")).digest()
        )

    header_b64 = _b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")

    der_sig = private_key.sign(signing_input, ec.ECDSA(SHA256()))
    r, s = decode_dss_signature(der_sig)
    raw_sig = r.to_bytes(32, "big") + s.to_bytes(32, "big")
    sig_b64 = _b64url_encode(raw_sig)

    return f"{header_b64}.{payload_b64}.{sig_b64}"


# --- Handle / DID / PDS resolution ----------------------------------------


HANDLE_REGEX_HOST_CHAR = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-."


def normalise_handle(raw: str) -> str:
    """Trim leading ``@`` and lowercase. Returns empty string when invalid."""
    handle = (raw or "").strip().lstrip("@").lower()
    if not handle or "." not in handle:
        return ""
    if any(ch not in HANDLE_REGEX_HOST_CHAR for ch in handle):
        return ""
    return handle


async def resolve_handle_to_did(handle: str, client: httpx.AsyncClient) -> str:
    """Resolve a Bluesky handle to a DID.

    Uses the AT-proto well-known endpoint first (much more reliable than
    DNS-over-HTTPS), then falls back to ``com.atproto.identity.resolveHandle``
    on the public Bluesky entryway.
    """
    url = f"https://{handle}/.well-known/atproto-did"
    try:
        resp = await client.get(url, timeout=8)
        if resp.status_code == 200:
            text = (resp.text or "").strip()
            if text.startswith("did:"):
                return text
    except httpx.HTTPError as exc:
        logger.debug("well-known lookup for %s failed: %s", handle, exc)

    # Fallback to the public entryway's resolveHandle xrpc method.
    fallback = f"{BSKY_API_FALLBACK}/xrpc/com.atproto.identity.resolveHandle?handle={quote(handle)}"
    resp = await client.get(fallback, timeout=8)
    if resp.status_code != 200:
        raise ValueError(f"Could not resolve handle {handle!r} (HTTP {resp.status_code})")
    data = resp.json() or {}
    did = data.get("did")
    if not isinstance(did, str) or not did.startswith("did:"):
        raise ValueError(f"resolveHandle returned no DID for {handle!r}")
    return did


async def fetch_did_document(did: str, client: httpx.AsyncClient) -> dict:
    """Fetch the DID document for a ``did:plc:*`` or ``did:web:*`` DID."""
    if did.startswith("did:plc:"):
        url = f"https://plc.directory/{did}"
    elif did.startswith("did:web:"):
        host = did[len("did:web:"):]
        url = f"https://{host}/.well-known/did.json"
    else:
        raise ValueError(f"Unsupported DID method: {did}")
    resp = await client.get(url, timeout=8)
    resp.raise_for_status()
    return resp.json() or {}


def pds_endpoint_from_did_document(doc: dict) -> str:
    """Extract the AT-proto PDS service endpoint from a DID document."""
    services = doc.get("service") or []
    for entry in services:
        if not isinstance(entry, dict):
            continue
        sid = entry.get("id") or ""
        stype = entry.get("type") or ""
        if sid in {"#atproto_pds", "atproto_pds"} or stype == "AtprotoPersonalDataServer":
            endpoint = entry.get("serviceEndpoint")
            if isinstance(endpoint, str) and endpoint.startswith("http"):
                return endpoint.rstrip("/")
    raise ValueError("DID document does not advertise an atproto_pds endpoint")


@dataclass
class ResolvedIdentity:
    handle: str
    did: str
    pds: str


async def resolve_identity(handle: str, client: httpx.AsyncClient) -> ResolvedIdentity:
    """Round-trip a handle to ``(handle, did, pds_url)``."""
    did = await resolve_handle_to_did(handle, client)
    doc = await fetch_did_document(did, client)
    pds = pds_endpoint_from_did_document(doc)
    return ResolvedIdentity(handle=handle, did=did, pds=pds)


# --- Authorization-server discovery ---------------------------------------


@dataclass
class AuthServerMetadata:
    issuer: str
    authorization_endpoint: str
    token_endpoint: str
    pushed_authorization_request_endpoint: str
    revocation_endpoint: str | None = None
    scopes_supported: list[str] = field(default_factory=list)
    dpop_signing_alg_values_supported: list[str] = field(default_factory=list)


async def discover_auth_server_for_pds(
    pds_url: str, client: httpx.AsyncClient
) -> AuthServerMetadata:
    """Walk the PDS → protected-resource → authorization-server discovery chain."""
    pr_url = pds_url.rstrip("/") + "/.well-known/oauth-protected-resource"
    pr_resp = await client.get(pr_url, timeout=8)
    pr_resp.raise_for_status()
    pr = pr_resp.json() or {}
    auth_servers = pr.get("authorization_servers") or []
    if not auth_servers:
        raise ValueError(f"PDS {pds_url} advertises no authorization_servers")
    issuer = auth_servers[0]

    as_url = issuer.rstrip("/") + "/.well-known/oauth-authorization-server"
    as_resp = await client.get(as_url, timeout=8)
    as_resp.raise_for_status()
    meta = as_resp.json() or {}
    par = meta.get("pushed_authorization_request_endpoint")
    auth_ep = meta.get("authorization_endpoint")
    token_ep = meta.get("token_endpoint")
    if not (par and auth_ep and token_ep):
        raise ValueError(
            f"AS {issuer} metadata missing required endpoints "
            "(pushed_authorization_request_endpoint, authorization_endpoint, "
            "token_endpoint)"
        )

    return AuthServerMetadata(
        issuer=meta.get("issuer", issuer),
        authorization_endpoint=auth_ep,
        token_endpoint=token_ep,
        pushed_authorization_request_endpoint=par,
        revocation_endpoint=meta.get("revocation_endpoint"),
        scopes_supported=list(meta.get("scopes_supported") or []),
        dpop_signing_alg_values_supported=list(meta.get("dpop_signing_alg_values_supported") or []),
    )


# --- PKCE helpers ----------------------------------------------------------


def make_pkce_pair() -> tuple[str, str]:
    """Return ``(code_verifier, code_challenge_S256)`` per RFC 7636."""
    verifier = _b64url_encode(secrets.token_bytes(32))
    challenge = _b64url_encode(hashlib.sha256(verifier.encode("ascii")).digest())
    return verifier, challenge


# --- Localhost client_id helpers ------------------------------------------


def localhost_client_id(redirect_uri: str, scope: str = DEFAULT_SCOPES) -> str:
    """Build the synthesised localhost client_id URL the AS will accept
    without fetching client metadata."""
    return (
        f"{LOCALHOST_CLIENT_BASE}"
        f"?redirect_uri={quote(redirect_uri, safe='')}"
        f"&scope={quote(scope, safe='')}"
    )


# --- High-level: PAR push --------------------------------------------------


@dataclass
class PendingAuth:
    """Everything the callback needs to finish the OAuth flow."""

    handle: str
    did: str
    pds: str
    auth_server: AuthServerMetadata
    redirect_uri: str
    code_verifier: str
    state: str
    private_key_pem: str
    project_slug: str | None = None
    pre_create: dict | None = None
    dpop_nonce: str | None = None  # last-known AS nonce, replayed on token call


def _is_dpop_nonce_error(resp: httpx.Response) -> bool:
    if resp.status_code not in (400, 401):
        return False
    try:
        body = resp.json() or {}
    except Exception:
        return False
    err = body.get("error", "")
    return err == "use_dpop_nonce"


async def push_authorization_request(
    pending: PendingAuth, client: httpx.AsyncClient
) -> str:
    """Send the PAR request, return the ``request_uri`` URN.

    Retries once if the AS demands a DPoP nonce. The ``DPoP-Nonce`` value
    from the response is also stashed on ``pending`` for the token call.
    """
    par_url = pending.auth_server.pushed_authorization_request_endpoint
    code_challenge = _b64url_encode(
        hashlib.sha256(pending.code_verifier.encode("ascii")).digest()
    )
    client_id = localhost_client_id(pending.redirect_uri)
    form = {
        "client_id": client_id,
        "response_type": "code",
        "redirect_uri": pending.redirect_uri,
        "scope": DEFAULT_SCOPES,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "state": pending.state,
        "login_hint": pending.handle,
    }

    async def _do(nonce: str | None) -> httpx.Response:
        proof = sign_dpop_proof(
            pending.private_key_pem, "POST", par_url, nonce=nonce
        )
        return await client.post(
            par_url, data=form, headers={"DPoP": proof}, timeout=15
        )

    resp = await _do(None)
    if _is_dpop_nonce_error(resp):
        nonce = resp.headers.get("DPoP-Nonce")
        if not nonce:
            raise RuntimeError("PAR demanded a DPoP nonce but did not provide one")
        pending.dpop_nonce = nonce
        resp = await _do(nonce)

    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"PAR rejected by {par_url}: HTTP {resp.status_code} {resp.text}"
        )

    new_nonce = resp.headers.get("DPoP-Nonce")
    if new_nonce:
        pending.dpop_nonce = new_nonce

    body = resp.json() or {}
    request_uri = body.get("request_uri")
    if not request_uri:
        raise RuntimeError(f"PAR succeeded but returned no request_uri: {body}")
    return request_uri


def authorization_redirect_url(pending: PendingAuth, request_uri: str) -> str:
    """Build the URL we redirect the user's browser to so they can consent."""
    client_id = localhost_client_id(pending.redirect_uri)
    return (
        f"{pending.auth_server.authorization_endpoint}"
        f"?client_id={quote(client_id, safe='')}"
        f"&request_uri={quote(request_uri, safe='')}"
    )


# --- Token exchange + refresh ---------------------------------------------


async def exchange_code_for_tokens(
    pending: PendingAuth, code: str, client: httpx.AsyncClient
) -> dict:
    """Trade the auth code for ``{access_token, refresh_token, sub, ...}``.

    Replays ``DPoP-Nonce`` once on ``use_dpop_nonce``. Updates
    ``pending.dpop_nonce`` from the response so the resource-server side
    can pick up where we left off.
    """
    token_url = pending.auth_server.token_endpoint
    client_id = localhost_client_id(pending.redirect_uri)
    form = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": pending.redirect_uri,
        "client_id": client_id,
        "code_verifier": pending.code_verifier,
    }

    async def _do(nonce: str | None) -> httpx.Response:
        proof = sign_dpop_proof(
            pending.private_key_pem, "POST", token_url, nonce=nonce
        )
        return await client.post(
            token_url, data=form, headers={"DPoP": proof}, timeout=15
        )

    resp = await _do(pending.dpop_nonce)
    if _is_dpop_nonce_error(resp):
        nonce = resp.headers.get("DPoP-Nonce")
        if not nonce:
            raise RuntimeError("Token endpoint demanded DPoP nonce but did not provide one")
        pending.dpop_nonce = nonce
        resp = await _do(nonce)

    if resp.status_code != 200:
        raise RuntimeError(
            f"Token exchange failed at {token_url}: HTTP {resp.status_code} {resp.text}"
        )

    new_nonce = resp.headers.get("DPoP-Nonce")
    if new_nonce:
        pending.dpop_nonce = new_nonce

    body = resp.json() or {}
    if "access_token" not in body:
        raise RuntimeError(f"Token exchange succeeded but returned no access_token: {body}")
    return body


async def refresh_tokens(
    *,
    refresh_token: str,
    private_key_pem: str,
    token_endpoint: str,
    redirect_uri: str,
    nonce: str | None,
    client: httpx.AsyncClient,
) -> dict:
    """Use a refresh_token to mint a new access_token (and a rotated
    refresh_token). Returns the same shape as the initial token exchange."""
    client_id = localhost_client_id(redirect_uri)
    form = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
    }

    async def _do(n: str | None) -> httpx.Response:
        proof = sign_dpop_proof(private_key_pem, "POST", token_endpoint, nonce=n)
        return await client.post(
            token_endpoint, data=form, headers={"DPoP": proof}, timeout=15
        )

    resp = await _do(nonce)
    if _is_dpop_nonce_error(resp):
        new_nonce = resp.headers.get("DPoP-Nonce")
        if not new_nonce:
            raise RuntimeError("Refresh endpoint demanded DPoP nonce but did not provide one")
        resp = await _do(new_nonce)

    if resp.status_code != 200:
        raise RuntimeError(
            f"Refresh failed: HTTP {resp.status_code} {resp.text}"
        )
    body = resp.json() or {}
    body["dpop_nonce"] = resp.headers.get("DPoP-Nonce") or nonce
    return body


# --- Convenience ------------------------------------------------------------


def credentialed_bundle(
    *,
    handle: str,
    did: str,
    pds: str,
    auth_server_issuer: str,
    token_endpoint: str,
    redirect_uri: str,
    private_key_pem: str,
    access_token: str,
    refresh_token: str,
    expires_in: int,
    dpop_nonce: str | None = None,
) -> dict:
    """Shape the bundle written into Keychain after a successful OAuth.

    The poster reads this exact shape on every send. Field names are
    stable; new fields can be added but never renamed.
    """
    return {
        "auth_method": "oauth",
        "handle": handle,
        "did": did,
        "pds": pds,
        "auth_server_issuer": auth_server_issuer,
        "token_endpoint": token_endpoint,
        "redirect_uri": redirect_uri,
        "private_key_pem": private_key_pem,
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_at": int(time.time()) + int(expires_in or 7200),
        "dpop_nonce": dpop_nonce,
    }


def parse_handle_from_redirect_uri(redirect_uri: str) -> str | None:
    """Best-effort extraction of the host:port we'll use as the OAuth
    redirect target. Used when callers need to derive a redirect URL
    from a request's ``origin`` header."""
    parsed = urlparse(redirect_uri)
    if not parsed.scheme or not parsed.netloc:
        return None
    return parsed.netloc


# Convenience exports for tests and callers that just need the verifier
# encoder without pulling cryptography directly.
__all__ = [
    "AuthServerMetadata",
    "DEFAULT_SCOPES",
    "LOCALHOST_CLIENT_BASE",
    "PendingAuth",
    "ResolvedIdentity",
    "_b64url_decode",
    "_b64url_encode",
    "authorization_redirect_url",
    "credentialed_bundle",
    "discover_auth_server_for_pds",
    "exchange_code_for_tokens",
    "fetch_did_document",
    "generate_keypair_pem",
    "jwk_thumbprint",
    "load_private_key",
    "localhost_client_id",
    "make_pkce_pair",
    "normalise_handle",
    "pds_endpoint_from_did_document",
    "public_jwk",
    "push_authorization_request",
    "refresh_tokens",
    "resolve_handle_to_did",
    "resolve_identity",
    "sign_dpop_proof",
]
