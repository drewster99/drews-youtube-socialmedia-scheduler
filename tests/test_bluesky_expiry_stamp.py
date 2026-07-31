"""Regression tests for ``bluesky_oauth.credentialed_bundle`` token stamping.

The bundle written into Keychain after a Bluesky OAuth used to fabricate a
2-hour ``expires_at`` whenever the issuer omitted ``expires_in``
(``int(expires_in or 7200)``). A fabricated expiry is worse than an absent
one: the refresh sweep waits confidently past a token that actually died
sooner. The decision (already made for Threads) is to record NULL / unknown —
NO ``expires_at`` — when the lifetime is unknown, and to route stamping
through the single source of mutation, ``social_credentials.stamp_token_metadata``.

Bluesky is safe to leave "unknown": ``BlueskyPoster.refresh_if_stale`` treats
an absent/0 ``expires_at`` as due, and Bluesky refresh is non-destructive.
"""

from __future__ import annotations

import importlib
import time

import pytest


def _make_bundle(expires_in: int | None) -> dict:
    """Build a credentialed bundle, importing the service lazily.

    Lazy import (rather than module scope) because ``isolated_data_dir`` purges
    ``yt_scheduler.*`` from ``sys.modules`` to re-freeze config; a module-level
    import would capture a stale module object.
    """
    bluesky_oauth = importlib.import_module("yt_scheduler.services.bluesky_oauth")
    return bluesky_oauth.credentialed_bundle(
        handle="alice.bsky.social",
        did="did:plc:abc123",
        pds="https://pds.example",
        auth_server_issuer="https://auth.example",
        token_endpoint="https://auth.example/token",
        redirect_uri="http://127.0.0.1:8008/api/oauth/bluesky/callback",
        private_key_pem="-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----",
        access_token="access-token-value",
        refresh_token="refresh-token-value",
        expires_in=expires_in,
    )


def test_present_expires_in_records_expiry(isolated_data_dir) -> None:
    before = int(time.time())
    bundle = _make_bundle(3600)
    after = int(time.time())

    # acquired_at is always stamped.
    assert before <= bundle["acquired_at"] <= after
    # A known lifetime records expires_at = acquired + lifetime.
    assert "expires_at" in bundle
    assert before + 3600 <= bundle["expires_at"] <= after + 3600


@pytest.mark.parametrize("missing", [None, 0])
def test_missing_expires_in_records_no_expiry(isolated_data_dir, missing) -> None:
    before = int(time.time())
    bundle = _make_bundle(missing)
    after = int(time.time())

    # acquired_at is still stamped — only the lifetime is unknown.
    assert before <= bundle["acquired_at"] <= after
    # No fabricated future expiry: unknown lifetime => NO expires_at key.
    assert "expires_at" not in bundle, (
        "missing/zero expires_in must record NULL/unknown, never a fabricated "
        f"lifetime; got expires_at={bundle.get('expires_at')!r}"
    )
