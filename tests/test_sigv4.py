"""SigV4 presigning is pure and deterministic, so it is testable exactly.

These assert on fixed hex output with an injected clock. A signing bug otherwise
surfaces only as an opaque 403 from the provider, with nothing in the message
pointing at the signature.
"""

from __future__ import annotations

import urllib.parse
from datetime import datetime, timezone

import pytest

from yt_scheduler.services import sigv4

FIXED_NOW = datetime(2026, 7, 27, 12, 0, 0, tzinfo=timezone.utc)
ACCESS_KEY_ID = "AKIAIOSFODNN7EXAMPLE"
SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
HOST = "64102e7e8155344538065755e9cd8a7a.r2.cloudflarestorage.com"
BUCKET = "drewsvideosocials-threads-videos"


def _presign(**overrides) -> str:
    kwargs = dict(
        method="GET", host=HOST, bucket=BUCKET, key="abc123.mp4",
        access_key_id=ACCESS_KEY_ID, secret_access_key=SECRET,
        region="auto", expires_seconds=7200, now=FIXED_NOW,
    )
    kwargs.update(overrides)
    return sigv4.presign_url(**kwargs)


def _query(url: str) -> dict[str, str]:
    return {k: v[0] for k, v in urllib.parse.parse_qs(urllib.parse.urlsplit(url).query).items()}


def test_signature_is_stable_for_fixed_inputs():
    """Pins current output so a refactor can't silently change what gets signed.

    This is a regression guard, not an external proof — the external check is
    :func:`test_signing_key_derivation_matches_aws_worked_example` below. What
    makes this value trustworthy is that this exact signer was run against live
    Cloudflare R2, which accepted its signatures for PUT, GET and DELETE.
    """
    assert _presign() == (
        f"https://{HOST}/{BUCKET}/abc123.mp4"
        "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
        f"&X-Amz-Credential={ACCESS_KEY_ID}%2F20260727%2Fauto%2Fs3%2Faws4_request"
        "&X-Amz-Date=20260727T120000Z"
        "&X-Amz-Expires=7200"
        "&X-Amz-SignedHeaders=host"
        "&X-Amz-Signature=504bd8a95b3b97e33f2345cb8ed44be9c4e188813f4500df79dbda01395c9a1c"
    )


def test_signing_key_derivation_matches_aws_worked_example():
    """AWS's *published* key-derivation example — an authority outside this repo.

    Chain is AWS4<secret> -> date -> region -> service -> aws4_request. If this
    passes, the hard part of SigV4 is provably right rather than merely
    self-consistent.
    """
    key = sigv4.derive_signing_key(
        "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20150830", "us-east-1", "iam"
    )
    assert key.hex() == (
        "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
    )


def test_method_changes_the_signature():
    """Each verb is authorized separately — a GET URL must not permit a PUT."""
    signatures = {
        method: _query(_presign(method=method))["X-Amz-Signature"]
        for method in ("GET", "PUT", "DELETE")
    }
    assert len(set(signatures.values())) == 3


def test_key_and_expiry_are_bound_into_the_signature():
    base = _query(_presign())["X-Amz-Signature"]
    assert _query(_presign(key="other.mp4"))["X-Amz-Signature"] != base
    assert _query(_presign(expires_seconds=60))["X-Amz-Signature"] != base
    assert _query(_presign(region="us-east-1"))["X-Amz-Signature"] != base


def test_path_is_bucket_then_key_and_separators_survive():
    url = _presign(key="nested/path/clip.mp4")
    assert urllib.parse.urlsplit(url).path == f"/{BUCKET}/nested/path/clip.mp4"


def test_special_characters_in_key_are_encoded_not_doubled():
    """S3 does not double-encode the path. A space becomes %20 exactly once."""
    path = urllib.parse.urlsplit(_presign(key="a b.mp4")).path
    assert path == f"/{BUCKET}/a%20b.mp4"


def test_unsigned_payload_is_used_so_the_body_need_not_be_hashed():
    """Query-string auth signs a placeholder, which is what allows presigning an
    upload before reading the file and a download for a third party."""
    assert sigv4.UNSIGNED_PAYLOAD == "UNSIGNED-PAYLOAD"
    assert "X-Amz-Signature" in _presign()


@pytest.mark.parametrize("expires", [0, -1, sigv4.MAX_EXPIRES_SECONDS + 1])
def test_invalid_expiry_raises_rather_than_producing_a_doomed_url(expires):
    """Over the ceiling the provider returns a 403 whose body never mentions
    expiry, so this has to fail where the cause is visible."""
    with pytest.raises(sigv4.SigningError):
        _presign(expires_seconds=expires)


def test_maximum_expiry_is_allowed():
    assert _query(_presign(expires_seconds=sigv4.MAX_EXPIRES_SECONDS))["X-Amz-Expires"] == str(
        sigv4.MAX_EXPIRES_SECONDS
    )


def test_empty_key_raises():
    with pytest.raises(sigv4.SigningError):
        _presign(key="")
