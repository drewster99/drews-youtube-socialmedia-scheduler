"""AWS Signature Version 4 presigning, for S3-compatible object stores.

Pure functions over stdlib ``hmac``/``hashlib`` — no network, no clock of its
own, no dependency. A presigned URL carries its whole authorization in the
query string, which is what lets us hand one to a third party (Meta fetches
Threads video from a URL rather than accepting an upload) without giving that
party a credential.

``now`` is always injected rather than read here so a signature is reproducible:
the tests pin it to published AWS test vectors and compare exact hex.
"""

from __future__ import annotations

import hashlib
import hmac
import urllib.parse
from datetime import datetime

ALGORITHM = "AWS4-HMAC-SHA256"

# Query-string auth signs this placeholder instead of a body hash, so the
# signature is independent of the payload. That is what makes it possible to
# sign an upload URL before reading the file, and to sign a download URL for a
# recipient who will fetch bytes we never send them directly.
UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"

# SigV4's own ceiling on X-Amz-Expires. Exceeding it produces a 403 whose body
# does not mention expiry, so it is worth catching here where the cause is
# obvious.
MAX_EXPIRES_SECONDS = 604800  # 7 days


class SigningError(ValueError):
    """A signing input is invalid — raised before any URL is produced."""


def _sign(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


def derive_signing_key(secret_access_key: str, date_stamp: str, region: str,
                       service: str = "s3") -> bytes:
    """Derive the SigV4 signing key.

    The secret never travels. This chain is what scopes a captured signature to
    one date, region and service.
    """
    key = f"AWS4{secret_access_key}".encode("utf-8")
    for part in (date_stamp, region, service, "aws4_request"):
        key = _sign(key, part)
    return key


def presign_url(
    *,
    method: str,
    host: str,
    bucket: str,
    key: str,
    access_key_id: str,
    secret_access_key: str,
    region: str,
    expires_seconds: int,
    now: datetime,
    service: str = "s3",
) -> str:
    """Build a presigned URL authorizing exactly one method on one object.

    Path-style addressing (``/<bucket>/<key>``), which is what Cloudflare R2's
    account endpoint expects.
    """
    if expires_seconds <= 0:
        raise SigningError(f"expires_seconds must be positive; got {expires_seconds}")
    if expires_seconds > MAX_EXPIRES_SECONDS:
        raise SigningError(
            f"expires_seconds caps at {MAX_EXPIRES_SECONDS} (7 days); got {expires_seconds}"
        )
    if not key:
        raise SigningError("object key is required")

    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    # safe="/" keeps path separators literal while percent-encoding everything
    # else. S3 does not double-encode the path, unlike most other AWS services.
    canonical_uri = (
        f"/{urllib.parse.quote(bucket, safe='')}/{urllib.parse.quote(key, safe='/')}"
    )

    credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
    query = {
        "X-Amz-Algorithm": ALGORITHM,
        "X-Amz-Credential": f"{access_key_id}/{credential_scope}",
        "X-Amz-Date": amz_date,
        "X-Amz-Expires": str(expires_seconds),
        "X-Amz-SignedHeaders": "host",
    }
    # Sorted by key and RFC3986-encoded, or the signature will not match.
    canonical_query = "&".join(
        f"{urllib.parse.quote(name, safe='')}={urllib.parse.quote(value, safe='')}"
        for name, value in sorted(query.items())
    )

    canonical_request = "\n".join([
        method.upper(),
        canonical_uri,
        canonical_query,
        f"host:{host}\n",
        "host",
        UNSIGNED_PAYLOAD,
    ])
    string_to_sign = "\n".join([
        ALGORITHM,
        amz_date,
        credential_scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])
    signature = hmac.new(
        derive_signing_key(secret_access_key, date_stamp, region, service),
        string_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    return f"https://{host}{canonical_uri}?{canonical_query}&X-Amz-Signature={signature}"
