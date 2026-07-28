"""Temporary public hosting for media a platform fetches instead of accepting.

Threads is the only such platform today, and the requirement covers **images
and video alike**: its API takes them as an ``image_url`` / ``video_url`` that
Meta cURLs itself, so the file has to be reachable over HTTPS for the length of
one post. This module puts it in a **private** Cloudflare R2 bucket and vends a
short-lived presigned GET URL — the URL is the credential, scoped to one object,
one verb and one time window, so the bucket is never world-readable.

Nothing here inspects the media kind: the object key keeps the source suffix and
``Content-Type`` comes from ``mimetypes``, so a JPEG and an MP4 take the same
path. Choosing ``IMAGE`` vs ``VIDEO`` is the caller's job.

Cleanup is deliberately not this module's job. The bucket enforces Object Lock
with a 24-hour minimum retention, so an early delete is impossible; the bucket's
7-day lifecycle rule removes objects instead. Access ends when the signature
expires, which happens long before the bytes do.
"""

from __future__ import annotations

import asyncio
import logging
import mimetypes
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from yt_scheduler.config import (
    MEDIA_HOSTING_ACCESS_KEY_ID_FIELD,
    MEDIA_HOSTING_CONNECTION_TEST_TIMEOUT_SECONDS,
    MEDIA_HOSTING_NAMESPACE,
    MEDIA_HOSTING_SECRET_ACCESS_KEY_FIELD,
    MEDIA_HOSTING_UPLOAD_CHUNK_BYTES,
    MEDIA_HOSTING_UPLOAD_TIMEOUT_SECONDS,
)
from yt_scheduler.services import sigv4

logger = logging.getLogger(__name__)

# R2 requires the literal "auto" in the credential scope. A bucket's location
# hint (e.g. ENAM) is a placement decision and is NOT this value — using it
# produces a signature R2 rejects with an opaque 403.
REGION = "auto"

# Long enough to cover container create -> FINISHED -> publish, which completes
# in seconds to minutes, and far shorter than the object's 7-day life. This is
# the real access control: once it lapses the object is unreachable even though
# Object Lock means it still exists.
DOWNLOAD_URL_TTL_SECONDS = 2 * 60 * 60

UPLOAD_URL_TTL_SECONDS = 60 * 60

SETTING_ACCOUNT_ID = "media_hosting_account_id"
SETTING_BUCKET = "media_hosting_bucket"


class MediaHostingNotConfigured(RuntimeError):
    """No R2 credentials/bucket are set. Never downgraded to a silent skip —
    a caller that needs hosting must fail loudly and name the missing setup."""


class MediaHostingError(RuntimeError):
    """An upload or verification against the bucket failed."""


@dataclass(frozen=True)
class MediaHostingConfig:
    account_id: str
    bucket: str
    access_key_id: str
    secret_access_key: str

    @property
    def host(self) -> str:
        return f"{self.account_id}.r2.cloudflarestorage.com"


@dataclass(frozen=True)
class HostedObject:
    key: str
    url: str
    size_bytes: int
    content_type: str


async def _load_settings_values() -> tuple[str, str]:
    from yt_scheduler.database import get_db

    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT key, value FROM settings WHERE key IN (?, ?)",
        (SETTING_ACCOUNT_ID, SETTING_BUCKET),
    )
    values = {row["key"]: (row["value"] or "").strip() for row in rows}
    return values.get(SETTING_ACCOUNT_ID, ""), values.get(SETTING_BUCKET, "")


async def load_config() -> MediaHostingConfig | None:
    """Return the hosting config, or ``None`` when it isn't fully set up.

    Partial configuration counts as unconfigured: a bucket with no key, or a
    key with no bucket, cannot produce a working URL, and reporting it as
    "configured" would defer the failure to send time.
    """
    from yt_scheduler.services.keychain import load_secret_async

    account_id, bucket = await _load_settings_values()
    access_key_id = (await load_secret_async(
        MEDIA_HOSTING_NAMESPACE, MEDIA_HOSTING_ACCESS_KEY_ID_FIELD) or "").strip()
    secret_access_key = (await load_secret_async(
        MEDIA_HOSTING_NAMESPACE, MEDIA_HOSTING_SECRET_ACCESS_KEY_FIELD) or "").strip()

    if not (account_id and bucket and access_key_id and secret_access_key):
        return None
    return MediaHostingConfig(
        account_id=account_id, bucket=bucket,
        access_key_id=access_key_id, secret_access_key=secret_access_key,
    )


async def is_configured() -> bool:
    return await load_config() is not None


async def require_config() -> MediaHostingConfig:
    config = await load_config()
    if config is None:
        raise MediaHostingNotConfigured(
            "Media hosting isn't configured. Set the Cloudflare R2 account ID, "
            "bucket and API token under Settings → Media hosting."
        )
    return config


def _presign(config: MediaHostingConfig, *, method: str, key: str,
             expires_seconds: int, now: datetime | None = None) -> str:
    return sigv4.presign_url(
        method=method,
        host=config.host,
        bucket=config.bucket,
        key=key,
        access_key_id=config.access_key_id,
        secret_access_key=config.secret_access_key,
        region=REGION,
        expires_seconds=expires_seconds,
        now=now or datetime.now(timezone.utc),
    )


async def _stream_file(path: Path, chunk_size: int):
    """Yield a file in chunks so a multi-gigabyte upload never lands in memory.

    Must be an *async* iterable: ``httpx.AsyncClient`` refuses a plain file
    handle outright ("Attempted to send an sync request with an AsyncClient
    instance"). Reads go through a worker thread so disk I/O doesn't stall the
    event loop while the rest of the app is serving.

    ``chunk_size`` is required rather than defaulted: a default argument binds
    at import time, which is how a test that patched the module constant
    silently exercised nothing.
    """
    with path.open("rb") as handle:
        while True:
            chunk = await asyncio.to_thread(handle.read, chunk_size)
            if not chunk:
                return
            yield chunk


async def host_file(local_path: str | Path, *,
                    config: MediaHostingConfig | None = None) -> HostedObject:
    """Upload a file and return a short-lived URL a third party can fetch.

    The object key is a UUID, which keeps it unguessable and — because the key
    contains no characters needing percent-encoding — keeps SigV4's path
    encoding rules from ever mattering.
    """
    import httpx

    config = config or await require_config()
    path = Path(local_path)
    if not path.is_file():
        raise MediaHostingError(f"Can't host {path.name}: not a readable file at {path}")
    size_bytes = path.stat().st_size
    if size_bytes == 0:
        raise MediaHostingError(f"Can't host {path.name}: file is empty")

    content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    key = f"{uuid.uuid4().hex}{path.suffix}"

    upload_url = _presign(config, method="PUT", key=key,
                          expires_seconds=UPLOAD_URL_TTL_SECONDS)
    try:
        async with httpx.AsyncClient(timeout=MEDIA_HOSTING_UPLOAD_TIMEOUT_SECONDS) as client:
            response = await client.put(
                upload_url,
                content=_stream_file(path, chunk_size=MEDIA_HOSTING_UPLOAD_CHUNK_BYTES),
                # Content-Length is mandatory, not merely nice: without it httpx
                # sends the generator as Transfer-Encoding: chunked, which an S3
                # PUT rejects. Content-Type matters because R2 stores it and
                # serves it back — unset arrives as application/octet-stream,
                # which the fetching platform may refuse.
                headers={
                    "Content-Type": content_type,
                    "Content-Length": str(size_bytes),
                },
            )
    except httpx.HTTPError as exc:
        raise MediaHostingError(f"Upload of {path.name} to R2 failed: {exc}") from exc

    if response.status_code >= 400:
        raise MediaHostingError(
            f"Upload of {path.name} to R2 failed: HTTP {response.status_code} "
            f"{response.text[:300]}"
        )

    logger.info("Hosted %s as %s (%.1f MB, %s)",
                path.name, key, size_bytes / 1e6, content_type)
    return HostedObject(
        key=key,
        url=_presign(config, method="GET", key=key,
                     expires_seconds=DOWNLOAD_URL_TTL_SECONDS),
        size_bytes=size_bytes,
        content_type=content_type,
    )


async def verify_round_trip() -> dict:
    """Upload a few bytes and read them back through a presigned URL.

    Exercises the real signing and permission path, so a wrong account id, an
    under-scoped token or a truncated secret surfaces here with its actual
    error rather than as an opaque failure from a platform later.

    Deliberately does not delete: Object Lock forbids it inside the retention
    window, and the bucket's lifecycle rule removes the object anyway.
    """
    import httpx

    config = await require_config()
    key = f"connection-test/{uuid.uuid4().hex}.txt"
    payload = f"drews-video-social-scheduler connection test {uuid.uuid4().hex}".encode()

    async with httpx.AsyncClient(
        timeout=MEDIA_HOSTING_CONNECTION_TEST_TIMEOUT_SECONDS
    ) as client:
        try:
            put = await client.put(
                _presign(config, method="PUT", key=key,
                         expires_seconds=UPLOAD_URL_TTL_SECONDS),
                content=payload,
                headers={"Content-Type": "text/plain",
                         "Content-Length": str(len(payload))},
            )
        except httpx.HTTPError as exc:
            raise MediaHostingError(f"Could not reach R2: {exc}") from exc
        if put.status_code >= 400:
            raise MediaHostingError(
                f"Upload rejected: HTTP {put.status_code} {put.text[:300]}"
            )

        download_url = _presign(config, method="GET", key=key,
                                expires_seconds=DOWNLOAD_URL_TTL_SECONDS)
        try:
            got = await client.get(download_url)
        except httpx.HTTPError as exc:
            raise MediaHostingError(f"Upload worked but download failed: {exc}") from exc
        if got.status_code >= 400:
            raise MediaHostingError(
                f"Upload worked but download was rejected: HTTP {got.status_code} "
                f"{got.text[:300]}"
            )
        if got.content != payload:
            raise MediaHostingError(
                "Downloaded content did not match what was uploaded."
            )
        served_type = (got.headers.get("content-type") or "").split(";")[0].strip()

    return {
        "bucket": config.bucket,
        "account_id": config.account_id,
        "key": key,
        "bytes": len(payload),
        "content_type_served": served_type or "<none>",
        "download_url_ttl_seconds": DOWNLOAD_URL_TTL_SECONDS,
    }
