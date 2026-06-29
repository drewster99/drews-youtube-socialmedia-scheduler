"""Social media posting — multi-platform with Keychain credential storage."""

from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
import re
import time
from pathlib import Path

import httpx

from yt_scheduler.services.keychain import (
    store_secret_async,
)

logger = logging.getLogger(__name__)


# --- Twitter / X — OAuth 2.0 refresh ----------------------------------------


_TWITTER_TOKEN_URL = "https://api.twitter.com/2/oauth2/token"


async def _twitter_refresh_bearer(creds: dict[str, str]) -> str | None:
    """Mint a fresh access_token using the stored refresh_token, persist it
    back into Keychain, and return the new bearer.

    Operates on a *local copy* of ``creds`` so it never mutates the caller's
    dict across await points. Callers must re-read the persisted bundle after
    this returns if they need the updated token — never rely on ``creds``
    being updated in place.

    Returns ``None`` only when there's no refresh path (no refresh_token /
    no client_id). Raises :class:`RuntimeError` when the API *rejects* the
    refresh (terminal — re-auth needed). Network/transport errors propagate
    as ``httpx`` exceptions (transient — caller should not flag re-auth)."""
    refresh = creds.get("refresh_token")
    client_id = creds.get("client_id")
    if not refresh or not client_id:
        return None

    cred_uuid = creds.get("uuid")
    if not cred_uuid:
        # Every credential created by upsert_credential has a uuid. A missing
        # uuid means a legacy bare-key bundle that the system can no longer
        # route, so we cannot safely persist — fail loudly instead of silently
        # writing to flat keys that nothing reads back.
        raise RuntimeError(
            "X credential bundle is missing 'uuid' — re-OAuth to generate a fresh bundle."
        )

    body = {
        "grant_type": "refresh_token",
        "refresh_token": refresh,
        "client_id": client_id,
    }
    auth = None
    if creds.get("client_secret"):
        auth = (client_id, creds["client_secret"])
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(_TWITTER_TOKEN_URL, data=body, auth=auth)
    if resp.status_code != 200:
        raise RuntimeError(
            f"X token refresh rejected ({resp.status_code}): {resp.text}"
        )
    payload = resp.json() or {}
    new_bearer = payload.get("access_token")
    if not new_bearer:
        raise RuntimeError(f"X token refresh response missing access_token: {payload}")
    new_refresh = payload.get("refresh_token")

    # Work on a local copy — never mutate the caller's dict across awaits.
    updated = dict(creds)
    updated["bearer_token"] = new_bearer
    if new_refresh:
        updated["refresh_token"] = new_refresh
    # X access tokens live ~2h; persist the expiry so the background refresh
    # job can pre-emptively renew before it lapses.
    if payload.get("expires_in"):
        updated["expires_at"] = int(time.time()) + int(payload["expires_in"])

    await store_secret_async("twitter", f"cred.{cred_uuid}", json.dumps(updated))
    return new_bearer


# --- Twitter / X v2 media upload (OAuth 2.0 user context) ------------------

# https://docs.x.com/x-api/media/upload-media — simple upload for small
# images, chunked (initialize / append / finalize / status) for video and
# large files. Single-file size limits per X docs:
#   images: 5 MB     — simple upload
#   GIFs:   15 MB    — simple upload
#   video:  512 MB   — chunked upload required
# We pick the path based on content type + size and fall back to text-only
# with a logged warning if anything fails.
#
# The chunked flow uses the v2 sub-path endpoints — POST /initialize
# (JSON body), POST /{id}/append (multipart per segment), POST /{id}/finalize
# (empty body). The old v1.1-style "POST /2/media/upload with command=INIT"
# form is *not* accepted on the v2 base path (it's treated as the simple,
# image-only upload there).

_TWITTER_V2_MEDIA = "https://api.x.com/2/media/upload"
_TWITTER_V2_MEDIA_INIT = "https://api.x.com/2/media/upload/initialize"
_TWITTER_SIMPLE_LIMIT = 5 * 1024 * 1024  # 5 MB; images only
_TWITTER_VIDEO_CHUNK = 4 * 1024 * 1024


class _TwitterBearerExpired(RuntimeError):
    """Internal: a media-upload call got a 401. Kept distinct from a generic
    upload failure so :meth:`TwitterPoster.post` can run its refresh-and-retry
    instead of giving up on the attachment."""


def _twitter_media_category(mime: str | None) -> str:
    if not mime:
        return "tweet_image"
    if mime == "image/gif":
        return "tweet_gif"
    if mime.startswith("video/"):
        return "tweet_video"
    return "tweet_image"


async def _twitter_v2_simple_upload(
    bearer_token: str, media_path: Path, mime: str
) -> str:
    """Upload an image/GIF in a single request. Returns the media_id string."""
    with media_path.open("rb") as f:
        files = {"media": (media_path.name, f, mime)}
        data = {"media_category": _twitter_media_category(mime)}
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                _TWITTER_V2_MEDIA,
                headers={"Authorization": f"Bearer {bearer_token}"},
                files=files,
                data=data,
            )
    if resp.status_code == 401:
        raise _TwitterBearerExpired(resp.text)
    if resp.status_code != 200:
        raise RuntimeError(f"v2 media upload failed ({resp.status_code}): {resp.text}")
    body = resp.json() or {}
    media_id = (body.get("data") or {}).get("id") or body.get("media_id_string")
    if not media_id:
        raise RuntimeError(f"v2 media upload response missing id: {body}")
    return str(media_id)


async def _twitter_v2_chunked_upload(
    bearer_token: str, media_path: Path, mime: str
) -> str:
    """Chunked upload (initialize / append / finalize / status) for video and
    large files via the v2 media-upload sub-path endpoints."""
    headers = {"Authorization": f"Bearer {bearer_token}"}
    total_bytes = media_path.stat().st_size

    async with httpx.AsyncClient(timeout=120) as client:
        # initialize — JSON body; returns the media id used by every later step.
        init = await client.post(
            _TWITTER_V2_MEDIA_INIT,
            headers=headers,
            json={
                "media_type": mime,
                "total_bytes": total_bytes,
                "media_category": _twitter_media_category(mime),
            },
        )
        if init.status_code == 401:
            raise _TwitterBearerExpired(init.text)
        if init.status_code != 200:
            raise RuntimeError(f"v2 media INIT failed ({init.status_code}): {init.text}")
        media_id = ((init.json() or {}).get("data") or {}).get("id")
        if not media_id:
            raise RuntimeError(f"v2 media INIT response missing id: {init.json()}")
        media_id = str(media_id)

        # append — one multipart request per segment.
        with media_path.open("rb") as f:
            segment = 0
            while True:
                chunk = f.read(_TWITTER_VIDEO_CHUNK)
                if not chunk:
                    break
                append = await client.post(
                    f"{_TWITTER_V2_MEDIA}/{media_id}/append",
                    headers=headers,
                    data={"segment_index": str(segment)},
                    files={"media": (media_path.name, chunk, mime)},
                )
                if append.status_code == 401:
                    raise _TwitterBearerExpired(append.text)
                if append.status_code not in (200, 204):
                    raise RuntimeError(
                        f"v2 media APPEND segment {segment} failed "
                        f"({append.status_code}): {append.text}"
                    )
                segment += 1

        # finalize — empty body.
        finalize = await client.post(
            f"{_TWITTER_V2_MEDIA}/{media_id}/finalize", headers=headers,
        )
        if finalize.status_code == 401:
            raise _TwitterBearerExpired(finalize.text)
        if finalize.status_code != 200:
            raise RuntimeError(f"v2 media FINALIZE failed ({finalize.status_code}): {finalize.text}")

        # status — wait for async transcoding (videos only).
        processing = ((finalize.json() or {}).get("data") or {}).get("processing_info")
        deadline = time.monotonic() + 120
        while processing and processing.get("state") in {"pending", "in_progress"}:
            wait = max(int(processing.get("check_after_secs") or 1), 1)
            await asyncio.sleep(wait)
            if time.monotonic() > deadline:
                raise RuntimeError("v2 media STATUS timed out (>120s)")
            status = await client.get(
                _TWITTER_V2_MEDIA,
                headers=headers,
                params={"command": "STATUS", "media_id": media_id},
            )
            if status.status_code == 401:
                raise _TwitterBearerExpired(status.text)
            if status.status_code != 200:
                raise RuntimeError(f"v2 media STATUS failed ({status.status_code}): {status.text}")
            processing = ((status.json() or {}).get("data") or {}).get("processing_info")

        if processing and processing.get("state") == "failed":
            raise RuntimeError(f"v2 media transcoding failed: {processing}")

    return media_id


async def _twitter_v2_upload(bearer_token: str, media_path: Path) -> str:
    mime, _ = mimetypes.guess_type(media_path.name)
    size = media_path.stat().st_size
    if (mime or "").startswith("video/") or size > _TWITTER_SIMPLE_LIMIT:
        return await _twitter_v2_chunked_upload(bearer_token, media_path, mime or "video/mp4")
    return await _twitter_v2_simple_upload(bearer_token, media_path, mime or "image/jpeg")


class CredentialAuthError(RuntimeError):
    """Raised by a poster when the platform rejected our credentials and
    no automatic recovery (e.g. token refresh) is possible.

    The send-path catches this, marks the credential ``needs_reauth``,
    and surfaces a clear message to the UI prompting the user to
    reconnect. Carries the credential's UUID so the route handler
    doesn't have to reach back into the bundle.
    """

    def __init__(self, uuid: str | None, message: str) -> None:
        super().__init__(message)
        self.uuid = uuid


class MediaUploadError(RuntimeError):
    """Raised by a poster when the caller asked for media to be attached but
    it couldn't be — a failed upload, an unsupported attachment, or a missing
    file. The send-path treats this like any other post failure (marks the
    post ``failed`` and surfaces the message), so nothing is published in a
    degraded form. The user can then drop the attachment and retry.
    """


def _http_error_detail(resp: httpx.Response) -> str:
    """Build a human-readable failure string from a non-2xx httpx response,
    preferring the provider's own error body.

    ``response.raise_for_status()`` throws away the response body and yields
    only a generic ``'400 Bad Request' for url ...`` line, which hides the
    real reason the provider rejected the call. Meta (Threads / Facebook
    Graph) returns ``{"error": {"message", "code", "error_subcode",
    "fbtrace_id"}}``; surface those fields so the actual cause reaches the UI.
    """
    try:
        data = resp.json()
    except (ValueError, json.JSONDecodeError):
        text = (resp.text or "").strip()
        return f"HTTP {resp.status_code}: {text}" if text else f"HTTP {resp.status_code}"
    err = data.get("error") if isinstance(data, dict) else None
    if isinstance(err, dict):
        parts = [
            f"{key}={err[key]}"
            for key in ("message", "code", "error_subcode", "type", "fbtrace_id")
            if err.get(key) not in (None, "")
        ]
        if parts:
            return f"HTTP {resp.status_code}: " + ", ".join(parts)
    if isinstance(err, str) and err:
        return f"HTTP {resp.status_code}: {err}"
    return f"HTTP {resp.status_code}: {json.dumps(data)[:500]}"


class SocialPoster:
    """Base class for social media platform posters.

    A poster is bound to a single credential via its ``bundle`` dict at
    construction time. The Phase A→B fallback path lets callers
    instantiate without a bundle, in which case ``_get_creds()`` picks
    the first active bundle for the platform — that path keeps existing
    install-wide call sites working until they migrate to the
    per-credential factories.
    """

    platform: str = ""

    required_keys: list[str] = []

    def __init__(self, bundle: dict | None = None) -> None:
        self._bundle: dict | None = bundle

    async def _get_creds(self) -> dict[str, str]:
        """Return the bundle this poster is bound to, falling back to the
        platform's first active credential bundle when none is set.

        The fallback resolves deterministically (oldest active credential by id,
        the same ordering the send-path pre-check uses) and never merges keys
        across accounts — the old behaviour picked whichever ``cred.*`` entry
        happened to JSON-parse first in Keychain dict order, or blended every
        account's keys into one synthetic bundle, either of which could post
        from / with the wrong account. No credential → empty dict, so
        ``is_configured()`` reports "not configured" rather than raising.
        """
        if self._bundle is not None:
            return self._bundle
        from yt_scheduler.services.social_credentials import (
            get_first_active_credential,
            load_bundle,
        )

        cred = await get_first_active_credential(self.platform)
        if cred is None:
            return {}
        bundle = await load_bundle(self.platform, cred["uuid"])
        if bundle is None:
            logger.warning(
                "First active %s credential %s has no readable bundle",
                self.platform, str(cred.get("uuid"))[:8],
            )
            return {}
        return bundle

    @classmethod
    def bundle_is_configured(cls, bundle: dict) -> bool:
        """Check whether the given bundle has all keys this poster needs."""
        return all(bundle.get(k) for k in cls.required_keys)

    async def post(
        self,
        text: str,
        media_path: str | None = None,
        *,
        media_paths: list[str] | None = None,
        alt_texts: list[str] | None = None,
    ) -> dict:
        """Post content. Returns ``{"url": "...", "id": "..."}`` on success.

        Two media-input forms (back-compat + multi):

        - ``media_path`` (legacy single-string positional) — preserved so
          older callers don't need a churn pass.
        - ``media_paths`` (keyword-only list) — preferred. When both are
          supplied, ``media_paths`` wins. ``alt_texts`` is parallel to
          ``media_paths``; defaults to empty strings if omitted.

        Subclasses that only use the first item should call
        :meth:`_resolve_media_inputs` to normalise inputs to a list.
        """
        raise NotImplementedError

    @staticmethod
    def _resolve_media_inputs(
        media_path: str | None,
        media_paths: list[str] | None,
        alt_texts: list[str] | None,
    ) -> tuple[list[str], list[str]]:
        """Centralise the legacy / multi-media reconciliation logic so each
        poster handles inputs the same way. Returns ``(paths, alts)`` —
        same length, alts padded with ``""`` when callers provide fewer
        alts than paths."""
        paths: list[str] = []
        if media_paths:
            paths = [p for p in media_paths if p]
        elif media_path:
            paths = [media_path]
        alts = list(alt_texts or [])
        # Pad alts to match paths length.
        while len(alts) < len(paths):
            alts.append("")
        return paths, alts

    @staticmethod
    def _require_paths_managed(paths: list[str], platform: str) -> None:
        """Refuse to upload any attachment that lives outside the managed media
        directory. Defense-in-depth behind the write-boundary check in
        ``update_post``: a row poisoned by a direct DB edit, an old import, or a
        future code path must never cause an arbitrary on-disk file to be
        published to a social account."""
        from yt_scheduler import config

        bad = [p for p in paths if not config.is_managed_media_path(p)]
        if bad:
            names = ", ".join(Path(p).name for p in bad)
            raise MediaUploadError(
                f"Can't post to {platform}: attachment{'s' if len(bad) > 1 else ''} "
                f"outside the managed media directory — {names}. Re-attach from "
                "the media library, then retry. Nothing was posted."
            )

    @staticmethod
    def _require_paths_exist(paths: list[str], platform: str) -> None:
        """Abort the post if any requested attachment is gone from disk.

        Posting the text without an attachment the user explicitly composed
        is worse than not posting — surface it so they can re-attach or drop
        it and retry.
        """
        # Containment is checked first: an out-of-tree path is reported as a
        # policy violation rather than (if it happens to exist) being read.
        SocialPoster._require_paths_managed(paths, platform)
        missing = [p for p in paths if not Path(p).exists()]
        if missing:
            names = ", ".join(Path(p).name for p in missing)
            raise MediaUploadError(
                f"Can't post to {platform}: attachment file{'s' if len(missing) > 1 else ''} "
                f"missing — {names}. Re-attach or remove the attachment, then retry. "
                "Nothing was posted."
            )

    async def is_configured(self) -> bool:
        """Check if this poster's credentials are complete."""
        creds = await self._get_creds()
        return all(creds.get(k) for k in self.required_keys)

    async def refresh_if_stale(self, *, window_secs: int = 0) -> bool:
        """Proactively refresh this credential's access token if it expires
        within ``window_secs``. Returns ``True`` if a refresh happened.

        Default: no-op (``False``) — the platform has no refresh flow (tokens
        are long-lived or never expire). Subclasses with refresh tokens
        override this. A terminal failure raises :class:`CredentialAuthError`
        so the caller can mark the credential ``needs_reauth``.
        """
        return False


class TwitterPoster(SocialPoster):
    platform = "twitter"
    # OAuth 2.0 user-context with media.write scope is the only supported
    # path. Tweets go through v2 ``POST /2/tweets``; image/GIF/video uploads
    # go through v2 ``POST /2/media/upload``.
    required_keys: list[str] = ["bearer_token"]

    async def refresh_if_stale(self, *, window_secs: int = 0) -> bool:
        creds = await self._get_creds()
        uuid = creds.get("uuid")
        # Need the rotating refresh token + the OAuth client id to refresh.
        if not (uuid and creds.get("refresh_token") and creds.get("client_id")):
            return False
        expires_at = int(creds.get("expires_at") or 0)
        # Skip only when we *know* it's still fresh. If the bundle predates the
        # expires_at field (it's 0), refresh once anyway — that backfills the
        # expiry so future sweeps behave normally and the bearer doesn't drift
        # stale while waiting for a 401.
        if expires_at and expires_at - window_secs > int(time.time()):
            return False
        from yt_scheduler.services.social_credentials import (
            clear_needs_reauth,
            get_credential_lock,
            load_bundle,
        )
        async with get_credential_lock(uuid):
            fresh = await load_bundle("twitter", uuid)
            # Use the re-read bundle as the source of truth; fall back to the
            # original creds only when Keychain returns nothing. Never mutate
            # `creds` (which may be self._bundle) across await points.
            current = fresh or creds
            expires_at = int(current.get("expires_at") or 0)
            if expires_at and expires_at - window_secs > int(time.time()):
                return False
            try:
                new_bearer = await _twitter_refresh_bearer(current)
            except RuntimeError as exc:
                raise CredentialAuthError(uuid, str(exc)) from exc
            if not new_bearer:
                return False  # no refresh token in the bundle — nothing to do
            await clear_needs_reauth(uuid)
            return True

    async def post(
        self,
        text: str,
        media_path: str | None = None,
        *,
        media_paths: list[str] | None = None,
        alt_texts: list[str] | None = None,
    ) -> dict:
        text = (text or "").strip()
        creds = await self._get_creds()
        bearer = creds.get("bearer_token")
        if not bearer:
            raise CredentialAuthError(
                creds.get("uuid"),
                "X is not configured. Click 'Connect with X (OAuth 2.0)' in Settings.",
            )

        paths, _alts = self._resolve_media_inputs(media_path, media_paths, alt_texts)
        # Twitter API caps: 4 images per tweet, OR exactly 1 video, OR 1 GIF.
        # Mixed media (image + video) isn't supported by the API. Slice to the
        # first 4 up front; if the first asset is a video, only that one goes.
        paths = paths[:4]
        self._require_paths_exist(paths, "X")
        if paths:
            first_mime = mimetypes.guess_type(Path(paths[0]).name)[0] or ""
            if first_mime.startswith("video/"):
                upload_paths = [Path(paths[0])]
            else:
                upload_paths = [
                    Path(p) for p in paths
                    if not (mimetypes.guess_type(p)[0] or "").startswith("video/")
                ][:4]
        else:
            upload_paths = []

        import tweepy

        async def _upload_all(token: str) -> list[str] | None:
            """Upload every selected asset against ``token``. A 401 surfaces as
            :class:`_TwitterBearerExpired` so the caller can refresh and retry;
            any other failure is a :class:`MediaUploadError` — we never post
            without an attachment the caller explicitly composed."""
            ids: list[str] = []
            for p in upload_paths:
                try:
                    ids.append(await _twitter_v2_upload(token, p))
                except (_TwitterBearerExpired, CredentialAuthError):
                    raise
                except Exception as exc:
                    raise MediaUploadError(
                        f"Couldn't attach {p.name} to the X post: {exc}. Re-run "
                        "Connect with X to refresh the media.write scope, or check "
                        "the file size/format. Nothing was posted — remove the "
                        "attachment to post text only."
                    ) from exc
            return ids or None

        def _tweet(token: str, media_ids: list[str] | None):
            # Two stacked tweepy gotchas with OAuth 2.0 user-context:
            # 1. ``tweepy.Client(access_token=...)`` makes tweepy build an
            #    OAuth1Session — must use ``bearer_token=``.
            # 2. ``create_tweet`` defaults ``user_auth=True``, which again
            #    routes through OAuth1 even when constructed with bearer_token
            #    only — must pass ``user_auth=False``. Skipping either yields
            #    "Consumer key must be string or bytes, not NoneType".
            return tweepy.Client(bearer_token=token).create_tweet(
                text=text, media_ids=media_ids, user_auth=False,
            )

        from yt_scheduler.services.social_credentials import (
            clear_needs_reauth,
            get_credential_lock,
            load_bundle,
        )

        uuid = creds.get("uuid")

        async def _refresh_under_lock() -> str:
            """Refresh the bearer inside the per-credential lock.

            Uses double-checked locking: another waiter (e.g. the background
            refresh job) may have already refreshed while we were queued on the
            lock, in which case the re-read bundle already has a valid bearer
            and we skip the network call entirely — and more importantly we
            never present a consumed single-use refresh token to the API.

            Returns the fresh bearer string, or raises CredentialAuthError for
            terminal failures."""
            if not uuid:
                raise CredentialAuthError(
                    None,
                    "X bearer expired and there's no refresh token — re-OAuth.",
                )
            async with get_credential_lock(uuid):
                # Re-read from Keychain: another coroutine may have refreshed
                # while we were waiting on the lock, giving us a valid bearer
                # without a network round-trip and without burning the token.
                fresh = await load_bundle("twitter", uuid)
                if fresh and fresh.get("bearer_token") and fresh.get("bearer_token") != bearer:
                    logger.info("Twitter bearer already refreshed by another waiter; reusing.")
                    return fresh["bearer_token"]

                current = fresh or creds
                if not (current.get("refresh_token") and current.get("client_id")):
                    raise CredentialAuthError(
                        uuid,
                        "X bearer expired and there's no refresh token — re-OAuth.",
                    )
                try:
                    new_b = await _twitter_refresh_bearer(current)
                except RuntimeError as rexc:
                    raise CredentialAuthError(
                        uuid,
                        "X bearer expired and the refresh was rejected — re-OAuth.",
                    ) from rexc
                if not new_b:
                    raise CredentialAuthError(
                        uuid,
                        "X bearer expired and there's no refresh token — re-OAuth.",
                    )
                return new_b

        try:
            try:
                media_ids = await _upload_all(bearer)
                response = _tweet(bearer, media_ids)
            except (tweepy.errors.Unauthorized, _TwitterBearerExpired):
                # Bearer expired (~2h lifetime). Refresh under the per-credential
                # lock so a concurrent background refresh can't race us to the
                # single-use refresh token.
                new_bearer = await _refresh_under_lock()
                if uuid:
                    # The bearer just refreshed and we're about to retry the
                    # tweet — clear any stale needs_reauth flag a prior flap set,
                    # matching the background refresh_if_stale path.
                    await clear_needs_reauth(uuid)
                logger.info("Twitter bearer refreshed; retrying tweet.")
                # Re-upload against the fresh token — media ids from the prior
                # auth session may not be valid.
                try:
                    media_ids = await _upload_all(new_bearer)
                    response = _tweet(new_bearer, media_ids)
                except (tweepy.errors.Unauthorized, _TwitterBearerExpired) as exc2:
                    raise CredentialAuthError(
                        uuid,
                        "X rejected the refreshed bearer — re-OAuth.",
                    ) from exc2

            tweet_id = response.data["id"]
            return {"url": f"https://x.com/i/status/{tweet_id}", "id": tweet_id}
        except (CredentialAuthError, MediaUploadError):
            raise
        except Exception as e:
            raise RuntimeError(f"Twitter post failed: {e}") from e


# Trailing chars we always peel off the end of a URL — common sentence
# punctuation that virtually never belongs to the URI itself.
_BSKY_URL_ALWAYS_STRIP = ".,;:!?'\">"

# Closing brackets that we only strip when there is no matching opener
# inside the URL. This preserves URLs like
# `https://en.wikipedia.org/wiki/Foo_(bar)` (where the `)` is part of the
# path) while still trimming the prose-level wrapper in `(see …)`.
_BSKY_URL_PAIRED_CLOSERS = {b")": b"(", b"]": b"[", b"}": b"{"}

# URL detector — matches http(s) up to the next whitespace or angle bracket.
# Bluesky facets work in UTF-8 byte offsets, so we run this over the encoded
# bytes to keep `index.byteStart`/`byteEnd` aligned with what the server sees.
_BSKY_URL_RE = re.compile(rb"https?://[^\s<>]+")

# Bare-domain detector — `example.com`, `www.foo.io/bar`, etc. with NO scheme.
# Bluesky won't auto-link these (it doesn't even auto-link scheme'd URLs), so we
# synthesize an `https://` URI for the facet. The negative lookbehind keeps it
# from firing inside an already-matched http(s) URL (preceding `/`), inside an
# email address (`@`), or mid-label. `dev` precedes `de` and `com` precedes `co`
# so the longer TLD wins the alternation; the trailing `(?![a-z0-9-])` stops a
# TLD from matching the head of a longer label (e.g. `co` in `community`).
_BSKY_BARE_DOMAIN_RE = re.compile(
    rb"(?<![\w./@#-])(?:[a-z0-9][a-z0-9-]*\.)+"
    rb"(?:com|org|net|edu|gov|info|dev|app|io|ai|me|tv|xyz|co|us|uk|ca|de|fr|nl|so)"
    rb"(?![a-z0-9-])(?:/[^\s<>]*)?",
    re.IGNORECASE,
)

# Hashtag detector — `#` followed by at least one letter, then word chars.
# A leading letter avoids matching things like "#1" (numeric) which Bluesky
# also rejects as a tag. Tag value sent to the server omits the leading `#`.
_BSKY_TAG_RE = re.compile(rb"(?:^|(?<=\s))#([A-Za-z][\w]*)")


def _trim_trailing_url_punct(uri_bytes: bytes) -> bytes:
    while uri_bytes:
        last = uri_bytes[-1:]
        if last.decode("ascii", errors="ignore") in _BSKY_URL_ALWAYS_STRIP:
            uri_bytes = uri_bytes[:-1]
            continue
        opener = _BSKY_URL_PAIRED_CLOSERS.get(last)
        if opener is None:
            break
        if opener in uri_bytes[:-1]:
            break
        uri_bytes = uri_bytes[:-1]
    return uri_bytes


def _build_bluesky_facets(text: str) -> list[dict]:
    """Return Bluesky richtext facets for URLs and hashtags found in *text*.

    Bluesky's PDS does not auto-detect links or tags — without facets the
    text renders as plain prose. Byte offsets are computed against the
    UTF-8 encoding because that's the indexing the server uses.
    """
    encoded = text.encode("utf-8")
    facets: list[dict] = []

    for match in _BSKY_URL_RE.finditer(encoded):
        start = match.start()
        trimmed = _trim_trailing_url_punct(encoded[start:match.end()])
        end = start + len(trimmed)
        if end <= start:
            continue
        uri = trimmed.decode("utf-8", errors="ignore")
        facets.append({
            "index": {"byteStart": start, "byteEnd": end},
            "features": [{"$type": "app.bsky.richtext.facet#link", "uri": uri}],
        })

    # Bare domains (no scheme) — link with a synthesized https:// URI. The
    # byte offsets index the original text; only the `uri` string gets the
    # prefix. The lookbehind in the pattern already prevents overlap with the
    # scheme'd-URL matches above.
    for match in _BSKY_BARE_DOMAIN_RE.finditer(encoded):
        start = match.start()
        trimmed = _trim_trailing_url_punct(match.group(0))
        end = start + len(trimmed)
        if end <= start:
            continue
        uri = "https://" + trimmed.decode("utf-8", errors="ignore")
        facets.append({
            "index": {"byteStart": start, "byteEnd": end},
            "features": [{"$type": "app.bsky.richtext.facet#link", "uri": uri}],
        })

    for match in _BSKY_TAG_RE.finditer(encoded):
        start = match.start()
        end = match.end()
        tag = match.group(1).decode("utf-8", errors="ignore")
        facets.append({
            "index": {"byteStart": start, "byteEnd": end},
            "features": [{"$type": "app.bsky.richtext.facet#tag", "tag": tag}],
        })

    return facets


class BlueskyPoster(SocialPoster):
    platform = "bluesky"
    # OAuth-only: a Bluesky bundle must contain the per-credential ES256
    # key, the access/refresh tokens, the resolved PDS, and the AS's token
    # endpoint (so we can refresh without re-discovery). app_password is
    # gone; credentials that pre-date OAuth are wiped on boot.
    required_keys = [
        "auth_method",
        "handle",
        "did",
        "pds",
        "private_key_pem",
        "access_token",
        "refresh_token",
        "token_endpoint",
        "redirect_uri",
    ]

    async def post(
        self,
        text: str,
        media_path: str | None = None,
        *,
        media_paths: list[str] | None = None,
        alt_texts: list[str] | None = None,
    ) -> dict:
        from datetime import datetime, timezone

        from yt_scheduler.services import bluesky_oauth
        from yt_scheduler.services.social_credentials import save_bundle

        text = (text or "").strip()

        creds = await self._get_creds()
        if creds.get("auth_method") != "oauth":
            raise CredentialAuthError(
                creds.get("uuid"),
                "Bluesky credential is not OAuth-authenticated. "
                "Click Connect with Bluesky in Settings to re-authenticate.",
            )

        paths, alts = self._resolve_media_inputs(media_path, media_paths, alt_texts)
        self._require_paths_exist(paths, "Bluesky")
        existing = [(Path(p), a) for p, a in zip(paths, alts)]

        # Bluesky caps: 4 images per post OR 1 video per post (mutually
        # exclusive). When the first asset is a video we use the
        # `app.bsky.embed.video` lexicon and ignore any siblings; otherwise
        # we batch up to 4 images via `app.bsky.embed.images`.
        embed_kind: str | None = None
        if existing:
            first_path, _ = existing[0]
            first_mime = mimetypes.guess_type(first_path.name)[0] or ""
            embed_kind = "video" if first_mime.startswith("video/") else "images"

        from yt_scheduler.services import bluesky_http

        try:
            await self._ensure_fresh_token(creds)
        except RuntimeError as exc:
            # refresh_tokens raises RuntimeError on a non-200 from the
            # AS — invalid_grant, expired_token, etc. all mean the
            # user has to re-OAuth.
            raise CredentialAuthError(creds.get("uuid"), str(exc)) from exc

        embed = None
        if embed_kind == "video":
            first_path, first_alt = existing[0]
            blob = await self._upload_blob(
                creds, first_path, bluesky_oauth, save_bundle
            )
            embed = {
                "$type": "app.bsky.embed.video",
                "video": blob,
                "alt": first_alt or "",
            }
        elif embed_kind == "images":
            images_payload: list[dict] = []
            for path_obj, alt in existing[:4]:
                mime = mimetypes.guess_type(path_obj.name)[0] or ""
                if mime.startswith("video/"):
                    # Mixed batches aren't supported by the embed.images
                    # lexicon; skip the video so the batch still posts.
                    continue
                blob = await self._upload_blob(
                    creds, path_obj, bluesky_oauth, save_bundle
                )
                images_payload.append({"image": blob, "alt": alt or ""})
            if images_payload:
                embed = {
                    "$type": "app.bsky.embed.images",
                    "images": images_payload,
                }

        record = {
            "$type": "app.bsky.feed.post",
            "text": text,
            "createdAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        facets = _build_bluesky_facets(text)
        if facets:
            record["facets"] = facets
        if embed is not None:
            record["embed"] = embed

        create_url = f"{creds['pds'].rstrip('/')}/xrpc/com.atproto.repo.createRecord"

        async def _do() -> bluesky_http.Response:
            proof = bluesky_oauth.sign_dpop_proof(
                creds["private_key_pem"], "POST", create_url,
                nonce=creds.get("dpop_nonce_pds"),
                access_token=creds["access_token"],
            )
            return await bluesky_http.post(
                create_url,
                headers={
                    "Authorization": f"DPoP {creds['access_token']}",
                    "DPoP": proof,
                    "Content-Type": "application/json",
                },
                json={
                    "repo": creds["did"],
                    "collection": "app.bsky.feed.post",
                    "record": record,
                },
                timeout=60,
            )

        resp = await _do()
        if resp.status_code in (400, 401):
            try:
                retried = await self._handle_dpop_or_token_error(
                    resp, creds, bluesky_oauth, save_bundle,
                )
            except RuntimeError as exc:
                raise CredentialAuthError(creds.get("uuid"), str(exc)) from exc
            if retried:
                resp = await _do()

        if resp.status_code == 401:
            # Still 401 after refresh+retry — the credential is dead.
            raise CredentialAuthError(
                creds.get("uuid"),
                f"Bluesky rejected the credential after refresh: {resp.text}",
            )
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Bluesky createRecord failed: HTTP {resp.status_code} {resp.text}"
            )

        await self._stash_pds_nonce(creds, resp, save_bundle)

        body = resp.json() or {}
        uri = body.get("uri", "")
        # AT-URI shape: at://<did>/app.bsky.feed.post/<rkey>. A malformed
        # URI here would silently produce an empty bsky.app/post/ path,
        # so be strict.
        rkey = self._rkey_from_at_uri(uri)
        return {
            "url": f"https://bsky.app/profile/{creds['handle']}/post/{rkey}",
            "id": uri,
        }

    @staticmethod
    def _rkey_from_at_uri(uri: str) -> str:
        """Pull the record key off an AT-URI, raising if the shape is wrong.

        Expected: ``at://<did>/<collection>/<rkey>`` with a non-empty
        rkey. Anything else is a bug (or a server returning an
        unexpected response shape) — surface it loudly.
        """
        if not uri or not uri.startswith("at://"):
            raise RuntimeError(
                f"Bluesky createRecord returned no usable AT-URI: {uri!r}"
            )
        parts = uri[len("at://"):].split("/")
        if len(parts) < 3 or not parts[-1]:
            raise RuntimeError(
                f"Bluesky createRecord returned malformed AT-URI: {uri!r}"
            )
        return parts[-1]

    @staticmethod
    async def _stash_pds_nonce(creds: dict, resp, save_bundle) -> None:
        new_nonce = resp.headers.get("DPoP-Nonce")
        if new_nonce and new_nonce != creds.get("dpop_nonce_pds"):
            creds["dpop_nonce_pds"] = new_nonce
            await save_bundle("bluesky", creds["uuid"], creds)

    async def _handle_dpop_or_token_error(
        self,
        resp,
        creds: dict,
        bluesky_oauth,
        save_bundle,
    ) -> bool:
        """Return True if the caller should retry the original request.

        The PDS issues nonces in its own sequence (separate from the AS),
        so we update ``dpop_nonce_pds`` here. Token refresh writes back
        to ``dpop_nonce_as``.
        """
        try:
            body = resp.json() or {}
        except Exception:
            return False
        err = body.get("error", "")
        if err == "use_dpop_nonce":
            nonce = resp.headers.get("DPoP-Nonce")
            if nonce:
                creds["dpop_nonce_pds"] = nonce
                await save_bundle("bluesky", creds["uuid"], creds)
                return True
        if resp.status_code == 401 or err in ("invalid_token", "expired_token"):
            await self._refresh_access_token(creds, bluesky_oauth, save_bundle)
            return True
        return False

    # Pre-emptively refresh when the access token has under this many
    # seconds of lifetime left. Bluesky access tokens live ~2h, so a
    # 15-minute window means a single posting batch never needs a
    # mid-batch refresh, while a token revoked at the AS still gets
    # caught lazily on a 401 from the PDS.
    _PRE_REFRESH_WINDOW_SECS = 15 * 60

    async def _refresh_under_lock(
        self, creds: dict, *, window_secs: int,
    ) -> bool:
        """Refresh the access token, serialised per-credential via
        ``get_credential_lock``. Re-reads the stored bundle inside the lock so
        a concurrent refresh on another path (e.g. the background job vs a
        post) doesn't leave us presenting a now-consumed refresh token.
        Returns True if a refresh was performed, False if it was already
        fresh / not possible. RuntimeError from the AS propagates."""
        from yt_scheduler.services import bluesky_oauth
        from yt_scheduler.services.social_credentials import (
            clear_needs_reauth,
            get_credential_lock,
            load_bundle,
            save_bundle,
        )
        uuid = creds.get("uuid")

        async def _do() -> bool:
            if uuid:
                fresh = await load_bundle("bluesky", uuid)
                if fresh:
                    creds.update(fresh)
            expires_at = int(creds.get("expires_at") or 0)
            if expires_at and expires_at - window_secs > int(time.time()):
                return False
            if not creds.get("refresh_token"):
                return False
            await self._refresh_access_token(creds, bluesky_oauth, save_bundle)
            if uuid:
                # A successful refresh means the session is alive — clear any
                # stale needs-reauth flag (e.g. one set by a transient blip).
                await clear_needs_reauth(uuid)
            return True

        if uuid:
            async with get_credential_lock(uuid):
                return await _do()
        return await _do()

    async def _ensure_fresh_token(self, creds: dict) -> None:
        # Quick out before taking the lock; _refresh_under_lock re-checks inside it.
        expires_at = int(creds.get("expires_at") or 0)
        if expires_at and expires_at - self._PRE_REFRESH_WINDOW_SECS > int(time.time()):
            return
        if not creds.get("refresh_token"):
            return
        await self._refresh_under_lock(
            creds, window_secs=self._PRE_REFRESH_WINDOW_SECS,
        )

    async def refresh_if_stale(self, *, window_secs: int = 0) -> bool:
        creds = await self._get_creds()
        if not creds.get("refresh_token"):
            return False
        expires_at = int(creds.get("expires_at") or 0)
        if expires_at and expires_at - window_secs > int(time.time()):
            return False
        try:
            return await self._refresh_under_lock(creds, window_secs=window_secs)
        except RuntimeError as exc:
            raise CredentialAuthError(creds.get("uuid"), str(exc)) from exc

    async def _refresh_access_token(
        self, creds: dict, bluesky_oauth, save_bundle,
    ) -> None:
        result = await bluesky_oauth.refresh_tokens(
            refresh_token=creds["refresh_token"],
            private_key_pem=creds["private_key_pem"],
            token_endpoint=creds["token_endpoint"],
            redirect_uri=creds["redirect_uri"],
            nonce=creds.get("dpop_nonce_as"),
        )
        creds["access_token"] = result["access_token"]
        if result.get("refresh_token"):
            creds["refresh_token"] = result["refresh_token"]
        if result.get("expires_in"):
            creds["expires_at"] = int(time.time()) + int(result["expires_in"])
        if result.get("dpop_nonce_as"):
            creds["dpop_nonce_as"] = result["dpop_nonce_as"]
        await save_bundle("bluesky", creds["uuid"], creds)

    async def _upload_blob(
        self, creds: dict, path: Path, bluesky_oauth, save_bundle,
    ) -> dict:
        from yt_scheduler.services import bluesky_http

        url = f"{creds['pds'].rstrip('/')}/xrpc/com.atproto.repo.uploadBlob"
        mime, _ = mimetypes.guess_type(path.name)
        mime = mime or "application/octet-stream"
        # File can be large (~512 MB); read on a worker thread to avoid
        # blocking the event loop.
        data = await asyncio.to_thread(path.read_bytes)

        async def _do() -> bluesky_http.Response:
            proof = bluesky_oauth.sign_dpop_proof(
                creds["private_key_pem"], "POST", url,
                nonce=creds.get("dpop_nonce_pds"),
                access_token=creds["access_token"],
            )
            return await bluesky_http.post(
                url,
                headers={
                    "Authorization": f"DPoP {creds['access_token']}",
                    "DPoP": proof,
                    "Content-Type": mime,
                },
                content=data,
                timeout=120,
            )

        resp = await _do()
        if resp.status_code in (400, 401):
            try:
                retried = await self._handle_dpop_or_token_error(
                    resp, creds, bluesky_oauth, save_bundle,
                )
            except RuntimeError as exc:
                raise CredentialAuthError(creds.get("uuid"), str(exc)) from exc
            if retried:
                resp = await _do()

        if resp.status_code == 401:
            raise CredentialAuthError(
                creds.get("uuid"),
                f"Bluesky uploadBlob rejected after refresh: {resp.text}",
            )
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Bluesky uploadBlob failed: HTTP {resp.status_code} {resp.text}"
            )

        await self._stash_pds_nonce(creds, resp, save_bundle)
        body = resp.json() or {}
        blob = body.get("blob")
        if not isinstance(blob, dict):
            raise RuntimeError(f"uploadBlob returned no blob: {body}")
        return blob

    @classmethod
    def bundle_is_configured(cls, bundle: dict) -> bool:
        if bundle.get("auth_method") != "oauth":
            return False
        return all(bundle.get(k) for k in cls.required_keys)


class MastodonPoster(SocialPoster):
    platform = "mastodon"
    required_keys = ["access_token", "instance_url"]

    async def post(
        self,
        text: str,
        media_path: str | None = None,
        *,
        media_paths: list[str] | None = None,
        alt_texts: list[str] | None = None,
    ) -> dict:
        text = (text or "").strip()
        creds = await self._get_creds()

        paths, _alts = self._resolve_media_inputs(media_path, media_paths, alt_texts)
        self._require_paths_exist(paths, "Mastodon")
        # Mastodon's default cap is 4 media per status; some instances raise
        # this. We cap conservatively. Video/image mixing is platform-
        # configurable; we don't filter — let Mastodon reject if needed.
        existing = paths[:4]

        try:
            from mastodon import Mastodon
            from mastodon.errors import (
                MastodonGatewayTimeoutError,
                MastodonNetworkError,
                MastodonReadTimeout,
                MastodonServerError,
                MastodonUnauthorizedError,
            )

            client = Mastodon(
                access_token=creds["access_token"],
                api_base_url=creds.get("instance_url", "https://mastodon.social"),
            )

            try:
                media_ids: list | None = None
                if existing:
                    media_ids = []
                    for p in existing:
                        # mastodon.py is synchronous; uploading a multi-MB
                        # video this way would block the event loop for the
                        # whole transfer. Run it on a worker thread instead.
                        media = await asyncio.to_thread(client.media_post, p)
                        mid = media["id"]
                        # Video / large uploads come back still processing
                        # (``url`` is null); attaching one to a status 422s
                        # with "files that have not finished processing".
                        # Retry transient network / 5xx errors with a small
                        # consecutive-failure cap so one blip doesn't
                        # abort the post the way the old bare-except did.
                        # Auth errors propagate to the outer handler.
                        if media.get("url") is None:
                            consecutive_failures = 0
                            for _ in range(60):  # up to ~60s total wait
                                await asyncio.sleep(1)
                                try:
                                    media = await asyncio.to_thread(client.media, mid)
                                except (
                                    MastodonNetworkError,
                                    MastodonReadTimeout,
                                    MastodonServerError,
                                    MastodonGatewayTimeoutError,
                                ):
                                    consecutive_failures += 1
                                    if consecutive_failures >= 5:
                                        break
                                    continue
                                consecutive_failures = 0
                                if media.get("url") is not None:
                                    break
                        media_ids.append(mid)

                status = await asyncio.to_thread(client.status_post, text, media_ids=media_ids)
            except MastodonUnauthorizedError as exc:
                # Either media_post or status_post can raise this if the
                # access token has been revoked/expired.
                raise CredentialAuthError(
                    creds.get("uuid"),
                    "Mastodon rejected the access token — re-OAuth.",
                ) from exc
            return {"url": status["url"], "id": str(status["id"])}
        except CredentialAuthError:
            raise
        except Exception as e:
            raise RuntimeError(f"Mastodon post failed: {e}") from e


class LinkedInPoster(SocialPoster):
    platform = "linkedin"
    required_keys = ["access_token", "person_urn"]

    async def post(
        self,
        text: str,
        media_path: str | None = None,
        *,
        media_paths: list[str] | None = None,
        alt_texts: list[str] | None = None,
    ) -> dict:
        """Post to LinkedIn with optional media.

        Implements the three-step asset upload chain documented at
        https://learn.microsoft.com/en-us/linkedin/marketing/integrations/community-management/shares/vector-asset-api ::

            1. ``POST /v2/assets?action=registerUpload`` for each asset.
            2. ``PUT`` the binary to the returned ``uploadUrl``.
            3. ``POST /v2/ugcPosts`` with ``shareMediaCategory: IMAGE | VIDEO``
               and the asset URN(s).

        LinkedIn doesn't allow mixing image and video in a single share; if
        the first asset is a video we use ``VIDEO`` and ignore image siblings.
        Otherwise ``IMAGE`` with up to 9 image URNs (the personal-share UI
        cap; the API itself accepts more on company pages — verify against
        the live doc if a higher cap matters).
        """
        text = (text or "").strip()
        creds = await self._get_creds()
        token = creds.get("access_token")
        owner_urn = creds.get("person_urn")
        if not token or not owner_urn:
            raise CredentialAuthError(
                creds.get("uuid"),
                "LinkedIn is not configured. Click 'Connect with LinkedIn' in Settings.",
            )

        paths, alts = self._resolve_media_inputs(media_path, media_paths, alt_texts)
        self._require_paths_exist(paths, "LinkedIn")
        existing = [(Path(p), a) for p, a in zip(paths, alts)]

        share_media_category = "NONE"
        media_blocks: list[dict] = []
        if existing:
            first_mime = mimetypes.guess_type(existing[0][0].name)[0] or ""
            is_video = first_mime.startswith("video/")
            share_media_category = "VIDEO" if is_video else "IMAGE"
            uploadable = (
                [existing[0]]
                if is_video
                else [
                    (p, a) for (p, a) in existing
                    if not (mimetypes.guess_type(p.name)[0] or "").startswith("video/")
                ][:9]
            )
            try:
                async with httpx.AsyncClient(timeout=120) as client:
                    for path_obj, alt in uploadable:
                        asset_urn = await self._linkedin_upload_asset(
                            client, token, owner_urn, path_obj, is_video,
                            creds.get("uuid"),
                        )
                        block: dict = {
                            "status": "READY",
                            "media": asset_urn,
                        }
                        if alt:
                            block["description"] = {"text": alt}
                        media_blocks.append(block)
            except CredentialAuthError:
                raise
            except Exception as exc:
                raise MediaUploadError(
                    f"Couldn't attach media to the LinkedIn post: {exc}. Nothing "
                    "was posted — remove the attachment to post text only, then "
                    "retry."
                ) from exc

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0",
        }

        share_content: dict = {
            "shareCommentary": {"text": text},
            "shareMediaCategory": share_media_category,
        }
        if media_blocks:
            share_content["media"] = media_blocks

        body = {
            "author": owner_urn,
            "lifecycleState": "PUBLISHED",
            "specificContent": {
                "com.linkedin.ugc.ShareContent": share_content,
            },
            "visibility": {"com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"},
        }

        try:
            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.post(
                    "https://api.linkedin.com/v2/ugcPosts", headers=headers, json=body,
                )
                if resp.status_code == 401:
                    raise CredentialAuthError(
                        creds.get("uuid"),
                        "LinkedIn rejected the access token — re-OAuth.",
                    )
                if resp.status_code >= 400:
                    raise RuntimeError(
                        f"LinkedIn post failed: {_http_error_detail(resp)}"
                    )
                post_id = resp.headers.get("x-restli-id", "")
                return {
                    "url": f"https://www.linkedin.com/feed/update/{post_id}",
                    "id": post_id,
                }
        except (CredentialAuthError, MediaUploadError):
            raise
        except Exception as e:
            raise RuntimeError(f"LinkedIn post failed: {e}") from e

    @staticmethod
    async def _linkedin_upload_asset(
        client: httpx.AsyncClient,
        token: str,
        owner_urn: str,
        path: Path,
        is_video: bool,
        cred_uuid: str | None,
    ) -> str:
        """Run the three-step LinkedIn asset upload. Returns the asset URN
        (e.g. ``urn:li:digitalmediaAsset:abc123...``) on success."""
        recipe = (
            "urn:li:digitalmediaRecipe:feedshare-video"
            if is_video
            else "urn:li:digitalmediaRecipe:feedshare-image"
        )
        register_payload = {
            "registerUploadRequest": {
                "recipes": [recipe],
                "owner": owner_urn,
                "serviceRelationships": [
                    {
                        "relationshipType": "OWNER",
                        "identifier": "urn:li:userGeneratedContent",
                    }
                ],
            }
        }
        register_headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0",
        }
        # Step 1: registerUpload
        resp = await client.post(
            "https://api.linkedin.com/v2/assets?action=registerUpload",
            headers=register_headers,
            json=register_payload,
        )
        if resp.status_code == 401:
            raise CredentialAuthError(
                cred_uuid, "LinkedIn rejected the access token — re-OAuth.",
            )
        if resp.status_code >= 400:
            raise RuntimeError(
                f"LinkedIn registerUpload failed: {_http_error_detail(resp)}"
            )
        data = resp.json()
        value = data.get("value") or {}
        upload_mech = (
            value.get("uploadMechanism", {})
            .get("com.linkedin.digitalmedia.uploading.MediaUploadHttpRequest", {})
        )
        upload_url = upload_mech.get("uploadUrl")
        asset_urn = value.get("asset")
        if not upload_url or not asset_urn:
            raise RuntimeError(
                f"LinkedIn registerUpload returned no uploadUrl/asset: {data}"
            )

        # Step 2: PUT bytes to the upload URL.
        mime, _ = mimetypes.guess_type(path.name)
        put_headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": mime or ("video/mp4" if is_video else "image/jpeg"),
        }
        # File can be large (~512 MB); read on a worker thread to avoid
        # blocking the event loop.
        asset_bytes = await asyncio.to_thread(path.read_bytes)
        put_resp = await client.put(
            upload_url, headers=put_headers, content=asset_bytes
        )
        if put_resp.status_code == 401:
            # The upload URL carries the same bearer as registerUpload; a 401
            # here is an auth failure, so flag the credential for re-auth rather
            # than burying it as a generic media-upload error.
            raise CredentialAuthError(
                cred_uuid, "LinkedIn rejected the access token — re-OAuth.",
            )
        if put_resp.status_code not in (200, 201):
            raise RuntimeError(
                f"LinkedIn asset PUT failed: HTTP {put_resp.status_code} {put_resp.text}"
            )

        return asset_urn


class ThreadsPoster(SocialPoster):
    platform = "threads"
    required_keys = ["access_token", "user_id"]

    async def post(
        self,
        text: str,
        media_path: str | None = None,
        *,
        media_paths: list[str] | None = None,
        alt_texts: list[str] | None = None,
    ) -> dict:
        # Threads' API posts text only — it can't attach media. Generation
        # drops a Threads slot's media (and warns there), so a freshly
        # generated post never reaches this. A post created by older code may
        # still carry media_paths — refuse to post it silently text-only;
        # surface it so the user removes the attachment and retries.
        text = (text or "").strip()
        creds = await self._get_creds()
        if self._resolve_media_inputs(media_path, media_paths, alt_texts)[0]:
            raise MediaUploadError(
                "Threads can't attach media — its API is text-only. Remove the "
                "attachment from this post to send it as text, then retry."
            )

        try:
            import httpx

            access_token = creds["access_token"]
            user_id = creds["user_id"]

            async with httpx.AsyncClient() as client:
                create_resp = await client.post(
                    f"https://graph.threads.net/v1.0/{user_id}/threads",
                    params={"media_type": "TEXT", "text": text, "access_token": access_token},
                )
                if create_resp.status_code == 401:
                    raise CredentialAuthError(
                        creds.get("uuid"),
                        "Threads rejected the access token — re-OAuth.",
                    )
                if create_resp.status_code >= 400:
                    raise RuntimeError(
                        f"Threads container create failed: {_http_error_detail(create_resp)}"
                    )
                container_id = create_resp.json()["id"]

                # Threads' publish is a separate call from create, and the
                # container is not necessarily ready the instant it's created.
                # Publishing too early races the server and returns a 400, so
                # poll the container's status until it reports FINISHED.
                await self._await_container_finished(
                    client, container_id, access_token, creds.get("uuid"),
                )

                publish_resp = await client.post(
                    f"https://graph.threads.net/v1.0/{user_id}/threads_publish",
                    params={"creation_id": container_id, "access_token": access_token},
                )
                if publish_resp.status_code == 401:
                    raise CredentialAuthError(
                        creds.get("uuid"),
                        "Threads rejected the access token — re-OAuth.",
                    )
                if publish_resp.status_code >= 400:
                    raise RuntimeError(
                        f"Threads publish failed: {_http_error_detail(publish_resp)}"
                    )
                post_id = publish_resp.json()["id"]

                username = creds.get("username", "")
                return {"url": f"https://threads.net/@{username}/post/{post_id}", "id": post_id}
        except (CredentialAuthError, MediaUploadError):
            raise
        except Exception as e:
            raise RuntimeError(f"Threads post failed: {e}") from e

    _CONTAINER_POLL_ATTEMPTS = 10
    _CONTAINER_POLL_DELAY_SECONDS = 1.0

    async def _await_container_finished(
        self,
        client: httpx.AsyncClient,
        container_id: str,
        access_token: str,
        cred_uuid: str | None,
    ) -> None:
        """Poll a Threads media container until its status is ``FINISHED``.

        Raises if the container reports ``ERROR``/``EXPIRED``, if the status
        check itself fails, or if it never reaches ``FINISHED`` within the
        bounded attempt budget. Text containers are usually ready on the first
        poll; the loop exists to absorb the brief server-side processing gap
        that otherwise makes ``threads_publish`` return a 400.
        """
        last_status = "UNKNOWN"
        for attempt in range(self._CONTAINER_POLL_ATTEMPTS):
            status_resp = await client.get(
                f"https://graph.threads.net/v1.0/{container_id}",
                params={"fields": "status,error_message", "access_token": access_token},
            )
            if status_resp.status_code == 401:
                raise CredentialAuthError(
                    cred_uuid, "Threads rejected the access token — re-OAuth.",
                )
            if status_resp.status_code >= 400:
                raise RuntimeError(
                    f"Threads container status check failed: "
                    f"{_http_error_detail(status_resp)}"
                )
            data = status_resp.json()
            # Graph can answer 200 with an error object (and no status) rather
            # than a 4xx; surface that immediately instead of polling to timeout
            # and hiding the real cause.
            if isinstance(data, dict) and data.get("error"):
                raise RuntimeError(
                    f"Threads container status check failed: "
                    f"{_http_error_detail(status_resp)}"
                )
            last_status = data.get("status", "UNKNOWN")
            if last_status == "FINISHED":
                return
            if last_status in ("ERROR", "EXPIRED"):
                raise RuntimeError(
                    f"Threads container {last_status}: "
                    f"{data.get('error_message') or 'no detail from Threads'}"
                )
            if attempt < self._CONTAINER_POLL_ATTEMPTS - 1:
                await asyncio.sleep(self._CONTAINER_POLL_DELAY_SECONDS)
        raise RuntimeError(
            f"Threads container not ready to publish (last status={last_status}) "
            f"after {self._CONTAINER_POLL_ATTEMPTS} checks."
        )


# --- Registry ---

_POSTERS: dict[str, type[SocialPoster]] = {
    "twitter": TwitterPoster,
    "bluesky": BlueskyPoster,
    "mastodon": MastodonPoster,
    "linkedin": LinkedInPoster,
    "threads": ThreadsPoster,
}

ALL_PLATFORMS = list(_POSTERS.keys())

# Per-platform field definitions for the settings UI
PLATFORM_FIELDS: dict[str, list[dict]] = {
    "twitter": [
        # OAuth 2.0 user-context only. The OAuth 2.0 Connect button populates
        # bearer_token (and refresh_token + username); these fields are shown
        # so the user can see what's stored, with all values masked.
        {"key": "bearer_token", "label": "OAuth 2.0 access token", "type": "password", "secret": True},
        {"key": "refresh_token", "label": "Refresh token", "type": "password", "secret": True},
        {"key": "username", "label": "Username", "type": "text", "secret": False},
    ],
    # Bluesky is OAuth-only — there is no paste form. The Settings UI
    # renders the connected accounts list from /api/social-credentials,
    # not from this fields/stored payload. Leaving this empty makes
    # /api/settings/social return ``fields: []`` for bluesky so any
    # callers that introspect it know there's nothing to render.
    "bluesky": [],
    "mastodon": [
        {"key": "instance_url", "label": "Instance URL", "type": "text", "secret": False, "placeholder": "https://mastodon.social"},
        {"key": "access_token", "label": "Access Token", "type": "password", "secret": True},
    ],
    "linkedin": [
        {"key": "access_token", "label": "Access Token", "type": "password", "secret": True},
        {"key": "person_urn", "label": "Person URN", "type": "text", "secret": False, "placeholder": "urn:li:person:xxxxxxxx"},
    ],
    # Threads is OAuth-only at the network layer; fields stay empty so the
    # Settings UI doesn't paint a paste form. The "+ Add account" button
    # uses the popup flow on HTTPS origins and the short-lived token
    # exchange on HTTP origins (both go through Meta's OAuth endpoints —
    # the exchange is a redirect-less variant Meta provides specifically
    # for native/CLI clients that can't host an HTTPS callback).
    "threads": [],
}

PLATFORM_DESCRIPTIONS: dict[str, str] = {
    "twitter": "Click 'Connect with X (OAuth 2.0)' below. Requires a paid X API tier (Free tier can't post). Posts and media uploads use the v2 API.",
    "bluesky": "Click 'Connect with Bluesky' below and enter your handle (e.g. you.bsky.social). Bluesky's OAuth flow handles the rest. Free.",
    "mastodon": "Create an app in your instance's Settings → Development → New Application. Free.",
    "linkedin": "Requires LinkedIn app with w_member_social scope. Get person URN from /v2/me.",
    "threads": "Requires Meta developer app with threads_publish scope.",
}

# Detailed per-platform setup walkthroughs, shown in the Settings UI behind an
# info toggle. Each list is rendered as ordered steps. Links are left as plain
# URLs; the UI auto-linkifies http(s) occurrences.
PLATFORM_SETUP_GUIDES: dict[str, list[str]] = {
    "twitter": [
        "Sign up at https://developer.x.com and pick a paid tier — Free tier cannot post.",
        "Projects & Apps → Create App inside a Project (standalone apps can't call POST /2/tweets).",
        "Settings → User authentication settings → Edit. App permissions = Read and write. Type of App = Web App / Native App. Save.",
        "Add http://127.0.0.1:8008/api/oauth/twitter/callback to Callback URLs.",
        "Keys and tokens → OAuth 2.0 Client ID and Client Secret: copy these for the Connect button.",
        "Click Connect with X (OAuth 2.0) below and paste those values when prompted.",
    ],
    "bluesky": [
        "Click Connect with Bluesky below.",
        "Enter your handle when prompted (e.g. yourname.bsky.social — no @).",
        "A popup opens to bsky.social. Sign in and approve the requested scopes.",
        "When you land back here you're done — no app password to copy or paste.",
        "Tokens auto-refresh. If Bluesky revokes them or you sign out remotely, the credential will show 'needs re-auth' and you can click Connect again.",
    ],
    "mastodon": [
        "Sign in at your Mastodon instance (e.g. https://mastodon.social).",
        "Preferences → Development → New Application (https://mastodon.social/settings/applications/new).",
        "Name it, leave Redirect URI default, check scopes write:statuses and write:media, submit.",
        "Open the new app and copy Your access token.",
        "Instance URL: https://mastodon.social (or your instance, no trailing slash). Access Token: paste.",
    ],
    "linkedin": [
        "Create an app at https://www.linkedin.com/developers/apps. Must be associated with a LinkedIn Page you admin (create one at https://www.linkedin.com/company/setup/new/ if needed). Upload a 100x100 logo.",
        "Products tab → add Share on LinkedIn (grants w_member_social) and Sign In with LinkedIn using OpenID Connect (grants openid profile). Both auto-approve in seconds.",
        "Auth tab → OAuth 2.0 settings → add redirect URL http://localhost:8008/ and Update. Note the Client ID and Primary Client Secret at the top.",
        "Get an auth code: open this URL in a browser (replace <CLIENT_ID>): https://www.linkedin.com/oauth/v2/authorization?response_type=code&client_id=<CLIENT_ID>&redirect_uri=http://localhost:8008/&scope=openid%20profile%20w_member_social&state=abc — click Allow. Browser fails to load the redirect target; copy the code= value from the address bar.",
        "Exchange code for access token: curl -X POST https://www.linkedin.com/oauth/v2/accessToken -d grant_type=authorization_code -d code='<CODE>' -d redirect_uri=http://localhost:8008/ -d client_id='<CLIENT_ID>' -d client_secret='<CLIENT_SECRET>' — copy the access_token from the JSON response (valid ~60 days).",
        "Get Person URN: curl -H 'Authorization: Bearer <TOKEN>' https://api.linkedin.com/v2/userinfo — copy the sub value and prefix it with urn:li:person: (e.g. urn:li:person:abc12345).",
        "Paste Access Token and Person URN below. Note: tokens expire in 60 days — redo the last three steps when posts start returning 401.",
    ],
    "threads": [
        "Create a Meta developer app at https://developers.facebook.com/apps → Other → Business → Create. Open the app and add the Threads API product from Use cases.",
        "Add yourself as a Threads tester: App roles → Roles → Add People → Threads Tester. Accept the invite at https://www.threads.net/settings/privacy (Invitations).",
        "Open the Graph API Explorer at https://developers.facebook.com/tools/explorer/. Top-right: pick your app. Add permissions threads_basic and threads_content_publish. Click Generate Access Token and accept consent. Copy the short-lived token.",
        "App settings → Basic — click Show next to App Secret and copy it.",
        "Click the green Exchange Short-Lived Token button below. Paste the App Secret and the short-lived token. The app exchanges for a 60-day long-lived token, fetches your user_id + username, and stores everything in Keychain.",
    ],
}


def get_poster(platform: str) -> SocialPoster:
    """Return a poster bound to the platform's first active credential.

    Phase A→B transitional helper. Send paths that know which credential
    to use should call :func:`get_poster_for_account` instead.
    """
    cls = _POSTERS.get(platform)
    if not cls:
        raise ValueError(f"Unknown platform: {platform}. Available: {ALL_PLATFORMS}")
    return cls()


def decode_media_paths(post_row: dict) -> list[str]:
    """Pull a media-paths list out of a social_posts row.

    Prefers the new JSON-array column (``media_paths``); falls back to the
    legacy single-string column (``media_path``) for any row written before
    migration 010 or by an older code path. Empty / NULL ⇒ ``[]``.
    """
    raw = post_row.get("media_paths")
    if raw:
        try:
            decoded = json.loads(raw)
            if isinstance(decoded, list):
                return [str(p) for p in decoded if p]
        except (TypeError, ValueError):
            pass
    legacy = post_row.get("media_path")
    return [str(legacy)] if legacy else []


async def find_recent_duplicate_post(
    *,
    platform: str,
    social_account_id: int | None,
    content: str,
    media_path: str | None = None,
    exclude_post_id: int | None = None,
    lookback_days: int = 30,
) -> dict | None:
    """Look for a previously-sent post with the same content AND same
    media going to the same target. Returns the matching row (with id,
    posted_at, post_url, content, media_path, social_account_id) or
    ``None`` if there is no recent dup.

    Match criteria:

    * same ``platform``,
    * same ``social_account_id`` (when both sides have one — null/null is
      compared by content alone, which is the conservative call),
    * identical ``content`` after stripping leading/trailing whitespace
      (internal newlines and indentation are preserved — only the edges
      are normalised, since AI blocks frequently emit a stray leading
      space or trailing newline),
    * identical ``media_path`` — same text with different attached media
      is NOT a duplicate. NULL/empty-string media is treated as a single
      "no media" bucket; switching from no-media to media (or vice
      versa) is also not a duplicate.
    * status is ``posted`` or ``sending``,
    * occurred within the last ``lookback_days`` days.

    The bulk-send path uses this as a pre-flight check; if the user
    confirms despite the dup, the route re-runs with ``confirm=true``
    and skips the check. The scheduler-fired paths
    (``publish_video_job``, ``_send_scheduled_post``) call this as a
    hard gate and skip-with-log on a hit, so an old auto-schedule that
    happens to produce identical content can't double-post even when
    nobody's at the keyboard.
    """
    from yt_scheduler.database import get_db

    normalised = (content or "").strip()
    if not normalised:
        return None  # empty post can't be a dup of anything meaningful

    # Normalise media to "" for the no-media bucket so NULL and "" both
    # compare equal. SQLite NULL semantics would otherwise make
    # ``media_path = ?`` always false when one side is NULL.
    media_key = (media_path or "").strip()

    db = await get_db()
    sql_parts = [
        "SELECT id, video_id, platform, content, media_path, social_account_id, "
        "       posted_at, post_url, status, "
        "       COALESCE(posted_at, created_at) AS event_at "
        "FROM social_posts "
        "WHERE platform = ? AND TRIM(content) = ? "
        "AND COALESCE(TRIM(media_path), '') = ? "
        "AND status IN ('posted', 'sending') "
        "AND COALESCE(posted_at, created_at) >= datetime('now', '-' || ? || ' days')"
    ]
    params: list = [platform, normalised, media_key, lookback_days]
    if social_account_id is not None:
        sql_parts.append(
            "AND (social_account_id = ? OR social_account_id IS NULL)"
        )
        params.append(int(social_account_id))
    if exclude_post_id is not None:
        sql_parts.append("AND id != ?")
        params.append(int(exclude_post_id))
    sql_parts.append("ORDER BY event_at DESC LIMIT 1")

    cursor = await db.execute(" ".join(sql_parts), tuple(params))
    row = await cursor.fetchone()
    if row is None:
        return None
    return dict(row)


async def get_poster_for_account(social_account_id: int) -> SocialPoster:
    """Build a poster bound to a specific ``social_accounts`` row."""
    from yt_scheduler.services.social_credentials import (
        get_credential_by_id,
        load_bundle,
    )

    cred = await get_credential_by_id(social_account_id)
    if cred is None:
        raise ValueError(f"Credential {social_account_id} not found")
    if cred.get("deleted_at") is not None:
        raise ValueError(
            f"Credential {social_account_id} ({cred['label']}) was deleted"
        )

    cls = _POSTERS.get(cred["platform"])
    if not cls:
        raise ValueError(f"Unknown platform: {cred['platform']}")

    bundle = await load_bundle(cred["platform"], cred["uuid"])
    if bundle is None:
        raise ValueError(
            f"No bundle stored for credential {social_account_id} "
            f"({cred['label']}) — the Keychain entry was likely deleted "
            "out of band."
        )
    return cls(bundle=bundle)


async def get_poster_for_uuid(platform: str, uuid: str) -> SocialPoster:
    """Build a poster from an explicit (platform, credential UUID) pair."""
    from yt_scheduler.services.social_credentials import load_bundle

    cls = _POSTERS.get(platform)
    if not cls:
        raise ValueError(f"Unknown platform: {platform}. Available: {ALL_PLATFORMS}")
    bundle = await load_bundle(platform, uuid)
    if bundle is None:
        raise ValueError(f"No bundle stored at {platform}:cred.{uuid}")
    return cls(bundle=bundle)
