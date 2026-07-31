"""Social media posting — multi-platform with Keychain credential storage."""

from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
import os
import re
import tempfile
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx

# Aliased: MastodonPoster binds a local `media` to a Mastodon attachment dict,
# which would shadow a bare module import inside that method. Matches how the
# routers already refer to this module.
from yt_scheduler import config
from yt_scheduler.services import media as media_service
from yt_scheduler.services.keychain import KeychainWriteError, SecretsIndexError

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
    async with httpx.AsyncClient(
        timeout=config.TWITTER_BEARER_REFRESH_TIMEOUT_SECONDS
    ) as client:
        resp = await client.post(_TWITTER_TOKEN_URL, data=body, auth=auth)
    # A 5xx or 429 is the endpoint having a bad day, not a verdict on the
    # credential — the same distinction the Threads refresh makes.
    if resp.status_code >= 500 or resp.status_code == 429:
        raise TokenEndpointUnavailable(
            f"X token endpoint unavailable: {_http_error_detail(resp)}"
        )
    if resp.status_code != 200:
        raise RuntimeError(f"X token refresh rejected: {_http_error_detail(resp)}")
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
    # X access tokens live ~2h; persist the acquisition time and expiry so the
    # background refresh job can pre-emptively renew before it lapses.
    from yt_scheduler.services.social_credentials import save_bundle, stamp_token_metadata

    expires_in = payload.get("expires_in")
    stamp_token_metadata(
        updated,
        expires_in_seconds=int(expires_in) if expires_in else None,
    )
    # save_bundle rather than a raw Keychain write: it also mirrors the token
    # metadata onto the credential row for the Settings list.
    await save_bundle("twitter", cred_uuid, updated)
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
        async with httpx.AsyncClient(
            timeout=config.TWITTER_SIMPLE_UPLOAD_TIMEOUT_SECONDS
        ) as client:
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

    async with httpx.AsyncClient(
        timeout=config.TWITTER_CHUNKED_UPLOAD_TIMEOUT_SECONDS
    ) as client:
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
                chunk = f.read(config.TWITTER_VIDEO_UPLOAD_CHUNK_BYTES)
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


class TokenEndpointUnavailable(Exception):
    """The X token endpoint couldn't answer — a 5xx, a 429 rate limit, or
    similar. Says nothing about the credential, so callers retry later and
    never flag ``needs_reauth``.

    Deliberately NOT a RuntimeError: the refresh paths convert RuntimeError
    to :class:`CredentialAuthError` (terminal), and a conversion site that
    forgets this class must fail transient, never terminal.
    """


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
        # Cap it: a Cloudflare HTML 502 page must not flow whole into
        # social_posts.error and the logs.
        text = (resp.text or "").strip()[:500]
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
        # RFC 6749 token-endpoint shape: {"error": "invalid_grant",
        # "error_description": "..."} — the description is the useful half.
        description = data.get("error_description")
        if isinstance(description, str) and description:
            return f"HTTP {resp.status_code}: {err}: {description}"
        return f"HTTP {resp.status_code}: {err}"
    return f"HTTP {resp.status_code}: {json.dumps(data)[:500]}"


def _exception_host(exc: BaseException) -> str | None:
    """Host the failing request targeted, or None.

    ``httpx.HTTPError.request`` *raises* RuntimeError when the exception was
    constructed without one, so it can never be read bare inside an error path.
    """
    try:
        return exc.request.url.host  # type: ignore[attr-defined]
    except (AttributeError, RuntimeError):
        return None


def _exception_detail(exc: BaseException) -> str:
    """Describe ``exc`` for a user who has to decide what to do next.

    ``f"...failed: {exc}"`` is not enough: several exceptions the posting paths
    actually raise stringify to the empty string — ``httpx.ConnectError``,
    ``httpx.ReadTimeout`` and ``httpx.HTTPStatusError`` among them — which
    produced stored errors like ``"Threads post failed: "`` with no detail at
    all. Always return something the user can act on.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        return _http_error_detail(exc.response)

    # TimeoutException is a subclass of TransportError, so it must be tested first.
    if isinstance(exc, (httpx.TimeoutException, httpx.TransportError)):
        target = _exception_host(exc) or "the provider"
        verb = "timed out contacting" if isinstance(exc, httpx.TimeoutException) else "could not reach"
        text = str(exc).strip()
        return (
            f"{type(exc).__name__}: {verb} {target}"
            + (f" ({text})" if text else "")
            + ". Check your network connection, then use Send to retry."
        )

    text = str(exc).strip()
    if text:
        return f"{type(exc).__name__}: {text}"
    # No message at all — the type name is the only signal left. Better a bare
    # class name than a bare colon.
    return f"{type(exc).__name__} (no further detail; see the server log)"


# Every transcoded copy is named `social_<pid>_<platform>_<random>.mp4`. The
# PID is in the name so the startup sweep can tell a file *it* leaked from one
# another live instance is uploading right now. That case is real, not
# theoretical: uvicorn runs the lifespan before it binds the port, so a stray
# `yt-scheduler` against the same data dir executes every startup sweep and only
# then dies on EADDRINUSE. It also covers our own process — the scheduler starts
# before the sweeps, so a restored publish job can already be mid-send.
_DERIVED_NAME_STEM = "social"

# Backstop for PID reuse, where the number in a leaked file's name has since
# been handed to an unrelated live process and the liveness check says "keep".
# A derived file's whole life is one transcode (capped by media's 30-minute
# ffmpeg timeout) plus one upload of a file small enough to satisfy a platform
# byte cap, so this leaves hours of headroom.
_DERIVED_MAX_AGE_SECONDS: float = 12 * 60 * 60


def _owning_pid_of_derived_file(name: str) -> int | None:
    """The PID embedded in a derived-media filename, or ``None`` when the name
    is not one we wrote. An unrecognised name is never deleted."""
    parts = name.split("_")
    if len(parts) < 4 or parts[0] != _DERIVED_NAME_STEM:
        return None
    try:
        pid = int(parts[1])
    except ValueError:
        return None
    # A non-positive pid makes os.kill address a process *group*. A filename
    # must never be able to steer that.
    return pid if pid > 0 else None


def _process_is_running(pid: int) -> bool:
    """Whether ``pid`` names a live process.

    Anything other than a definite "no such process" counts as running, so an
    unexpected errno can never be the reason a file gets deleted.
    """
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        # PermissionError included: the process exists, we just don't own it.
        return True
    return True


def cleanup_orphan_derived_media() -> int:
    """Delete transcoded send copies whose writing process is gone.

    :meth:`SocialPoster.prepared_media` removes its own output in a ``finally``,
    which covers every in-process outcome including exceptions — but not
    SIGKILL, a panic, or power loss mid-send. Each survivor is a full
    transcoded video and nothing else would ever remove it.

    Two guards keep it off a file still in use: the embedded PID must name no
    live process, or the file must be older than any send could plausibly be.
    Returns the number removed.
    """
    from yt_scheduler import config

    derived_dir = config.derived_media_dir()
    if not derived_dir.is_dir():
        return 0
    try:
        entries = list(derived_dir.iterdir())
    except OSError as exc:
        logger.warning("Derived-media sweep could not read %s: %s", derived_dir, exc)
        return 0

    now = time.time()
    removed = 0
    for path in entries:
        owning_pid = _owning_pid_of_derived_file(path.name)
        if owning_pid is None:
            # This module is the only writer here by contract, so an
            # unrecognised entry means something changed. Report it; never
            # widen the sweep to cover it.
            logger.warning(
                "Unrecognised entry in the derived-media directory; leaving it "
                "in place: %s", path,
            )
            continue
        try:
            age_seconds = now - path.stat().st_mtime
        except OSError as exc:
            logger.debug("Could not stat derived media %s: %s", path, exc)
            continue
        if _process_is_running(owning_pid) and age_seconds < _DERIVED_MAX_AGE_SECONDS:
            continue
        try:
            path.unlink()
            removed += 1
        except FileNotFoundError:
            pass  # the owner's finally beat us to it
        except OSError as exc:
            logger.warning("Could not remove orphan derived media %s: %s", path, exc)
    return removed


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

    # False when the platform's API cannot take an attachment from us at all,
    # so :meth:`post` skips media preparation entirely rather than transcoding
    # a file the poster is about to refuse.
    accepts_media: bool = True

    # True when the platform fetches attachments from a URL instead of taking
    # an upload, so posting media needs configured media hosting. Distinct from
    # ``accepts_media``: that is a permanent property of the platform's API,
    # this is a dependency we can satisfy or fail to.
    requires_hosted_media: bool = False

    # Whether this platform HAS a token-refresh flow at all. Separate from
    # :meth:`refresh_if_stale` returning False, which means "nothing due right
    # now" — the two were indistinguishable, and Threads spent 60 days
    # inheriting the no-op default while its token quietly aged out. A platform
    # that sets this False is asserting its credentials don't expire; one that
    # sets it True must override refresh_if_stale.
    supports_token_refresh: bool = False

    # How close to expiry a token must be before the background sweep renews
    # it. Platform behavior, so it lives on the poster; the numbers live in
    # config with the scheduler's other knobs. Meaningful only when
    # supports_token_refresh is True.
    token_refresh_window_secs: int = config.SOCIAL_TOKEN_REFRESH_WINDOW_SECONDS

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

        This is a template method: it normalises the media inputs, brings any
        attachment inside the platform's limits (see :meth:`prepared_media`),
        and hands the result to :meth:`_post_prepared`, which subclasses
        implement. Doing it here rather than in each poster means a video is
        checked against the destination's envelope on *every* send path —
        smart queue, publish fan-out, and manual Send alike — instead of
        depending on five implementations staying in agreement.
        """
        paths, alts = self._resolve_media_inputs(media_path, media_paths, alt_texts)
        if not self.accepts_media:
            # Nothing to prepare, and preparing would burn a transcode on an
            # attachment the poster is about to reject anyway. Its own check
            # produces the platform-specific explanation.
            return await self._post_prepared(
                text, media_paths=paths, alt_texts=alts
            )
        # Checked before probing so a missing attachment reports as missing
        # rather than as an unreadable file.
        self._require_paths_exist(paths, self.platform)
        async with self.prepared_media(paths) as prepared:
            return await self._post_prepared(
                text, media_paths=prepared, alt_texts=alts
            )

    async def _post_prepared(
        self,
        text: str,
        media_path: str | None = None,
        *,
        media_paths: list[str] | None = None,
        alt_texts: list[str] | None = None,
    ) -> dict:
        """Platform-specific send. ``media_paths`` are already upload-ready."""
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

    async def media_limits(self) -> "media_service.PlatformMediaLimits":
        """What this platform accepts for a video attachment.

        Static for every platform whose limits are published globally.
        Mastodon overrides this: its caps are per-instance and the only
        authority is the instance itself.
        """
        return PLATFORM_MEDIA_LIMITS[self.platform]

    @asynccontextmanager
    async def prepared_media(self, paths: list[str]):
        """Yield upload-ready paths, transcoding any video this platform
        would reject, and deleting whatever we produced on the way out.

        A source already inside the platform's envelope is yielded untouched
        — no re-encode, no quality loss, no time spent. Only what actually
        violates a limit gets re-encoded, and only as far as needed.

        Raises :class:`media_service.MediaTooLongError` when the clip is longer than
        the platform allows. That is unfixable by encoding, so the caller
        skips this platform rather than failing it.
        """
        from yt_scheduler import config

        limits = await self.media_limits()
        prepared: list[str] = []
        temporaries: list[Path] = []
        try:
            for path in paths:
                mime = mimetypes.guess_type(Path(path).name)[0] or ""
                if not mime.startswith("video/"):
                    prepared.append(path)
                    continue
                probe = await asyncio.to_thread(media_service.probe_video_file, path)
                if probe is None:
                    raise MediaUploadError(
                        f"Can't post to {self.platform}: {Path(path).name} could "
                        "not be probed, so we can't tell whether it fits the "
                        "platform's limits. Nothing was posted."
                    )
                if not media_service.violates_limits(probe, limits):
                    prepared.append(path)
                    continue
                # Derived files must live inside UPLOAD_DIR: every poster
                # re-checks its attachments with _require_paths_managed, which
                # rejects anything outside it. A system temp dir would fail
                # that check on every transcoded post.
                derived_dir = config.derived_media_dir()
                derived_dir.mkdir(parents=True, exist_ok=True)
                handle, destination_name = tempfile.mkstemp(
                    prefix=f"{_DERIVED_NAME_STEM}_{os.getpid()}_{self.platform}_",
                    suffix=".mp4", dir=str(derived_dir),
                )
                # mkstemp hands back an open descriptor we never write through
                # — ffmpeg opens the path itself. Close it or every transcode
                # leaks one for the life of the process.
                os.close(handle)
                destination = Path(destination_name)
                temporaries.append(destination)
                await asyncio.to_thread(
                    media_service.transcode_for_platform,
                    path, destination, limits, probe=probe,
                )
                logger.info(
                    "Transcoded %s for %s: %dx%d %.1f MB -> %.1f MB",
                    Path(path).name, self.platform, probe.display_width or 0,
                    probe.display_height or 0, (probe.size_bytes or 0) / 1e6,
                    destination.stat().st_size / 1e6,
                )
                prepared.append(str(destination))
            yield prepared
        finally:
            for temp in temporaries:
                try:
                    temp.unlink(missing_ok=True)
                except OSError as exc:
                    logger.warning(
                        "Could not remove derived media %s: %s", temp, exc
                    )

    async def is_configured(self) -> bool:
        """Check if this poster's credentials are complete."""
        creds = await self._get_creds()
        return all(creds.get(k) for k in self.required_keys)

    async def refresh_if_stale(self, *, window_secs: int = 0) -> bool:
        """Proactively refresh this credential's access token if it expires
        within ``window_secs``. Returns ``True`` if a refresh happened.

        Default: no-op (``False``). Only correct for a platform whose
        credentials genuinely don't expire — say so by leaving
        ``supports_token_refresh`` False. Do not inherit this because a refresh
        flow hasn't been written yet: the sweep would report success having
        done nothing, and the token would die on schedule with no warning.
        That is exactly what happened to Threads.

        A terminal failure raises :class:`CredentialAuthError` so the caller
        can mark the credential ``needs_reauth``.
        """
        return False


class TwitterPoster(SocialPoster):
    # Declares what refresh_if_stale below already does, so the two can't
    # drift apart silently.
    supports_token_refresh = True
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
            except TokenEndpointUnavailable as exc:
                logger.warning(
                    "X token refresh deferred for %s; will retry: %s", uuid, exc
                )
                return False
            except (KeychainWriteError, SecretsIndexError):
                # A LOCAL storage failure is not a provider verdict — telling
                # the user to re-OAuth would run a flow whose result the
                # broken Keychain couldn't even persist.
                logger.error("Keychain write failed persisting X refresh for %s", uuid)
                raise
            except RuntimeError as exc:
                raise CredentialAuthError(
                    uuid,
                    f"X token could not be refreshed ({exc}). "
                    "Reconnect X in Settings.",
                ) from exc
            if not new_bearer:
                return False  # no refresh token in the bundle — nothing to do
            await clear_needs_reauth(uuid)
            return True

    async def _post_prepared(
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
                except (KeychainWriteError, SecretsIndexError):
                    # Local storage failure, not a provider verdict — the post
                    # fails loudly via the generic handler without the false
                    # Reconnect nag. TokenEndpointUnavailable escapes here
                    # naturally for the same reason (not a RuntimeError).
                    logger.error(
                        "Keychain write failed persisting X refresh for %s", uuid
                    )
                    raise
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
                response = await asyncio.to_thread(_tweet, bearer, media_ids)
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
                    response = await asyncio.to_thread(_tweet, new_bearer, media_ids)
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
            raise RuntimeError(f"Twitter post failed: {_exception_detail(e)}") from e


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
    # Declares what refresh_if_stale below already does, so the two can't
    # drift apart silently.
    supports_token_refresh = True
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

    async def _post_prepared(
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
        except (KeychainWriteError, SecretsIndexError):
            logger.error(
                "Keychain write failed persisting Bluesky refresh for %s",
                creds.get("uuid"),
            )
            raise
        except RuntimeError as exc:
            # refresh_tokens raises RuntimeError only on an AS *verdict*
            # (invalid_grant, expired_token, …), which means the user has to
            # re-OAuth. AS outages raise AuthServerUnavailable, which is not
            # a RuntimeError and so fails the post without the reauth flag.
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
            # Without aspectRatio, clients guess the shape and lay the post
            # out wrong — a 9:16 clip renders letterboxed into a 16:9 box.
            # The lexicon rejects any dimension below 1, so a probe that
            # comes back without usable numbers is simply omitted.
            probe = await asyncio.to_thread(
                media_service.probe_video_file, str(first_path)
            )
            if probe and probe.display_width and probe.display_height:
                # Display, not coded: the ratio has to describe the frame
                # Bluesky will actually render, or a rotated clip lays out
                # sideways.
                embed["aspectRatio"] = {
                    "width": int(probe.display_width),
                    "height": int(probe.display_height),
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
                timeout=config.BLUESKY_POST_TIMEOUT_SECONDS,
            )

        resp = await _do()
        if resp.status_code in (400, 401):
            try:
                retried = await self._handle_dpop_or_token_error(
                    resp, creds, bluesky_oauth, save_bundle,
                )
            except (KeychainWriteError, SecretsIndexError):
                logger.error(
                    "Keychain write failed persisting Bluesky refresh for %s",
                    creds.get("uuid"),
                )
                raise
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

    async def _persist_nonce_field(self, creds: dict, field: str, value: str) -> None:
        """Persist one DPoP nonce without clobbering a concurrent token refresh.

        ``save_bundle`` writes the whole dict, so an unlocked write of this
        coroutine's stale ``creds`` would overwrite access/refresh tokens that a
        concurrent locked refresh just rotated. Read-modify-write the freshest
        stored bundle under the same per-credential lock instead.
        """
        from yt_scheduler.services.social_credentials import (
            get_credential_lock,
            load_bundle,
            save_bundle,
        )

        creds[field] = value  # in-memory: this coroutine's next request uses it
        uuid = creds.get("uuid")
        if not uuid:
            return
        async with get_credential_lock(uuid):
            stored = await load_bundle("bluesky", uuid)
            if stored is None:
                return
            creds.update(stored)  # pull in any concurrent token refresh
            if stored.get(field) == value:
                return
            stored[field] = value
            creds[field] = value
            await save_bundle("bluesky", uuid, stored)

    async def _stash_pds_nonce(self, creds: dict, resp, save_bundle) -> None:
        new_nonce = resp.headers.get("DPoP-Nonce")
        if new_nonce and new_nonce != creds.get("dpop_nonce_pds"):
            await self._persist_nonce_field(creds, "dpop_nonce_pds", new_nonce)

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
                await self._persist_nonce_field(creds, "dpop_nonce_pds", nonce)
                return True
        if resp.status_code == 401 or err in ("invalid_token", "expired_token"):
            # Serialise against the background sweep and any sibling post via the
            # per-credential lock, double-checked on token identity: without it
            # both paths present the same single-use rotating refresh token and
            # the loser's rejection wrongly flags a healthy credential.
            await self._refresh_under_lock(
                creds, window_secs=0, stale_token=creds.get("access_token"),
            )
            return True
        return False

    # Pre-emptively refresh when the access token has under this many
    # seconds of lifetime left. Bluesky access tokens live ~2h, so a
    # 15-minute window means a single posting batch never needs a
    # mid-batch refresh, while a token revoked at the AS still gets
    # caught lazily on a 401 from the PDS.
    _PRE_REFRESH_WINDOW_SECS = 15 * 60

    async def _refresh_under_lock(
        self, creds: dict, *, window_secs: int, stale_token: str | None = None,
    ) -> bool:
        """Refresh the access token, serialised per-credential via
        ``get_credential_lock``. Re-reads the stored bundle inside the lock so
        a concurrent refresh on another path (e.g. the background job vs a
        post) doesn't leave us presenting a now-consumed refresh token.
        Returns True if a refresh was performed, False if it was already
        fresh / not possible. RuntimeError from the AS propagates.

        ``stale_token`` marks the reactive (post-time 401) path. There the
        freshness gate is token *identity*, not the clock: a 401 proves the
        token is dead whatever ``expires_at`` claims, so we refresh unless a
        concurrent waiter already rotated the access token out from under us.
        """
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
            if stale_token is not None:
                current = creds.get("access_token")
                if current and current != stale_token:
                    logger.info(
                        "Bluesky access token already refreshed by another waiter; "
                        "reusing it instead of burning the refresh token."
                    )
                    return False
            else:
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
        from yt_scheduler.services.bluesky_oauth import AuthServerUnavailable

        try:
            return await self._refresh_under_lock(creds, window_secs=window_secs)
        except AuthServerUnavailable as exc:
            logger.warning(
                "Bluesky token refresh deferred for %s; will retry: %s",
                creds.get("uuid"), exc,
            )
            return False
        except (KeychainWriteError, SecretsIndexError):
            logger.error(
                "Keychain write failed persisting Bluesky refresh for %s",
                creds.get("uuid"),
            )
            raise
        except RuntimeError as exc:
            raise CredentialAuthError(
                creds.get("uuid"),
                f"Bluesky token could not be refreshed ({exc}). "
                "Reconnect Bluesky in Settings.",
            ) from exc

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
        from yt_scheduler.services.social_credentials import stamp_token_metadata

        creds["access_token"] = result["access_token"]
        if result.get("refresh_token"):
            creds["refresh_token"] = result["refresh_token"]
        expires_in = result.get("expires_in")
        stamp_token_metadata(
            creds,
            expires_in_seconds=int(expires_in) if expires_in else None,
        )
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
                timeout=config.BLUESKY_BLOB_UPLOAD_TIMEOUT_SECONDS,
            )

        resp = await _do()
        if resp.status_code in (400, 401):
            try:
                retried = await self._handle_dpop_or_token_error(
                    resp, creds, bluesky_oauth, save_bundle,
                )
            except (KeychainWriteError, SecretsIndexError):
                logger.error(
                    "Keychain write failed persisting Bluesky refresh for %s",
                    creds.get("uuid"),
                )
                raise
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

    # Instance limits change only when an admin reconfigures the server, so a
    # long TTL is fine; the point of caching is to keep an HTTP round trip off
    # every single send, not to track a moving value.
    #
    # Class-level on purpose, and mutated in place on purpose. A poster is
    # constructed per send, so an instance-level cache would never be read
    # twice; `self._cache[k] = v` loads the class dict and mutates it rather
    # than creating an instance attribute (only rebinding would shadow it).
    # Keyed by instance, never by account: the limits belong to the server, so
    # two accounts on one instance correctly share an entry and nothing
    # account-scoped is stored. Growth needs no bound — the failure path
    # returns before the write, so only a server that answered with usable
    # numbers gets an entry. Tests clear it via an autouse conftest fixture.
    _INSTANCE_LIMITS_TTL_SECONDS = 6 * 60 * 60
    _instance_limits_cache: dict[str, tuple[float, media_service.PlatformMediaLimits]] = {}

    async def media_limits(self) -> media_service.PlatformMediaLimits:
        """Read this instance's real caps from ``/api/v2/instance``.

        Mastodon's limits are per-instance and vary enormously —
        mastodon.social allows a 99 MiB / 8.29 Mpx video where the built-in
        defaults are 40 MB / 2.3 Mpx. Hardcoding the defaults would reject
        files the instance would happily accept, so the instance is the only
        authority worth asking.

        A fetch failure falls back to Mastodon's built-in defaults, which are
        the *most restrictive* case — so the fallback can only cause a
        needless re-encode, never an upload the instance rejects.
        """
        creds = await self._get_creds()
        base = (creds.get("instance_url") or "").rstrip("/")
        if not base:
            return PLATFORM_MEDIA_LIMITS["mastodon"]

        cached = self._instance_limits_cache.get(base)
        if cached and (time.time() - cached[0]) < self._INSTANCE_LIMITS_TTL_SECONDS:
            return cached[1]

        # Direct from-import: this method's local ``config`` (the instance's
        # configuration JSON below) shadows the module-level config import.
        from yt_scheduler.config import MASTODON_INSTANCE_PROBE_TIMEOUT_SECONDS

        try:
            async with httpx.AsyncClient(
                timeout=MASTODON_INSTANCE_PROBE_TIMEOUT_SECONDS
            ) as client:
                response = await client.get(f"{base}/api/v2/instance")
                response.raise_for_status()
                config = (response.json().get("configuration") or {})
                attachments = config.get("media_attachments") or {}
            defaults = PLATFORM_MEDIA_LIMITS["mastodon"]
            reported = {
                "max_bytes": attachments.get("video_size_limit"),
                "max_pixels": attachments.get("video_matrix_limit"),
                "max_frame_rate": attachments.get("video_frame_rate_limit"),
            }
            # A 200 carrying none of these is not an answer — most likely
            # something other than Mastodon at this URL. Treated as a failed
            # read so it takes the strict-defaults path and is not cached.
            if not any(reported.values()):
                raise ValueError(
                    "no video limits in configuration.media_attachments"
                )
            # A field this server didn't report stays at Mastodon's built-in
            # default rather than becoming None, which PlatformMediaLimits
            # reads as "no limit" — that would skip a re-encode the server
            # requires, and pin that wrong answer for the TTL.
            limits = media_service.PlatformMediaLimits(
                max_bytes=reported["max_bytes"] or defaults.max_bytes,
                max_pixels=reported["max_pixels"] or defaults.max_pixels,
                max_frame_rate=reported["max_frame_rate"] or defaults.max_frame_rate,
            )
        except Exception as exc:
            logger.warning(
                "Could not read media limits from %s (%s); using Mastodon's "
                "built-in defaults, which are stricter", base, exc,
            )
            return PLATFORM_MEDIA_LIMITS["mastodon"]

        self._instance_limits_cache[base] = (time.time(), limits)
        return limits

    async def _post_prepared(
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
            raise RuntimeError(f"Mastodon post failed: {_exception_detail(e)}") from e


class LinkedInPoster(SocialPoster):
    platform = "linkedin"
    required_keys = ["access_token", "person_urn"]

    async def _post_prepared(
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
                async with httpx.AsyncClient(
                    timeout=config.LINKEDIN_MEDIA_UPLOAD_TIMEOUT_SECONDS
                ) as client:
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
            async with httpx.AsyncClient(
                timeout=config.LINKEDIN_POST_TIMEOUT_SECONDS
            ) as client:
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
            raise RuntimeError(f"LinkedIn post failed: {_exception_detail(e)}") from e

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


class ThreadsPublishOutcomeUnknown(RuntimeError):
    """The publish request was sent but its outcome could not be settled.

    The message must reach the UI verbatim: a blind retry risks a double
    post, so it tells the user to check their profile first. Passed through
    the poster's catch-all unwrapped for exactly that reason.
    """


# Consecutive sweeps on which a credential's refresh 5xx'd AND live
# verification couldn't answer, keyed by credential uuid. Consulted only for
# bundles with no recorded expiry (pre-stamping) — with both endpoints
# unreadable and no date to consult, this bound is what stops the dead-token
# forever-loop. Process memory by design: a restart re-arms the bound, and the
# guarded population only shrinks as bundles get stamped.
_threads_refresh_dual_failures: dict[str, int] = {}
# ≈3 hours of consecutive 20-minute sweeps — longer than any observed Meta
# blip, far shorter than the weeks the old optimistic reading allowed.
_THREADS_DUAL_FAILURE_FLAG_THRESHOLD = 9


# Meta error codes that mean "try again later", not "this credential is dead".
# 4 = application request limit, 17 = user request limit, 32 = page rate limit,
# 341 = temporarily blocked for policies, 613 = calls-per-second limit.
_META_TRANSIENT_ERROR_CODES: frozenset[int] = frozenset({4, 17, 32, 341, 613})


def _is_transient_meta_error(status_code: int, payload: dict | None) -> bool:
    """True when Meta is throttling rather than rejecting the credential.

    Rate limits come back as ordinary 4xx with a code in the body, so status
    alone can't tell them apart from a genuinely dead token. 429 is included
    for the same reason Twitter's refresh handles it.
    """
    if status_code == 429:
        return True
    error = (payload or {}).get("error") or {}
    try:
        code = int(error.get("code"))
    except (TypeError, ValueError):
        return False
    return code in _META_TRANSIENT_ERROR_CODES


def _threads_token_is_too_new(bundle: dict) -> bool:
    """True when the token is inside Meta's 24-hour refresh minimum.

    The bundle already stamps ``acquired_at``; asking it is sturdier than
    matching the words Meta happens to use in the error.
    """
    acquired = bundle.get("acquired_at")
    if not acquired:
        return False
    try:
        age = time.time() - float(acquired)
    except (TypeError, ValueError):
        return False
    return 0 <= age < 24 * 60 * 60


class ThreadsPoster(SocialPoster):
    # Threads fetches media from a URL rather than accepting an upload, for
    # images and video alike. media_hosting puts the file in a private R2
    # bucket and hands Meta a short-lived signed URL.
    accepts_media = True
    requires_hosted_media = True

    platform = "threads"
    required_keys = ["access_token", "user_id"]

    # Threads has no separate refresh token: it renews using the access token
    # itself via the th_refresh_token grant. That is why this didn't follow the
    # Twitter/Bluesky shape and ended up unimplemented.
    supports_token_refresh = True

    # A lapsed 60-day token is unrecoverable (manual re-OAuth only), so
    # refresh with a week of margin rather than the default 45 minutes — a
    # sleeping laptop during any one sweep must be a non-event. Meta renews
    # any token at least 24 hours old; the reactive "too early" branch in
    # refresh_if_stale handles the younger-than-that case.
    token_refresh_window_secs = config.THREADS_TOKEN_REFRESH_WINDOW_SECONDS

    _TOKEN_TTL_FALLBACK_SECONDS = 60 * 24 * 3600

    # Threads' image ceiling. PLATFORM_MEDIA_LIMITS covers video only (and
    # prepared_media probes only video), so an oversized image would otherwise
    # upload fine and be rejected by Meta with no useful explanation.
    MAX_IMAGE_BYTES = 8 * 1024 * 1024

    async def _post_prepared(
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

        # Threads takes one media object per post; more than one needs a
        # CAROUSEL container, which we don't build. Dropping the extras
        # silently would publish something the user didn't compose.
        if len(paths) > 1:
            raise MediaUploadError(
                f"Threads takes one attachment per post, but this one has "
                f"{len(paths)}. Carousels aren't supported yet — remove the "
                "extras and retry. Nothing was posted."
            )

        container_params = await self._media_container_params(paths)

        try:
            import httpx

            access_token = creds["access_token"]
            user_id = creds["user_id"]

            async with httpx.AsyncClient(
                timeout=config.THREADS_POST_TIMEOUT_SECONDS
            ) as client:
                create_resp = await client.post(
                    f"https://graph.threads.net/v1.0/{user_id}/threads",
                    params={
                        **container_params,
                        "text": text,
                        "access_token": access_token,
                    },
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
                    attempts=(
                        self._TEXT_CONTAINER_POLL_ATTEMPTS
                        if container_params["media_type"] == "TEXT"
                        else self._MEDIA_CONTAINER_POLL_ATTEMPTS
                    ),
                )

                from datetime import datetime, timezone

                publish_started_at = datetime.now(timezone.utc)
                try:
                    publish_resp = await client.post(
                        f"https://graph.threads.net/v1.0/{user_id}/threads_publish",
                        params={"creation_id": container_id, "access_token": access_token},
                    )
                except self._PUBLISH_AMBIGUOUS_TRANSPORT_ERRORS as exc:
                    # The request may have reached Meta; the response is what
                    # was lost. Retrying blind can double-post — resolve the
                    # actual outcome from the container's status instead.
                    return await self._resolve_ambiguous_publish(
                        client, container_id, user_id, access_token,
                        cred_uuid=creds.get("uuid"), text=text,
                        publish_started_at=publish_started_at, cause=exc,
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
                try:
                    post_id = publish_resp.json()["id"]
                except (KeyError, TypeError, ValueError) as exc:
                    # Meta said 200, so the publish almost certainly landed —
                    # a bare KeyError here would invite the same blind retry
                    # as a lost response. Same ambiguity, same resolver.
                    return await self._resolve_ambiguous_publish(
                        client, container_id, user_id, access_token,
                        cred_uuid=creds.get("uuid"), text=text,
                        publish_started_at=publish_started_at, cause=exc,
                    )

                username = creds.get("username", "")
                return {"url": f"https://threads.net/@{username}/post/{post_id}", "id": post_id}
        except (CredentialAuthError, MediaUploadError, ThreadsPublishOutcomeUnknown):
            raise
        except Exception as e:
            raise RuntimeError(f"Threads post failed: {_exception_detail(e)}") from e

    async def refresh_if_stale(self, *, window_secs: int = 0) -> bool:
        """Renew the long-lived token before it lapses.

        Meta's ``th_refresh_token`` grant takes the current access token and
        returns a new 60-day one, so a token refreshed on schedule never needs
        another OAuth. Once it *has* expired there is no recovery here — that
        raises :class:`CredentialAuthError` so the credential is flagged
        ``needs_reauth`` instead of failing every post with an opaque 500,
        which is what happened before this existed.

        A refresh 5xx is ambiguous (Meta 500s both outages and dead tokens),
        so it is tie-broken by a live ``GET /me`` check plus a recorded-expiry
        backstop rather than read optimistically forever.
        """
        creds = await self._get_creds()
        uuid = creds.get("uuid")
        if not (uuid and creds.get("access_token")):
            return False
        if not self._token_is_due(creds, window_secs):
            return False

        from yt_scheduler.services.social_credentials import (
            clear_needs_reauth,
            get_credential_lock,
            load_bundle,
            save_bundle,
            stamp_token_metadata,
        )

        async with get_credential_lock(uuid):
            # Re-read inside the lock: another sweep or a send-path refresh may
            # have renewed it while we waited, and reusing the stale token
            # would spend a second refresh for nothing.
            current = await load_bundle("threads", uuid) or creds
            if not self._token_is_due(current, window_secs):
                return False
            token = current.get("access_token")
            if not token:
                return False

            import httpx

            try:
                async with httpx.AsyncClient(
                    timeout=config.THREADS_TOKEN_REFRESH_TIMEOUT_SECONDS
                ) as client:
                    resp = await client.get(
                        "https://graph.threads.net/refresh_access_token",
                        params={
                            "grant_type": "th_refresh_token",
                            "access_token": token,
                        },
                    )
            except httpx.HTTPError as exc:
                # Network trouble is not a credential problem; the next sweep
                # retries rather than flagging a working token as dead.
                logger.warning("Threads token refresh could not reach Meta: %s", exc)
                return False

            if resp.status_code != 200:
                detail = _http_error_detail(resp)
                # Meta answers BOTH a real outage AND a refresh of an
                # already-dead token with a 5xx here (an expired token gets
                # the generic 500 code=1), so the status alone is not a
                # verdict either way. Reading it optimistically forever is
                # how a dead June token 500'd every 20 minutes for weeks
                # while Settings showed a healthy credential. GET /me is the
                # tiebreaker: it answers a live token 200 and a dead one 4xx.
                if resp.status_code >= 500:
                    from yt_scheduler.services.social_identity import verify_live

                    verdict = await verify_live(self.platform, current)
                    if verdict["ok"]:
                        _threads_refresh_dual_failures.pop(uuid, None)
                        logger.warning(
                            "Threads token refresh got %s from Meta but the "
                            "token still verifies; will retry: %s",
                            resp.status_code, detail,
                        )
                        return False
                    if verdict.get("unreachable"):
                        expires_at = int(current.get("expires_at") or 0)
                        if expires_at and expires_at <= int(time.time()):
                            # Both endpoints down AND the recorded expiry has
                            # passed: th_refresh_token cannot renew a token
                            # past expiry, so waiting out the outage cannot
                            # save this credential.
                            raise CredentialAuthError(
                                uuid,
                                f"Threads refresh failed ({detail}), live "
                                f"verification got no answer "
                                f"({verdict['detail']}), and the token is "
                                "past its recorded expiry. An expired token "
                                "can only be replaced — reconnect Threads in "
                                "Settings.",
                            )
                        if not expires_at:
                            # Pre-stamping bundle: no recorded expiry to
                            # consult, and Meta is 5xx-ing both endpoints —
                            # which is also its signature for a dead token.
                            # Retry a bounded number of sweeps, then flag; a
                            # false flag during a marathon outage self-heals
                            # at the next successful refresh.
                            failures = _threads_refresh_dual_failures.get(uuid, 0) + 1
                            _threads_refresh_dual_failures[uuid] = failures
                            if failures >= _THREADS_DUAL_FAILURE_FLAG_THRESHOLD:
                                raise CredentialAuthError(
                                    uuid,
                                    "Threads refresh has returned 5xx and "
                                    f"live verification has been unanswerable "
                                    f"for {failures} consecutive attempts, "
                                    "and this credential predates expiry "
                                    "tracking. Reconnect Threads in Settings.",
                                )
                        logger.warning(
                            "Threads token refresh got %s and live "
                            "verification couldn't reach Meta either; will "
                            "retry: %s / %s",
                            resp.status_code, detail, verdict["detail"],
                        )
                        return False
                    raise CredentialAuthError(
                        uuid,
                        f"Threads token could not be refreshed ({detail}) and "
                        f"live verification rejected it ({verdict['detail']}). "
                        "Reconnect Threads in Settings — an expired token can "
                        "only be replaced, not renewed.",
                    )
                # Transient, NOT a dead credential. Two shapes:
                #
                # 1. Too new — Meta refuses to refresh a token younger than
                #    24h. The bundle records acquired_at, so ask that rather
                #    than pattern-matching English that Meta can reword.
                # 2. Throttled — Meta rate limits arrive as ordinary 4xx, so
                #    without this a single throttled sweep flips a perfectly
                #    healthy token to needs_reauth and every send then
                #    fast-fails "reconnect".
                if _threads_token_is_too_new(current):
                    logger.info(
                        "Threads token is younger than Meta's 24h refresh "
                        "minimum; will retry later.",
                    )
                    return False
                if "24 hours" in detail or "too early" in detail.lower():
                    logger.info("Threads token too new to refresh yet: %s", detail)
                    return False
                try:
                    error_payload = resp.json() or {}
                except ValueError:
                    error_payload = {}
                if _is_transient_meta_error(resp.status_code, error_payload):
                    logger.warning(
                        "Threads token refresh throttled or transiently "
                        "rejected (%s): %s — will retry.",
                        resp.status_code, detail,
                    )
                    return False
                raise CredentialAuthError(
                    uuid,
                    f"Threads token could not be refreshed ({detail}). Reconnect "
                    "Threads in Settings — an expired token can only be replaced, "
                    "not renewed.",
                )

            data = resp.json()
            new_token = data.get("access_token")
            if not new_token:
                raise CredentialAuthError(
                    uuid,
                    "Threads refresh returned no access_token. Reconnect Threads "
                    "in Settings.",
                )
            expires_in = int(data.get("expires_in") or self._TOKEN_TTL_FALLBACK_SECONDS)
            updated = dict(current)
            updated["access_token"] = new_token
            stamp_token_metadata(updated, expires_in_seconds=expires_in)
            await save_bundle("threads", uuid, updated)
            await clear_needs_reauth(uuid)
            _threads_refresh_dual_failures.pop(uuid, None)
            logger.info("Threads token refreshed; valid for %.0f more days",
                        expires_in / 86400)
            return True

    async def _resolve_ambiguous_publish(
        self, client, container_id: str, user_id: str, access_token: str,
        *, cred_uuid: str | None, text: str, publish_started_at, cause: Exception,
    ) -> dict:
        """Settle a publish whose response was lost, using read-only status
        checks — never a second publish, which is what mints duplicates.
        """
        last_status: str | None = None
        last_read_succeeded = False
        consecutive_failures = 0
        checks_failed = 0
        for attempt in range(self._PUBLISH_RESOLVE_ATTEMPTS):
            if attempt:
                await asyncio.sleep(self._PUBLISH_RESOLVE_DELAY_SECONDS)
            try:
                resp = await client.get(
                    f"https://graph.threads.net/v1.0/{container_id}",
                    params={"fields": "status,error_message",
                            "access_token": access_token},
                    timeout=config.THREADS_PUBLISH_RESOLVE_TIMEOUT_SECONDS,
                )
            except httpx.HTTPError:
                checks_failed += 1
                consecutive_failures += 1
                last_read_succeeded = False
                if consecutive_failures >= self._PUBLISH_RESOLVE_MAX_CONSECUTIVE_CHECK_FAILURES:
                    break
                continue
            consecutive_failures = 0
            if resp.status_code == 401:
                raise CredentialAuthError(
                    cred_uuid,
                    "Threads publish outcome unknown — check your Threads "
                    "profile for this post before resending. The follow-up "
                    "status check was rejected (401): re-OAuth Threads in "
                    f"Settings. Container {container_id}.",
                )
            if resp.status_code >= 400:
                # A 4xx on the status read is "can't verify", never proof of
                # "not published".
                checks_failed += 1
                last_read_succeeded = False
                continue
            data = resp.json()
            if not isinstance(data, dict) or data.get("error"):
                checks_failed += 1
                last_read_succeeded = False
                continue
            last_read_succeeded = True
            last_status = data.get("status")
            if last_status == "PUBLISHED":
                logger.warning(
                    "Threads publish response was lost (%s) but container %s "
                    "reports PUBLISHED; recovering the post identity.",
                    _exception_detail(cause), container_id,
                )
                return await self._published_result_after_lost_response(
                    client, container_id, user_id, access_token,
                    text=text, publish_started_at=publish_started_at,
                )
            if last_status in ("ERROR", "EXPIRED"):
                raise RuntimeError(
                    f"Threads publish failed: after the response was lost, "
                    f"container {container_id} reported {last_status} "
                    f"({data.get('error_message') or 'no detail from Threads'}). "
                    "Nothing was published — Send again to retry."
                )
            # FINISHED / IN_PROGRESS: not published *yet* — Meta may still be
            # completing the publish we never heard back about; keep watching.
        if last_status == "FINISHED" and last_read_succeeded:
            # Still unpublished after the whole window — but a finite poll
            # cannot prove Meta won't complete it later, so this stays an
            # honest "probably", never a confident "retry is safe".
            raise ThreadsPublishOutcomeUnknown(
                "Check your Threads profile for this post before resending: "
                f"the publish response was lost ({_exception_detail(cause)}) "
                f"and container {container_id} still read FINISHED "
                f"(unpublished) after {self._PUBLISH_RESOLVE_ATTEMPTS} checks "
                "— probably safe to Send again, but verify first."
            )
        raise ThreadsPublishOutcomeUnknown(
            "Check your Threads profile for this post before resending — "
            "retrying may publish a duplicate. The publish request was sent "
            f"but the response was lost ({_exception_detail(cause)}), and "
            f"follow-up status checks could not settle it (last "
            f"status={last_status or 'unavailable'}, {checks_failed} check(s) "
            f"failed). Container {container_id}."
        )

    async def _published_result_after_lost_response(
        self, client, container_id: str, user_id: str, access_token: str,
        *, text: str, publish_started_at,
    ) -> dict:
        """Recover the published post's permalink after a lost response.

        The post is provably live (container status PUBLISHED), so failures
        here must degrade to posted-with-warning — failing the send would
        reopen the exact duplicate trap this resolver closes.
        """
        from datetime import datetime, timedelta

        recovered_warning = (
            "Threads confirmed this post after a lost publish response."
        )
        try:
            resp = await client.get(
                f"https://graph.threads.net/v1.0/{container_id}",
                params={"fields": "id,permalink", "access_token": access_token},
                timeout=config.THREADS_PUBLISH_RESOLVE_TIMEOUT_SECONDS,
            )
            if resp.status_code == 200:
                data = resp.json()
                permalink = data.get("permalink") if isinstance(data, dict) else None
                if permalink:
                    return {"url": permalink, "id": data.get("id"),
                            "warning": recovered_warning}
        except httpx.HTTPError:
            pass

        # Fallback: find the post in the account's recent threads, fenced by
        # text AND timestamp so an older identical post can never be matched.
        try:
            resp = await client.get(
                f"https://graph.threads.net/v1.0/{user_id}/threads",
                params={"fields": "id,permalink,text,timestamp", "limit": "10",
                        "access_token": access_token},
                timeout=config.THREADS_PUBLISH_RESOLVE_TIMEOUT_SECONDS,
            )
            if resp.status_code == 200:
                entries = (resp.json() or {}).get("data") or []
                fence = publish_started_at - timedelta(
                    seconds=self._PUBLISH_CLOCK_SKEW_ALLOWANCE_SECONDS
                )
                matches = []
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    # Graph omits `text` on empty posts; absent means empty
                    # here, not unknown.
                    if (entry.get("text") or "").strip() != (text or "").strip():
                        continue
                    try:
                        stamp = datetime.fromisoformat(entry.get("timestamp") or "")
                    except (TypeError, ValueError):
                        continue  # unverifiable, never "a match"
                    if stamp.tzinfo is None:
                        continue
                    if stamp >= fence:
                        matches.append(entry)
                # Unique match only: with two candidates, picking either
                # risks attaching the wrong permalink. Liveness is already
                # proven, so degrading is safe and honest.
                if len(matches) == 1 and matches[0].get("permalink"):
                    return {"url": matches[0]["permalink"],
                            "id": matches[0].get("id"),
                            "warning": recovered_warning}
        except httpx.HTTPError:
            pass

        logger.warning(
            "Threads container %s is PUBLISHED but the permalink could not "
            "be recovered.", container_id,
        )
        return {
            "url": "", "id": None,
            "warning": "Published to Threads, but the permalink could not be "
                       "recovered — open Threads to find the post.",
        }

    def _token_is_due(self, creds: dict, window_secs: int) -> bool:
        """Whether this token is close enough to expiry to be worth renewing.

        An absent ``expires_at`` means the bundle predates expiry tracking, so
        it is treated as due: refreshing once backfills the field and every
        later sweep can reason properly. Not knowing is not the same as being
        fine — that assumption is what let this token die.
        """
        expires_at = int(creds.get("expires_at") or 0)
        if not expires_at:
            return True
        return expires_at - window_secs <= int(time.time())

    async def _media_container_params(self, paths: list[str]) -> dict[str, str]:
        """Build the container's media fields, hosting the attachment if any.

        Returns the ``TEXT`` container unchanged when there is no attachment,
        so a text-only Threads post takes exactly the path it always has.
        """
        from yt_scheduler.services import media_hosting

        if not paths:
            return {"media_type": "TEXT"}

        path = Path(paths[0])
        mime = mimetypes.guess_type(path.name)[0] or ""
        if mime.startswith("video/"):
            media_type, url_field = "VIDEO", "video_url"
        elif mime.startswith("image/"):
            media_type, url_field = "IMAGE", "image_url"
            size = path.stat().st_size
            if size > self.MAX_IMAGE_BYTES:
                raise MediaUploadError(
                    f"Can't post to threads: {path.name} is "
                    f"{size / 1e6:.1f} MB, over Threads' "
                    f"{self.MAX_IMAGE_BYTES / 1e6:.0f} MB image limit. "
                    "Nothing was posted."
                )
        else:
            raise MediaUploadError(
                f"Can't post to threads: {path.name} is neither image nor video "
                f"(detected {mime or 'unknown type'}). Nothing was posted."
            )

        try:
            hosted = await media_hosting.host_file(path)
        except media_hosting.MediaHostingNotConfigured as exc:
            # Never quietly degrade to a text-only post: the user composed an
            # attachment and would have no way to tell it had been dropped.
            raise MediaUploadError(
                f"Can't post media to Threads — {exc} Threads fetches media "
                "from a URL rather than accepting an upload, so hosting is "
                "required. Nothing was posted."
            ) from exc
        except media_hosting.MediaHostingError as exc:
            raise MediaUploadError(
                f"Can't post to threads: hosting {path.name} failed — {exc} "
                "Nothing was posted."
            ) from exc

        return {"media_type": media_type, url_field: hosted.url}

    # Transport failures on the publish call where the request may have
    # REACHED Meta even though no response came back — retrying blind mints a
    # duplicate post. Connect-phase errors are deliberately absent: no
    # connection means the request never arrived, so plain failure + retry is
    # safe. (ConnectTimeout subclasses TimeoutException, so a blanket
    # TimeoutException catch here would be wrong.)
    _PUBLISH_AMBIGUOUS_TRANSPORT_ERRORS = (
        httpx.ReadTimeout,          # request sent in full; response never arrived
        httpx.ReadError,
        httpx.WriteTimeout,         # unknowable how much of the request was written
        httpx.WriteError,
        httpx.RemoteProtocolError,  # server dropped the connection without replying
    )
    # ~2 minutes of read-only status checks: the ambiguous timeout itself
    # proves Meta's publish commit can trail the request by >2 minutes of
    # client patience, so a short window would read FINISHED and wrongly
    # bless a duplicate-minting retry.
    _PUBLISH_RESOLVE_ATTEMPTS = 24
    _PUBLISH_RESOLVE_DELAY_SECONDS = 5.0
    # Stop early when the network is clearly down — further checks are noise.
    _PUBLISH_RESOLVE_MAX_CONSECUTIVE_CHECK_FAILURES = 4
    # Meta's clock vs ours, when fencing the permalink fallback by timestamp.
    _PUBLISH_CLOCK_SKEW_ALLOWANCE_SECONDS = 120

    _TEXT_CONTAINER_POLL_ATTEMPTS = 10

    # Meta: "wait on average 30 seconds before publishing a Threads media
    # container to give our server enough time to fully process the upload."
    # Meta downloads the file from our signed URL during this window, so the
    # budget has to cover their fetch as well as their transcode.
    _MEDIA_CONTAINER_POLL_ATTEMPTS = 150

    _CONTAINER_POLL_DELAY_SECONDS = 1.0

    async def _await_container_finished(
        self,
        client: httpx.AsyncClient,
        container_id: str,
        access_token: str,
        cred_uuid: str | None,
        *,
        attempts: int | None = None,
    ) -> None:
        """Poll a Threads media container until its status is ``FINISHED``.

        Raises if the container reports ``ERROR``/``EXPIRED``, if the status
        check itself fails, or if it never reaches ``FINISHED`` within the
        bounded attempt budget. Text containers are usually ready on the first
        poll; the loop exists to absorb the brief server-side processing gap
        that otherwise makes ``threads_publish`` return a 400.
        """
        budget = attempts if attempts is not None else self._TEXT_CONTAINER_POLL_ATTEMPTS
        last_status = "UNKNOWN"
        for attempt in range(budget):
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
            if attempt < budget - 1:
                await asyncio.sleep(self._CONTAINER_POLL_DELAY_SECONDS)
        raise RuntimeError(
            f"Threads container not ready to publish (last status={last_status}) "
            f"after {budget} checks."
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


def platform_accepts_attached_media(platform: str) -> bool:
    """Whether this platform's API can take a media upload from us at all.

    A permanent property of the platform, not of any one file: no re-encode
    makes an attachment postable to a platform that only fetches media from a
    public URL. Lets callers decide up front instead of failing at send time.
    """
    poster_class = _POSTERS.get(platform)
    if poster_class is None:
        raise ValueError(f"Unknown platform: {platform}. Available: {ALL_PLATFORMS}")
    return poster_class.accepts_media


def platform_requires_hosted_media(platform: str) -> bool:
    """Whether attaching media on this platform depends on configured hosting.

    True for platforms that fetch from a URL rather than accepting an upload.
    Lets callers report "hosting isn't set up" up front instead of discovering
    it at send time.
    """
    poster_class = _POSTERS.get(platform)
    if poster_class is None:
        raise ValueError(f"Unknown platform: {platform}. Available: {ALL_PLATFORMS}")
    return poster_class.requires_hosted_media

# Per-platform video envelopes. Same shape of idea as
# DEFAULT_MAX_CHARS_BY_PLATFORM in services/templates.py, which already
# encodes a per-platform text limit — this is the media equivalent.
#
# Numbers are the published caps as of 2026-07. Where a platform does not
# publish one (Bluesky's resolution), the field stays None rather than being
# guessed. Mastodon's entry is a floor only: its real caps are per-instance
# and MastodonPoster.media_limits() reads them from the instance.
PLATFORM_MEDIA_LIMITS: dict[str, media_service.PlatformMediaLimits] = {
    # Bluesky publishes no numbers in its docs; these are the constants the
    # first-party client enforces (VIDEO_MAX_SIZE_MB / VIDEO_MAX_DURATION_MS).
    "bluesky": media_service.PlatformMediaLimits(
        max_bytes=300 * 1000 * 1000,
        max_duration_seconds=180,
        video_codecs=("h264", "hevc", "vp8", "vp9"),
    ),
    # 140s and 1920x1200 apply to standard accounts. Premium raises both, but
    # assuming the higher tier would produce uploads that fail for most users.
    "twitter": media_service.PlatformMediaLimits(
        max_bytes=512 * 1000 * 1000,
        max_duration_seconds=140,
        max_pixels=1920 * 1200,
        video_codecs=("h264",),
        audio_codecs=("aac",),
    ),
    # Overridden per-instance; see MastodonPoster.media_limits(). These are
    # Mastodon's own built-in defaults, used only if the instance can't be
    # reached — they are the most restrictive case, so a file that passes
    # them passes anywhere.
    "mastodon": media_service.PlatformMediaLimits(
        max_bytes=40 * 1024 * 1024,
        max_pixels=2_304_000,
        max_frame_rate=60,
    ),
    "linkedin": media_service.PlatformMediaLimits(
        max_bytes=5 * 1000 * 1000 * 1000,
        max_duration_seconds=15 * 60,
        max_pixels=4096 * 2304,
        video_codecs=("h264",),
    ),
    "threads": media_service.PlatformMediaLimits(
        max_bytes=1000 * 1000 * 1000,
        max_duration_seconds=300,
        max_long_edge=1920,
        max_frame_rate=60,
        video_codecs=("h264", "hevc"),
    ),
}

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
    media_paths: list[str] | None = None,
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
    * identical media — same text with different attached media is NOT a
      duplicate. Pass ``media_paths`` (the full list actually being sent);
      ``media_path`` remains accepted as the single-attachment shorthand.
      No media is a single bucket, so switching from no-media to media (or
      vice versa) is also not a duplicate.
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

    # Compare the FULL attachment list, not just the legacy first path: the send
    # path posts every entry of ``media_paths``, so two posts differing only in
    # their 2nd-4th attachments are not duplicates. Both sides run through
    # decode_media_paths, which normalises the legacy single-string column and the
    # JSON array to the same shape — a string compare in SQL would be fragile,
    # since migration 010's json_array() emits ["a","b"] while json.dumps emits
    # ["a", "b"].
    if media_paths is not None:
        expected_media = [str(p).strip() for p in media_paths if p]
    else:
        expected_media = decode_media_paths({"media_path": media_path})

    db = await get_db()
    sql_parts = [
        "SELECT id, video_id, platform, content, media_path, media_paths, "
        "       social_account_id, posted_at, post_url, status, "
        "       COALESCE(posted_at, created_at) AS event_at "
        "FROM social_posts "
        "WHERE platform = ? AND TRIM(content) = ? "
        "AND status IN ('posted', 'sending') "
        "AND COALESCE(posted_at, created_at) >= datetime('now', '-' || ? || ' days')"
    ]
    params: list = [platform, normalised, lookback_days]
    if social_account_id is not None:
        sql_parts.append(
            "AND (social_account_id = ? OR social_account_id IS NULL)"
        )
        params.append(int(social_account_id))
    if exclude_post_id is not None:
        sql_parts.append("AND id != ?")
        params.append(int(exclude_post_id))
    # Media is compared in Python below, so this can't be LIMIT 1 any more — and
    # deliberately no LIMIT at all: a cap that hid an older same-media post would
    # silently let the duplicate through and publish it twice, the exact failure
    # this guard exists to prevent. The query is already scoped to one platform,
    # one account, one exact content string and the lookback window, so the
    # candidate set is tiny.
    sql_parts.append("ORDER BY event_at DESC")

    cursor = await db.execute(" ".join(sql_parts), tuple(params))
    rows = await cursor.fetchall()
    for row in rows:
        candidate = dict(row)
        candidate_media = [p.strip() for p in decode_media_paths(candidate)]
        if candidate_media == expected_media:
            return candidate
    return None


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
