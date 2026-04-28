"""Social media posting — multi-platform with Keychain credential storage."""

from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
import time
from pathlib import Path

import httpx

from yt_scheduler.services.keychain import (
    load_all_secrets,
    store_secret,
)

logger = logging.getLogger(__name__)


# --- Twitter / X — OAuth 2.0 refresh ----------------------------------------


_TWITTER_TOKEN_URL = "https://api.twitter.com/2/oauth2/token"


async def _twitter_refresh_bearer(creds: dict[str, str]) -> str | None:
    """Mint a fresh access_token using the stored refresh_token, persist it
    back into Keychain, and return the new bearer. Returns None if refresh
    isn't possible (no refresh_token or client_id, or the API rejects it)."""
    refresh = creds.get("refresh_token")
    client_id = creds.get("client_id")
    if not refresh or not client_id:
        return None

    body = {
        "grant_type": "refresh_token",
        "refresh_token": refresh,
        "client_id": client_id,
    }
    auth = None
    if creds.get("client_secret"):
        auth = (client_id, creds["client_secret"])
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(_TWITTER_TOKEN_URL, data=body, auth=auth)
    except Exception as exc:
        logger.warning("Twitter token refresh network error: %s", exc)
        return None
    if resp.status_code != 200:
        logger.warning(
            "Twitter token refresh rejected (%s): %s", resp.status_code, resp.text
        )
        return None
    payload = resp.json() or {}
    new_bearer = payload.get("access_token")
    if not new_bearer:
        return None
    new_refresh = payload.get("refresh_token")

    creds["bearer_token"] = new_bearer
    if new_refresh:
        creds["refresh_token"] = new_refresh

    cred_uuid = creds.get("uuid")
    if cred_uuid:
        store_secret("twitter", f"cred.{cred_uuid}", json.dumps(creds))
    else:
        store_secret("twitter", "bearer_token", new_bearer)
        if new_refresh:
            store_secret("twitter", "refresh_token", new_refresh)
    return new_bearer


# --- Twitter / X v2 media upload (OAuth 2.0 user context) ------------------

# https://docs.x.com/x-api/media/upload-media — simple upload for small
# images, chunked (INIT / APPEND / FINALIZE / STATUS) for video and large
# files. Single-file size limits per X docs:
#   images: 5 MB     — simple upload
#   GIFs:   15 MB    — simple upload
#   video:  512 MB   — chunked upload required
# We pick the path based on content type + size and fall back to text-only
# with a logged warning if anything fails.

_TWITTER_V2_MEDIA = "https://api.x.com/2/media/upload"
_TWITTER_SIMPLE_LIMIT = 5 * 1024 * 1024  # 5 MB; images only
_TWITTER_VIDEO_CHUNK = 4 * 1024 * 1024


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
    """Chunked INIT / APPEND / FINALIZE / STATUS for video and large files."""
    headers = {"Authorization": f"Bearer {bearer_token}"}
    total_bytes = media_path.stat().st_size

    async with httpx.AsyncClient(timeout=120) as client:
        # INIT
        init = await client.post(
            _TWITTER_V2_MEDIA,
            headers=headers,
            data={
                "command": "INIT",
                "total_bytes": str(total_bytes),
                "media_type": mime,
                "media_category": _twitter_media_category(mime),
            },
        )
        if init.status_code != 200:
            raise RuntimeError(f"v2 media INIT failed ({init.status_code}): {init.text}")
        body = init.json() or {}
        media_id = (body.get("data") or {}).get("id") or body.get("media_id_string")
        if not media_id:
            raise RuntimeError(f"v2 media INIT response missing id: {body}")
        media_id = str(media_id)

        # APPEND
        with media_path.open("rb") as f:
            segment = 0
            while True:
                chunk = f.read(_TWITTER_VIDEO_CHUNK)
                if not chunk:
                    break
                files = {"media": (media_path.name, chunk, mime)}
                append = await client.post(
                    _TWITTER_V2_MEDIA,
                    headers=headers,
                    data={
                        "command": "APPEND",
                        "media_id": media_id,
                        "segment_index": str(segment),
                    },
                    files=files,
                )
                if append.status_code not in (200, 204):
                    raise RuntimeError(
                        f"v2 media APPEND segment {segment} failed "
                        f"({append.status_code}): {append.text}"
                    )
                segment += 1

        # FINALIZE
        finalize = await client.post(
            _TWITTER_V2_MEDIA,
            headers=headers,
            data={"command": "FINALIZE", "media_id": media_id},
        )
        if finalize.status_code != 200:
            raise RuntimeError(f"v2 media FINALIZE failed ({finalize.status_code}): {finalize.text}")

        # STATUS — wait for async transcoding (videos only)
        info = (finalize.json() or {}).get("data") or finalize.json() or {}
        processing = info.get("processing_info")
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
            if status.status_code != 200:
                raise RuntimeError(f"v2 media STATUS failed ({status.status_code}): {status.text}")
            processing = ((status.json() or {}).get("data") or {}).get("processing_info") \
                or (status.json() or {}).get("processing_info")

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

    def _get_creds(self) -> dict[str, str]:
        """Return the bundle this poster is bound to, falling back to the
        first active credential bundle for the platform when none is set.
        """
        if self._bundle is not None:
            return self._bundle
        secrets = load_all_secrets(self.platform)
        for key, value in secrets.items():
            if key.startswith("cred."):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    continue
        return {k: v for k, v in secrets.items() if k != "_migrated_v8"}

    @classmethod
    def bundle_is_configured(cls, bundle: dict) -> bool:
        """Check whether the given bundle has all keys this poster needs."""
        return all(bundle.get(k) for k in cls.required_keys)

    async def post(self, text: str, media_path: str | None = None) -> dict:
        """Post content. Returns {"url": "...", "id": "..."} on success."""
        raise NotImplementedError

    async def is_configured(self) -> bool:
        """Check if this poster's credentials are complete."""
        creds = self._get_creds()
        return all(creds.get(k) for k in self.required_keys)


class TwitterPoster(SocialPoster):
    platform = "twitter"
    # OAuth 2.0 user-context with media.write scope is the only supported
    # path. Tweets go through v2 ``POST /2/tweets``; image/GIF/video uploads
    # go through v2 ``POST /2/media/upload``.
    required_keys: list[str] = ["bearer_token"]

    async def post(self, text: str, media_path: str | None = None) -> dict:
        text = (text or "").strip()
        creds = self._get_creds()
        bearer = creds.get("bearer_token")
        if not bearer:
            raise CredentialAuthError(
                creds.get("uuid"),
                "X is not configured. Click 'Connect with X (OAuth 2.0)' in Settings.",
            )

        has_media = bool(media_path) and Path(media_path).exists() if media_path else False

        import tweepy

        async def _upload_media(token: str) -> str | None:
            try:
                return await _twitter_v2_upload(token, Path(media_path))
            except Exception as exc:
                logger.warning(
                    "Twitter v2 media upload failed (%s); posting text-only. "
                    "Re-run Connect with X to refresh the media.write scope if "
                    "this persists.",
                    exc,
                )
                return None

        try:
            media_ids = None
            if has_media:
                media_id = await _upload_media(bearer)
                if media_id:
                    media_ids = [media_id]

            try:
                # Two stacked tweepy gotchas with OAuth 2.0 user-context:
                # 1. ``tweepy.Client(access_token=...)`` makes tweepy try
                #    to build an OAuth1Session — must use ``bearer_token=``.
                # 2. ``create_tweet`` defaults ``user_auth=True``, which
                #    again routes through OAuth1 even when constructed
                #    with bearer_token only — must pass ``user_auth=False``
                #    so tweepy uses our bearer. Skipping either one yields
                #    "Consumer key must be string or bytes, not NoneType".
                client = tweepy.Client(bearer_token=bearer)
                response = client.create_tweet(
                    text=text, media_ids=media_ids, user_auth=False,
                )
            except tweepy.errors.Unauthorized as exc:
                # Bearer expired (~2h lifetime). Try once with refresh_token.
                new_bearer = await _twitter_refresh_bearer(creds)
                if not new_bearer:
                    raise CredentialAuthError(
                        creds.get("uuid"),
                        "X bearer expired and refresh failed — re-OAuth.",
                    ) from exc
                logger.info("Twitter bearer refreshed; retrying tweet.")
                # Re-upload media against the fresh token — the old media_id
                # belongs to the original auth session and may not be valid.
                if has_media:
                    media_id = await _upload_media(new_bearer)
                    media_ids = [media_id] if media_id else None
                client = tweepy.Client(bearer_token=new_bearer)
                try:
                    response = client.create_tweet(
                        text=text, media_ids=media_ids, user_auth=False,
                    )
                except tweepy.errors.Unauthorized as exc2:
                    raise CredentialAuthError(
                        creds.get("uuid"),
                        "X rejected the refreshed bearer — re-OAuth.",
                    ) from exc2

            tweet_id = response.data["id"]
            return {"url": f"https://x.com/i/status/{tweet_id}", "id": tweet_id}
        except CredentialAuthError:
            raise
        except Exception as e:
            raise RuntimeError(f"Twitter post failed: {e}") from e


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

    async def post(self, text: str, media_path: str | None = None) -> dict:
        from datetime import datetime, timezone

        from yt_scheduler.services import bluesky_oauth
        from yt_scheduler.services.social_credentials import save_bundle

        text = (text or "").strip()

        creds = self._get_creds()
        if creds.get("auth_method") != "oauth":
            raise CredentialAuthError(
                creds.get("uuid"),
                "Bluesky credential is not OAuth-authenticated. "
                "Click Connect with Bluesky in Settings to re-authenticate.",
            )

        async with httpx.AsyncClient(follow_redirects=True) as client:
            try:
                await self._ensure_fresh_token(creds, client, bluesky_oauth, save_bundle)
            except RuntimeError as exc:
                # refresh_tokens raises RuntimeError on a non-200 from the
                # AS — invalid_grant, expired_token, etc. all mean the
                # user has to re-OAuth.
                raise CredentialAuthError(creds.get("uuid"), str(exc)) from exc

            embed = None
            if media_path and Path(media_path).exists():
                blob = await self._upload_blob(creds, Path(media_path), client, bluesky_oauth, save_bundle)
                embed = {
                    "$type": "app.bsky.embed.images",
                    "images": [{"alt": "Video thumbnail", "image": blob}],
                }

            record = {
                "$type": "app.bsky.feed.post",
                "text": text,
                "createdAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            if embed is not None:
                record["embed"] = embed

            create_url = f"{creds['pds'].rstrip('/')}/xrpc/com.atproto.repo.createRecord"

            async def _do() -> httpx.Response:
                proof = bluesky_oauth.sign_dpop_proof(
                    creds["private_key_pem"], "POST", create_url,
                    nonce=creds.get("dpop_nonce_pds"),
                    access_token=creds["access_token"],
                )
                return await client.post(
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
                )

            resp = await _do()
            if resp.status_code in (400, 401):
                try:
                    retried = await self._handle_dpop_or_token_error(
                        resp, creds, client, bluesky_oauth, save_bundle,
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

            self._stash_pds_nonce(creds, resp, save_bundle)

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
    def _stash_pds_nonce(creds: dict, resp: httpx.Response, save_bundle) -> None:
        new_nonce = resp.headers.get("DPoP-Nonce")
        if new_nonce and new_nonce != creds.get("dpop_nonce_pds"):
            creds["dpop_nonce_pds"] = new_nonce
            save_bundle("bluesky", creds["uuid"], creds)

    async def _handle_dpop_or_token_error(
        self,
        resp: httpx.Response,
        creds: dict,
        client: httpx.AsyncClient,
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
                save_bundle("bluesky", creds["uuid"], creds)
                return True
        if resp.status_code == 401 or err in ("invalid_token", "expired_token"):
            await self._refresh_access_token(creds, client, bluesky_oauth, save_bundle)
            return True
        return False

    # Pre-emptively refresh when the access token has under this many
    # seconds of lifetime left. Bluesky access tokens live ~2h, so a
    # 15-minute window means a single posting batch never needs a
    # mid-batch refresh, while a token revoked at the AS still gets
    # caught lazily on a 401 from the PDS.
    _PRE_REFRESH_WINDOW_SECS = 15 * 60

    async def _ensure_fresh_token(
        self, creds: dict, client: httpx.AsyncClient, bluesky_oauth, save_bundle,
    ) -> None:
        expires_at = int(creds.get("expires_at") or 0)
        if expires_at and expires_at - self._PRE_REFRESH_WINDOW_SECS > int(time.time()):
            return
        if not creds.get("refresh_token"):
            return
        await self._refresh_access_token(creds, client, bluesky_oauth, save_bundle)

    async def _refresh_access_token(
        self, creds: dict, client: httpx.AsyncClient, bluesky_oauth, save_bundle,
    ) -> None:
        result = await bluesky_oauth.refresh_tokens(
            refresh_token=creds["refresh_token"],
            private_key_pem=creds["private_key_pem"],
            token_endpoint=creds["token_endpoint"],
            redirect_uri=creds["redirect_uri"],
            nonce=creds.get("dpop_nonce_as"),
            client=client,
        )
        creds["access_token"] = result["access_token"]
        if result.get("refresh_token"):
            creds["refresh_token"] = result["refresh_token"]
        if result.get("expires_in"):
            creds["expires_at"] = int(time.time()) + int(result["expires_in"])
        if result.get("dpop_nonce_as"):
            creds["dpop_nonce_as"] = result["dpop_nonce_as"]
        save_bundle("bluesky", creds["uuid"], creds)

    async def _upload_blob(
        self, creds: dict, path: Path, client: httpx.AsyncClient, bluesky_oauth, save_bundle,
    ) -> dict:
        url = f"{creds['pds'].rstrip('/')}/xrpc/com.atproto.repo.uploadBlob"
        mime, _ = mimetypes.guess_type(path.name)
        mime = mime or "application/octet-stream"
        data = path.read_bytes()

        async def _do() -> httpx.Response:
            proof = bluesky_oauth.sign_dpop_proof(
                creds["private_key_pem"], "POST", url,
                nonce=creds.get("dpop_nonce_pds"),
                access_token=creds["access_token"],
            )
            return await client.post(
                url,
                headers={
                    "Authorization": f"DPoP {creds['access_token']}",
                    "DPoP": proof,
                    "Content-Type": mime,
                },
                content=data,
            )

        resp = await _do()
        if resp.status_code in (400, 401):
            try:
                retried = await self._handle_dpop_or_token_error(
                    resp, creds, client, bluesky_oauth, save_bundle,
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

        self._stash_pds_nonce(creds, resp, save_bundle)
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

    async def post(self, text: str, media_path: str | None = None) -> dict:
        text = (text or "").strip()
        creds = self._get_creds()

        try:
            from mastodon import Mastodon
            from mastodon.errors import MastodonUnauthorizedError

            client = Mastodon(
                access_token=creds["access_token"],
                api_base_url=creds.get("instance_url", "https://mastodon.social"),
            )

            try:
                media_ids = None
                if media_path and Path(media_path).exists():
                    media = client.media_post(media_path)
                    media_ids = [media["id"]]

                status = client.status_post(text, media_ids=media_ids)
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

    async def post(self, text: str, media_path: str | None = None) -> dict:
        text = (text or "").strip()
        creds = self._get_creds()

        try:
            import httpx

            headers = {
                "Authorization": f"Bearer {creds['access_token']}",
                "Content-Type": "application/json",
                "X-Restli-Protocol-Version": "2.0.0",
            }

            body = {
                "author": creds["person_urn"],
                "lifecycleState": "PUBLISHED",
                "specificContent": {
                    "com.linkedin.ugc.ShareContent": {
                        "shareCommentary": {"text": text},
                        "shareMediaCategory": "NONE",
                    }
                },
                "visibility": {"com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"},
            }

            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    "https://api.linkedin.com/v2/ugcPosts", headers=headers, json=body
                )
                if resp.status_code == 401:
                    raise CredentialAuthError(
                        creds.get("uuid"),
                        "LinkedIn rejected the access token — re-OAuth.",
                    )
                resp.raise_for_status()
                post_id = resp.headers.get("x-restli-id", "")
                return {"url": f"https://www.linkedin.com/feed/update/{post_id}", "id": post_id}
        except CredentialAuthError:
            raise
        except Exception as e:
            raise RuntimeError(f"LinkedIn post failed: {e}") from e


class ThreadsPoster(SocialPoster):
    platform = "threads"
    required_keys = ["access_token", "user_id"]

    async def post(self, text: str, media_path: str | None = None) -> dict:
        text = (text or "").strip()
        creds = self._get_creds()

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
                create_resp.raise_for_status()
                container_id = create_resp.json()["id"]

                publish_resp = await client.post(
                    f"https://graph.threads.net/v1.0/{user_id}/threads_publish",
                    params={"creation_id": container_id, "access_token": access_token},
                )
                if publish_resp.status_code == 401:
                    raise CredentialAuthError(
                        creds.get("uuid"),
                        "Threads rejected the access token — re-OAuth.",
                    )
                publish_resp.raise_for_status()
                post_id = publish_resp.json()["id"]

                username = creds.get("username", "")
                return {"url": f"https://threads.net/@{username}/post/{post_id}", "id": post_id}
        except CredentialAuthError:
            raise
        except Exception as e:
            raise RuntimeError(f"Threads post failed: {e}") from e


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

    bundle = load_bundle(cred["platform"], cred["uuid"])
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
    bundle = load_bundle(platform, uuid)
    if bundle is None:
        raise ValueError(f"No bundle stored at {platform}:cred.{uuid}")
    return cls(bundle=bundle)
