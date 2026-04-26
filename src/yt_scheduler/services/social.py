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
        creds = self._get_creds()
        bearer = creds.get("bearer_token")
        if not bearer:
            raise RuntimeError(
                "X is not configured. Click 'Connect with X (OAuth 2.0)' in Settings."
            )

        has_media = bool(media_path) and Path(media_path).exists() if media_path else False

        try:
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

            media_ids = None
            if has_media:
                media_id = await _upload_media(bearer)
                if media_id:
                    media_ids = [media_id]

            try:
                client = tweepy.Client(access_token=bearer)
                response = client.create_tweet(text=text, media_ids=media_ids)
            except tweepy.errors.Unauthorized:
                # Bearer expired (~2h lifetime). Try once with refresh_token.
                new_bearer = await _twitter_refresh_bearer(creds)
                if not new_bearer:
                    raise
                logger.info("Twitter bearer refreshed; retrying tweet.")
                # Re-upload media against the fresh token — the old media_id
                # belongs to the original auth session and may not be valid.
                if has_media:
                    media_id = await _upload_media(new_bearer)
                    media_ids = [media_id] if media_id else None
                client = tweepy.Client(access_token=new_bearer)
                response = client.create_tweet(text=text, media_ids=media_ids)

            tweet_id = response.data["id"]
            return {"url": f"https://x.com/i/status/{tweet_id}", "id": tweet_id}
        except Exception as e:
            raise RuntimeError(f"Twitter post failed: {e}") from e


class BlueskyPoster(SocialPoster):
    platform = "bluesky"
    required_keys = ["handle", "app_password"]

    async def post(self, text: str, media_path: str | None = None) -> dict:
        creds = self._get_creds()

        try:
            from atproto import Client

            client = Client()
            client.login(creds["handle"], creds["app_password"])

            embed = None
            if media_path and Path(media_path).exists():
                with open(media_path, "rb") as f:
                    img_data = f.read()
                upload = client.upload_blob(img_data)
                embed = {
                    "$type": "app.bsky.embed.images",
                    "images": [{"alt": "Video thumbnail", "image": upload.blob}],
                }

            response = client.send_post(text=text, embed=embed)
            return {"url": f"https://bsky.app/profile/{creds['handle']}", "id": str(response.uri)}
        except Exception as e:
            raise RuntimeError(f"Bluesky post failed: {e}") from e


class MastodonPoster(SocialPoster):
    platform = "mastodon"
    required_keys = ["access_token", "instance_url"]

    async def post(self, text: str, media_path: str | None = None) -> dict:
        creds = self._get_creds()

        try:
            from mastodon import Mastodon

            client = Mastodon(
                access_token=creds["access_token"],
                api_base_url=creds.get("instance_url", "https://mastodon.social"),
            )

            media_ids = None
            if media_path and Path(media_path).exists():
                media = client.media_post(media_path)
                media_ids = [media["id"]]

            status = client.status_post(text, media_ids=media_ids)
            return {"url": status["url"], "id": str(status["id"])}
        except Exception as e:
            raise RuntimeError(f"Mastodon post failed: {e}") from e


class LinkedInPoster(SocialPoster):
    platform = "linkedin"
    required_keys = ["access_token", "person_urn"]

    async def post(self, text: str, media_path: str | None = None) -> dict:
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
                resp.raise_for_status()
                post_id = resp.headers.get("x-restli-id", "")
                return {"url": f"https://www.linkedin.com/feed/update/{post_id}", "id": post_id}
        except Exception as e:
            raise RuntimeError(f"LinkedIn post failed: {e}") from e


class ThreadsPoster(SocialPoster):
    platform = "threads"
    required_keys = ["access_token", "user_id"]

    async def post(self, text: str, media_path: str | None = None) -> dict:
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
                create_resp.raise_for_status()
                container_id = create_resp.json()["id"]

                publish_resp = await client.post(
                    f"https://graph.threads.net/v1.0/{user_id}/threads_publish",
                    params={"creation_id": container_id, "access_token": access_token},
                )
                publish_resp.raise_for_status()
                post_id = publish_resp.json()["id"]

                username = creds.get("username", "")
                return {"url": f"https://threads.net/@{username}/post/{post_id}", "id": post_id}
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
    "bluesky": [
        {"key": "handle", "label": "Handle", "type": "text", "secret": False, "placeholder": "you.bsky.social"},
        {"key": "app_password", "label": "App Password", "type": "password", "secret": True},
    ],
    "mastodon": [
        {"key": "instance_url", "label": "Instance URL", "type": "text", "secret": False, "placeholder": "https://mastodon.social"},
        {"key": "access_token", "label": "Access Token", "type": "password", "secret": True},
    ],
    "linkedin": [
        {"key": "access_token", "label": "Access Token", "type": "password", "secret": True},
        {"key": "person_urn", "label": "Person URN", "type": "text", "secret": False, "placeholder": "urn:li:person:xxxxxxxx"},
    ],
    "threads": [
        {"key": "access_token", "label": "Access Token", "type": "password", "secret": True},
        {"key": "user_id", "label": "User ID", "type": "text", "secret": False},
        {"key": "username", "label": "Username", "type": "text", "secret": False},
    ],
}

PLATFORM_DESCRIPTIONS: dict[str, str] = {
    "twitter": "Click 'Connect with X (OAuth 2.0)' below. Requires a paid X API tier (Free tier can't post). Posts and media uploads use the v2 API.",
    "bluesky": "Use an App Password from bsky.app → Settings → App Passwords. Free.",
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
        "Open https://bsky.app/settings/app-passwords (Settings → Privacy and security → App passwords).",
        "Click Add App Password, name it (e.g. Youtube Publisher), leave DM access unchecked, Create.",
        "Copy the generated password — shown only once, format xxxx-xxxx-xxxx-xxxx.",
        "Handle field: your full handle like yourname.bsky.social (no @).",
        "App Password field: paste the generated password.",
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
