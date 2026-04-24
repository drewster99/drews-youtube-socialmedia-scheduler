"""Social media posting — multi-platform with Keychain credential storage."""

from __future__ import annotations

from pathlib import Path

from youtube_publisher.services.keychain import load_all_secrets, load_secret


class SocialPoster:
    """Base class for social media platform posters."""

    platform: str = ""

    # Keys that are secrets (stored in Keychain) vs. non-secrets (also in Keychain for simplicity)
    # All platform config is stored securely since even handles can be sensitive.
    required_keys: list[str] = []

    def _get_creds(self) -> dict[str, str]:
        """Load all credentials for this platform from Keychain/secrets."""
        return load_all_secrets(self.platform)

    async def post(self, text: str, media_path: str | None = None) -> dict:
        """Post content. Returns {"url": "...", "id": "..."} on success."""
        raise NotImplementedError

    async def is_configured(self) -> bool:
        """Check if this platform has all required credentials."""
        creds = self._get_creds()
        return all(creds.get(k) for k in self.required_keys)


class TwitterPoster(SocialPoster):
    platform = "twitter"
    required_keys = ["api_key", "api_secret", "access_token", "access_token_secret"]

    async def post(self, text: str, media_path: str | None = None) -> dict:
        creds = self._get_creds()

        try:
            import tweepy

            client = tweepy.Client(
                consumer_key=creds["api_key"],
                consumer_secret=creds["api_secret"],
                access_token=creds["access_token"],
                access_token_secret=creds["access_token_secret"],
            )

            media_ids = None
            if media_path and Path(media_path).exists():
                auth = tweepy.OAuth1UserHandler(
                    creds["api_key"],
                    creds["api_secret"],
                    creds["access_token"],
                    creds["access_token_secret"],
                )
                api_v1 = tweepy.API(auth)
                media = api_v1.media_upload(media_path)
                media_ids = [media.media_id]

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
        # Labels match the X / twitter developer portal's OAuth 1.0 Keys panel at
        # developer.x.com so users can copy values straight across without guessing
        # which "API Key" means which. Storage keys stay snake_case for back-compat.
        {"key": "api_key", "label": "Consumer Key", "type": "text", "secret": True},
        {"key": "api_secret", "label": "Consumer Secret", "type": "password", "secret": True},
        {"key": "access_token", "label": "Access Token", "type": "text", "secret": True},
        {"key": "access_token_secret", "label": "Access Token Secret", "type": "password", "secret": True},
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
    "twitter": "Requires Twitter API v2 Basic plan ($100/mo). Create app at developer.x.com.",
    "bluesky": "Use an App Password from bsky.app → Settings → App Passwords. Free.",
    "mastodon": "Create an app in your instance's Settings → Development → New Application. Free.",
    "linkedin": "Requires LinkedIn app with w_member_social scope. Get person URN from /v2/me.",
    "threads": "Requires Meta developer app with threads_publish scope.",
}


def get_poster(platform: str) -> SocialPoster:
    """Get the poster instance for a platform."""
    cls = _POSTERS.get(platform)
    if not cls:
        raise ValueError(f"Unknown platform: {platform}. Available: {ALL_PLATFORMS}")
    return cls()
