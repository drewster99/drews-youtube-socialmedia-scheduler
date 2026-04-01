"""Social media posting — multi-platform."""

from __future__ import annotations

import json
from pathlib import Path

from youtube_publisher.database import get_db


class SocialPoster:
    """Base class for social media platform posters."""

    platform: str = ""

    async def post(self, text: str, media_path: str | None = None) -> dict:
        """Post content. Returns {"url": "...", "id": "..."} on success."""
        raise NotImplementedError

    async def is_configured(self) -> bool:
        """Check if this platform's credentials are configured."""
        db = await get_db()
        rows = await db.execute_fetchall(
            "SELECT value FROM settings WHERE key = ?",
            (f"social_{self.platform}_configured",),
        )
        return bool(rows) and rows[0]["value"] == "1"


class TwitterPoster(SocialPoster):
    platform = "twitter"

    async def post(self, text: str, media_path: str | None = None) -> dict:
        db = await get_db()
        rows = await db.execute_fetchall(
            "SELECT key, value FROM settings WHERE key LIKE 'social_twitter_%'"
        )
        creds = {r["key"].replace("social_twitter_", ""): r["value"] for r in rows}

        try:
            import tweepy

            client = tweepy.Client(
                consumer_key=creds.get("api_key", ""),
                consumer_secret=creds.get("api_secret", ""),
                access_token=creds.get("access_token", ""),
                access_token_secret=creds.get("access_token_secret", ""),
            )

            media_ids = None
            if media_path and Path(media_path).exists():
                # v1.1 API needed for media upload
                auth = tweepy.OAuth1UserHandler(
                    creds.get("api_key", ""),
                    creds.get("api_secret", ""),
                    creds.get("access_token", ""),
                    creds.get("access_token_secret", ""),
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

    async def post(self, text: str, media_path: str | None = None) -> dict:
        db = await get_db()
        rows = await db.execute_fetchall(
            "SELECT key, value FROM settings WHERE key LIKE 'social_bluesky_%'"
        )
        creds = {r["key"].replace("social_bluesky_", ""): r["value"] for r in rows}

        try:
            from atproto import Client

            client = Client()
            client.login(creds.get("handle", ""), creds.get("app_password", ""))

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
            return {"url": f"https://bsky.app/profile/{creds.get('handle', '')}", "id": str(response.uri)}
        except Exception as e:
            raise RuntimeError(f"Bluesky post failed: {e}") from e


class MastodonPoster(SocialPoster):
    platform = "mastodon"

    async def post(self, text: str, media_path: str | None = None) -> dict:
        db = await get_db()
        rows = await db.execute_fetchall(
            "SELECT key, value FROM settings WHERE key LIKE 'social_mastodon_%'"
        )
        creds = {r["key"].replace("social_mastodon_", ""): r["value"] for r in rows}

        try:
            from mastodon import Mastodon

            client = Mastodon(
                access_token=creds.get("access_token", ""),
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

    async def post(self, text: str, media_path: str | None = None) -> dict:
        """Post to LinkedIn using their API.

        Note: LinkedIn API requires an approved app with posting permissions.
        This uses the Share API (v2).
        """
        db = await get_db()
        rows = await db.execute_fetchall(
            "SELECT key, value FROM settings WHERE key LIKE 'social_linkedin_%'"
        )
        creds = {r["key"].replace("social_linkedin_", ""): r["value"] for r in rows}

        try:
            import httpx

            headers = {
                "Authorization": f"Bearer {creds.get('access_token', '')}",
                "Content-Type": "application/json",
                "X-Restli-Protocol-Version": "2.0.0",
            }

            person_urn = creds.get("person_urn", "")
            body = {
                "author": person_urn,
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

    async def post(self, text: str, media_path: str | None = None) -> dict:
        """Post to Threads using Meta's Threads API."""
        db = await get_db()
        rows = await db.execute_fetchall(
            "SELECT key, value FROM settings WHERE key LIKE 'social_threads_%'"
        )
        creds = {r["key"].replace("social_threads_", ""): r["value"] for r in rows}

        try:
            import httpx

            access_token = creds.get("access_token", "")
            user_id = creds.get("user_id", "")

            async with httpx.AsyncClient() as client:
                # Step 1: Create media container
                create_resp = await client.post(
                    f"https://graph.threads.net/v1.0/{user_id}/threads",
                    params={
                        "media_type": "TEXT",
                        "text": text,
                        "access_token": access_token,
                    },
                )
                create_resp.raise_for_status()
                container_id = create_resp.json()["id"]

                # Step 2: Publish
                publish_resp = await client.post(
                    f"https://graph.threads.net/v1.0/{user_id}/threads_publish",
                    params={
                        "creation_id": container_id,
                        "access_token": access_token,
                    },
                )
                publish_resp.raise_for_status()
                post_id = publish_resp.json()["id"]

                return {"url": f"https://threads.net/@{creds.get('username', '')}/post/{post_id}", "id": post_id}
        except Exception as e:
            raise RuntimeError(f"Threads post failed: {e}") from e


def get_poster(platform: str) -> SocialPoster:
    """Get the poster instance for a platform."""
    posters = {
        "twitter": TwitterPoster(),
        "bluesky": BlueskyPoster(),
        "mastodon": MastodonPoster(),
        "linkedin": LinkedInPoster(),
        "threads": ThreadsPoster(),
    }
    if platform not in posters:
        raise ValueError(f"Unknown platform: {platform}. Available: {list(posters.keys())}")
    return posters[platform]


ALL_PLATFORMS = ["twitter", "bluesky", "mastodon", "linkedin", "threads"]
