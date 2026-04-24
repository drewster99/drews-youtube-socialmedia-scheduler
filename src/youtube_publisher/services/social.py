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

# Detailed per-platform setup walkthroughs, shown in the Settings UI behind an
# info toggle. Each list is rendered as ordered steps. Links are left as plain
# URLs; the UI auto-linkifies http(s) occurrences.
PLATFORM_SETUP_GUIDES: dict[str, list[str]] = {
    "twitter": [
        "Sign up at https://developer.x.com and subscribe to Basic tier ($200/mo) or Pay-Per-Use with credits — Free tier cannot post.",
        "Projects & Apps → Create App inside a Project (standalone apps cannot call POST /2/tweets).",
        "Settings → User authentication settings → Edit. Set App permissions to Read and write. Save.",
        "Back on Keys and tokens: regenerate Access Token and Access Token Secret after changing permissions (existing tokens keep the old read-only grant).",
        "Paste Consumer Key, Consumer Secret, Access Token, and Access Token Secret below.",
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
    """Get the poster instance for a platform."""
    cls = _POSTERS.get(platform)
    if not cls:
        raise ValueError(f"Unknown platform: {platform}. Available: {ALL_PLATFORMS}")
    return cls()
