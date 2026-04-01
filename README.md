# YouTube Publisher

A local web app for managing YouTube video publishing with AI-powered descriptions and cross-platform social media posting.

## What It Does

1. **Upload & Prepare** — Upload video to YouTube as unlisted, set title/tags/thumbnail
2. **AI Description** — Auto-captions arrive → transcript is downloaded → Claude generates an SEO-friendly description → prepends your pinned links
3. **Social Posts** — Generate platform-specific posts from templates with `{{variable}}` and `{{ai: prompt}}` blocks
4. **Scheduled Publish** — Set a publish time; video goes public automatically
5. **Comment Moderation** — Background job polls for comments matching your keyword blocklist and removes them
6. **Community Post** — Generates a copy/paste community post (YouTube API doesn't support creating these programmatically)

## Setup

### 1. Prerequisites

- Python 3.11+
- FFmpeg (for clip/GIF extraction): `brew install ffmpeg`
- A Google Cloud project with YouTube Data API v3 enabled

### 2. Google Cloud Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project (or use existing)
3. Enable **YouTube Data API v3**
4. Go to **APIs & Services → Credentials**
5. Create **OAuth 2.0 Client ID** → Application type: **Desktop app**
6. Download the JSON file (this is your `client_secret.json`)

### 3. Install

```bash
git clone https://github.com/drewster99/youtube-mpc-server.git
cd youtube-mpc-server

python -m venv .venv
source .venv/bin/activate

pip install -e ".[social,dev]"
```

### 4. Configure

```bash
cp .env.example .env
# Edit .env and add your ANTHROPIC_API_KEY
```

### 5. Run

```bash
youtube-publisher
```

Open http://127.0.0.1:8008 in your browser.

### 6. Authenticate with YouTube

1. Go to **Settings** in the web UI
2. Upload your `client_secret.json`
3. Click **Authenticate with YouTube**
4. Complete the Google OAuth flow in the browser

### 7. Configure Social Media (optional)

In **Settings**, add credentials for each platform you want to post to:

| Platform | What You Need |
|----------|--------------|
| **Twitter/X** | API Key, API Secret, Access Token, Access Token Secret (requires Basic plan, $100/mo) |
| **Bluesky** | Handle + App Password (free, create at bsky.app → Settings → App Passwords) |
| **Mastodon** | Instance URL + Access Token (free, create app in instance Settings → Development) |
| **LinkedIn** | Access Token + Person URN (requires approved LinkedIn app) |
| **Threads** | Access Token + User ID (requires Meta developer app) |

## Usage

### Typical Workflow

1. **Upload** — Go to Upload, fill in title/tags/thumbnail, upload video as unlisted
2. **Wait for captions** — The app polls YouTube every 15 minutes for auto-generated captions
3. **Generate description** — Once captions arrive, click "Generate Description" on the video detail page. Review and edit the AI-generated SEO description. Click "Apply to YouTube."
4. **Generate social posts** — Click "Generate Social Posts" to create platform-specific posts from your template. Review, edit, approve each one.
5. **Publish** — Either set a scheduled publish time or click "Publish Now"
6. **Post to socials** — Send approved posts to each platform
7. **Copy community post** — Copy the pre-formatted community post text

### Templates

Templates live in the **Templates** section. Each template defines per-platform post formats with:

- **`{{variable}}`** — Replaced with video metadata (title, url, tags, etc.)
- **`{{ai: prompt}}`** — Sent to Claude for AI generation. Variables inside are resolved first.

Example:
```
{{ai: Write a punchy tweet about "{{title}}" that's under 280 chars}}

{{url}}
```

### Comment Moderation

Add keywords in **Moderation → Blocked Keywords**. The background job checks every 30 minutes and rejects matching comments. Supports plain text matching and regex patterns.

## Project Structure

```
src/youtube_publisher/
├── app.py              # FastAPI app setup
├── main.py             # Entry point
├── config.py           # Configuration
├── database.py         # SQLite schema and access
├── routers/            # API routes
│   ├── auth_routes.py
│   ├── video_routes.py
│   ├── social_routes.py
│   ├── template_routes.py
│   └── settings_routes.py
├── services/           # Business logic
│   ├── auth.py         # YouTube OAuth
│   ├── youtube.py      # YouTube API wrapper
│   ├── ai.py           # Claude API
│   ├── templates.py    # Template engine
│   ├── social.py       # Social media posting
│   ├── media.py        # FFmpeg clip/GIF extraction
│   ├── moderation.py   # Comment moderation
│   └── scheduler.py    # Background jobs
├── static/             # CSS, JS
└── templates_html/     # Jinja2 HTML templates
```

## API Quota Notes

YouTube API has a daily quota of 10,000 units. Key costs:
- Video upload: **100 units**
- Metadata update: **50 units**
- List comments: **1 unit**
- Caption operations: **50–450 units**
- Search: **100 units**

The app minimizes quota usage by batching requests and caching data locally.
