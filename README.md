# Drew's YT Scheduler

A local web app for managing YouTube video publishing across multiple projects, with AI-powered descriptions, multiple transcript backends, tier-aware templates, per-video activity logs, and cross-platform social media scheduling.

## What it does

1. **Multiple projects** — Each project pairs a YouTube channel with its own social accounts, templates, blocklist, and auto-action settings. Home page lists all projects + recent activity + upcoming scheduled items.
2. **Upload or import** — Upload new videos directly, or import existing ones from YouTube. Imported videos pull description, tags, thumbnail (downloaded locally), and the YouTube transcript when available.
3. **Tier-aware** — Videos are auto-classified by duration (hook < 50s, short 50–180s, segment 180–720s, video ≥ 720s). Each template declares which tiers it applies to.
4. **Multi-transcript** — Each video can hold transcripts from YouTube auto-captions, mlx-whisper (per model), whisper.cpp, macOS SFSpeechRecognizer, plus a user-edited version. Pick the active one in the chooser modal.
5. **AI everywhere, prompts editable** — SEO description, tag generation, and per-platform social copy run through Claude. The prompt bodies live in `prompt_templates` and are editable like any other template.
6. **Auto-actions** — Per-project matrix (Uploads | Imports) for: auto-transcribe, auto-description, auto-tags (add vs replace), auto-thumbnail (ffmpeg keyframe at 25%), per-platform auto-gen-socials. Wiring rules: tags + description fire only on first transcript set; socials fire only on first description set.
7. **Per-video log** — Right-sidebar activity feed: created / imported / uploaded / metadata_updated (per-field diff modal) / publish_scheduled / published / social_post_scheduled (in-app modal of saved text) / social_post_published (link to live post).
8. **Scheduled publish + scheduled socials** — Schedule the video itself, plus per-post social scheduling via APScheduler `DateTrigger` jobs. Re-schedule pattern holds a per-post lock so two concurrent reschedules can't double-fire.
9. **Comment moderation** — Per-project keyword/regex blocklist. Run-now button surfaces actual results. Background job runs on a configurable interval.
10. **Built-in OAuth 2.0** — Twitter/X (PKCE), Mastodon (per-instance dynamic registration), LinkedIn (Authorization Code), Threads (token exchange). One-click "Connect with …" buttons in General Settings.
11. **macOS menubar app** — Optional self-contained `.app` bundle that embeds Python and the package, runs the FastAPI server in the background, and shows a play-rectangle icon in the menu bar (Open web UI / Restart server / View logs / Quit).

## Setup

### Prerequisites

- Python 3.11+
- FFmpeg (for clip/GIF extraction + auto-thumbnail keyframes): `brew install ffmpeg`
- A Google Cloud project with YouTube Data API v3 enabled

### Google Cloud setup

1. Create a project at [Google Cloud Console](https://console.cloud.google.com/).
2. Enable **YouTube Data API v3**.
3. **APIs & Services → Credentials → Create OAuth 2.0 Client ID** → Application type **Desktop app**.
4. Download the JSON; that's your `client_secret.json`.

### Install (Python / web UI)

```bash
git clone https://github.com/drewster99/drews-youtube-socialmedia-scheduler.git
cd drews-youtube-socialmedia-scheduler

python -m venv .venv
source .venv/bin/activate
pip install -e ".[social,dev,transcription-mlx]"

cp .env.example .env  # then edit and set ANTHROPIC_API_KEY
yt-scheduler            # serves http://127.0.0.1:8008
```

### Install (macOS .app)

```bash
macos/build.sh
open "macos/build/Drew's YT Scheduler.app"
```

The build embeds a relocatable Python 3.12 + all deps, copies the source + migrations into `Contents/Resources/yt_scheduler_src/`, and produces a ~360 MB self-contained `.app`. The menu-bar icon is a play-rectangle ▶︎ in the top-right.

## Configuration

Environment variables (loaded from `.env`; legacy `YTP_*` names are honoured as a fallback):

| Variable | Default | Purpose |
|----------|---------|---------|
| `ANTHROPIC_API_KEY` | — | Claude API key (also configurable from Settings) |
| `ANTHROPIC_MODEL` | `claude-sonnet-4-6` | Default Claude model. Override per install via Settings → Anthropic → Model name (stored in DB). |
| `DYS_HOST` | `127.0.0.1` | Bind address |
| `DYS_PORT` | `8008` | Server port |
| `DYS_DATA_DIR` | `~/.drews-yt-scheduler` | Data directory (legacy `~/.youtube-publisher` is auto-renamed on first run) |
| `DYS_COMMENT_CHECK_MINUTES` | `30` | Moderation poll interval |
| `DYS_CAPTION_CHECK_MINUTES` | `15` | Auto-caption poll interval |

## Usage

### Typical workflow

1. **Add a project** — Home → Add project. Each project has its own dashboard, templates, moderation, and settings.
2. **Authenticate YouTube** — General Settings → upload `client_secret.json`. (Per-project OAuth keying is in place; UI flow lands with future polish.)
3. **Connect socials** — General Settings → "Connect with X / Mastodon / LinkedIn / Threads". Bluesky uses a handle + app password.
4. **Configure auto-actions** — Project Settings → toggle the Uploads | Imports matrix and the Posting-to-social-media defaults.
5. **Upload or import** — Dashboard → "Upload new video", or pick from "YouTube videos available to import".
6. **Compose socials from a template** — Video detail → "Socials from template" → pick template, check accounts, write `{{user_message}}`, Generate. Edit each card individually, then Post now or Schedule.
7. **Publish** — Either schedule the video itself (dashed-amber pill on the detail screen) or click "Publish + post now" to flip privacy to public + fire all approved socials immediately.

### Templates

Templates live under each project at `Project / Templates`. Each defines per-platform post formats with:

- **`{{variable}}`** — Substituted from video metadata. Available: `title`, `url`, `description`, `description_short` (first 150), `description_medium` (first 500), `tags`, `hashtags`, `thumbnail_path`, `tier`, `transcript`, `user_message`.
- **`{{ai: prompt}}`** — Sent to Claude. Variables inside are substituted first.
- **Applies to** — Chip selector for which tiers (hook / short / segment / video) the template targets.
- **Media** — `thumbnail | full_video | clip | gif | none`. Hook-tier templates default to `full_video`; others to `thumbnail`.

Two templates ship by default per project: `new_video` (full announcement scaffold) and `new_message` (`{{user_message}}` only — useful for one-off posts).

### Prompt templates

LLM prompts (description-from-transcript, tags-from-metadata, …) flow through the same template engine and are seeded with editable defaults in `prompt_templates`. Modify via the API or edit directly; missing rows fall back to a hard-coded seed.

### Comment moderation

Project / Moderation → Add keywords (plain or regex). "Run check now" actually runs and returns counts. Background polling fires on the interval set in General Settings.

## Project structure

```
src/yt_scheduler/
├── app.py                 FastAPI setup, lifespan, page routes
├── main.py                CLI entry (yt-scheduler / serve / install / auth / status)
├── config.py              Env vars + data-dir migration
├── database.py            aiosqlite handle, runs migrations on first connect
├── migrations.py          Migration runner (NNN_*.sql in /migrations or _migrations/)
├── models/                Pydantic models for routes
├── routers/               FastAPI routers (project / video / transcript / social / template / settings / oauth / import)
├── services/              auth, youtube, ai, templates, prompts, social, social_identity,
│                          scheduler, moderation, transcription, transcripts, tiers,
│                          imports, project_settings, projects, events, auto_actions,
│                          keychain, daemon, media
├── static/                CSS + vanilla JS
└── templates_html/        Jinja2 pages (home, dashboard, video_detail, templates, template_edit,
                           moderation, settings, project_settings, upload, socials_compose, base)
migrations/                001_baseline → 007_scheduled_posts
macos/                     Swift menubar app + build.sh
tests/                     pytest suite (88 tests)
```

## API quota notes

YouTube Data API daily quota is 10,000 units. Notable costs: video upload 100, metadata update 50, list comments 1, caption operations 50–450, search 100. The app minimizes by batching and caching locally.

## License

See `LICENSE` (or repository settings).
