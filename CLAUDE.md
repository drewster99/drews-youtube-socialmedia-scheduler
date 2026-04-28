# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

YouTube Publisher is a local web application for managing the YouTube video publishing workflow. It uploads videos as unlisted drafts, generates SEO descriptions via Claude AI from auto-captions, creates platform-specific social media posts, supports scheduled publishing, and performs background comment moderation. Written in Python 3.11+ with FastAPI, SQLite (async via aiosqlite), and Jinja2 templates.

## Development Commands

```bash
# Setup
python -m venv .venv && source .venv/bin/activate
pip install -e ".[social,dev]"
cp .env.example .env  # then add ANTHROPIC_API_KEY

# Run (dev mode with auto-reload)
yt-scheduler --reload

# Run (production)
yt-scheduler

# Lint/format
ruff check src/
ruff format src/

# Tests
pytest
pytest tests/test_foo.py::test_bar  # single test

# CLI subcommands
yt-scheduler auth [client_secret.json]  # YouTube OAuth flow
yt-scheduler install                     # Install as background service (launchd/systemd)
yt-scheduler uninstall
yt-scheduler status
```

Web UI runs at `http://127.0.0.1:8008` by default.

## Architecture

### Layer Structure

```
CLI (main.py) → FastAPI app (app.py) → Routers → Services → External APIs
                                                     ↓
                                               SQLite (database.py)
```

- **`main.py`** — CLI entry point dispatching to `serve`, `install`, `auth`, `status` subcommands
- **`app.py`** — FastAPI setup, lifespan (DB init, scheduler start, restore jobs), static files, HTML page routes
- **`config.py`** — All configuration from env vars / `.env` file. Data lives in `~/.yt-scheduler/`
- **`database.py`** — Single global `aiosqlite` connection, schema auto-created on first connect

### Routers (`routers/`)

Each router owns a domain of API endpoints under `/api/`:
- **`video_routes`** — Upload, list, update metadata, generate descriptions, publish/schedule
- **`social_routes`** — Generate posts from templates, edit/approve/send to platforms
- **`template_routes`** — CRUD for post templates
- **`settings_routes`** — Credential management, blocklist, YouTube auth status
- **`auth_routes`** — OAuth status endpoint

When you add, remove, or change the shape of any HTTP endpoint, update `API.md` in the same change.

### Services (`services/`)

Business logic layer, each service wraps one concern:
- **`youtube.py`** — YouTube Data API v3 (upload, metadata, captions, comments)
- **`ai.py`** — Claude API for description generation and template AI blocks
- **`social.py`** — Multi-platform posting (Twitter/X via tweepy, Bluesky via atproto, Mastodon, LinkedIn, Threads)
- **`templates.py`** — Template engine with `{{variable}}` and `{{ai: prompt}}` syntax
- **`auth.py`** — YouTube OAuth flow + credential storage (Keychain on macOS, encrypted JSON fallback)
- **`scheduler.py`** — APScheduler background jobs (scheduled publish, caption polling, comment moderation)
- **`moderation.py`** — Comment filtering against blocklist (supports plain text and regex)
- **`transcription.py`** — On-device transcription with multiple backends (MLX Whisper, faster-whisper, whisper.cpp, macOS SFSpeechRecognizer)
- **`media.py`** — FFmpeg clip/GIF extraction
- **`keychain.py`** — macOS Keychain wrapper via `security` CLI
- **`daemon.py`** — Service installation (launchd on macOS, systemd on Linux)

### Frontend

Server-rendered HTML via Jinja2 (`templates_html/`) with vanilla JS (`static/js/app.js`). No build step.

### macOS Menubar App (`macos/`)

Native SwiftUI app that embeds a Python runtime and manages the server as a subprocess. Built via `macos/build.sh`.

## Key Design Decisions

- **Single SQLite database** — No external DB server; `aiosqlite` for async access; schema auto-migrates via `CREATE TABLE IF NOT EXISTS`
- **Global DB connection** — `database.get_db()` returns a module-level singleton connection
- **Credentials in Keychain** — On macOS, social media tokens stored in system Keychain (`com.nuclearcyborg.drews-socialmedia-scheduler.*`), with encrypted JSON fallback on other platforms
- **Template syntax** — `{{variable}}` for metadata substitution, `{{ai: prompt}}` for Claude generation; variables inside AI blocks are resolved first
- **Video lifecycle** — `draft → uploaded → captioned → ready → published`; captions polled every 15 min via background job
- **Scheduled publishing** — Sets `publish_at` on video, APScheduler fires at that time to flip privacy to public and send all approved social posts

## Configuration

All via environment variables (loaded from `.env`):

| Variable | Default | Purpose |
|----------|---------|---------|
| `ANTHROPIC_API_KEY` | — | Claude API key |
| `ANTHROPIC_MODEL` | `claude-sonnet-4-20250514` | Model for AI generation |
| `YTP_HOST` | `127.0.0.1` | Server bind address |
| `YTP_PORT` | `8008` | Server port |
| `YTP_DATA_DIR` | `~/.yt-scheduler` | Data directory |
| `YTP_COMMENT_CHECK_MINUTES` | `30` | Comment moderation poll interval |
| `YTP_CAPTION_CHECK_MINUTES` | `15` | Caption availability poll interval |

## YouTube API Quota

Daily quota is 10,000 units. Notable costs: video upload (100), metadata update (50), caption operations (50-450), search (100), list comments (1). The app minimizes usage by batching requests and caching locally.
