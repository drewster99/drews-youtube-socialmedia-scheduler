# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working agreement (read first)

These are hard rules from repeated, explicit user feedback. Follow them every session.

**A. NEVER run the app, server, tests, or any project Python.** Do not run `yt-scheduler`, the server, `pytest` (whole suite or a single file), `.venv/bin/python -m yt_scheduler...`, or any one-off script that imports `yt_scheduler`. Running unsigned code hits the real macOS Keychain and fires password prompts that disrupt the user mid-work. The user runs the app and tests; you do not — not "to verify", not "just one file", not ever. Verify instead at the static level: read the code and trace the logic, `ruff check`, `python -m py_compile`, `node --check`, Jinja2 `get_template` parse, and grep the source. (`ffmpeg`/`sqlite3` against scratch data is fine ONLY if it does not import `yt_scheduler`.) When verification truly requires execution, hand it to the user. Note: a running server on `:8008` is the bundled macOS app's Python and won't reflect source edits until the user rebuilds.

**B. No fallbacks, no silent defaults.** If a value the current code always populates is missing, raise and name it — never `x.get(k, default)` / `|| default` / a default model/codec/etc. "for convenience". A wrong default is worse than a loud error. Legitimate back-compat fallbacks (e.g. a column that is NULL only on pre-migration rows) are fine — say so in a comment.

**C. Surface errors — never a misleading "fine" state.** Every server-side failure (preview cuts, transcription, generation, sends, anything a user action kicks off) must surface in the UI with the real error. Never a silent fallback that shows different/wrong content, and never log-only. The render path checks the error field BEFORE any fallback chain.

**D. Follow the agreed plan/prototype.** When a plan or prototype already exists (e.g. `clip_proto/`), implement THAT — don't invent a different design or "optimize away" deliberate decisions. Read the plan first; if production diverges from it, report the divergence and align to the plan rather than improvising a third option.

**E. Secrets only in the Keychain.** The Anthropic key and OAuth/YouTube tokens live only in the macOS Keychain (encrypted-JSON fallback on other platforms), set via Settings. Never read or store them from env vars, `.env`, or anywhere on disk in plaintext, and never add an env-var fallback. Non-secret config (host/port/intervals/model name) in `.env` is fine.

**F. Frontend ↔ server contract.** The web UI talks to `/api/...` for everything (all reads and writes). The ONLY direct file access is read-only `GET /uploads/<name>`. The server vends ready URLs and owns naming/sanitization — never hand the browser absolute filesystem paths, and don't add `StaticFiles` mounts over data dirs.

**G. Git / scope discipline.** Don't auto-create branches — ask first (this includes the "branch before committing on the default branch" default). Commit/push only when asked. In audit/analysis workflows (e.g. `/stupid`), spawned agents propose only; apply edits only for items the user green-lights.

## Plan

- Always plan your work. Don't just start coding.
- Always validate your assumptions. If you only THINK a thing to be true, figure out how to verify it
- Always challenge your assumptions. You might have a good idea, but there could be a better one -- or you could be wrong.
- Think about the user's intent. Sometimes user's don't say exactly what they want. Make sure you're getting it right.
- Clarify. Ask clarifying questions as needed. Make sure you're not assuming your way through building.
- Always surface errors.
- No hidden defaults.
- Always follow best practices for each domain you are working within.
- Excellence and precision -- all the way.
- Logging, error reporting, performance timing, testability, unit tests, regression tests.
- If you fix a bug, fix it so that it can't happen again, for example by adding regression tests.
- The user wants to be abble to verify and validate everything. Make sure you have full and complete logs.
- Think of the simplest thing that might work and start there.
- Pay attention to the CURRENT architecture of the project when designing and architecting
- Single source of truth.
- Make good use of the javascript console
- When composing AI prompts, generally:
  - Short and clear
  - Step by step instructions
  - Main instruction is system prompt. User data in user prompt.
  - Prefer JSON input
  - Prefer JSON response
  - When possible, use a JSON schema for both input and output, where the LLM knows and will validate against the schema before our app ever sees it.
  - Minimize token use when possible
- Brevity is a non-goal. Clarity is the only thing that matters.
- Naming things: Again, brevity is not a goal. The name should be clear as to what it is and how it is used. When used at the call sight, it should be obvious what is happening without adding any comments
- Comments that explain WHY you are doing a thing are a good idea.
- Comments that explain WHAT you are doing usually means that you didn't properly name things or otherwise havfe disorganized code. In some cases where the work done is particularly complex, a comment may be added explaining WHAT you are doing. In 90%+ of cases, this should be unnecessary.
- Doc comments are helpful but keep them short
- Avoid putting values into comments, since they go stale quickly

## Project Overview

Drew's Video + Socials Scheduler is a local web application for managing the YouTube video publishing workflow. It uploads videos as unlisted drafts, generates SEO descriptions via Claude AI from auto-captions, creates platform-specific social media posts, supports scheduled publishing, and performs background comment moderation. Written in Python 3.11+ with FastAPI, SQLite (async via aiosqlite), and Jinja2 templates.

## Development Commands

These document how **the user** runs and tests the app. Per Working agreement rule A, you (Claude) do not run any of them — not `yt-scheduler`, not `pytest`, not the CLI subcommands. They're listed here for reference and for the user's use.

```bash
# Setup
python -m venv .venv && source .venv/bin/activate
pip install -e ".[social,dev]"
cp .env.example .env  # optional; only non-secret settings. Set the Anthropic API key from the web UI (Settings → Anthropic) — it's stored in Keychain, never in .env.

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
yt-scheduler export-all backup.dysbak    # Passphrase-encrypted bundle of the data dir + all Keychain secrets
yt-scheduler import-all backup.dysbak    # Restore a bundle (server must be stopped; replaces data, keeps a .pre-import-* copy)
```

`export-all`/`import-all` read the passphrase from `DYS_BUNDLE_PASSPHRASE` if set (used by the macOS app), otherwise prompt interactively. Bundle logic is in `services/backup.py`; secret enumeration is `keychain.export_all_secrets()`/`import_all_secrets()`.

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
- **`video_routes`** — Upload, list, update metadata, generate descriptions, publish/schedule. Also Replace-source / Attach-source flow with codec + quality reporting on `/file-info`.
- **`promo_routes`** — Per-parent promo videos, schedule-all, and Generate-from-source (preview → poll → confirm cuts proposed clips and inserts them as promos through the existing chain).
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
- **`transcription.py`** — On-device transcription with multiple backends (MLX Whisper, whisper.cpp, macOS SFSpeechRecognizer)
- **`media.py`** — FFmpeg clip/GIF extraction; ffprobe-based video probing; hardware-encoder (videotoolbox) detection; browser-codec allowlist + source-quality warnings; 9:16 vertical crop filter
- **`clipper.py`** — Generate-from-source: per-kind Claude tool_use calls proposing clip ranges from a parent's SRT transcript, optional Claude-vision pass refining crop position, ffmpeg cut execution gated by separate hardware (4) and software (8) semaphores
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
- **Promo source-file provenance** — `videos.source_file_origin` (migration 026) tracks where the local file came from: `uploaded` (manual upload), `youtube_download` (re-fetched from YouTube, lossy), `user_attached` (Replace-source master), or `generated_clip` (Generate-from-source cut). Replace-source and the YouTube re-download path both honour this enum so a user-attached master can't be silently clobbered.
- **Generate-from-source** — On the Promo screen, "Generate from source" runs three parallel Claude tool_use calls (hook / short / segment) over the parent's SRT transcript, proposes ranges, optionally runs a Claude-vision pass on 3 keyframes per range to refine 9:16 crop position, then cuts and inserts the accepted ranges through the existing promo chain. Concurrency: 4 hardware-encoder cuts, 8 software cuts, 4 promo chains, 4 vision calls — all gated by per-purpose `asyncio.Semaphore`s.

## Configuration

All via environment variables (loaded from `.env`). Secrets are **never** read from env/`.env` — the Anthropic API key and all OAuth tokens live only in the macOS Keychain (encrypted-JSON fallback elsewhere), set via the Settings page.

| Variable | Default | Purpose |
|----------|---------|---------|
| `ANTHROPIC_MODEL` | `claude-sonnet-4-6` | Model for AI generation |
| `DYS_HOST` | `127.0.0.1` | Server bind address |
| `DYS_PORT` | `8008` | Server port |
| `DYS_DATA_DIR` | `~/.yt-scheduler` | Data directory |
| `DYS_COMMENT_CHECK_MINUTES` | `30` | Comment moderation poll interval |
| `DYS_CAPTION_CHECK_MINUTES` | `15` | Caption availability poll interval |
| `DYS_THREADS_REDIRECT_URL` | `https://nuclearcyborg.com/apps/scheduler/callback-threads-redirect` | Override for the Threads OAuth `redirect_uri` bounce page (Meta rejects `http://`); bounce-page source is in `cloudflare/` |

Legacy `YTP_*` names (`YTP_HOST`, `YTP_PORT`, `YTP_DATA_DIR`, etc.) are still honored as a fallback for older `.env` files.

## YouTube API Quota

Daily quota is 10,000 units. Notable costs: video upload (100), metadata update (50), caption operations (50-450), search (100), list comments (1). The app minimizes usage by batching requests and caching locally.
