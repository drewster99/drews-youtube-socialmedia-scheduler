# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working agreement (read first)

These are hard rules from repeated, explicit user feedback. Follow them every session.

> ### ⚠️ NOTHING YOU EDIT TAKES EFFECT UNTIL THE MAC APP IS REBUILT AND RELAUNCHED
>
> Editing `src/` changes **nothing** that is running. The server executes a *copy* of the
> package installed inside the app bundle:
>
> ```
> …/macos/build/Drew's Video + Socials Scheduler.app/Contents/Resources/python/
>     bin/python3.12                                  ← the interpreter that runs
>     lib/python3.12/site-packages/yt_scheduler/      ← the code that runs
> ```
>
> **NOT** `src/yt_scheduler/`. There is no auto-reload, no editable install, no symlink.
> Templates, JS, routers, services, migrations — all of it is the bundled copy.
>
> To make a change real: `macos/build.sh --debug`, then quit and relaunch the app.
> Until then `/api/build` reports the old `build_number`, the browser serves the old
> HTML/JS, and any behaviour you "verify" against `:8008` is the *previous* build.
>
> Corollary: never conclude a fix works by poking the running server after only editing
> source. Either rebuild first, or verify with `pytest` — which *does* import `src/`.

**A. NEVER run the app or server. DO run the tests.** Never run `yt-scheduler` or the server — unsigned code hits the real macOS Keychain and fires password prompts that disrupt the user mid-work. The user runs the app; you do not.

`pytest` is yours to run (the user does not run it). The full suite is safe to run, including while the menubar app is up: `tests/conftest.py` installs a session-wide guard that wraps `aiosqlite.connect` and `sqlite3.connect` and raises `ProductionDatabaseAccess` on any path inside the real data dir. `config.DATA_DIR`/`DB_PATH` still freeze at import time, so a test that reaches `yt_scheduler.database` without first pointing `DYS_DATA_DIR` at a tmp dir *would* target the real `publisher.db` — the guard is what stops it, not each test's own care. Use the `isolated_db` / `isolated_data_dir` fixtures for new tests. Never delete or write the user's DB.

Two footguns the guard does not cover:

- **Leaked connections wedge the process.** `aiosqlite.Connection` is a non-daemon thread, so a test that calls `get_db()` without `close_db()` passes and then hangs the interpreter in `threading._shutdown()`. `conftest.py` closes leaked connections after every test; keep that net in place.
- **Purging `sys.modules` orphans module references.** Many tests reset `yt_scheduler.*` to re-freeze `config`. A test that captured `import yt_scheduler.database as database` at *import* time then patches a dead module object while the code under test talks to a fresh one. Resolve such modules lazily inside the fixture (`importlib.import_module`), not at module scope.

(Historical note: this rule used to claim five named tests "hang because they open the production DB and block on the SQLite write lock." That was a misdiagnosis — they isolate correctly and pass; they hung on the leaked-connection thread above.)

Static checks are still the cheap first pass, not a substitute: `ruff check`, `python -m py_compile`, `node --check`, Jinja2 `get_template` parse, grep. (`ffmpeg`/`sqlite3` against scratch data is fine.) `pytest` imports `src/` directly, so it is the only way to exercise an edit without rebuilding — see the callout above.

**B. No fallbacks, no silent defaults.** If a value the current code always populates is missing, raise and name it — never `x.get(k, default)` / `|| default` / a default model/codec/etc. "for convenience". A wrong default is worse than a loud error. Legitimate back-compat fallbacks (e.g. a column that is NULL only on pre-migration rows) are fine — say so in a comment.

**C. Surface errors — never a misleading "fine" state.** Every server-side failure (preview cuts, transcription, generation, sends, anything a user action kicks off) must surface in the UI with the real error. Never a silent fallback that shows different/wrong content, and never log-only. The render path checks the error field BEFORE any fallback chain.

**D. Follow the agreed plan/prototype.** When a plan or prototype already exists (e.g. `clip_proto/`), implement THAT — don't invent a different design or "optimize away" deliberate decisions. Read the plan first; if production diverges from it, report the divergence and align to the plan rather than improvising a third option.

**E. Secrets only in the macOS Keychain — never a file.** The Anthropic key and OAuth/YouTube tokens live only in the macOS Keychain, set via Settings. Never read or store them from env vars, `.env`, or anywhere on disk, and never add an env-var fallback. Non-secret config (host/port/intervals/model name) in `.env` is fine.

`keychain.py` has exactly one secret store, the macOS Keychain, and off macOS every entry point raises `UnsupportedPlatform`. There is no file fallback: the old `_is_macos()`-false branch wrote `<DATA_DIR>/secrets.json` as **plaintext JSON** (and was reachable on macOS too, when a Keychain write returned an error code), and it was removed — a fallback that silently downgrades credential security is worse than the error it hid. What remains on disk is `secrets.json` as a **key index only** (every value is the `KEYCHAIN_SENTINEL`, never a secret): the Keychain answers questions about an exact (service, account) pair, so something must record which keys exist for `load_all`/`delete_all`/`export_all`. Tests use an in-memory Keychain (`tests/conftest.install_in_memory_keychain`) — a fake secret for fake calls, never the real login Keychain and never a file.

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

## Server log

The packaged app's stdout/stderr — including the full traceback of every failed send — goes to `~/Library/Logs/com.nuclearcyborg.drews-socialmedia-scheduler/server.log`. The redirect happens in `main._redirect_stdio_to_log`, triggered by `DYS_REDIRECT_LOGS=1` (set in the embedded launch agent plist); terminal/dev runs keep printing to the console. This file is the first place to look when the UI reports an opaque failure, and reading it is always safe (no Keychain, no DB). The macOS menubar app surfaces the same file: **View → View Logs** opens it, and **Tools → Monitor server** tails it live.

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
- **`templates.py`** — Template engine: `{{variable}}` (strict — undefined names raise), `{{variable!}}` (required non-empty), `{{variable??default}}` (default on missing/blank), `{{#variable}}…{{/variable}}` / `{{^variable}}…{{/variable}}` sections, and `{{ai: prompt}}` blocks
- **`auth.py`** — YouTube OAuth flow + credential storage (macOS Keychain only; see rule E)
- **`scheduler.py`** — APScheduler background jobs (scheduled publish, caption polling, comment moderation)
- **`moderation.py`** — Comment filtering against blocklist (supports plain text and regex)
- **`transcription.py`** — On-device transcription. Auto-detect order is Apple SpeechAnalyzer (`macos-speech`) → `whisper.cpp`. **MLX Whisper is opt-in only** — never auto-selected, because MLX never returns freed Metal buffers to the OS (a 12-clip batch once left 30 GB resident on an idle server). When it is chosen, the buffer cache is capped, trimmed after every run, and the cached model weights are dropped after an idle timeout. There is no default `model`: naming a Whisper backend without one is an error, not a silent `large-v3`.
- **`media.py`** — FFmpeg clip/GIF extraction; ffprobe-based video probing; hardware-encoder (videotoolbox) detection; browser-codec allowlist + source-quality warnings; 9:16 vertical crop filter
- **`clipper.py`** — Generate-from-source: per-kind Claude tool_use calls proposing clip ranges from a parent's word-stream transcript, ffmpeg cut execution gated by separate hardware (2) and software (8) semaphores
- **`media_hosting.py`** — Temporary hosting for platforms that *fetch* media from a URL instead of accepting an upload (Threads only, today). Uploads to a private Cloudflare R2 bucket and returns a presigned GET URL valid two hours. Images and video take the identical path — nothing inspects the media kind. No delete: the bucket enforces Object Lock with a 24-hour minimum retention, so its 7-day lifecycle rule is the cleanup and signed-URL expiry is the access control.
- **`sigv4.py`** — AWS Signature V4 presigning over stdlib `hmac`/`hashlib`, for the S3-compatible R2 API. Pure functions with an injected clock, so signatures are reproducible and testable against fixed vectors.
- **`smart_queue_reconcile.py`** / **`smart_queue_reconcile_handlers.py`** — Push template edits onto schedules already built from that template. Slot membership and post text are both decided at Accept, so without this a later edit reached nothing already scheduled. Four job kinds: `slots_added` (render + schedule for every pending item), `slots_removed` (delete those pending posts), `slot_body_changed` (re-render them), `applies_to_removed` (delete posts for videos the queue no longer takes — matched with `tier_matches_item_type`, since `applies_to` holds tiers and `videos.item_type` holds kinds). *Widening* applies-to is deliberately no-op. Both deleting handlers retire a queue item left with no postings to `state='removed'`: item state is derived from the posting rows, so an item with none would sit in the `scheduled` count forever and keep its video from being queued again. Jobs are persisted and drained by a single process-wide worker, so only one runs at a time; a queue with unfinished jobs refuses `PATCH` (409).
- **`keychain.py`** — macOS Keychain wrapper via `security` CLI
- **`daemon.py`** — Service installation (launchd on macOS, systemd on Linux)

### Frontend

Server-rendered HTML via Jinja2 (`templates_html/`) with vanilla JS (`static/js/app.js`). No build step.

### macOS Menubar App (`macos/`)

Native SwiftUI app that embeds a Python runtime and manages the server as a subprocess. Built via `macos/build.sh`.

## Key Design Decisions

- **Single SQLite database** — No external DB server; `aiosqlite` for async access; schema auto-migrates via `CREATE TABLE IF NOT EXISTS`
- **Global DB connection** — `database.get_db()` returns a module-level singleton connection
- **Credentials in the macOS Keychain only** — Social media tokens live in the system Keychain (`com.nuclearcyborg.drews-socialmedia-scheduler.*`). There is no file store for secret values; `secrets.json` is a key index only (rule E)
- **Template syntax** — `{{variable}}` for metadata substitution (strict: an undefined name is an error naming every undefined variable; defined-but-blank renders empty), `{{variable!}}` required-non-empty, `{{variable??default}}` default on missing/blank, `{{#variable}}…{{/variable}}` sections (content renders only when the variable has content; `{{^variable}}` is the inverse; resolved before media/AI passes), `{{ai: prompt}}` for Claude generation; variables inside AI blocks are resolved first. Promo children resolve description prompts via the `<key>_promo` variant chain (saved promo row → promo seed → saved base row → base seed).
- **A row's kind is stored, never inferred from its shape** — `videos.youtube_video_id` (migration 037) is the YouTube video id, or NULL when the item has no YouTube video. Read it via `models.video.youtube_video_id_of()` / `is_youtube_backed()`, which **raise** if the column wasn't selected rather than reporting "no YouTube video". This replaced `len(id) == 11`, which appeared in five places and inferred a record's type from the length of its primary key — `videos.id` was doing double duty as both identity and the YouTube video id. Never reintroduce that pattern: if a discriminator is needed, add a column. (The old rule survives only inside migration 037's backfill, used once against the data it describes.)
- **Video lifecycle** — `draft → uploaded → captioned → ready → published`; captions polled every 15 min via background job
- **Scheduled publishing** — Sets `publish_at` on video, APScheduler fires at that time to flip privacy to public and send all approved social posts
- **Promo source-file provenance** — `videos.source_file_origin` (migration 026) tracks where the local file came from: `uploaded` (manual upload), `youtube_download` (re-fetched from YouTube, lossy), `user_attached` (Replace-source master), or `generated_clip` (Generate-from-source cut). Replace-source and the YouTube re-download path both honour this enum so a user-attached master can't be silently clobbered.
- **Outbound HTTP timeouts live in `config.py`** — the "Outbound HTTP call budgets" section defines `DEFAULT_API_CALL_TIMEOUT_SECONDS` plus a named constant per call, and separate bulk-transfer budgets and chunk sizes (Twitter chunked upload, R2/Threads media upload) that deliberately don't follow the default. Never hardcode a timeout at a call site, and never let a call ride on httpx's implicit 5-second default — that default cut off a real Threads publish mid-call the first day a working token met a media post, while every surrounding call had an explicit budget. A regression test greps the service files for numeric `timeout=` literals.
- **Failed sends stay visible app-wide** — `GET /api/social/failed-posts` lists posts in status `failed`; `static/js/failed-sends-banner.js` (loaded by `base.html` on every page, same pattern as the reconcile banner) shows them until each post is retried successfully or deleted. `social_posts.status` is the single source of truth — no separate "dismissed" state. An error that exists only in a transient toast is treated as unsurfaced: scheduled sends fail with no page open at all.
- **Threads media via hosted URLs** — Threads takes images and video as an `image_url` / `video_url` that Meta fetches itself, so `ThreadsPoster` uploads the prepared file through `media_hosting.py` and sends the signed URL. `accepts_media` states what the *platform* can do (permanently true); `requires_hosted_media` states what *we* depend on, letting `smart_queue_accept` skip a slot with a real reason when hosting isn't configured. Media containers get a longer poll budget than text ones because Meta downloads the file during that window. More than one attachment is refused rather than truncated — carousels aren't built.
- **Template edits reconcile asynchronously** — A template change queues persisted jobs rather than doing the work in the save request: each one re-renders N posts with an Anthropic round-trip apiece, which must survive both the HTTP connection and a restart. `routers/template_routes._reconciling` wraps *every* template mutation, so a new endpoint cannot forget to trigger it. Progress is polled from `GET /api/reconcile-status` by `static/js/reconcile-banner.js`, loaded in `base.html` on every page — the work rewrites real schedules, so it must be visible from wherever the user is standing. Manual `/re-render` and `/backfill-slots` enqueue the same two job kinds over every enabled slot rather than running inline, so there is one implementation, one progress surface, and no request that blocks for minutes. Every schedule mutation returns `409` while a queue has unfinished jobs.
- **Threads tokens must be refreshed, not just minted** — Meta issues 60-day Threads tokens and renews them via `grant_type=th_refresh_token` using *the access token itself*, not a separate refresh token. That shape doesn't match Twitter/Bluesky, so `ThreadsPoster` originally inherited the base `refresh_if_stale` no-op and a real token silently aged out — every post then failed with an opaque `HTTP 500 code=1` for weeks while the refresh sweep reported success. `supports_token_refresh` now states whether a platform *has* a refresh flow, separately from `refresh_if_stale` returning False for "nothing due"; a test asserts the flag never disagrees with the code.
- **Token metadata is stamped at mint/refresh and mirrored to the DB** — every flow that obtains or renews a token calls `social_credentials.stamp_token_metadata` on the bundle (`acquired_at` always; `expires_at` when the issuer reported a lifetime), and `save_bundle` / `upsert_credential` mirror both onto `social_accounts.token_acquired_at` / `token_expires_at` (migrations 040 / 039) so Settings shows a token's age, lifetime and expiry without a Keychain read. NULL means unknown, never "doesn't expire" (Mastodon tokens genuinely have no expiry; LinkedIn's ~60-day tokens have no refresh flow, so their recorded expiry is the only warning before posting fails). Don't write these columns anywhere else — the bundle is the source of truth and the mirror rides on the bundle writes.
- **Generate-from-source** — On the Promo screen, "Generate from source" runs three parallel Claude tool_use calls (hook / short / segment) over the parent's word-stream transcript, proposes ranges by unit INDEX (never timestamps), then cuts and inserts the accepted ranges through the existing promo chain. 9:16 reframing is the on-device Swift `clipcrop` (YOLO head-tracking) — the old Claude-vision crop pass is gone, and `x_shift_normalized` survives only as a deprecated no-op parameter. Concurrency: 2 hardware-encoder cuts, 8 software cuts — each gated by its own `asyncio.Semaphore`. The promo-chain semaphore (4) is shared: full chains, mid-stream retries, and bulk description updates all take it, so it is the single ceiling on background Claude + YouTube pressure.
- **Clip-proposal prompts are split: contract in code, voice in the DB** — the system prompt for a proposal call is assembled by `clipper._build_index_system_text`. The role, the transcript's line format, and the tool contract are code; the editorial middle (what makes a good clip of this kind, how its title should read) is a user-editable row, `promo_clip_proposals_<kind>`, resolved by `clipper.editorial_block_for_kind` with the seed in `services/prompts` as the fallback. A prompt edit can change the voice but can never break the output format. The user turn carries only the run's material — parent title, already-covered titles, the count, the numbered transcript. Duration bands live in `_PER_KIND_BOUNDS` alone: the validator, the modal's labels, and the count boxes all derive from it, and the model is told nothing it isn't also filtered by. A blank saved body raises `EmptyPromptBodyError` rather than falling back to the seed.
- **Every Claude call is fully logged** — `ai.create_message` / `create_message_async` wrap the SDK so a call cannot exist without a request+response pair in the server log (model, max_tokens, system, messages, tools; then stop_reason, content, usage). Base64 media is summarised, not dumped. `stop_reason == "max_tokens"` logs a WARNING **before** the payload dump, so a formatting failure can't take the truncation signal with it: a truncated tool_use response arrives as a normal 200 and just looks like fewer results. Never call `client.messages.create` directly. Logging can never fail the call it describes — both log steps are guarded and report at ERROR, because a `json.dumps` bug must not kill a request or discard a response already paid for.
- **A rejected clip proposal is data, not a log line** — `_validate_indexed_proposals` returns `(accepted, rejected)` and `propose_clips_for_kind_indexed` returns a `KindProposals` carrying `accepted` / `rejected` / `error` / `raw_count`. `error` (the Claude call failed, count unknown) and `rejected` (the call succeeded, these candidates were refused) are deliberately separate facts. All three reach the review page, because a count alone cannot distinguish "Claude found 7" from "Claude found 23 and we discarded 16" — which is exactly how the original silent-filter bug hid. Validation runs in two passes: independent checks (indices, duration band, title vs the parent's existing clips), then the survivors are ranked **rating → longer → the model's own order** and taken greedily. That ranking is what makes the output cap keep the best N rather than the first N, and what decides which of two overlapping candidates wins; it is deterministic, never random, so a rerun reproduces.
- **One kind's failure never takes down the others** — `propose_all_clips` gathers with `return_exceptions=True`. Beyond the UX, a bare `gather` propagates the first exception *without cancelling its siblings*, so the other Claude calls keep running and bill tokens for results nobody reads.
- **`server.log` rotates at startup only, before the `dup2`** — `main._rotate_server_log` renames the file aside past `SERVER_LOG_MAX_BYTES`, and it must stay ahead of `os.open`/`os.dup2` in `_redirect_stdio_to_log`. Never put a `RotatingFileHandler` on this path: fds 1/2 follow the *inode* through a rename, so the first roll would send every traceback to `server.log.1` and later rolls to an unlinked inode no tool can read — silent loss of the error channel by the machinery meant to preserve it.
- **Retiring a prompt seed is a declared act** — drop it from `_SEEDS_BY_KEY` *and* add it to `prompts._RETIRED_KEYS` with the reason. `get_prompt_with_fallback` then raises `RetiredPromptKey` on every install, instead of raising `KeyError` on a fresh DB while silently generating from retired text on one that has a stale saved row. Saved rows are never deleted — they are the user's writing. A prompt being *renamed* wants a key migration instead, or the customisation is stranded.

## Configuration

All via environment variables (loaded from `.env`). Secrets are **never** read from env/`.env` — the Anthropic API key and all OAuth tokens live only in the macOS Keychain, set via the Settings page (rule E).

| Variable | Default | Purpose |
|----------|---------|---------|
| `ANTHROPIC_MODEL` | `claude-sonnet-4-6` | Model for AI generation |
| `DYS_HOST` | `127.0.0.1` | Server bind address |
| `DYS_PORT` | `8008` | Server port |
| `DYS_DATA_DIR` | `~/.yt-scheduler` | Data directory |
| `DYS_COMMENT_CHECK_MINUTES` | `30` | Comment moderation poll interval |
| `DYS_CAPTION_CHECK_MINUTES` | `15` | Caption availability poll interval |
| `DYS_MAX_SOURCE_FILE_GIB` | `64` | Ceiling on any uploaded source file. One value for both upload paths; enforced before the bytes are read (chunked `/init`, or `Content-Length` on multipart) |
| `DYS_UPLOAD_CHUNK_MIB` | `64` | Wire chunk announced by `/api/uploads/init`. Each chunk is one ArrayBuffer request body in the browser — turn it down if an engine baulks at the size |
| `DYS_THREADS_REDIRECT_URL` | `https://nuclearcyborg.com/apps/scheduler/callback-threads-redirect` | Override for the Threads OAuth `redirect_uri` bounce page (Meta rejects `http://`); bounce-page source is in `cloudflare/` |

Legacy `YTP_*` names (`YTP_HOST`, `YTP_PORT`, `YTP_DATA_DIR`, etc.) are still honored as a fallback for older `.env` files.

## YouTube API Quota

Daily quota is 10,000 units. Notable costs: video upload (100), metadata update (50), caption operations (50-450), search (100), list comments (1). The app minimizes usage by batching requests and caching locally.
