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
- **`comments.py`** — Channel-wide comment mirror: sweeps `commentThreads.list(allThreadsRelatedToChannelId=…)` into `youtube_comments` and serves the dashboard's Recent comments from SQLite
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
- **Failed sends stay visible app-wide** — `GET /api/social/failed-posts` lists posts in status `failed`; `static/js/failed-sends-banner.js` (loaded by `base.html` on every page, same pattern as the reconcile banner) shows them until each post is retried successfully, skipped, or deleted. `social_posts.status` is the single source of truth. An error that exists only in a transient toast is treated as unsurfaced: scheduled sends fail with no page open at all. Ordering is by `failed_at`, **never by `id`** — id is creation order, so a post written weeks ago that failed minutes ago sorted below one written yesterday that failed last week, which is how a five-day-old failure came to head the banner over four from the same afternoon. NULL `failed_at` is a pre-migration row, old by definition, and sorts last.
- **Automatic retry is limited to connect-phase failures, and that is the whole design** — `services/send_failures.py` classifies a send exception at raise time (never by matching the stored message, which is written for a human and gets reworded). "Transient" is too coarse a test: a DNS failure, refused connection or pool timeout means the request **provably never arrived**, so re-sending cannot duplicate; a read/write timeout or a dropped connection means it **may have arrived and only the response was lost**, so a retry mints a second post in front of a real audience. `ThreadsPoster._PUBLISH_AMBIGUOUS_TRANSPORT_ERRORS` already drew that line for one call with a resolve loop behind it; `send_failures` generalises it for the automatic path, which has no such fallback. Anything unrecognised — auth, validation, the non-public-video refusal, a bare 5xx — defaults to **not** retryable, because a wrong "no" costs one click and a wrong "yes" cannot be undone. `retry_failed_sends_job` then has three defences: an atomic claim out of `'failed'` (the job and a user pressing Retry cannot both own the row), `find_recent_duplicate_post` (the one collision a row cannot see from itself — a smart queue re-adding a video creates NEW posts with the same content, so a stale failure retrying afterwards would be the second copy; it is retired to `'skipped'` rather than left looping), and `retry_until`, stamped once on the first failure so repeated failures cannot walk the deadline forward forever. Window comes from the queue's `missed_grace_hours` when there is a queue — "is this still worth posting late?" is editorial and the user answered it there.
- **Giving up on a failed send makes it `'skipped'`, not hidden** — the same state `smart_queue_disposition`'s `remove` sets. An earlier attempt added a `dismissed_at` flag; that was a second source of truth for "is this post done with", and it forced an un-dismiss rule to stop it silencing a recurring failure — machinery that only existed because the state was dishonest. A post nobody is sending is not a failed post that happens to be filtered out. The error text and `failed_at` are kept: it records a decision, it does not rewrite history.
- **`social_posts.intended_at` is the anchor that survives failure** — `mark_failed` clears `scheduled_at` on purpose (a row still holding its scheduling columns is resurrected and re-sent by the restore pass), but `smart_queue_disposition.within_grace` measured the post-late window from exactly that column, so **every** failed post reported as outside its window — one that failed three hours into a 24-hour grace read as expired. `intended_at` mirrors `scheduled_at` when a post is scheduled and is never cleared; callers pass `scheduled_at or intended_at`.
- **`models.social_post.mark_failed` is the only writer of the `'failed'` state** — the mirror of `mark_posted`, and it stamps `social_posts.failed_at` (migration 044). That statement previously existed as ten byte-identical copies across `services/scheduler` and `routers/social_routes`, so a failure carried no time at all: `posted_at` is only set on success, `created_at` is when the post row was *written* (days or weeks earlier for a smart-queue post), and marking a row failed **clears** `scheduled_at` — which is why a failure rendered as a bare "unscheduled". A five-day-old failure was indistinguishable from one happening now. Both surfaces show the age (banner, and the smart-schedule "Didn't go out" list); NULL `failed_at` means a pre-migration row and renders no time rather than a substituted one. A test greps `src/` so an eleventh copy of the raw UPDATE can't reappear and silently skip the stamp.
- **Comments are mirrored locally, not fetched on view** — a project is bound 1:1 to a channel, so `commentThreads.list(allThreadsRelatedToChannelId=…)` returns the whole project's comments for **1 quota unit per 100-thread page**; the per-video `commentThreads.list(videoId=…)` the video detail page uses would need one call per video (~4,300 units/day on a 90-video channel at the moderation interval). A 4-hourly job (`scheduler.sync_comments_job`) upserts into `youtube_comments` (migration 043) and the dashboard renders from SQLite, so the page never waits on YouTube and a revoked token empties nothing. Deliberately separate from `moderation.py`, which fetches per video to enforce the blocklist and stores only what it acted on — that is enforcement, this is the conversation. Ordering is by the top-level comment's time, so a *reply* on an old video never floats its thread up; the sweep therefore walks pages rather than stopping at the first thread it already has, and follow-up `comments.list(parentId=…)` calls fill in threads whose stored reply count is short of `totalReplyCount`. That gate alone was wrong twice over: the fetch stopped at 100 replies, so a busier thread could never satisfy it and was re-requested every sweep forever with no progress (it now pages, and a thread at `COMMENT_SYNC_MAX_REPLY_PAGES`×100 is no longer called short — nor counted as pending, which would suspend the missing-comment inference for good); and a gate that only chases a shortfall can never *correct* anything, so a reply held by YouTube after we stored it kept rendering as live — the held/likely-spam buckets list threads by their **top-level** comment, so nothing else ever mentions that reply again. Threads are therefore also re-read on a clock (`COMMENT_REPLY_REFRESH_HOURS`, tracked per thread in `replies_refreshed_at`), incomplete ones first and least-recently-refreshed within each group, so a limited budget rotates instead of starving. Both budgets (`COMMENT_SYNC_MAX_PAGES`, `COMMENT_SYNC_MAX_REPLY_FETCHES`) report when they truncate rather than presenting a partial sweep as a complete one. `first_seen_at` is when *we* stored the row, never moved by a re-sweep — it is the watermark a push notifier will read; nothing reads it yet. The read path excludes comments the blocklist already rejected (`moderation_log.action = 'deleted'`), because YouTube keeps returning rejected comments and the feed would otherwise hand back the spam moderation removed; a *failed* rejection (`'error'`) is still live and stays visible. That predicate is one constant shared by the listing and the count — if they disagreed, "Showing 10 of N" would count threads the list can't return and Load more would stall short of the end.
- **The dashboard lists comment *threads*, paged by thread** — `comments.list_recent_threads` groups on `COALESCE(parent_comment_id, comment_id)`, and `limit`/`offset` count threads, not comments. The flat, comment-paged list this replaced sorted strictly by `published_at DESC`, so a reply landed rows away from the comment it answered and an answered thread read as ignored — the user hit exactly that and concluded they had never replied. Paging by comment would reintroduce it at the page boundary, which is why the count is `count_threads`. YouTube's model is exactly two levels — a reply to a reply is still parented to the top-level comment, and the `@mention` prefix is the only (textual, unstructured) signal of who it addresses — so `_THREAD_KEY` is the whole hierarchy and there is no recursion. Threads order by newest *visible* comment so a reply on a months-old video comes back up; within a thread, chronological. `owner_has_replied` ("the channel has spoken here at all") and `awaiting_owner_reply` ("the newest comment is not the channel's") are deliberately separate: a viewer who answers your answer puts the ball back in your court, and that thread must not read as handled. A thread whose top-level comment isn't visible — the blocklist rejects a comment without rejecting its replies — returns `top_level_comment: None` and `parent_unavailable: True`; the replies are real comments, so they render under a stated gap rather than being dropped or promoted to top-level, which would present an answer as the question.
- **A blocklist match the dashboard still shows is one we failed to remove** — the listing carries `moderation_action` / `moderation_matched_keyword` per comment, and the UI badges them. `'deleted'` can never appear (those rows are filtered out), so the only values that reach the page are `'error'` — matched, rejection FAILED, comment still live on YouTube — and `'pending'`, an in-flight claim. Rendering an `'error'` comment as an ordinary one is the misleading-fine state rule C exists to forbid: the moderation log says it was acted on, the page implied nothing happened, and both were half true. The `moderation_log` join is a LEFT JOIN because `idx_moderation_log_comment_unique` is UNIQUE on (project_id, comment_id) — it cannot multiply a comment into several rows. YouTube's *own* moderation state is a third, separate thing — see the next bullet.
- **YouTube's moderation state is swept per bucket, and disappearance is inferred** — `comments.snippet.moderationStatus` (populated only for an owner-authorized request) says what *YouTube* thinks, as against `moderation_log` which says what *we* did. YouTube's list filter defaults to `published`, so held and likely-spam threads are **not** a subset of the normal sweep: `_sweep_extra_buckets` asks for each of `youtube.LISTABLE_MODERATION_STATUSES` by name and stamps `youtube_comments.moderation_status` (migration 045). Each bucket reports `ok` separately from `threads: 0` — "no held comments" and "we could not ask" are different answers — and a non-primary bucket's failure is recorded, never fatal; `published` failure still raises, because that bucket *is* the sweep and degrading it would turn a revoked token into an empty comments box. The resource's own field wins over the bucket when both exist (per-comment beats per-query); a reply read through the un-filtered `comments.list` carries no status, so the upsert `COALESCE`s rather than letting that NULL erase a known value every sweep. NULL is unknown, never "published". **`rejected` can never be listed** — the filter accepts only `heldForReview` / `likelySpam` / `published`, so `list_channel_comment_threads` raises on it rather than returning an empty bucket that reads as "nothing was rejected". That is also why *reporting* a comment in Studio (which "permanently hides it from your channel") and an author deleting their own comment are invisible to every query we can make: the comment simply stops coming back. Since the mirror is upsert-only and never deletes, such a comment would otherwise sit on the dashboard looking live forever — so a **complete** sweep (nothing truncated, every supplementary bucket read, no thread owing a reply fetch, not suspiciously empty, not a mass disappearance) stamps `last_seen_in_sweep_at` on the **top-level comments** it saw. A truncated sweep stamps nothing: a comment we did not read is not a comment YouTube stopped returning. Four rules make that inference survivable, and every one of them exists because it was broken first: **(a) top-level only** — a thread carries a ~5-reply preview and a fully-stored thread is never re-read, so stamping replies condemned every reply past the preview on the *second* sweep; **(b) two strikes** — the yardstick is `comment_sweep_runs.previous_swept_at`, not the newest stamp, because one miss is routinely transient (the three buckets are read minutes apart; `order=time` pagination skips under a new arrival). That history is *persisted* rather than derived, because it cannot be derived: each sweep overwrites the stamp on everything it saw, so the previous value stops appearing on any row and the only stamps left are the newest and the stale one, which can never be older than itself; **(c) one sweep per project** — `SweepAlreadyRunning` / HTTP 409, because the sweep that STARTS first can FINISH last (up to 50 sequential reply round trips) and would then write the newer stamp over the older comment set; **(d) refuse mass disappearances** — `_MASS_DISAPPEARANCE_*`, since a video flipped back to unlisted looks exactly like a genuine bulk removal, and `suspicious_empty_sweep` is only this same guard at 100%. The stamp is also clamped monotonic: it is a sweep *ordinal* spelled as a time, so a backwards clock step would otherwise invert the comparison and condemn the comments just confirmed alive. Blast radius is why all this care: a false flag also removes the comment from `visible`, so its thread stops asking for a reply. `awaiting_owner_reply` counts only viewer-visible comments, so a spam reply neither demands an answer nor masks the real question under it — with unknown status counting as visible, since a needless badge beats a hidden question.
- **A thumbs-up counts as answering; the creator heart is invisible to us** — `comments.snippet.viewerRating` is the rating given by whoever authorized the request, and every sweep authorizes as the channel owner, so `'like'` on a mirrored row means *the user* thumbs-upped that comment (migration 047). It therefore clears `awaiting_owner_reply` and sets the thread's `owner_liked_last_word`, which the UI shows — a thread that stopped asking for a reply must say why, or it looks like the rule missed it. Only a *positive* rating can do this: YouTube reports a dislike as `'none'`, so the absence of a like is never evidence of anything, and NULL is unknown rather than `'none'`. The **creator heart** is a different gesture and has **no representation in the Data API at all** — a hearted comment is indistinguishable from an untouched one, so it can never be wired up; don't go looking for it again.
- **The sweep's outcome is persisted, because the sweep runs unattended** — `comment_sweep_runs` (migration 046) holds the last run per project, written by `sync_project_comments` on both paths *including the one that re-raises*, and returned by `GET …/comments` as `last_sweep` so the dashboard shows a persistent warning listing each distinct problem. The sweep's normal caller is a 4-hourly job, so log-only reporting is exactly the unsurfaced-error case rule C forbids: the page went on rendering a stale mirror under a reassuring "Synced 4 hours ago". `_record_sweep_run` never raises — it describes failures and must not become one, least of all on the path already reporting one. The toast and the warning both derive their problem list from the same `sweepProblems()`, so the two can never tell different stories. Related hardening in the same pass: a failed reply follow-up is collected per-thread rather than aborting the sweep (the threads are already stored by then), and a sweep that returns **zero** threads while the mirror holds comments is refused as `suspicious_empty_sweep` rather than believed — believing it would mark every stored comment "gone from YouTube" in a single tick, and a wiped channel is indistinguishable from an API change that answers empty instead of erroring.
- **`static/js/datetime.js` is the one timestamp formatter** — loaded in `<head>` by `base.html`, exposing `window.dysDateTime` (`ensureUtc`, `formatWhen`, `formatAge`, `formatWhenWithAge`). Head placement is deliberate: the banners format during a fetch they start at parse time, and a page's own JS sits in the content block, which the parser reaches *before* the end-of-body script tags. `ensureUtc` is mandatory before `new Date()` on any DB timestamp — SQLite writes `datetime('now')` space-separated and naive, which is not ISO 8601, so the browser falls back to local-time parsing and shifts every stamp by the viewer's offset. This lived as six byte-identical `_ensureUtc` copies (dashboard, home, moderation, promo_videos, socials_compose, video_detail), none reachable from `static/js/` — which is why the banner could not format a timestamp at all. Two tests hold the line: no template may define its own, and any template using `dysDateTime` must extend `base.html`.
- **Threads media via hosted URLs** — Threads takes images and video as an `image_url` / `video_url` that Meta fetches itself, so `ThreadsPoster` uploads the prepared file through `media_hosting.py` and sends the signed URL. `accepts_media` states what the *platform* can do (permanently true); `requires_hosted_media` states what *we* depend on, letting `smart_queue_accept` skip a slot with a real reason when hosting isn't configured. Media containers get a longer poll budget than text ones because Meta downloads the file during that window. More than one attachment is refused rather than truncated — carousels aren't built.
- **Template edits reconcile asynchronously** — A template change queues persisted jobs rather than doing the work in the save request: each one re-renders N posts with an Anthropic round-trip apiece, which must survive both the HTTP connection and a restart. `routers/template_routes._reconciling` wraps *every* template mutation, so a new endpoint cannot forget to trigger it. Progress is polled from `GET /api/reconcile-status` by `static/js/reconcile-banner.js`, loaded in `base.html` on every page — the work rewrites real schedules, so it must be visible from wherever the user is standing. Manual `/re-render` and `/backfill-slots` enqueue the same two job kinds over every enabled slot rather than running inline, so there is one implementation, one progress surface, and no request that blocks for minutes. Every schedule mutation returns `409` while a queue has unfinished jobs.
- **Threads tokens must be refreshed, not just minted** — Meta issues 60-day Threads tokens and renews them via `grant_type=th_refresh_token` using *the access token itself*, not a separate refresh token. That shape doesn't match Twitter/Bluesky, so `ThreadsPoster` originally inherited the base `refresh_if_stale` no-op and a real token silently aged out — every post then failed with an opaque `HTTP 500 code=1` for weeks while the refresh sweep reported success. `supports_token_refresh` now states whether a platform *has* a refresh flow, separately from `refresh_if_stale` returning False for "nothing due"; a test asserts the flag never disagrees with the code.
- **Token metadata is stamped at mint/refresh and mirrored to the DB** — every flow that obtains or renews a token calls `social_credentials.stamp_token_metadata` on the bundle (`acquired_at` always; `expires_at` when the issuer reported a lifetime), and `save_bundle` / `upsert_credential` mirror both onto `social_accounts.token_acquired_at` / `token_expires_at` (migrations 040 / 039) so Settings shows a token's age, lifetime and expiry without a Keychain read. NULL means unknown, never "doesn't expire" (Mastodon tokens genuinely have no expiry; LinkedIn's ~60-day tokens have no refresh flow, so their recorded expiry is the only warning before posting fails). Don't write these columns anywhere else — the bundle is the source of truth and the mirror rides on the bundle writes.
- **Generate-from-source** — On the Promo screen, "Generate from source" runs three parallel Claude tool_use calls (hook / short / segment) over the parent's word-stream transcript, proposes ranges by unit INDEX (never timestamps), then cuts and inserts the accepted ranges through the existing promo chain. 9:16 reframing is the on-device Swift `clipcrop` (YOLO head-tracking) — the old Claude-vision crop pass is gone, and `x_shift_normalized` survives only as a deprecated no-op parameter. Concurrency: 2 hardware-encoder cuts, 8 software cuts — each gated by its own `asyncio.Semaphore`. The promo-chain semaphore (4) is shared: full chains, mid-stream retries, and bulk description updates all take it, so it is the single ceiling on background Claude + YouTube pressure.
- **Clip-proposal prompts are split: contract in code, voice in the DB** — the system prompt for a proposal call is assembled by `clipper._build_index_system_text`. The role, the transcript's line format, and the tool contract are code; the editorial middle (what makes a good clip of this kind, how its title should read) is a user-editable row, `promo_clip_proposals_<kind>`, resolved by `clipper.editorial_block_for_kind` with the seed in `services/prompts` as the fallback. A prompt edit can change the voice but can never break the output format. The user turn carries only the run's material — parent title, already-covered titles, the count, the numbered transcript. Duration bands live in `_PER_KIND_BOUNDS` alone: the validator, the modal's labels, and the count boxes all derive from it, and the model is told nothing it isn't also filtered by. A blank saved body raises `EmptyPromptBodyError` rather than falling back to the seed.
- **Every Claude call is fully logged** — `ai.create_message` / `create_message_async` wrap the SDK so a call cannot exist without a request+response pair in the server log (model, max_tokens, system, messages, tools; then stop_reason, content, usage). Base64 media is summarised, not dumped. `stop_reason == "max_tokens"` logs a WARNING **before** the payload dump, so a formatting failure can't take the truncation signal with it: a truncated tool_use response arrives as a normal 200 and just looks like fewer results. Never call `client.messages.create` directly. Logging can never fail the call it describes — both log steps are guarded and report at ERROR, because a `json.dumps` bug must not kill a request or discard a response already paid for.
- **The clip-proposal call passes its own timeout, on purpose** — `config.CLIP_PROPOSAL_TIMEOUT_SECONDS` (900 s, a generation not a fetch, so it deliberately leaves the 120 s default). Supplying it also disables an SDK behaviour we don't want: for a non-streaming call with no timeout given, the Anthropic SDK estimates duration from `max_tokens` (`3600 * max_tokens / 128_000`) and raises `ValueError: Streaming is required…` above ~21,333 — plus a stricter per-model table capping Opus 4.x at 8,192. It raises locally, so the request never leaves the machine and every kind of a generate run fails identically. That estimate treats a *ceiling* as a *prediction* at a flat ~35 tokens/sec, which penalises exactly the generous `max_tokens` you want in order to never truncate (real responses are 1–6k against a 25k ceiling). Tests pin both halves: that the call passes a timeout, and that an explicit timeout still bypasses the heuristic.
- **A rejected clip proposal is data, not a log line** — `_validate_indexed_proposals` returns `(accepted, rejected)` and `propose_clips_for_kind_indexed` returns a `KindProposals` carrying `accepted` / `rejected` / `error` / `raw_count`. `error` (the Claude call failed, count unknown) and `rejected` (the call succeeded, these candidates were refused) are deliberately separate facts. All three reach the review page, because a count alone cannot distinguish "Claude found 7" from "Claude found 23 and we discarded 16" — which is exactly how the original silent-filter bug hid. Validation runs in two passes: independent checks (indices, duration band, title vs the parent's existing clips), then the survivors are ranked **rating → longer → earliest in the transcript** and taken greedily. That ranking is what makes the output cap keep the best N rather than the first N, and what decides which of two overlapping candidates wins; it is deterministic, never random, so a rerun reproduces.
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
| `DYS_COMMENT_CHECK_MINUTES` | `30` | Comment *moderation* poll interval (blocklist enforcement) |
| `DYS_COMMENT_SYNC_MINUTES` | `240` | Channel-wide comment *mirror* sweep interval — populates the dashboard's Recent comments |
| `DYS_CAPTION_CHECK_MINUTES` | `15` | Caption availability poll interval |
| `DYS_MAX_SOURCE_FILE_GIB` | `64` | Ceiling on any uploaded source file. One value for both upload paths; enforced before the bytes are read (chunked `/init`, or `Content-Length` on multipart) |
| `DYS_UPLOAD_CHUNK_MIB` | `64` | Wire chunk announced by `/api/uploads/init`. Each chunk is one ArrayBuffer request body in the browser — turn it down if an engine baulks at the size |
| `DYS_THREADS_REDIRECT_URL` | `https://nuclearcyborg.com/apps/scheduler/callback-threads-redirect` | Override for the Threads OAuth `redirect_uri` bounce page (Meta rejects `http://`); bounce-page source is in `cloudflare/` |

Legacy `YTP_*` names (`YTP_HOST`, `YTP_PORT`, `YTP_DATA_DIR`, etc.) are still honored as a fallback for older `.env` files.

## YouTube API Quota

Daily quota is 10,000 units. Notable costs: video upload (100), metadata update (50), caption operations (50-450), search (100), list comments (1). The app minimizes usage by batching requests and caching locally.
