# YouTube Publisher — Backend API Reference

This file documents every HTTP endpoint exposed by the FastAPI backend running at http://127.0.0.1:8008. The server binds to loopback only and has **no application-layer authentication** — anything that can open a TCP connection to the port can call any endpoint.

Generated from the router source. When endpoints change, update this file. (CLAUDE.md tracks this convention.)

## Routers / sections

- [Application-level routes (`app.py`)](#application-level-routes-apppy) — HTML pages, static mounts, build identity
- [Projects (`/api/projects`)](#projects-apiprojects) — `project_routes.py`
- [Comments (`/api/projects/{slug}/comments`)](#comments-apiprojectsslugcomments) — `comment_routes.py`
- [YouTube auth (`/auth`)](#youtube-auth-auth) — `auth_routes.py`
- [Videos (`/api/videos`)](#videos-apivideos) — `video_routes.py`
- [Transcripts (`/api/videos/{video_id}/transcripts`)](#transcripts-apivideosvideo_idtranscripts) — `transcript_routes.py`
- [Social posts (`/api/social`)](#social-posts-apisocial) — `social_routes.py`
- [Templates (`/api/templates`)](#templates-apitemplates) — `template_routes.py`
- [Text expansion (`/api/expand_text`)](#text-expansion-apiexpand_text) — `expand_routes.py` (the canonical renderer; every other rendering path delegates here)
- [Item images (`/api/videos/{video_id}/images`)](#item-images-apivideosvideo_idimages) — `item_image_routes.py`
- [Custom variables](#custom-variables) — `global_variable_routes.py`, `project_variable_routes.py`, `item_variable_routes.py`
- [Settings (`/api/settings`)](#settings-apisettings) — `settings_routes.py`
- [Built-in social OAuth flows (`/api/oauth`)](#built-in-social-oauth-flows-apioauth) — `oauth_routes.py`
- [Social credentials (`/api/social-credentials`)](#social-credentials-apisocial-credentials) — `social_credentials_routes.py`
- [YouTube imports (`/api/projects/{slug}/imports`)](#youtube-imports-apiprojectsslugimports) — `import_routes.py`
- [Chunked uploads (`/api/uploads`)](#chunked-uploads-apiuploads) — `uploads_routes.py`

## Conventions

- All `/api/*` endpoints return JSON. Non-`/api` routes return HTML, redirects, or static binary content.
- Error responses follow FastAPI's shape: `{"detail": "..."}` with the appropriate 4xx/5xx status. A `detail` field may be a string or, for some 409/4xx flows, an embedded object (e.g. duplicate-post payloads).
- Many timestamp columns come straight out of SQLite's `datetime('now')`, which produces a naive UTC string (`YYYY-MM-DD HH:MM:SS`). The frontend normalises these via `_ensureUtc()` before formatting. Endpoints that use `datetime.isoformat()` return proper tz-aware ISO 8601 strings.
- Tag lists round-trip through SQLite as JSON-encoded strings in a single column. The API accepts and returns them as native arrays — JSON encoding/decoding is internal.
- Build identity: every response is stamped with `X-DYS-Build-Id` and `X-DYS-Build-Kind` headers via middleware. Clients SHOULD send `X-DYS-Build-Id` back; the server logs a warning when the IDs disagree (a stale tab is talking to a newer server).
- Missing/expired credentials raise the app-level `CredentialAuthError`. In send-path routes this becomes HTTP 401; in scheduled jobs it is logged and the credential row is flagged with `needs_reauth=1`.
- A non-existent `{slug}` path argument always 404s with `Project '<slug>' not found`. A non-existent `{video_id}` 404s with `Video not found`. Validation failures from FastAPI itself surface as 422.

---

## Application-level routes (`app.py`)

These are registered directly on the `FastAPI` instance, not on a router, and are not prefixed.

### `GET /api/build`

**Purpose** — Return the running server's build identity so the macOS shell and browser tabs can compare against their own copy.

**Response 200** — JSON:

```json
{
  "kind": "debug" | "release",
  "version": "0.0.0-dev",
  "build_number": "0",
  "build_date": "2026-04-27T12:00:00+00:00",
  "build_id": "dev-abc123def456"
}
```

**Notes** — When running from source (no bundle-injected `_build_info.py`), each process gets a fresh UUID, so any tab loaded against a previous server sees a mismatch.

### `GET /` → `home.html`
Home — projects list, upcoming items, recent activity. Returns HTML.

### `GET /settings` → `settings.html`
General Settings page (Anthropic key, intervals, OAuth client credentials, etc.). HTML.

### `GET /projects/{slug}` → `dashboard.html`
Project dashboard. **404** if `slug` not found.

### `GET /projects/{slug}/videos/{video_id}` → `video_detail.html`
Per-video detail page. **404** on bad `slug`.

### `GET /projects/{slug}/templates` → `templates.html`
List of templates within a project. **404** on bad `slug`.

### `GET /projects/{slug}/templates/{name}` → `template_edit.html`
Single-template editor. **404** on bad `slug`.

### `GET /projects/{slug}/moderation` → `moderation.html`
Comment-moderation page for a project. **404** on bad `slug`.

### `GET /projects/{slug}/settings` → `project_settings.html`
Per-project settings page. **404** on bad `slug`.

### `GET /projects/{slug}/socials-compose` → `socials_compose.html`
Socials-from-template composer wizard. **404** on bad `slug`.

### `GET /upload` → `upload.html`
Upload form. Reached from the Dashboard's "Upload new video" button.

### `GET /videos/{video_id}` → 307 Redirect
Backwards-compatibility redirect to the **owning** project's detail page (`/projects/{slug}/videos/{video_id}`). `404` when the video doesn't exist — redirecting an unknown id to the default project just produced a second, misleading 404.

### `GET /templates` → 307 Redirect
Backwards-compatibility redirect to `/projects/<DEFAULT_PROJECT_SLUG>/templates`.

### `GET /templates/{name}` → 307 Redirect
Backwards-compatibility redirect to the project that owns the template name: the default project when it owns one by that name, otherwise the single owning project. `404` when no project owns the name, or when several non-default projects do (the detail names the candidate projects) — a guessed redirect landed in an editor that auto-creates a junk template on a missing name.

### `GET /moderation` → 307 Redirect
Backwards-compatibility redirect to `/projects/<DEFAULT_PROJECT_SLUG>/moderation`.

### Static & media

- `GET /static/*` — `StaticFiles` mount serving the app's own bundled assets from `src/yt_scheduler/static/`.
- `GET /media/{filename}` — serves a single file from the configured `UPLOAD_DIR` (typically `~/.yt-scheduler/uploads/`). This is an explicit handler (`media_routes.py`), **not** a directory mount: `filename` must be a single bare name (no separators, no `..`, no leading slash); anything else, a non-existent file, or a missing upload dir → `404`. Supports HTTP `Range` requests (so `<video>` seeking works); responses carry `Cache-Control: no-cache` but the handler does not emit `304`. The API never hands the client absolute filesystem paths — it returns `/media/<name>` URLs (see `thumbnail_url`, `video_file_url`, item-image `url`, `media_urls`), which keeps the client portable and a remotely-hosted server / CLI client viable.

---

## Projects (`/api/projects`)

Source: `src/yt_scheduler/routers/project_routes.py`

### `GET /api/projects/recent-events`

Aliases: `GET /api/projects/__recent-events` (excluded from OpenAPI schema).

**Purpose** — Newest activity log entries across all projects, for the Home page feed.

**Query params**

| Name | Type | Required | Description |
|---|---|---|---|
| `limit` | int | optional | Max rows. Default `7`, max `200`. |
| `offset` | int | optional | Rows to skip, for paging further back. Default `0`. |

Both are **refused with `400`, never clamped** — a silently corrected limit returns a page the caller did not ask for, which reads as the feed having ended.

Ordering is `(created_at DESC, id DESC)`. The tiebreak is load-bearing: a batch operation writes many events with an identical `created_at`, and a `created_at`-only sort is not a total order, so rows would swap between pages and paging would repeat some events and skip others. A page shorter than `limit` is how a caller knows it has reached the end.

**Response 200** — Array of `video_events` rows joined to `videos` and `projects`:

```json
[
  {
    "id": 42,
    "video_id": "abc123",
    "type": "social_post_published",
    "payload": { ... },
    "created_at": "2026-04-27 12:00:00",
    "video_title": "...",
    "project_id": 1,
    "project_name": "Default",
    "project_slug": "default"
  }
]
```

`type` is one of: `created`, `imported`, `uploaded`, `metadata_updated`, `publish_scheduled`, `published`, `social_post_scheduled`, `social_post_published` (see `services/events.py`). `payload` shape varies by type.

### `GET /api/projects/upcoming`

**Purpose** — Upcoming scheduled publishes across all projects.

**Query params** — `limit` (int, default `7`).

**Response 200** — Array:

```json
[
  {
    "video_id": "abc123",
    "title": "...",
    "publish_at": "2026-04-28T15:00:00+00:00",
    "project_id": 1,
    "project_name": "Default",
    "project_slug": "default"
  }
]
```

Filters to videos with `publish_at IS NOT NULL AND status != 'published'`, ordered ascending by `publish_at`.

### `GET /api/projects`

**Purpose** — List all projects.

**Response 200** — Array of project dicts:

```json
[
  {
    "id": 1,
    "name": "Default",
    "slug": "default",
    "youtube_channel_id": "UC...",
    "created_at": "...",
    "updated_at": "...",
    "video_count": 12,
    "scheduled_count": 2
  }
]
```

### `POST /api/projects`

**Purpose** — Create a project.

**Request body** — JSON:

```json
{
  "name": "AI Chess Machine",
  "slug": "ai-chess",
  "kind": "github",
  "project_url": "https://github.com/me/ai-chess"
}
```

`name` is required. `slug` is optional and is auto-derived from `name` via `slugify()` when omitted; must match `^[a-z0-9][a-z0-9-]*$`. `kind` is informational only at create time (`"youtube" | "github" | "social"`); the actual constraint that gates `episode/short/segment` items is whether `youtube_channel_id` is bound, and only the YouTube OAuth flow can bind one. `project_url` is the value behind `{{project_url}}`; for YouTube projects it's auto-populated by the OAuth bind, for GitHub or social-only projects the user supplies it here.

**Response 200** — Newly inserted project row (same shape as `GET /api/projects/{slug}`).

**Errors**

- `400` — `name` empty, slug invalid, or slug collision.

**Side effects** — Inserts into `projects`. Slug is immutable thereafter.

### `GET /api/projects/{slug}`

**Purpose** — Fetch a single project by slug.

**Response 200**:

```json
{
  "id": 1,
  "name": "Default",
  "slug": "default",
  "youtube_channel_id": "UC..." | null,
  "project_url": "https://www.youtube.com/@..." | null,
  "created_at": "...",
  "updated_at": "..."
}
```

**Errors** — `404` if slug not found.

### `PATCH /api/projects/{slug}`

**Purpose** — Update a project's display name and/or `project_url`. Slug is intentionally not renamed.

**Request body** — Any subset:

```json
{ "name": "New Name", "project_url": "https://github.com/me/x" }
```

Pass `project_url: ""` (or `null`) to clear it.

**Response 200** — Updated project dict.

**Errors** — `404` (unknown slug), `400` (empty name).

### `POST /api/projects/{slug}/youtube/refresh-channel-url`

**Purpose** — Re-pull the channel handle from YouTube's `channels.list` and overwrite `projects.project_url` with the canonical channel URL. Used when the upstream channel handle changes (rare) or when the user wants to revert a hand-edited URL back to the canonical YouTube form.

Unlike the OAuth bind, which only seeds `project_url` when it's `NULL`, this endpoint **always overwrites**.

**Response 200** — `{"project_url": "https://www.youtube.com/@...", "channel_handle": "@..."}`.

**Errors** — `404` (unknown slug), `400` (project has no YouTube channel bound), `401` (credentials missing/expired), `502` (YouTube API call failed).

### `DELETE /api/projects/{slug}`

**Purpose** — Delete a project and everything scoped to it.

**Response 200** — `{"status": "ok"}`. Returns OK even when the project doesn't exist (idempotent).

**Errors** — `400` if attempting to delete the Default project.

**Cascades** — Enables `PRAGMA foreign_keys = ON` and deletes the `projects` row. Every table whose `project_id` column declares `REFERENCES projects(id) ON DELETE CASCADE` is wiped for this project (chain visible in `migrations/002_projects.sql`, `006_prompt_templates.sql`, `008_per_project_credentials.sql`):

- `videos` → cascades again to `transcripts` and `video_events` (also `ON DELETE CASCADE` on `video_id`).
- `templates` → cascades to `template_slots`.
- `prompt_templates`, `project_settings`, `project_social_defaults`, `project_social_accounts`, `blocklist`, `moderation_log`.

`social_accounts` rows survive (credentials are install-wide, not per-project). `social_posts` rows are deleted indirectly via the `videos → social_posts` chain. APScheduler jobs (`publish_<video_id>`, `social_post_<id>`) for the removed videos/posts **are NOT torn down by FK** — they become orphans that hit the "row vanished" no-op branch when they fire. (Practical impact is low because scheduled rows are rarely orphaned by project deletion, but worth knowing.)

### `GET /api/projects/{slug}/auto-actions`

**Purpose** — Per-project auto-action toggles (auto-transcribe, auto-description, auto-tags, auto-thumbnail, auto-socials) for the upload and import columns.

**Response 200**:

```json
{
  "upload": {
    "auto_transcribe": true,
    "auto_transcribe_backend": null,
    "auto_transcribe_model": null,
    "auto_description": true,
    "auto_tags": false,
    "auto_tags_include_title": true,
    "auto_tags_include_description": true,
    "auto_tags_include_transcript": true,
    "auto_tags_mode": "replace",
    "auto_thumbnail": true,
    "auto_socials": { "twitter": false, "bluesky": false, "mastodon": false, "linkedin": false, "threads": false }
  },
  "import": { /* same keys, defaults differ: auto_description=false, auto_thumbnail=false, auto_tags_mode="add" */ }
}
```

Stored values are merged on top of the defaults from `services/project_settings.py`.

**Errors** — `404` (unknown slug).

### `PUT /api/projects/{slug}/auto-actions`

**Purpose** — Replace the project's auto-action settings.

**Request body** — `{ "upload": { ... }, "import": { ... } }`. Both must be objects.

**Response 200** — Same shape as `GET`.

**Errors** — `404` (unknown slug), `400` (`upload` or `import` not an object).

**Side effects** — Upserts JSON blobs into `project_settings`.

### `GET /api/projects/{slug}/posting-settings`

**Purpose** — Posting delay/spacing + per-tier default-template settings.

**Response 200**:

```json
{
  "post_video_delay_minutes": 15,
  "inter_post_spacing_minutes": 5,
  "default_template_video": "announce_video",
  "default_template_segment": "announce_video",
  "default_template_short": "announce_video",
  "default_template_hook": "announce_video"
}
```

**Errors** — `404` (unknown slug).

### `PUT /api/projects/{slug}/posting-settings`

**Purpose** — Replace posting settings (the body is merged with defaults on the next read).

**Request body** — Object of any of the keys above.

**Response 200** — Same shape as `GET`.

**Errors** — `404` (unknown slug), `400` (body not an object).

### `GET /api/projects/{slug}/promo-delays`

**Purpose** — Return the project's per-tier promo schedule delays, used by the "Schedule all" batch math. Merged with defaults so every tier/field is present.

**Response 200** — `{"hook": {"initial": {"value": N, "unit": "..."}, "subsequent": {...}}, "short": {...}, "segment": {...}}`. `unit` is one of `minutes | hours | days`. `initial` is the gap from the parent episode's publish time to the first promo of that tier; `subsequent` is the gap between consecutive promos of the tier. Defaults: hook 4h/99h, short 18h/6d, segment 3d/9d.

**Errors** — `404` (unknown slug).

### `PUT /api/projects/{slug}/promo-delays`

**Purpose** — Replace the per-tier promo schedule delays.

**Request body** — Same shape as `GET`. Every tier (`hook`, `short`, `segment`) and both fields (`initial`, `subsequent`) are required; each is `{"value": number ≥ 0, "unit": "minutes"|"hours"|"days"}`.

**Response 200** — Same shape as `GET`.

**Errors** — `404` (unknown slug), `400` (malformed payload — missing tier/field, bad unit, negative/non-numeric value, or a value larger than ~1 year).

### `GET /api/projects/{slug}/prompts`

**Purpose** — Return the project's LLM prompt templates, merged with built-in seed defaults so the UI can always show every editable prompt — even before the user has saved any custom row.

**Response 200** — Array of prompt records:

```json
[
  {
    "key": "description_from_transcript_prompt",
    "name": "Description from transcript",
    "body": "Generate an SEO-friendly YouTube video description...",
    "system": null,
    "is_default": true,
    "default_body": "Generate an SEO-friendly YouTube video description...",
    "default_system": null,
    "variables": ["title", "channel_name", "channel_name_block", "transcript", "transcript_truncated", "transcript_srt", "transcript_srt_truncated", "extra_instructions", "url", "episode_url", "project_url", "parent_url", "parent_title", "parent_description", "parent_tags", "parent_context_block"],
    "system_variables": [],
    "body_required": true
  }
]
```

**Fields:**

- `key` — opaque identifier used in the `PUT` URL. Stable across renames.
- `name` — display label.
- `body` — current user prompt (saved row's body, falling back to seed).
- `system` — current system prompt; `null` when the seed has none and the user hasn't saved one.
- `is_default` — `true` when no row has been saved (UI shows a "Default" badge).
- `default_body`, `default_system` — the seed values, for "Reset to default".
- `variables`, `system_variables` — variable names available in each field, for the editor's chip hints.
- `body_required` — `false` for system-only seeds (e.g. `ai_block_default_system_prompt`); the UI hides the body textarea in that case.

**Errors** — `404` (unknown slug).

### `PUT /api/projects/{slug}/prompts/{key}`

**Purpose** — Save a customised prompt for the project. Body and system are independently overridable; either can fall back to the seed.

**Path params** — `key` must be one of the seed keys returned by `GET /prompts`.

**Request body**:

```json
{
  "body": "Generate an SEO-friendly YouTube video description...",
  "system": "Return ONLY the description.",
  "name": "Description from transcript"
}
```

- `body` — required when the seed declares a non-empty body. Empty string is rejected for those keys.
- `system` — three-state:
  - **Field omitted** → preserve whatever's currently saved (no clobber on partial updates).
  - **`null`** → fall back to the seed default at read time.
  - **String (any, including `""`)** → exact override; `""` explicitly suppresses the system prompt.
- `name` — optional display label; defaults to the seed's name.

**Response 200** — `{"ok": true}`.

**Errors** — `400` (missing body for a seed that requires it), `404` (unknown slug, unknown `key`).

**Side effects** — Upserts into `prompt_templates` keyed on `(project_id, key)`.

### `GET /api/projects/{slug}/social-defaults`

**Purpose** — Return the project's chosen default credential per platform. Used to wire up which X / Bluesky / etc. account a generated post fires from.

**Response 200**:

```json
{
  "twitter": { "social_account_id": 1, "uuid": "...", "username": "alice", "label": "@alice @X" },
  "bluesky": null,
  "mastodon": null,
  "linkedin": null,
  "threads": null
}
```

A platform is `null` when no default is set or when the referenced credential was soft-deleted.

**Errors** — `404` (unknown slug).

### `PUT /api/projects/{slug}/social-defaults/{platform}`

**Purpose** — Set or clear the default credential for one platform.

**Path params** — `platform` must be one of `twitter`, `bluesky`, `mastodon`, `linkedin`, `threads`.

**Request body** — `{"social_account_id": <int> | null}`. `null` (or `""`) clears the default.

**Response 200** — Full social-defaults object (same shape as the `GET`).

**Errors** — `400` (unknown platform, non-int `social_account_id`, platform mismatch with credential), `404` (unknown slug, unknown / soft-deleted credential).

**Side effects** — Upserts into `project_social_defaults` (or deletes the row when `null`).

### `GET /api/projects/{slug}/youtube`

**Purpose** — Return the YouTube channel currently bound to this project. Used by Settings to show "Connected to channel X".

**Response 200**:

```json
{
  "channel_id": "UC...",
  "channel_title": "My Channel",
  "channel_handle": "@mychannel",
  "label": "@My Channel @YouTube",
  "authenticated": true,
  "needs_reauth": false
}
```

`channel_title` and `channel_handle` are populated only when the project's YouTube credentials are valid; otherwise they are empty strings. `needs_reauth` is true when a `youtube_channel_id` is bound but no usable credentials are loaded.

**Side effects** — Calls `youtube.channels().list(mine=True)` (1 quota unit) on each call when authenticated.

**Errors** — `404` (unknown slug).

---

## Comments (`/api/projects/{slug}/comments`)

Source: `src/yt_scheduler/routers/comment_routes.py`

Reads come from the local `youtube_comments` mirror, which a background job
refreshes (`DYS_COMMENT_SYNC_MINUTES`, default 240). Only `/sync` talks to
YouTube.

### `GET /api/projects/{slug}/comments`

**Purpose** — Stored comment *threads* for the whole project, for the dashboard's Recent comments section.

**Query params** — `limit` (int, default `10`, 1–200), `offset` (int, default `0`), `needs_reply` (bool, default `false` — restrict to threads still waiting on a reply). **`limit`/`offset` count threads, not comments** — a page boundary inside a thread would split a conversation across "Load more" and sort a reply away from the comment it answers.

**Response 200**:

```json
{
  "threads": [
    {
      "thread_key": "Ugx...",
      "last_activity_at": "2026-08-03T01:24:00Z",
      "top_level_comment": {
        "comment_id": "Ugx...",
        "youtube_video_id": "dQw4w9WgXcQ",
        "parent_comment_id": null,
        "author_display_name": "@viewer",
        "author_channel_id": "UC...",
        "author_profile_image_url": "https://yt3.ggpht.com/...",
        "text_display": "great episode",
        "like_count": 3,
        "total_reply_count": 1,
        "published_at": "2026-08-01T10:00:00Z",
        "youtube_updated_at": "2026-08-01T10:00:00Z",
        "first_seen_at": "2026-08-01 14:00:00",
        "is_channel_owner": false,
        "is_reply": false,
        "moderation_status": "published",
        "viewer_rating": "like",
        "is_missing_from_youtube": false,
        "moderation_action": null,
        "moderation_matched_keyword": null,
        "local_video_id": "dQw4w9WgXcQ",
        "video_title": "Ep 42 — Shipping Relay",
        "episode_number": 42
      },
      "parent_unavailable": false,
      "replies": [],
      "visible_comment_count": 1,
      "total_reply_count": 1,
      "owner_has_replied": false,
      "awaiting_owner_reply": true,
      "owner_liked_last_word": false,
      "youtube_video_id": "dQw4w9WgXcQ",
      "local_video_id": "dQw4w9WgXcQ",
      "video_title": "Ep 42 — Shipping Relay",
      "episode_number": 42
    }
  ],
  "total_threads": 137,
  "needs_reply_total": 4,
  "all_threads_total": 137,
  "last_synced_at": "2026-08-02 08:00:00",
  "channel_connected": true,
  "last_sweep": {
    "started_at": "2026-08-03 08:00:00",
    "finished_at": "2026-08-03 08:00:12",
    "ok": true,
    "was_complete": false,
    "error": null,
    "detail": { "…": "the /sync summary, verbatim" }
  }
}
```

`last_sweep` is the recorded outcome of the project's last sweep (`null` if none
has run). The sweep's usual caller is a 4-hourly background job, so a failure
happens with no page open — a toast nobody saw is not a surface. `ok` is whether
it ran to completion; `was_complete` is the stricter "it also read the whole
listable surface"; `finished_at` is `null` for a sweep that raised part-way.
`detail` is the `/sync` summary verbatim, so the dashboard can itemise exactly
what went wrong hours after the fact instead of rendering a stale mirror under a
reassuring "Synced 4 hours ago".

YouTube's comment model is exactly two levels — a reply to a reply is still
parented to the top-level comment — so `replies` is flat and there is nothing
deeper. Threads are ordered by `last_activity_at` (the newest *visible* comment
in the thread) descending, so a reply on a months-old video brings its thread
back up; comments within a thread are chronological, matching YouTube.

`top_level_comment` is `null` (and `parent_unavailable` true) when the thread's
parent is not visible — the blocklist rejected it (rejecting a comment does *not*
reject its replies) or it was never mirrored. Those replies are real comments, so
they are returned under a stated gap rather than dropped or promoted to
top-level.

`total_threads` counts the population the request actually paged over, so it
follows `needs_reply`. `needs_reply_total` and `all_threads_total` are always
unfiltered — they label the UI's filter, so both must be right in whichever mode
is showing.

`awaiting_owner_reply` is decided in SQL, not recomputed per thread in Python:
the list is paged and counted in SQL, and a second opinion could only disagree
with the population it was paged from. A thread is settled when we spoke last,
when we thumbs-upped its newest visible comment, **or** when it was marked
handled — see `POST …/threads/{thread_key}/handled`.

`owner_has_replied` and `awaiting_owner_reply` are different facts and both are
returned. The first is "the channel has spoken in this thread at all"; the second
is "the newest visible comment is not the channel's **and** the channel has not
thumbs-upped it" — the ball is back in your court after a viewer answers your
answer, and that thread must not read as handled.

`viewer_rating` is `comments.snippet.viewerRating`: the rating given by whoever
authorized the request, which for every sweep is the channel owner — so `"like"`
means *you* gave the comment a thumbs-up. A **thumbs-up counts as answering**, so
it clears `awaiting_owner_reply` and sets the thread's `owner_liked_last_word`
(which is why the thread stopped asking, and is surfaced rather than left as a
silent exception). Only a positive rating can do this: YouTube deliberately
reports a dislike as `"none"`, so the absence of a like is never evidence of
anything. `null` is unknown, never `"none"`.

The **creator heart** (the channel avatar shown on a comment in the YouTube UI)
is a different gesture and is **not exposed by the Data API at all** — no
property on the comments resource carries it. A hearted comment is
indistinguishable from an untouched one here; only the thumbs-up is visible.

Thread-level `total_reply_count` is YouTube's count, beside `replies.length`
which is what we hold. They disagree only while a sweep still owes this thread a
reply follow-up (`COMMENT_SYNC_MAX_REPLY_FETCHES`); the UI renders "showing 1 of
4" rather than presenting a truncated thread as complete.

Comments the blocklist already rejected on YouTube (a `moderation_log` row with
`action = 'deleted'`) are excluded, and `total_threads` counts the same filtered
set — YouTube keeps returning rejected comments, so without this the feed would
show the spam moderation removed. A **failed** rejection (`action = 'error'`) is
*not* excluded: that comment is still live, and it carries
`moderation_action: "error"` plus `moderation_matched_keyword` so the UI can say
so instead of rendering it as an ordinary comment. `moderation_action` is `null`
for a comment the blocklist never matched, and can be `"error"` or `"pending"`
(an in-flight claim) — never `"deleted"`, since those rows are filtered out.

`moderation_status` is **YouTube's** own state, which is a different thing from
our blocklist enforcement above: `published` (viewers can see it),
`heldForReview`, `likelySpam`, or `rejected`. `null` means unknown — a row stored
before the column existed, or one seen only through a bucket sweep that failed —
and is never to be read as "published". `awaiting_owner_reply` ignores anything
that is not visible to viewers, so a held or spam comment neither creates an
obligation to answer nor masks the genuine question beneath it; an unknown
(`null`) status counts as visible, because a needless badge is a smaller failure
than a hidden real question.

`is_missing_from_youtube` is true when the comment was returned by earlier
**complete** sweeps but missed by the last **two** of them. One miss is not
proof — the three moderation buckets are read minutes apart, so a comment moving
into an already-read bucket is in none of them, and `order=time` pagination can
skip a thread when the window shifts under a new arrival. Two strikes discards
those transients for the price of reporting a genuine removal one sweep later.
Replies are never flagged: a thread carries only a preview of its replies and a
fully-stored thread is never re-read, so a reply's absence proves nothing. Reporting a comment in YouTube
Studio ("permanently hidden from your channel"), or its author deleting it,
removes it without placing it in any listable bucket — and `commentThreads.list`
cannot filter on `rejected` at all, so there is no bucket to find it in. Since
the mirror is upsert-only and never deletes, falling out of a complete sweep is
the only available evidence. A *truncated* sweep never marks anything missing: a
comment we did not read is not a comment YouTube stopped returning.

`youtube_video_id` is `null` for a thread posted on the channel rather than on a
video. `local_video_id` / `video_title` / `episode_number` are `null` when the
thread is on a channel video this app never imported — it is still listed. Every
comment in a thread sits on the same video by construction, so the video is named
once on the thread. `is_channel_owner` is true when the author is the project's
own channel. `last_synced_at` is `null` when no sweep has ever stored a comment —
"never synced", which is not the same as "no comments".

**Errors** — `400` (`limit` outside 1–200, or negative `offset` — refused, not clamped); `404` (unknown slug).

### `POST /api/projects/{slug}/comments/threads/{thread_key}/handled`

**Purpose** — Mark one comment thread as dealt with, so it stops asking for a reply.

**Response 200** — `{"thread_key": "Ugx...", "handled_at": "2026-08-04 21:00:00"}`

For the resolutions YouTube cannot report. Chief among them the **creator
heart**: a real acknowledgement in the YouTube UI with *no representation in the
Data API at all*, so a hearted thread is indistinguishable from an untouched one
and would nag forever.

The stamp is **compared** against the thread's newest activity rather than
cleared by it, so a later reply un-handles the thread automatically — nothing has
to notice and reset a flag. This settles one exchange, never the conversation.

`DELETE` on the same path undoes it.

**Errors** — `404` (unknown slug, or no thread with that key in this project —
the state table is keyed by a caller-supplied string, so accepting an unknown one
would store a row that can never match anything and report success for work that
did nothing). An **orphan** thread, whose top-level comment the blocklist
removed, is a valid target: the key is matched as the listing computes it, not by
requiring a top-level row.

### `POST /api/projects/{slug}/comments/sync`

**Purpose** — Sweep the project's channel now and upsert what comes back. The background job does the same thing on its own schedule; this is the user asking for it immediately.

**Response 200**:

```json
{
  "project_slug": "drew-and-dan",
  "threads": 214,
  "comments_seen": 288,
  "new": 6,
  "updated": 282,
  "pages_truncated": false,
  "reply_fetches": 3,
  "reply_refreshes": 2,
  "threads_with_unfetched_replies": 0,
  "threads_at_reply_cap": 0,
  "refreshes_deferred": 0,
  "refreshes_deferred": 0,
  "threads_with_replies_truncated": 0,
  "reply_fetch_errors": [],
  "suspicious_empty_sweep": false,
  "moderation_buckets": {
    "heldForReview": {"ok": true, "threads": 2, "pages_truncated": false, "error": null},
    "likelySpam": {"ok": true, "threads": 0, "pages_truncated": false, "error": null}
  },
  "sweep_was_complete": true,
  "swept_at": "2026-08-03 12:00:00",
  "previous_swept_at": "2026-08-03 08:00:00",
  "mass_disappearance": null
}
```

`pages_truncated` is true when the sweep stopped at `COMMENT_SYNC_MAX_PAGES` with
pages still available; `threads_with_unfetched_replies` counts threads left over
by `COMMENT_SYNC_MAX_REPLY_FETCHES`. Both are reported so a partial sweep can't
present itself as a complete one; the next sweep picks up the remainder.

`moderation_buckets` reports the extra sweeps for the states viewers cannot see.
YouTube's default filter is `published`, so these are *not* a subset of the main
sweep and must be asked for by name. Each carries `ok` separately from
`threads: 0` — "no held comments" and "we could not ask" are different answers.
A bucket failure is recorded and skipped rather than aborting the sweep; a
failure of the `published` bucket is the sweep's failure and still raises (a
degraded one would turn a revoked token into a silently empty comments box).

A thread's replies are re-read for two distinct reasons, counted separately.
`reply_fetches` covers threads holding fewer replies than `totalReplyCount`;
`reply_refreshes` covers threads whose counts already match but whose replies
have not been re-read within `COMMENT_REPLY_REFRESH_HOURS`. The refresh exists
because it is the **only** way a reply's `moderation_status` can ever be
corrected: the held and likely-spam buckets list threads by their *top-level*
comment, so a reply held after we first stored it is never mentioned again by
any other call, and the dashboard would go on showing it as an ordinary live
comment. Incomplete threads are served before stale ones, and within each group
the least recently refreshed first, so a limited budget rotates through the
channel rather than starving the same threads every sweep.

`threads_with_unfetched_replies` counts only threads we know are **short** and
did not get to. A thread merely due for a staleness refresh is counted in
`refreshes_deferred` instead and does **not** make the sweep incomplete: the
refresh puts every reply-bearing thread on the due list once a day, so on any
channel with more of them than the budget that list is never empty, and counting
it as unread content would make every sweep permanently incomplete — which
permanently suspends `is_missing_from_youtube` and leaves the warning banner
nagging forever.

`threads_at_reply_cap` counts threads already holding as many replies as we are
willing to fetch (`COMMENT_SYNC_MAX_REPLY_PAGES` × 100). They are deliberately
**not** counted as having unfetched replies: no future sweep can make progress
on them, and treating them as pending would keep every sweep incomplete and so
permanently suspend `is_missing_from_youtube` for the project.
`threads_with_replies_truncated` counts threads whose reply read hit that page
cap during this sweep.

`reply_fetch_errors` lists threads whose reply follow-up failed. One unreadable
thread does not abort the sweep — the threads are already stored by that point,
and throwing a good sweep away over one bad follow-up would cost more than it
saves — but it does mean we did not see everything.

`suspicious_empty_sweep` is true when YouTube returned no threads at all while
the mirror holds comments. Believing that would mark every stored comment "gone
from YouTube" in one tick, and a wiped channel is indistinguishable from a broken
call that answers empty instead of erroring — so the inference is declined and
the reason reported rather than guessed either way.

`mass_disappearance` is non-null when the sweep failed to return a large share
of the comments it had previously seen (both an absolute floor and a fraction
must be exceeded). Believing that would condemn all of them at once, and a video
flipped back to unlisted looks identical to a genuine mass removal — so the
inference is declined and the counts reported. `suspicious_empty_sweep` is the
same guard with its threshold at 100%.

`swept_at` / `previous_swept_at` are the last two stamps written to the comment
rows; the older is the two-strike yardstick. They are persisted because the
comment rows cannot answer it — each sweep overwrites the stamp on everything it
saw, so the previous sweep's value stops appearing anywhere.

`sweep_was_complete` is true only when nothing was truncated, every
supplementary bucket was read and succeeded, no reply fetch failed, and the
sweep was neither suspiciously empty nor a mass disappearance. It is
the precondition for `swept_at` being stamped onto the rows that were seen. That
stamp is what makes `is_missing_from_youtube` computable; without it nothing can
be called missing.

Every sweep — including one that raises — is persisted to `comment_sweep_runs`
and returned by `GET …/comments` as `last_sweep`.

**Side effects** — `commentThreads.list(allThreadsRelatedToChannelId=…)` once per moderation bucket (3): up to `COMMENT_SYNC_MAX_PAGES` (20) pages for `published` and `COMMENT_SYNC_MAX_PAGES_PER_MODERATION_BUCKET` (5) for each of the other two, at 1 quota unit per page, plus 1 unit per reply follow-up (up to 50). Quota is per **installation**, not per channel — every project draws on the same 10,000/day pool. Upserts `youtube_comments` and `comment_sweep_runs`.

**Errors** — `400` (project has no YouTube channel bound); `404` (unknown slug); `409` (a sweep is already running for this project — refused rather than queued, because two concurrent sweeps corrupt the `is_missing_from_youtube` watermark); `500` (YouTube error, with the real error text).

---

## YouTube auth (`/auth`)

Source: `src/yt_scheduler/routers/auth_routes.py`

### `GET /auth/status`

**Purpose** — Status payload for the YouTube auth section of Settings.

**Query params** — `project_slug` (string, default `default`).

**Response 200** — One of:

```json
{ "authenticated": false, "client_secret_uploaded": true, "storage": "keychain", "project_slug": "default" }
```

```json
{
  "authenticated": true,
  "valid": true,
  "client_secret_uploaded": true,
  "storage": "keychain",
  "project_slug": "default",
  "client_id": "1234567890-abcdef..."
}
```

`client_id` is masked to first 20 chars when present.

### `POST /auth/login`

**Purpose** — Run the OAuth installed-app flow against `project_slug` (opens a system browser; this is the legacy CLI-style auth, distinct from the web flow under `/api/oauth/youtube/*`).

**Query params** — `project_slug` (string, default `default`).

**Response 200** — `{"status": "ok" | "error", "message": "..."}`. Always returns 200; failure is signalled in the body.

### `POST /auth/logout`

**Purpose** — Clear stored credentials for one project. Leaves the install-wide `client_secret` intact.

**Query params** — `project_slug` (string, default `default`).

**Response 200** — `{"status": "ok" | "error", "message": "..."}`.

### `POST /auth/upload-client-secret`

**Purpose** — Persist the Google Cloud OAuth client JSON to Keychain (no on-disk copy).

**Request body** — multipart/form-data with a single `file` field containing `client_secret.json` from Google Cloud Console.

**Response 200** — `{"status": "ok", "message": "Client secret saved to Keychain"}`.

**Errors** — `400` (file is not valid JSON or doesn't decode as UTF-8).

### `DELETE /auth/client-secret`

**Purpose** — Remove the install-wide OAuth client. After this, all projects' tokens become unusable until a new client secret is uploaded and re-auth runs.

**Response 200** — `{"status": "ok"}`.

### `GET /auth/client-secret/status`

**Purpose** — Cheap probe used by the UI to decide whether to show the upload form vs. the Connect button.

**Response 200** — `{"uploaded": true | false}`.

---

## Videos (`/api/videos`)

Source: `src/yt_scheduler/routers/video_routes.py`

### `GET /api/videos`

**Purpose** — List tracked videos.

**Query params**

| Name | Type | Required | Description |
|---|---|---|---|
| `project_slug` | string | optional | Filter to one project. When omitted, returns every video across every project (used by import / admin views). |
| `include_children` | bool | optional, default `false` | When `false`, hides rows whose `parent_item_id` is set (promo children); the Dashboard listing relies on this. Set `true` to retrieve children too (used by the import-dedup branch and admin views). |

**Response 200** — Array of video rows from `videos` (every column), ordered `created_at DESC`, with one transformation: the absolute-path columns `thumbnail_path` and `video_file_path` are **removed** and replaced by `thumbnail_url` (`/media/<name>` or `null`), `video_file_url` (`/media/<name>` or `null`), and `video_file_name` (the bare filename or `null`). `tags` is the raw JSON-encoded string from the column (the frontend `JSON.parse`s it).

**Errors** — `404` if `project_slug` is given but unknown.

### `GET /api/videos/transcription-backends`

**Purpose** — Enumerate which on-device transcription backends are usable on this machine.

**Response 200** — Array (shape determined by `services/transcription.list_available_backends()`); each element includes a backend id (e.g. `mlx-whisper`, `whisper.cpp`, `macos-speech`) and human-readable info.

### `GET /api/videos/scheduled`

**Purpose** — List videos that currently have an APScheduler `publish_*` job pending.

**Response 200** — Array of `{video_id, job_id, run_date}` (`run_date` is ISO 8601 or `null`).

### `GET /api/videos/{video_id}/events`

**Purpose** — Per-video activity log (newest first).

**Query params** — `limit` (int, default `200`).

**Response 200** — Array of `video_events` rows with `payload` decoded:

```json
[ { "id": 1, "video_id": "abc", "type": "uploaded", "payload": {"platform":"youtube","url":"..."}, "created_at": "..." } ]
```

### `GET /api/videos/{video_id}/auto-actions`

**Purpose** — Per-video background-chain progress for the polling UI. Written by the Promo flow **and** by the detail page's transcribe-then-describe chain (see [`POST .../generate-description`](#post-apivideosvideo_idgenerate-description)).

**Response 200** — `{"state": "...", "last_error": "..." | null, "progress_message": "..." | null, "updated_at": "..."}`. `state` is one of the persisted `videos.auto_action_state` values (`generating_title`, `uploading`, `probing`, `transcribing`, `generating_desc`, `generating_tags`, `pushing_metadata`, `ready`) or `failed:<step>` after a failure; `null` means no chain has ever touched this row. `progress_message` is the live human-readable line for steps that report granular progress (e.g. `"Transcribing on-device… 42%"`, from the Apple SpeechAnalyzer backend — the Whisper backends report none) and is `null` otherwise.

**Errors** — `404` (video not found).

### `POST /api/videos/{video_id}/auto-actions/retry`

**Purpose** — Re-run the Promo auto-action chain from a specific step onward. Used by the per-card "Retry <step>" button when state lands on `failed:<step>`.

**Query params** — `step` (one of the `PROMO_STEP_ORDER` values).

**Response 200** — `{"status": "ok", "video_id": "...", "step": "..."}`.

**Errors** — `400` (unknown step name, video missing).

**Side effects** — Sets `auto_action_state` back to `step`, kicks off `auto_actions._resume_promo_chain` in a background task. Steps are gated by idempotency (skip if the column they produce is already populated), so a retry that lands on a now-resolved failure walks straight through to `ready`.

### `GET /api/videos/{video_id}`

**Purpose** — Full details for a single video, plus a live YouTube readback.

**Response 200** — The local row (all `videos` columns, with `thumbnail_path` / `video_file_path` / `youtube_thumbnail_path` rewritten to `/media/...` URLs as `thumbnail_url` / `video_file_url` / `video_file_name` / `youtube_thumbnail_media_url`) plus either `youtube_data` (the full `videos.list()` response) or `youtube_data_error` (string) if the readback failed.

**Side effects** —

- **Auto-sync from YouTube (C2, migration 010)**: when a `youtube_data` readback succeeded, the four canonical user-editable fields (`title`, `description`, `tags`, `privacy_status`) are diffed against the local row and any drifted field is `UPDATE`d in place so the response always reflects current YouTube state.
- **Dual-thumbnail refresh (C3, migration 018)**: schedules a fire-and-forget background task (`services.thumbnail_sync.schedule_refresh`) that re-downloads the YouTube-side thumbnail when its URL changed and runs a Claude-vision compare against the user's local copy. Verdict lands on `thumbnail_compare_verdict` / `thumbnail_compared_at` for the next GET to pick up. Idempotent per video — overlapping requests share the same in-flight task.

**Errors** — `404` (no local row).

### `POST /api/videos/upload`

**Purpose** — Upload a video to YouTube and track it inside a project. The bytes have already been streamed to disk via the chunked-upload protocol (`POST /api/uploads/init` → `/chunk/{offset}` → `/finalize`) — this endpoint takes the resulting `upload_id` instead of a multipart body.

**Request body** — `application/json`:

| Field | Type | Required | Description |
|---|---|---|---|
| `upload_id` | string | yes | Finalized chunked-upload id for the video file. |
| `thumbnail_upload_id` | string | no | Finalized chunked-upload id for an optional thumbnail. |
| `title` | string | yes | Video title. |
| `description` | string | no | Default `""`. |
| `tags` | string | no | Comma-separated tag list. |
| `pinned_links` | string | no | Free-form text appended after generated descriptions. |
| `privacy_status` | string | no | `unlisted` (default), `private`, `public`. |
| `publish_at` | string | no | ISO 8601 future timestamp; tells YouTube to scheduled-publish at that time. |
| `project_slug` | string | no | Target project. Default `default`. |
| `item_type` | string | no | One of `episode | short | segment | hook`. Default `episode`. `standalone` is rejected here — standalone items don't go through YouTube; use `POST /api/videos/items` instead. |
| `parent_item_id` | string | no | Optional parent item id. Required-shape only for `short`, `segment`, `hook`; rejected for `episode`. The FK is enforced by `videos.parent_item_id REFERENCES videos(id) ON DELETE SET NULL`. |

The endpoint refuses with `400` when the target project has no YouTube channel bound (`youtube_channel_id IS NULL`) — uploads to YouTube need a bound channel.

**Response 200**:

```json
{
  "status": "ok",
  "video_id": "abc123",
  "youtube_url": "https://youtu.be/abc123",
  "thumbnail_error": "..."  // only present if thumbnail upload failed
}
```

**Errors** — `404` (unknown `project_slug`), `400` (project has no YT channel; invalid `item_type`; episode with non-empty `parent_item_id`; `parent_item_id` not found), `500` (YouTube upload failed).

**Side effects** — Saves files to `UPLOAD_DIR`; calls `youtube.upload_video` (~100 quota); inserts into `videos` with `item_type`, `parent_item_id`, and `url = "https://youtu.be/<id>"` populated from the upload result; records `created` (carrying `item_type`) and `uploaded` events; fires `auto_actions.run_post_create_actions(... source="upload")` in the background (transcribe / describe / etc.).

**Renderer (background path)** — When the project's auto-actions matrix has auto-gen-socials enabled, the background job renders each platform's slot body through the same engine as [`POST /api/expand_text`](#post-apiexpand_text). Same variables and same `{{var!}}` / `{{var??default}}` / `{{#var}}…{{/var}}` / `{{ai: ...}}` / `{{ai[system]: ...}}` semantics — there is no separate template engine for the auto path.

### `POST /api/videos/items`

**Purpose** — Create an item that does **not** go through YouTube. Used for `standalone` items (a GitHub-repo post with screenshots, an "AI Chess" project announcement, an image-only Bluesky post) and for `hook` items where the user wants to post the clip directly to social without also uploading to YouTube.

**Request body** — `application/json`. When a video / thumbnail is attached, its bytes go through the chunked-upload protocol first.

| Field | Type | Required | Description |
|---|---|---|---|
| `title` | string | yes | Item title — also serves as the social-post body's `{{title}}`. |
| `description` | string | no | Default `""`. Available as `{{description}}`. |
| `tags` | string | no | Comma-separated. Available as `{{tags}}` / `{{hashtags}}`. |
| `project_slug` | string | no | Default `default`. |
| `item_type` | string | no | One of `standalone | hook`. Default `standalone`. `episode/short/segment` are rejected here — they need YouTube and go through `/api/videos/upload`. |
| `parent_item_id` | string | no | Optional parent item id (e.g. a hook attaching to its episode). Rejected for `standalone`. |
| `url` | string | no | The value behind `{{url}}` for this item. For a hook attached to a parent, omit this and `{{url}}` will resolve to the hook's own URL (NULL → empty); use `{{episode_url}}` to link to the parent. |
| `upload_id` | string | no | Finalized chunked-upload id for the item's video file. When present, this is the file `{{video}}` attaches in templates and what the platform-specific Posters upload as a media asset. |
| `thumbnail_upload_id` | string | no | Finalized chunked-upload id for an optional thumbnail. |

**Response 200**:

```json
{
  "status": "ok",
  "video_id": "<22-char id>",
  "item_type": "standalone",
  "url": "https://github.com/me/x" | null
}
```

**Errors**

- `400` — Invalid `item_type`; `parent_item_id` set on a standalone; `parent_item_id` not found.
- `404` — `project_slug` not found.

**Side effects** — Saves uploaded files under `UPLOAD_DIR`; inserts a `videos` row with `item_type`, `parent_item_id`, `url`, and `status='ready'`; records a `created` event. **Does not call YouTube.** Use `POST /api/videos/{video_id}/images` afterwards to attach additional images.

### `PUT /api/videos/{video_id}`

**Purpose** — Update video metadata (title, description, tags, privacy, publish time, pinned links, status, manual tier override, optional episode number).

**Request body** — Object with any subset of: `title`, `description`, `tags` (list), `privacy_status`, `publish_at`, `pinned_links`, `status`, `tier`, `episode_number`. `episode_number` is local-only metadata (never sent to YouTube); `null` or `""` clears the value.

**Response 200** — `{"status": "ok"}`.

**Errors** — `404` (no local row), `400` (invalid `tier` value — must be one of `hook`, `short`, `segment`, `video`, `null`, or `""`; or `episode_number` not coercible to an integer), `500` (YouTube update failed).

**Side effects** — Calls `youtube.update_video_metadata` (50 quota), reads back via `youtube.get_video` to capture any silent coercion (privacy clamp, tag truncation), writes confirmed values to the DB, and records a `metadata_updated` event with a per-field `{old, new}` diff for changed tracked fields. When the body includes `publish_at`, that field is **not** written directly — it routes through `services/scheduler.apply_user_reschedule(...)` so the APScheduler `publish_<video_id>` job actually re-registers, `publish_at_manual` flips to `1`, and the promo cascade fires (children-of-parent when the row is a primary, same-tier siblings when the row is a child).

### `POST /api/videos/{video_id}/transcribe`

**Purpose** — Transcribe a video locally using on-device speech recognition.

**Query params** — `confirm_unlist` (bool, default `false`). See "private video" path below.

**Request body** (optional):

```json
{ "model": "large-v3", "language": "en", "backend": "mlx-whisper" }
```

`model` has **no default**. `language` is auto-detected when omitted.

`backend` forces a specific backend (`mlx-whisper`, `whisper.cpp`, `macos-speech`). Omit it and the service auto-detects in the order `macos-speech` → `whisper.cpp`. **`mlx-whisper` is never auto-selected** — it loads multi-gigabyte weights and a Metal allocator into the long-lived server process, so it runs only when named explicitly here or chosen in the UI.

Returns **400** when `backend` is unknown, or when a Whisper backend (`mlx-whisper`, `whisper.cpp`) is named without a `model`. A bare call with neither field resolves to `macos-speech`, which takes no model. The response echoes the resolved `model` (`null` for `macos-speech`) so the choice is never invisible.

**Response 200**:

```json
{
  "status": "ok",
  "backend": "mlx-whisper",
  "model": "large-v3",
  "language": "en",
  "segments": 152,
  "word_count": 1840,
  "has_word_timestamps": true,
  "characters": 12450,
  "srt_path": "...",
  "vtt_path": "...",
  "json_path": "...",
  "transcript_preview": "..."
}
```

**Errors**

- `404` — Video not found in DB.
- `400` — Video file not found locally and not imported (caller must re-upload), transcription backend error.
- `409` — Imported video is private on YouTube. Body is `{"private_video": true, "video_id": "...", "message": "..."}`. Caller re-issues with `?confirm_unlist=true` to flip the video to unlisted (`youtube.set_video_privacy`) and download it for transcription.

**Side effects** — Optionally flips YouTube privacy to `unlisted`; downloads video via pytubefix; runs transcription; writes SRT / VTT / JSON files; upserts a `transcripts` row; updates `videos.transcript`, `transcript_id`, `transcript_source`, status. Records a `metadata_updated{transcript: {old, new}}` event when the transcript changed.

### `POST /api/videos/{video_id}/generate-description`

**Purpose** — Generate an SEO description from the video's transcript. With no transcript, transcribes the video on-device first and then describes from the result (in the background).

**Request body** (optional):

```json
{ "extra_instructions": "...", "mode": "auto" | "transcript" | "frames" }
```

`mode=auto` (default) and `mode=transcript` both describe from the transcript. **Neither falls back to keyframes** — keyframes describe a different source than the caller asked for and route through a different prompt, which can silently drop whatever a project's transcript prompt requires. When no transcript exists, the route instead queues a background transcribe-then-describe chain and returns `202`. `frames` forces frame-based generation even when a transcript exists — the explicit escape hatch for a video whose content is purely visual.

**Response 200** — `{"description": "<full text incl. pinned_links>", "raw_ai_description": "<just the AI output>"}`.

**Response 202** — `{"queued": true, "state": "transcribing", "message": "..."}`. No transcript existed, so on-device transcription started in the background and the description will follow. Progress is published to `videos.auto_action_state` + `auto_action_progress_message` — poll [`GET /api/videos/{video_id}/auto-actions`](#get-apivideosvideo_idauto-actions). The work is detached from the request, so the client may navigate away. On completion the description is staged in `videos.generated_description` and the state becomes `ready`; a video with no usable speech ends at `failed:transcribing` with an error saying so rather than quietly falling back to keyframes.

**Errors**

- `404` — Video not found.
- `400` — No transcript and no local video file to transcribe from, or `mode=frames` without a local video file.
- `502` — Anthropic auth/transport failure (special-cased for 401 with a message asking the user to update their API key) or ffmpeg returning no usable keyframes.

**Side effects** — Calls Anthropic API; for frames mode also calls `ffmpeg` to extract keyframes; writes `videos.generated_description`. The applied description includes `pinned_links` appended after the AI text. The `202` path additionally runs on-device transcription and writes the transcript columns (as the upload/import auto-action chain does). Angle brackets (`<`, `>`) are substituted for guillemets in generated titles/descriptions — YouTube rejects them outright.

**Renderer** — `mode=transcript` (and the transcript leg of `auto`) substitutes the prompt body from `prompt_templates.description_from_transcript_prompt` through the same engine as [`POST /api/expand_text`](#post-apiexpand_text), then sends the substituted prompt to Claude in a single call. Any `{{ai: ...}}`, `{{var!}}`, `{{var??default}}`, or `{{#var}}…{{/var}}` section syntax in the prompt-template body is honoured. For a promo child (row has `parent_item_id`), resolution prefers the `description_from_transcript_prompt_promo` variant: saved promo row → promo seed → saved base row → base seed. `mode=frames` substitutes `prompt_templates.description_from_frames_prompt` and attaches the keyframes to the same user turn. Both modes render with the merged prompt-variable dict (parent fields, `episode_url`, inherited item variables, the `transcript*` family) and also send the saved `system_body` (if any) to Claude — edit it in Project Settings → LLM prompt templates.

### `POST /api/videos/{video_id}/generate-tags`

**Purpose** — Suggest YouTube tags for a video with Claude, from its metadata or from extracted keyframes.

**Request body** (optional):

```json
{ "mode": "metadata" | "frames" }
```

`mode=metadata` (default) uses the title + description + transcript. `mode=frames` samples keyframes from the local video file and tags from what's on screen — handy when there's no transcript.

**Response 200** — `{"tags": ["...", "..."]}` (lowercased, comma-stripped). The result is **not** persisted; the caller stages it in the editor and commits via the normal metadata update, mirroring `generate-description`'s staging behaviour.

**Errors**

- `404` — Video not found.
- `400` — `mode=frames` without a local video file.
- `502` — Anthropic auth/transport failure (special-cased for 401 with a message asking the user to update their API key) or ffmpeg returning no usable keyframes.

**Side effects** — Calls Anthropic API; for frames mode also calls `ffmpeg` to extract keyframes. No DB write.

**Renderer** — `mode=metadata` substitutes the prompt body from `prompt_templates.tags_from_metadata_prompt` through the same engine as [`POST /api/expand_text`](#post-apiexpand_text). `mode=frames` substitutes `prompt_templates.tags_from_frames_prompt` and sends it alongside the keyframes. Both render with the merged prompt-variable dict (parent fields and inherited item variables), so the seed's parent-tags guidance actually sees the parent's tags for promo children. (No `_promo` seed ships for the tags prompts yet, so promo resolution falls through to the base prompt.) Each row's saved `system_body` (or the seed's default — "You return ONLY a comma-separated list of tags, no preamble.") is sent as the system message; edit either in Project Settings → LLM prompt templates.

### `POST /api/videos/{video_id}/apply-description`

**Purpose** — Push the previously generated description to YouTube and into the local row.

**Response 200** — `{"status": "ok"}`.

**Errors** — `404` (video not found), `400` (no `generated_description` to apply).

**Side effects** — Calls `youtube.update_video_metadata` (50 quota); flips `videos.status` to `ready`; records a `metadata_updated{description}` event when changed.

### `POST /api/videos/{video_id}/publish`

**Purpose** — Publish an item immediately. Behaviour branches on `item_type`:

| `item_type` | YouTube step | Social step |
|---|---|---|
| `episode`, `short`, `segment` | Required: flip privacy to `public`. If the YT call fails, the social step does **not** run (so we don't blast a link to a non-public video). | Sends every `status='approved'` social post for the video. |
| `hook` | Optional: when `videos.url` looks like a YouTube URL (i.e. the hook was uploaded to YT), flip privacy. Otherwise the YT step is skipped. | Sends every `status='approved'` social post (the hook's video file is the social post's media). |
| `standalone` | **Skipped entirely.** No YT API call. The local row is still flipped to `status='published'`. | Sends every `status='approved'` social post. |

**Response 200** — Summary dict produced by `scheduler.publish_video_job`:

```json
{
  "video_id": "abc",
  "published": true,
  "youtube_skipped": true,           // present only when YT step was skipped
  "social_results": { "twitter": {"status": "posted", "url": "..."}, ... }
}
```

**Cascades** — **Sends every `status='approved'` social post for this video** (per-post status flipped to `'sending'` then `'posted'` on success, `'failed'` with `error` on failure). Posts already in `'sending'` are skipped (another worker holds them); per-post APScheduler jobs that were pending get claimed atomically — whichever path posts first wins, the loser sees the row already moved out of `'approved'` and bails.

**Side effects** — Holds the per-video publish lock. For YT-publishing types: calls `youtube.update_video_metadata(privacy_status="public")` (50 quota). For all types: sets `videos.status = 'published'`; records a `published` event (carrying `item_type` and `url`); iterates over `status='approved'` social posts and sends each one.

### `POST /api/videos/{video_id}/schedule`

**Purpose** — Schedule a video to flip to public (and fire its social posts) at a specific future time. User-driven reschedule path — also fires the promo cascade.

**Request body** — `{"publish_at": "2026-04-28T15:00:00-07:00", "force": false}` (ISO 8601; `force` optional).

**Response 200**:

```json
{ "status": "ok", "job_id": "publish_<video_id>", "publish_at": "...",
  "cascaded_children": ["..."], "cascaded_siblings": ["..."], "message": "..." }
```

**Errors** — `400` (missing `publish_at`, invalid format, time not in future). `409` with `{"detail": {"publish_blockers": ["..."], "hint": "..."}}` when the video's content is in an error state: empty/placeholder description on a YouTube-listed item, a `failed:*` auto-action chain, or draft/approved social posts holding render-error text. Re-send with `force: true` to schedule anyway — except the pending-generation placeholder, which the fire-time job refuses to push public regardless (fix the description). The fire-time job also refuses to send any approved post whose content is a render-error placeholder, marking it `failed` with the reason.

**Cascades** —

* **Per-post jobs are re-baselined.** Any pending scheduled posts for this video (rows with `scheduler_job_id IS NOT NULL`) are cancelled via `cancel_scheduled_post()` and re-scheduled at staggered offsets driven by the project's `post_video_delay_minutes` and `inter_post_spacing_minutes`. Hand-retimed per-post jobs from a prior `POST /api/social/posts/{post_id}/schedule` call are intentionally overwritten — re-scheduling the video is the explicit "reset everything" action.
* **Promo cascade.** When the target is a primary (`parent_item_id IS NULL`), every auto-anchored child (`publish_at_manual = 0`) shifts by the same delta the parent just moved; manually-overridden children stay put. When the target is a child, later same-`item_type` siblings whose `publish_at > old_publish_at` and `publish_at_manual = 0` shift by the same delta; manual siblings stay put. The IDs that were moved are returned in `cascaded_children` / `cascaded_siblings`.

**Side effects** — Sets `videos.publish_at_manual = 1` on the target (user-initiated). Registers an APScheduler `DateTrigger` job (`publish_<video_id>`); cancels and re-attaches per-post jobs (see Cascades above); sets `videos.status='scheduled'`, `videos.publish_at=<iso>`; records `publish_scheduled` and one `social_post_scheduled` event per re-attached post.

### `DELETE /api/videos/{video_id}/schedule`

**Purpose** — Cancel a previously scheduled publish.

**Response 200** — `{"status": "ok", "message": "Schedule cancelled"}`.

**Errors** — `404` if the video has no scheduled publish.

**Cascades** — **Cancels every pending per-post job for this video.** All `social_posts` rows with `scheduler_job_id IS NOT NULL` go through `cancel_scheduled_post()`: their APScheduler `DateTrigger` is removed and `scheduled_at` / `scheduler_job_id` are nulled. Already-posted rows are unaffected (their `scheduler_job_id` is already NULL).

**Side effects** — Removes the publish APScheduler job and all per-post jobs (see Cascades); clears `videos.publish_at`; resets `videos.status` to `'ready'`.

### `POST /api/videos/{video_id}/schedule-social`

**Purpose** — Stagger this video's approved social posts on a chosen timeline **without** touching the video itself. For use when the video is already public and only the social fan-out needs scheduling.

**Request body** — `{"first_post_at": "2026-04-25T14:00:00-07:00", "spacing_minutes": 60}`. `first_post_at` is ISO 8601, must be in the future, naive datetimes treated as UTC. `spacing_minutes` is optional (≥ 1, integer); when omitted, the project's `inter_post_spacing_minutes` setting is used. The override is per-batch — the project setting is not mutated. `0` is rejected because it would land every approved post on the same `DateTrigger` and fan-out simultaneously.

**Response 200** — `{"status": "ok", "scheduled": <int>, "errors": [{"post_id", "error"}, ...], "first_post_at": "<iso>", "spacing_minutes": <int|null>}`. `scheduled` counts approved posts a fresh job was registered for; `errors` carries one entry per post whose re-attach raised. The endpoint cancels pending jobs **before** re-scheduling, so a non-empty `errors` means those posts have no schedule at all — callers must surface the partial failure so the user can retry. `spacing_minutes` echoes the override (or `null` when none was supplied).

**Errors** — `400` on missing/invalid `first_post_at`, a past time, or a non-integer / `< 1` `spacing_minutes`; `404` if the video doesn't exist.

**Behavior** — Cancels every pre-existing pending per-post job for this video (rows with `scheduler_job_id IS NOT NULL`) via `cancel_scheduled_post()`, then re-schedules every `approved` post anchored at `first_post_at` and spaced by either the supplied `spacing_minutes` or the project's `inter_post_spacing_minutes`. Unlike the video-level `POST /api/videos/{video_id}/schedule` flow, **`post_video_delay_minutes` is intentionally ignored** here — "wait X minutes after the video goes live" has no meaning once the video is already live, so the picked time IS when the first approved post fires.

**Side effects** — Per-post `social_posts` rows get `scheduled_at`/`scheduler_job_id` set by `schedule_social_post`; **`videos.publish_at` and `videos.status` are NOT touched.** Records one `social_post_scheduled` event per re-attached post.

### `DELETE /api/videos/{video_id}/schedule-social`

**Purpose** — Cancel every pending per-post job for the video without touching the video's own `publish_at`/`status`. Pairs with `POST .../schedule-social` for the already-published case where `DELETE .../schedule` would also un-publish the video (reset `status='ready'`, null `publish_at`).

**Response 200** — `{"status": "ok", "cancelled": <int>}` where `cancelled` is the number of per-post jobs actually torn down (`0` is not an error).

**Side effects** — Calls `cancel_scheduled_post()` for each row with `scheduler_job_id IS NOT NULL`: their APScheduler `DateTrigger` is removed and `scheduled_at`/`scheduler_job_id` are nulled. Already-posted rows are unaffected. The video row itself is not modified.

### `GET /api/videos/{video_id}/captions`

**Purpose** — List YouTube caption tracks for the video.

**Response 200** — Array of caption resources from the YouTube API.

**Errors** — `500` on any YouTube error.

### `GET /api/videos/{video_id}/comments`

**Purpose** — List recent comment threads on the video.

**Query params** — `max_results` (int, default `50`).

**Response 200** — Array of comment thread resources from `youtube.list_comment_threads`.

**Errors** — `403` if comments are disabled on the video; `500` on any other YouTube error.

### `POST /api/videos/{video_id}/set-thumbnail`

**Purpose** — Upload and set a video thumbnail.

**Request body** — multipart/form-data with `file` field.

**Response 200** — `{"status": "ok"}`.

**Errors** — `500` (YouTube rejected the thumbnail).

**Side effects** — Saves file under `UPLOAD_DIR`; calls `youtube.set_thumbnail`; updates `videos.thumbnail_path`. Marks `videos.thumbnail_source='user'` and clears `thumbnail_compare_verdict` so the next `GET /api/videos/{id}` re-asks Claude whether the local copy matches what YouTube has.

### `POST /api/videos/{video_id}/thumbnail/use-youtube`

**Purpose** — Promote the cached YouTube-side thumbnail (`videos.youtube_thumbnail_path`, populated by the C3 background refresh) to be the active local thumbnail. Used from the Thumbnail-compare panel on the detail page when Claude flagged the user's copy and the live YouTube copy as different and the user prefers the live one.

**Response 200** — `{"status": "ok"}`.

**Errors** — `404` (no video row), `400` (no cached YouTube thumbnail yet — open the video so the background refresh has a chance to fetch one).

**Side effects** — Copies `youtube_thumbnail_path` over `thumbnail_path`, sets `thumbnail_source='youtube'`, marks `thumbnail_compare_verdict='same'` and stamps `thumbnail_compared_at`.

### `POST /api/videos/{video_id}/thumbnail/push-to-youtube`

**Purpose** — Upload the current local thumbnail back to YouTube via `youtube.set_thumbnail`. Used from the same compare panel when the user wants to keep what they uploaded and overwrite what YouTube currently shows.

**Response 200** — `{"status": "ok"}`.

**Errors** — `404` (no video row), `400` (no local thumbnail to push), `500` (YouTube rejected the upload).

**Side effects** — Marks `thumbnail_compare_verdict='same'` and stamps `thumbnail_compared_at` (best-effort — the next GET re-runs the compare if YouTube re-encoded the upload into something visually distinct).

### `GET /api/videos/{video_id}/file-info`

**Purpose** — Local-file details for the detail page's file-info popup.

**Response 200** — `{"has_file": bool, "original_name": str|null, "disk_name": str|null, "exists": bool, "can_reveal": bool, "source_origin": str|null, "duration_seconds": float|null, "width": int|null, "height": int|null, "bitrate_bps": int|null, "size_bytes": int|null, "codec_name": str|null, "container": str|null, "browser_playable": bool|null, "quality_warnings": [{"code": str, "message": str, ...}]}`. `original_name` is the filename the file was uploaded with (sanitized); `disk_name` is the app-chosen on-disk basename; `can_reveal` is true on macOS. Absolute server filesystem paths are never returned — the browser uses the API for every read or reveal. `source_origin` is one of `uploaded | youtube_download | user_attached | generated_clip` (or null for pre-026 rows / no local file). `generated_clip` is added without a schema change (the column is plain TEXT) and is stamped on rows produced by the Generate-from-source flow. `codec_name` (e.g. `h264`, `hevc`, `vp9`, `av1`, `prores`) and `container` (`mp4`, `mov`, `webm`, …) come straight from ffprobe. `browser_playable` is `true` when the `(codec, container)` pair is in the Safari/WKWebView allowlist (h264 / hevc in mp4·mov, vp9 in webm, av1 in mp4·webm), `false` when it's a recognised codec outside the allowlist, and `null` when the codec is unknown — UIs should treat `null` as "fall back to YouTube embed for preview". `quality_warnings` is a list of structured warnings about the source itself; current codes are `low_resolution` (carries `min_dimension`/`width`/`height`) when `min(width,height) < 1080`, and `youtube_download_lossy` when `source_origin == 'youtube_download'`. All other technical fields come from the same ffprobe call and are null when the file is missing or ffprobe couldn't read it.

**Errors** — `404` (no video row).

### `POST /api/videos/{video_id}/source-file`

**Purpose** — Attach or replace the local high-fidelity source file for a video. The YouTube-hosted video is never touched — this only swaps the local file used for clip extraction and other on-device tasks. Used by the "Replace source" / "Attach source" button on the video detail page.

**Query params** — `force` (int, default `0`): set to `1` to bypass the 422 sanity checks below in a single round-trip. The browser UI does **not** use this — it lets the server keep the upload and confirms via `/source-file/finalize` so multi-GB sources don't have to be re-uploaded. `force=1` remains supported for direct API callers and tests.

**Request body** — `application/json` with `{"upload_id": str}`. The bytes have already been streamed to disk via the chunked-upload protocol (`POST /api/uploads/init` → `/chunk/{offset}` → `/finalize`); this endpoint just consumes the finalized file.

**Response 200** — `{"status": "ok", "original_name": str, "duration_seconds": float|null, "width": int|null, "height": int|null, "bitrate_bps": int|null, "size_bytes": int|null, "source_origin": "user_attached", "transcript_cleared": bool}`. `transcript_cleared` is `true` when the call also blanked the row's transcript columns because the user forced past a `duration_mismatch`.

**Side effects** — The previous local file is *not* renamed or deleted: the row is re-pointed at the new file via a single atomic UPDATE, and the old file stays on disk as an orphan (intentional — trades disk hygiene for crash-safety; no half-written rename + DB state can co-occur). The row's `video_file_path`, `video_file_original_name`, `duration_seconds`, and `source_file_origin` are updated; `video_file_download_state` is cleared. When the caller forces past a `duration_mismatch` (either via `?force=1` or via the finalize-pending flow below), the row's `transcript`, `transcript_id`, `transcript_source`, `transcript_created_at`, `transcript_updated_at`, and `transcript_is_edited` are also cleared (the saved caption timestamps no longer align with the new file).

**Concurrency** — Calls for the same `video_id` are serialized via a per-video asyncio lock. Different videos still proceed in parallel.

**Errors**:

- `400` — no file in the request, truncated body, or ffprobe ran on the upload and found no video stream (the user picked a non-video file). Not overridable.
- `404` — video row not found.
- `413` — file exceeds the 10 GiB cap (checked from `Content-Length` upfront and again as a streaming counter during the copy, so a lying header still gets caught). Not overridable.
- `422` — sanity check failed. Body is `{"detail": {"issues": [{...}], "pending_token": str}}`. The uploaded file is **kept on disk** (not deleted on 422) and `pending_token` references it; the client POSTs the token to `/source-file/finalize` after the user confirms, or DELETEs `/source-file/pending/{token}` if the user cancels. Each issue is one of:
  - `{"code": "duration_mismatch", "expected_seconds": float, "incoming_seconds": float, "tolerance_seconds": float}` — the incoming file's duration differs from the row's `duration_seconds` by more than the tolerance (currently 2.0 s).
  - `{"code": "resolution_downgrade", "current": "WxH", "incoming": "WxH"}` — the incoming file is strictly smaller in both width and height than the current file. Only fires when a current file exists and isn't itself a `youtube_download` (replacing a YouTube re-download is always considered an upgrade).

### `POST /api/videos/{video_id}/source-file/finalize`

**Purpose** — Commit a Replace-Source upload that previously returned 422. Lets the client confirm past the sanity issues without re-uploading the entire body.

**Request body** — `{"pending_token": str}` — the token returned in the 422 `detail` of the original `POST /source-file` call.

**Response 200** — same shape as `POST /source-file` on success.

**Errors**:

- `404` — token unknown, expired (TTL is 30 minutes from upload), already consumed by a previous finalize/cancel, belongs to a different video, or its on-disk file is missing. The client must re-upload.

**Side effects** — Renames the pending file from `source_pending_<hex>.<ext>` to `source_<hex>.<ext>` (atomic, same filesystem) and runs the same DB UPDATE as the `force=1` path on `POST /source-file`. Uses the probe captured at upload time — no re-probe.

### `DELETE /api/videos/{video_id}/source-file/pending/{pending_token}`

**Purpose** — Drop a pending Replace-Source upload without finalizing. Called by the UI when the user dismisses the confirm dialog so the on-disk file is removed immediately instead of waiting for the 30-minute TTL.

**Response 200** — `{"status": "cancelled"}` when the entry existed, `{"status": "gone"}` when it was already finalized / cancelled / expired (idempotent).

### `POST /api/videos/{video_id}/reveal-file`

**Purpose** — Reveal the video's local file in Finder (macOS). The path is resolved server-side from the row and confirmed inside `UPLOAD_DIR` — the client never supplies a path.

**Response 200** — `{"revealed": true}`.

**Errors** — `404` (no video row, or no local file on disk), `501` (not macOS), `500` (`open -R` failed).

---

## Transcripts (`/api/videos/{video_id}/transcripts`)

Source: `src/yt_scheduler/routers/transcript_routes.py`

### `GET /api/videos/{video_id}/transcripts`

**Purpose** — List every transcript row for a video (different sources, edits, etc.).

**Response 200** — Array:

```json
[ { "id": 1, "video_id": "abc", "source": "mlx_whisper" | "whispercpp" | "apple_speech" | "youtube" | "user_edited", "source_detail": "large-v3" | null, "text": "...", "created_at": "..." } ]
```

### `PUT /api/videos/{video_id}/transcripts/active`

**Purpose** — Activate a chosen transcript for a video. Records the diff and re-uploads to YouTube as a caption track.

**Request body** — `{"transcript_id": <int>, "text": "<the text to commit>", "is_edited": <bool>}`. `text` is what the user is committing — may differ from the source row when they edited it in the chooser.

**Response 200**:

```json
{
  "status": "ok",
  "transcript_id": 5,
  "transcript_source": "mlx_whisper",
  "transcript_created_at": "...",
  "transcript_updated_at": "...",
  "transcript_is_edited": 0,
  "youtube_caption_id": "abc",       // present on success
  "youtube_status": "uploaded" | "failed",
  "youtube_error": "..."             // present on failure
}
```

**Errors** — `400` (`transcript_id` not an int, `text` empty, transcript doesn't belong to this video), `404` (video not found).

**Side effects** — Updates `videos.transcript*` fields; records `metadata_updated{transcript: {old, new}}` event when text changed; calls `youtube.upload_caption` (50–450 quota) — failure is non-fatal and surfaced via the response keys.

---

## Social posts (`/api/social`)

Source: `src/yt_scheduler/routers/social_routes.py`

### `POST /api/social/generate-posts/{video_id}`

**Purpose** — Render social posts for a video by running each template slot against video metadata and (re)create draft rows in `social_posts`.

**Request body** (optional):

```json
{
  "template_name": "announce_video",  // default
  "platforms": ["twitter", "bluesky"], // optional platform whitelist
  "slot_ids": [12, 17],                // optional template_slots.id whitelist (G1)
  "user_message": "...",               // exposed to templates as {{user_message}}
  "unresolved": {"repo": "empty", "video": "literal"},  // see "Errors" below
  "unresolved_ack": true               // proceed even with unresolved vars (all left literal)
}
```

`platforms` and `slot_ids` are both whitelist filters; they intersect. The latter is the precise filter — a template with two Mastodon slots routed to different accounts can be partially regenerated (one slot only) where `platforms` alone could only express "all Mastodon slots or none." The DELETE-before-regenerate also scopes by `slot_id` when `slot_ids` is supplied, so a single-slot regenerate never touches its sibling's rows. Legacy `social_posts` rows with `slot_id IS NULL` (pre-migration 021) still match by `platform` as a back-compat fallback.

`unresolved` maps each unresolved variable name to `"empty"` (substitute an empty string) or `"literal"` (leave `{{name}}` in the post). Passing the `unresolved` key — even `{}` — acknowledges the unresolved set so generation proceeds; `unresolved_ack: true` does the same without choosing per-name behavior.

**Response 200** — `{"posts": [...], "warnings": [...]}`. `posts` is the array of generated post snapshots; `warnings` is a list of human-readable strings (e.g. a Threads slot skipped because it uses `{{video}}`).

```json
{
  "posts": [
    { "slot_id": 7, "platform": "twitter", "content": "...", "media": "thumbnail" | "video" | "none", "media_urls": ["/media/<name>", ...], "media_filenames": ["<name>", ...], "max_chars": 280, "social_account_id": 1 }
  ],
  "warnings": ["Threads slot skipped — {{video}} attachments aren't supported on Threads yet (its API posts text only)."]
}
```

`media_urls` / `media_filenames` describe the media that was attached to the freshly-inserted row (resolved from the template's media directives, or the slot's legacy `media` fallback). The browser typically ignores this and re-fetches `GET /api/social/posts/{video_id}` instead.

**Query params** — `confirm_overwrite_scheduled` (bool, default `false`). When false, the route refuses to regenerate if any unsent post for this video is currently scheduled (has a non-NULL `scheduler_job_id`).

**Errors**

- `404` — Video or template not found.
- `409` — One or more posts are scheduled. Body: `{"detail": {"scheduled_overwrite": true, "needs_confirm": true, "scheduled": [{"post_id": int, "platform": str, "scheduled_at": "<ISO>"}, ...]}}`. Re-issue with `?confirm_overwrite_scheduled=true` to proceed.
- `409` — Template has variables with no value and neither `unresolved` nor `unresolved_ack` was provided. Body: `{"detail": {"unresolved": ["name", ...]}}`. Re-issue with `unresolved` (or `unresolved_ack: true`). Nothing is written or deleted before this gate.
- `400` — `slot_ids` is present but not a list of integers.
- `500` — A non-disabled slot has no `max_chars` (a data bug — every slot is created with a positive value).

**Side effects** — Holds the per-video publish lock. Renders every slot up front (before any destructive op) so the unresolved-vars gate can fire harmlessly. When `confirm_overwrite_scheduled=true`, calls `cancel_scheduled_post()` on each scheduled row first (tearing down its APScheduler `DateTrigger`) so no orphan jobs remain. Then deletes existing `social_posts` for the video where `status NOT IN ('posted','sending')` and inserts one fresh `draft` row per non-disabled, matching slot (each carrying the slot's `max_chars`). Threads slots whose body contains `{{video}}` are skipped (and reported in `warnings`). Template variables exposed: `title`, `url`, `description`, `description_short` (≤150), `description_medium` (≤500), `tags`, `hashtags`, `thumbnail_path`, `tier`, the `transcript*` family (`transcript`, `transcript_truncated`, `transcript_srt`, `transcript_srt_truncated`), `user_message`, `max_chars` (the slot's "Max characters" value). Slot bodies resolve `{{#var}}…{{/var}}` sections before the media pass, so a media directive inside a dropped section attaches nothing (and the legacy per-slot media fallback stays disabled for any body that declares a media directive, even one a section dropped). Also calls `youtube.get_video` to read the duration tier.

**Renderer** — Each slot's `body` is rendered through the same engine as [`POST /api/expand_text`](#post-apiexpand_text) (`services/templates.render`). All variables, `{{var!}}` / `{{var??default}}` / `{{ai: ...}}` / `{{ai[system]: ...}}` syntax, and recursive AI-block evaluation behave identically.

### `GET /api/social/failed-posts`

**Purpose** — All social posts whose most recent send attempt failed, newest first. Powers the app-wide failed-sends banner (`static/js/failed-sends-banner.js`, loaded by `base.html` on every page), which stays up until each post is retried successfully, skipped, or deleted — `social_posts.status` is the single source of truth. Giving up on a post makes it `'skipped'`, so it leaves this list because it is genuinely no longer failing, not because it is filtered out.

**Response 200** — Array of `{"id": int, "video_id": str, "platform": str, "error": str, "failed_at": "<naive UTC>"|null, "social_account_id": int|null, "video_title": str, "page_url": str}`. `page_url` is the ready link to the owning project's video-detail page — the server vends it because the detail route 404s unless the slug actually owns the video.

`failed_at` (migration 044) is when the most recent send attempt failed, stamped by `models.social_post.mark_failed` — the single writer of the `'failed'` state. NULL means unknown: the row failed before the column existed. Nothing substitutes `created_at`, which is when the post row was *written* and for a smart-queue post predates the attempt by days or weeks. Note that the other two time columns are useless here by construction: `posted_at` is only set on success, and marking a post failed *clears* `scheduled_at`.

### `GET /api/social/posts/{video_id}`

**Purpose** — All social posts for a video.

**Response 200** — Array of `social_posts` rows ordered by platform, with one transformation: the absolute-path columns `media_path` and `media_paths` are **removed** and replaced by `media_urls` (array of `/media/<name>` strings) and `media_filenames` (array of bare filenames). Both arrays are empty when the post has no attachment.

### `GET /api/social/posts/{post_id}/trace`

**Purpose** — Return the F-series debug-log trace for a generated social post (templates.render's per-step capture: template body, variable substitutions, rendered prompt, and every Claude round-trip with prompt/system/model/response/elapsed_ms). Powers the ⓘ-button modal on the video-detail page.

**Response 200** — `{"post_id": int, "created_at": "<ISO>", "trace": [...]}`. Each entry in `trace` is one of:

- `{"kind": "template_body", "text": str}`
- `{"kind": "variables", "values": {str: str}}`
- `{"kind": "substituted", "text": str}`
- `{"kind": "ai_call", "prompt": str, "system": str | null, "model": str, "response": str, "elapsed_ms": int}`
- `{"kind": "error", "message": str}` — render-time failures (media-directive parse, AI exception).

**Errors** — `404` (no trace for that `post_id` — either the trace was pruned by the 24h retention job, or the post was created before F2 landed).

**Retention** — Rows persist for 24h (`prune_social_post_traces_job` runs hourly, evicts anything older than 24 hours). FK cascade-deletes the trace when the parent `social_posts` row is removed.

### `PUT /api/social/posts/{post_id}`

**Purpose** — Edit a draft social post in place.

**Request body** — Any subset of `{"content": str, "status": str, "media_path": str, "media_paths": list[str] | null}`. `content` is auto-trimmed of leading/trailing whitespace at write time. `media_paths` accepts a list (replace the attachment set) or `[]`/`null` (clear all attachments); writing it also rewrites the legacy `media_path` column to the first entry (or null) so the two stay in sync. Every supplied media path must resolve inside `UPLOAD_DIR` (symlink-safe) — a path outside the managed media directory is rejected so a client can't attach an arbitrary on-disk file that would then be uploaded to a social platform.

**Note on media** — The `social_posts` table now has both `media_path` (legacy single-string column, kept for backwards compat) and `media_paths` (JSON array column, the canonical form). The post-generation paths and PUT endpoint write both. The send paths read `media_paths` first and fall back to `media_path`; the poster also re-checks containment before reading any attachment. Once all writers stop touching the legacy column it'll be dropped in a follow-up migration.

**Response 200** — `{"status": "ok"}`.

**Errors** — `400` (a `media_path`/`media_paths` entry is outside `UPLOAD_DIR`, or an invalid `status`).

### `POST /api/social/posts/{post_id}/skip`

**Purpose** — Give up on a failed send.

**Response 200** — `{"id": 42, "status": "skipped"}`

Sets `status = 'skipped'` — the state this codebase already uses for a post
nobody is going to send (`smart_queue_disposition`'s `remove` sets the same
thing). The post leaves `GET /api/social/failed-posts` because it is genuinely no
longer failing, not because it is filtered out: there is one state with one
meaning, and no hidden-but-still-failed rows to drift.

The error text, `failed_at` and the content are kept — this records a decision,
it does not rewrite what happened. The retry columns are cleared so the
automatic retry job cannot pick it up a minute later.

When the post belongs to a smart-queue item and was its last live posting, the
item is retired to `state = 'removed'`. An item left `'scheduled'` with nothing
that could ever send counts toward the queue's scheduled total forever *and*
permanently blocks its video from being selected again — the same zombie
`smart_queue_reconcile_handlers._retire_emptied_items` exists to prevent.

**Errors** — `404` (unknown post); `409` (the post is not in status `failed` —
on any other status this would silently change something the user is not looking
at; the status predicate is in the UPDATE itself, so a post that starts sending
between the check and the write is refused rather than stomped).

### `DELETE /api/social/posts/{post_id}`

**Purpose** — Remove a draft or failed social post. Backs the **Remove** button on the video-detail page (behind a confirm).

**Preconditions** — `status` must be `'draft'` or `'failed'` (`_REMOVABLE_POST_STATUSES`), **and** `smart_queue_item_id` must be NULL. Neither removable status has anything pending: a draft was never sent, and every failure path clears `scheduled_at`/`scheduler_job_id`, so nothing retries a failed post — for one outside a smart queue, removal is its only exit from `GET /api/social/failed-posts` and the app-wide banner. The other three statuses are refused: `posted` is the audit trail of a post the world has seen, `sending` is mid-flight, and `approved` may have a live per-post job behind it.

A post owned by a smart queue item is refused whatever its status. `smart_queue.list_queues` derives an item's bucket from its posting rows (`LEFT JOIN social_posts` … `ELSE i.state`), so deleting the row would leave the item reported as `scheduled` forever with nothing left to post, and its video never eligible to be queued again. Those posts have their own exit: `POST /api/projects/{slug}/smart-queues/{queue_id}/missed/{post_id}` with `action: "remove"`, which moves the *item* to `removed`.

Note that `failed` is not proof nothing reached the platform — a publish whose response was lost to a timeout is recorded as failed. Removing the row deletes our record, not the platform's post.

**Response 200** — `{"status": "ok", "cancelled_schedule": true | false}`. `cancelled_schedule` is true only in the odd case where the row still carried a `scheduler_job_id` (a `PUT` can set `status` back to `draft` without clearing it); the job is torn down before the row is deleted so no trigger fires against a missing post.

**Side effects** — Deletes the `social_posts` row; the matching `social_post_traces` row goes with it via `ON DELETE CASCADE`. The removal is logged (status, post id, platform, video id, content length, and the discarded `error` text when there was one) — the row itself is unrecoverable.

**Errors** — `404` (no such post); `409` (status is not removable, including the race where a send claims the row between the status read and the delete — the guard is repeated in the `DELETE` statement, so the send wins).

### `POST /api/social/posts/{post_id}/shorten`

**Purpose** — Ask the model to rewrite a generated post shorter, preserving meaning and every URL. Applies the result to `social_posts.content` in place.

**Request body** — `{"target_chars": int}` (optional; defaults to the post's `max_chars`, or 280 if that's null).

**Response 200** — `{"content": "<new text>", "previous": "<old text>", "char_count": int, "warning": str | null}`. `previous` lets the caller offer an Undo (via `PUT /api/social/posts/{post_id}` with `{"content": <previous>}`). `warning` is set if a URL from the original appears to be missing in the rewrite.

**Errors** — `404` (post not found), `400` (post is empty, or `target_chars` not positive), `502` (the model call failed or returned nothing).

**Renderer** — Uses `services/ai.call_ai_block` (one Claude round-trip).

### `POST /api/social/posts/{post_id}/send`

**Purpose** — Send a single social post.

**`post_id`** — `social_posts.id` returned by `POST /api/social/generate-posts/{video_id}` (in the array of created rows) or `GET /api/social/posts/{video_id}`.

**Prerequisites** — The row must already exist; a `failed` post is accepted as well as an `approved` one — this endpoint is a deliberate human send, and it is what the failed-sends banner's Retry uses. Unattended senders still take only `approved`. A manual attempt also ends the current automatic retry run, so an unedited AI-generated post can be sent directly. A prior `PUT /api/social/posts/{post_id}` is only needed if the caller wants to edit `content` or `media_path` before sending.

**Request body** — None.

**Query params** — `confirm_dup` (bool, default `false`). When false (default) the route refuses to resend duplicates of the last 30 days.

**Response 200** — `{"status": "ok", "url": "<post URL>"}`.

**Errors**

- `404` — Post not found.
- `400` — Resolved poster is misconfigured (no credentials), or platform routing yielded no usable poster.
- `401` — Resolved credential is flagged `needs_reauth=1` (pre-check), or the platform itself rejected the request as unauthorized (post-call). On post-call failure the credential's `needs_reauth` flag is set.
- `409` — A post with same `(platform, account, content, media_path)` was sent within the last 30 days. Body is `{"detail": {"duplicate": true, "platform": "...", "previous": {"id": int, "video_id": str, "posted_at": str, "post_url": str, "content_preview": str}, "needs_confirm": true}}`. Re-issue with `?confirm_dup=true`.
- `500` — Anything else from the platform, **including a media-attachment failure**: if the post has media attached but it can't be uploaded/attached (failed upload, missing file, or a platform that can't take media at all — e.g. Threads), the send is aborted and nothing is posted. The error message tells the user to re-attach or remove the attachment (`PUT …/posts/{id}` with `{"media_paths": []}`) and retry. The post is never published in a degraded, text-only form.

**Side effects** — Picks the poster via slot binding → project default → first active credential. On success: updates `social_posts.status='posted'`, `posted_at`, `post_url`; records `social_post_published`. On failure: sets `status='failed'`, `error=<message>`.

### `POST /api/social/posts/{post_id}/schedule`

**Purpose** — Schedule an individual social post via APScheduler `DateTrigger`.

**Query params** — `confirm_dup` (bool, default `false`).

**Request body** — `{"scheduled_at": "<ISO 8601>"}` (must be in the future).

**Response 200** — `{"status": "ok", "job_id": "social_post_<id>", "scheduled_at": "..."}`.

**Errors** — `400` (missing/invalid `scheduled_at`, time not in future), `404` (post not found), `409` (duplicate; same shape as `/send`).

**Side effects** — Updates `social_posts.scheduled_at`, `scheduler_job_id`, sets `status='approved'`. Adds APScheduler `DateTrigger`. Records `social_post_scheduled` event.

### `DELETE /api/social/posts/{post_id}/schedule`

**Purpose** — Cancel a scheduled per-post job.

**Response 200** — `{"status": "ok", "cancelled": true | false}`.

**Side effects** — Removes the APScheduler job (when present); clears `social_posts.scheduled_at` and `scheduler_job_id`.

### `POST /api/social/posts/{video_id}/send-all`

**Purpose** — Send every `status='approved'` post for the video.

**Query params** — `confirm_dup` (bool, default `false`).

**Response 200** — Array with one entry per post. A template can route two posts to two different accounts on the same platform, so results are keyed by `post_id`, not by platform; `account_label` names the credential each post was sent with (`null` when none resolved).

```json
[
  { "post_id": 11, "platform": "twitter",  "account_label": "@drew @X",       "status": "posted",       "url": "..." },
  { "post_id": 12, "platform": "mastodon", "account_label": "@drew@hachyderm", "status": "posted",       "url": "..." },
  { "post_id": 13, "platform": "mastodon", "account_label": "@alt@mas.to",     "status": "failed",       "error": "..." },
  { "post_id": 14, "platform": "bluesky",  "account_label": null,              "status": "skipped",      "reason": "not configured" },
  { "post_id": 15, "platform": "linkedin", "account_label": "Drew B",          "status": "needs_reauth", "error": "..." }
]
```

`posted` entries may also carry a `warning` field.

**Errors** — `409` when any approved post is a duplicate (body: `{"detail": {"duplicate": true, "duplicates": [<per-post entries>], "needs_confirm": true}}`). Per-post failures during the send loop are reported in the 200 response, not raised.

---

## Templates (`/api/templates`)

Source: `src/yt_scheduler/routers/template_routes.py`

All template endpoints implicitly scope to `project_id=1` (the Default project) via the service-layer default.

### `GET /api/templates`

**Purpose** — List all templates within the default project.

**Response 200** — Array:

```json
[ { "id": 1, "name": "announce_video", "description": "...", "applies_to": ["video"], "is_builtin": true, "platforms": { "twitter": {"template": "...", "media": "thumbnail", "max_chars": 280}, ... }, "slot_count": 5 } ]
```

### `GET /api/templates/{name}`

**Purpose** — Fetch one template by name with its full slot list.

**Response 200**:

```json
{
  "id": 1, "name": "...", "description": "...", "applies_to": ["video"], "is_builtin": true,
  "created_at": "...", "updated_at": "...",
  "slots": [
    {
      "id": 7, "template_id": 1, "platform": "twitter",
      "social_account_id": 2 | null,
      "is_builtin": true, "is_disabled": false, "order_index": 0,
      "body": "...", "media": "thumbnail", "max_chars": 280,
      "resolved_account": { "uuid": "...", "username": "alice", "platform": "twitter", "deleted": false } | null
    }
  ],
  "platforms": { "twitter": { "template": "...", "media": "thumbnail", "max_chars": 280 }, ... },
  "test_variables": { "title": "Saved title", "url": "https://...", "region": "US" }
}
```

`test_variables` is the persisted Preview-pane fixtures for the template-edit page (migration 016). Keys are template-variable names (the five seeded ones — `title`, `url`, `tags`, `description`, `user_message` — plus any custom keys the user added). Values are strings. An empty dict means "no override; the page falls back to its seeded defaults."

**Errors** — `404` (unknown name).

### `POST /api/templates`

**Purpose** — Create or upsert a template (compatibility shape with the legacy `platforms` map).

**Request body**:

```json
{
  "name": "...",
  "description": "...",
  "platforms": { "twitter": { "template": "...", "media": "thumbnail", "max_chars": 280 } },
  "applies_to": ["video", "short"]
}
```

**Response 200** — `{"status": "ok"}`.

**Errors** — `400` (no `name`, name not matching `^[A-Za-z0-9][A-Za-z0-9_-]*$`, empty `applies_to`, or DB integrity error).

**Side effects** — Upserts the template row plus one built-in slot per `platforms` key.

### `PUT /api/templates/{name}`

**Purpose** — Update an existing template.

**Request body** — Any subset of `description`, `platforms`, `applies_to`. Missing keys fall back to the existing values.

**Response 200** — `{"status": "ok"}`.

**Errors** — `404` (unknown name), `400` (validation failure).

### `DELETE /api/templates/{name}`

**Purpose** — Delete a template.

**Response 200** — `{"status": "ok"}`.

**Errors** — `400` if `name` is one of the protected built-in templates.

**Cascades** — Deleting a template cascades to all of its `template_slots` rows via `ON DELETE CASCADE` (`migrations/008_per_project_credentials.sql:77`). Already-generated `social_posts` rows are unaffected — they're denormalized snapshots of the rendered text at generation time and don't carry a slot FK.

### `PUT /api/templates/{name}/test-variables`

**Purpose** — Persist the Preview-pane test fixtures shown on the template-edit page (`templates.test_variables`, migration 016).

**Request body**:

```json
{
  "test_variables": {
    "title": "Saved title",
    "url": "https://example.com/x",
    "region": "US"
  }
}
```

Values are coerced to strings server-side (numbers → str, booleans → str, `null` → `""`) so the render engine never sees a non-string variable later. Keys must be strings; non-string keys return `400`. Sending `{"test_variables": {}}` clears the column back to NULL — the front-end then falls back to its seeded defaults.

**Response 200** — `{"status": "ok"}`.

**Errors** — `404` (unknown template), `400` (`test_variables` is not an object, or any key is not a string).

### `POST /api/templates/{name}/duplicate`

**Purpose** — Create a new template as a deep copy of `{name}` within the same project.

**Request body** — `{"new_name": "..."}`.

**Response 200** — The newly created template (same shape as `GET /api/templates/{name}`).

**Behaviour** — Copies the description, `applies_to`, `test_variables` (the Preview-pane fixtures, migration 016), and **every** slot verbatim (built-in slots stay built-in, disabled stays disabled, account bindings and order preserved). The new *template* row is never flagged `is_builtin` — only the two protected names carry that — so the copy is freely deletable.

**Errors** — `400` (missing `new_name`, or name not matching `^[A-Za-z0-9][A-Za-z0-9_-]*$`), `404` (source `{name}` not found), `409` (`new_name` already exists, or collides with a reserved built-in name).

### `GET /api/templates/{name}/slots`

**Purpose** — List every slot for a template.

**Response 200** — Array of slot dicts (same shape as the `slots` array under `GET /api/templates/{name}`).

**Errors** — `404` (unknown name).

### `POST /api/templates/{name}/slots`

**Purpose** — Add a non-builtin slot.

**Request body** — `{"platform": "twitter", "body": "...", "media": "thumbnail", "max_chars": 500, "social_account_id": 1, "is_disabled": false, "order_index": 3}`. Only `platform` is required.

**Response 200** — The newly created slot dict.

**Errors** — `400` (missing `platform`, non-int `social_account_id`, service-layer validation error, or `{"detail": {"section_error": "..."}}` when the body's `{{#…}}`/`{{^…}}`/`{{/…}}` section tags don't pair up), `404` (unknown template name).

### `PATCH /api/templates/{name}/slots/{slot_id}`

**Purpose** — Update a slot.

**Request body** — Any subset of `body`, `media`, `is_disabled`, `order_index`, `max_chars`, `social_account_id`. Pass `null` (or `""`) to clear `social_account_id`.

**Response 200** — Updated slot dict.

**Errors** — `404` (unknown template name, slot not found in this template), `400` (non-int `social_account_id`, validation failure, or `{"detail": {"section_error": "..."}}` when the body's section tags don't pair up).

### `DELETE /api/templates/{name}/slots/{slot_id}`

**Purpose** — Delete a slot.

**Response 200** — `{"status": "ok"}`.

**Errors** — `404` (unknown template, slot not found in this template), `400` (validation, e.g. trying to delete a built-in slot).

---

## Text expansion (`/api/expand_text`)

Source: `src/yt_scheduler/routers/expand_routes.py`

This is the canonical text-expansion endpoint. Every server-side rendering path delegates to the same engine — `services/templates.render(...)` — so the syntax and semantics described here are exactly what `POST /api/social/generate-posts/{video_id}`, the auto-gen-socials background job in `services/auto_actions.py`, and the prompt-template bodies in `services/ai.py` all see at runtime. There is no second renderer.

### `POST /api/expand_text`

**Purpose** — Render a template against variables, evaluating any `{{ai: ...}}` blocks via Claude. The UI's template editor preview pane uses this; you can also call it directly to render arbitrary text without writing a row anywhere.

**Request body**:

```json
{
  "template": "Hello {{name!}}, here is a {{ai[Be terse]: haiku about {{topic??the weather}}}}.",
  "variables": {"name": "Drew", "topic": "rain"},
  "default_system_prompt": "...optional system prompt for AI blocks without an inline override...",
  "model": "claude-sonnet-4-6",
  "max_tokens": 512
}
```

Only `template` is required; everything else has defaults.

**Template syntax** ("empty" below means missing, empty, or whitespace-only)

- `{{name}}` — substitute. An **undefined** name returns **400** with `{"detail": {"undefined_variables": ["<name>", ...]}}` listing every undefined name at once; a defined-but-empty value renders as empty text. (Pass `variables` for every bare name the template references.)
- `{{name!}}` — required content. A missing **or empty** value returns **400** with `{"detail": {"missing_required": "<name>"}}`. No fallback; use `??` if you want one.
- `{{name??default text}}` — optional with explicit fallback. When `name` is missing **or empty**, the literal string between `??` and `}}` is rendered. Default text is **absolute** — no variables, and no `{{`/`}}` braces at all, inside it. For empty fallback write `{{name??}}`.
- `{{#name}}…{{/name}}` — section: the enclosed content renders only when `name` has content. Content inside a dropped section is discarded before any other pass — its variables aren't checked, its `{{ai:}}` blocks never fire, its media directives attach nothing. Sections nest; close tags match by name.
- `{{^name}}…{{/name}}` — inverted section ("else"): renders only when `name` is missing or empty.
- An unclosed, stray, mismatched, or malformed section tag returns **400** with `{"detail": {"section_error": "<message>"}}`.
- `{{ai: prompt}}` — evaluate against Claude using `default_system_prompt` (or the built-in social-copywriter default).
- `{{ai[system text]: prompt}}` — per-block system override. `default_system_prompt` is ignored for this block. Inner blocks without their own `[...]` inherit `default_system_prompt`, **not** the outer override.
- AI blocks may be nested arbitrarily deep. The walker uses balanced `{{` / `}}` matching (Python `re` can't), resolves leaves first, splices each result into the parent prompt, then sends the parent. Sibling blocks are independent.
- An unbalanced `{{ai` opener with no matching `}}` is left in the output verbatim — the broken syntax surfaces instead of half a template silently shipping to Claude.

**Media directives** (only meaningful in the post-generation paths described below — `/api/expand_text` exposes the renderer but doesn't carry an item context, so directives there render as empty strings with no media attached):

- `{{video}}` — attach the item's primary video file to the social post; substitute to empty in the body. Silently skipped if no video file is set.
- `{{thumbnail}}` — attach the item's thumbnail; substitute to empty. Silently skipped if no thumbnail.
- `{{image:shortname}}` — attach the matching `item_images` row's image. Returns **400** with `{"detail": "Image shortname not found: ..."}` if no row matches.
- `{{image:*}}` — attach every image row in `order_index` order; substitute to empty.

**Built-in variables** provided by the post-generation paths (`/api/social/generate-posts/{video_id}` and the auto-actions background path) on top of whatever the caller passes:

- `{{title}}`, `{{description}}`, `{{description_short}}` (≤150), `{{description_medium}}` (≤500), `{{tags}}` (comma-joined), `{{hashtags}}` (top 5 as `#CamelCase`), `{{thumbnail_path}}`, `{{tier}}`, `{{user_message}}`.
- The transcript family (same values in every render path): `{{transcript}}` (plain text, SRT stripped), `{{transcript_truncated}}` (plain text, shared cap), `{{transcript_srt}}` (stored SRT verbatim), `{{transcript_srt_truncated}}` (SRT capped at a cue boundary).
- `{{max_chars}}` — the rendering slot's "Max characters" value, as a string of digits. Only the slot-rendering paths supply it (`/api/social/generate-posts/{video_id}`, the template-editor preview, and the auto-gen-socials job); a bare `{{max_chars}}` in other render paths is an undefined-variable error. Handy inside an `{{ai:}}` block so the model knows the platform limit.
- `{{url}}` — `videos.url`. Populated from the YouTube URL at upload / import for YT-backed items, NULL→empty string for standalone items unless explicitly set.
- `{{episode_url}}` — when the item has `parent_item_id` set, the parent's `url`; empty otherwise.
- `{{project_url}}` — `projects.project_url`. Auto-populated from the YouTube channel handle on OAuth bind for YT projects; set explicitly via `POST /api/projects` for non-YT projects; editable via `PATCH /api/projects/{slug}` and refreshable via `POST /api/projects/{slug}/youtube/refresh-channel-url`.

**Custom variables** are merged at every render via the four-level inheritance chain (lowest priority first): `global_variables` → `project_variables` → parent item's `item_variables` (when the item has a parent) → self item's `item_variables`. Built-ins always come from the self item — they never inherit. See "Custom variables" below for the per-scope CRUD endpoints.

`POST /api/expand_text` is the bottom of that hierarchy: it has no item context, so project / parent / item layers don't apply, but it **does** merge in `global_variables` automatically (with the caller's `variables` taking precedence on any key collision — the caller acts as the "self" level). To exercise the full chain, use `POST /api/social/generate-posts/{video_id}` or call the renderer through one of the auto-action paths.

**Response 200** — `{"rendered": "<rendered text>"}` on success, or `{"rendered": null, "error": "<message>"}` for non-required render failures (e.g., Anthropic API error).

**Errors**

- `400` if `template` is empty; `{"detail": {"missing_required": "<name>"}}` when a `{{var!}}` value is missing or empty; `{"detail": {"undefined_variables": [...]}}` when bare `{{var}}` names aren't supplied; `{"detail": {"section_error": "<message>"}}` for malformed `{{#…}}`/`{{^…}}`/`{{/…}}` section tags.

---

## Item images (`/api/videos/{video_id}/images`)

Source: `src/yt_scheduler/routers/item_image_routes.py`

Multi-image attachments per item, referenced from templates as `{{image:shortname}}` or `{{image:*}}`. Each row carries a unique-per-item `shortname` ([a-z0-9-], can't start with hyphen) plus optional `alt_text` and an `order_index` that controls the order in `{{image:*}}` expansion.

### `GET /api/videos/{video_id}/images`

**Response 200** — Array of image rows in `(order_index, id)` order. The absolute-path column `path` is **removed** and replaced by `url` (`/media/<name>`) and `filename` (the bare name):

```json
[
  { "id": 1, "video_id": "abc", "shortname": "cat", "url": "/media/abc__cat__photo.jpg", "filename": "abc__cat__photo.jpg", "alt_text": "a cat", "order_index": 0, "created_at": "..." }
]
```

### `POST /api/videos/{video_id}/images`

Multipart form upload.

**Form fields** — `file` (binary, required), `shortname` (required), `alt_text` (default `""`), `order_index` (int, default `0`).

**Response 200** — The created image row (same shape as above: `url` + `filename`, no `path`).

**Errors** — `404` (video not found), `400` (shortname collision, invalid shortname).

### `PATCH /api/videos/{video_id}/images/{image_id}`

**Body** — Any subset of `shortname`, `alt_text`, `order_index`. The image file is immutable; delete + re-upload to replace.

**Response 200** — The updated image row (same shape: `url` + `filename`, no `path`).

### `DELETE /api/videos/{video_id}/images/{image_id}`

Removes the row. The on-disk file is left in place (no cleanup) so accidental deletes are recoverable from `UPLOAD_DIR`.

---

## Custom variables

Three scopes form the four-level inheritance chain (with parent items providing the third inheriting layer): `global → project → parent item → self item`, lowest priority first. Each scope has its own router; all three accept the same body shape and validation rules.

**Key validation** — Keys must match `[a-z][a-z0-9_]*` (lowercase letter, then letters / digits / underscores). The validation is consistent with the renderer's variable pattern, so anything you can store here can be referenced as `{{key}}`.

### Global variables

Source: `src/yt_scheduler/routers/global_variable_routes.py`

- `GET /api/global-variables` — list all install-wide rows.
- `PUT /api/global-variables/{key}` — upsert. Body: `{"value": "..."}`. Returns the stored row.
- `DELETE /api/global-variables/{key}` — remove.

### Project variables

Source: `src/yt_scheduler/routers/project_variable_routes.py`

- `GET /api/projects/{slug}/variables` — list all rows for the project.
- `PUT /api/projects/{slug}/variables/{key}` — upsert.
- `DELETE /api/projects/{slug}/variables/{key}` — remove.

Errors: `404` if the project doesn't exist.

### Item variables

Source: `src/yt_scheduler/routers/item_variable_routes.py`

- `GET /api/videos/{video_id}/variables` — list all rows for the item.
- `PUT /api/videos/{video_id}/variables/{key}` — upsert.
- `DELETE /api/videos/{video_id}/variables/{key}` — remove.

Errors: `404` if the item doesn't exist.

---

## Settings (`/api/settings`)

Source: `src/yt_scheduler/routers/settings_routes.py`

### `GET /api/settings`

**Purpose** — Return every key/value pair from the `settings` table (non-secret only).

**Response 200** — `{"key1": "value1", "key2": "value2", ...}`.

### `PUT /api/settings`

**Purpose** — Upsert a flat key/value blob (only keys on the server allowlist; `anthropic_model` is excluded — it has its own endpoint).

**Request body** — An object whose keys are all on the allowlist; values are stringified before storage. The `comment_check_interval` / `caption_check_interval` keys must be a whole number of minutes between 1 and 1440 — they're validated at write time so a bad value can't be stored and then silently reverted to the config default at boot.

**Response 200** — `{"status": "ok"}`.

**Errors** — `400` (an unknown/disallowed key, or an interval that isn't a whole number of minutes in 1–1440).

### `GET /api/settings/anthropic`

**Purpose** — Anthropic API key + selected model status.

**Response 200**:

```json
{ "configured": true, "masked_key": "sk-ant-A...", "model": "claude-sonnet-4-6", "storage": "keychain" | "encrypted_json" }
```

### `PUT /api/settings/anthropic`

**Purpose** — Save Anthropic API key and/or model name.

**Request body** — `{"api_key": "sk-ant-...", "model": "claude-sonnet-4-..."}`. Either field optional but at least one required.

**Response 200** — `{"status": "ok", "storage": "..."}`.

**Errors** — `400` (both fields blank).

**Side effects** — Saves the key to Keychain; persists `anthropic_model` in the `settings` table; busts the in-process AI client cache.

### `DELETE /api/settings/anthropic`

**Purpose** — Remove the stored Anthropic API key.

**Response 200** — `{"status": "ok"}`.

### `GET /api/settings/media-hosting`

**Purpose** — Cloudflare R2 media-hosting status. Threads fetches video from a URL rather than accepting an upload, so a clip must be briefly reachable over HTTPS; see `services/media_hosting.py`.

**Response 200**:

```json
{
  "configured": true,
  "account_id": "64102e7e...",
  "bucket": "my-threads-videos",
  "masked_access_key_id": "a1b2c3…7f8e",
  "secret_set": true,
  "download_url_ttl_seconds": 7200,
  "storage": "keychain"
}
```

`configured` is true only when all four values are present — a partial config cannot produce a working URL. The secret access key is never returned, only `secret_set`.

### `PUT /api/settings/media-hosting`

**Purpose** — Save any subset of the four media-hosting values.

**Request body** — `{"account_id": "...", "bucket": "...", "access_key_id": "...", "secret_access_key": "..."}`. Every field is optional; blank fields are left untouched so the form can be re-saved without re-entering the secret.

**Response 200** — `{"status": "ok", "storage": "..."}`.

**Errors** — `400` (all four fields blank).

**Side effects** — Both keys go to Keychain under the `media_hosting` namespace; `media_hosting_account_id` and `media_hosting_bucket` persist in the `settings` table.

### `DELETE /api/settings/media-hosting`

**Purpose** — Remove the R2 credentials and the bucket/account settings.

**Response 200** — `{"status": "ok"}`.

### `POST /api/settings/media-hosting/test`

**Purpose** — Upload a few bytes and read them back through a presigned URL, so a wrong account ID, an under-scoped token or a truncated secret surfaces with its real error instead of failing opaquely at send time.

**Response 200** — `{"status": "ok", "detail": {"bucket": "...", "account_id": "...", "key": "connection-test/...", "bytes": 62, "content_type_served": "text/plain", "download_url_ttl_seconds": 7200}}`.

**Errors** — `400` (not configured), `502` (upload or read-back failed; the message carries the provider's response).

**Note** — Deliberately does not delete the test object: the bucket enforces Object Lock with a 24-hour minimum retention, so an early delete is impossible. The bucket's 7-day lifecycle rule removes it.

### `GET /api/settings/oauth-clients`

**Purpose** — Return configured social OAuth clients (X / LinkedIn / Threads). Used by the Settings UI.

**Response 200**:

```json
{
  "storage": "keychain",
  "platforms": {
    "twitter": {
      "configured": true, "client_id": "...", "client_secret_set": false,
      "secret_required": false, "display": "X / Twitter",
      "console_url": "https://developer.x.com", "console_label": "developer.x.com",
      "id_label": "Client ID", "secret_label": "Client Secret",
      "instructions": "...", "masked_secret": ""
    },
    "linkedin": { /* secret_required: true */ },
    "threads":  { /* secret_required: true */ }
  }
}
```

`client_secret` itself is never returned, only a flag and a masked preview.

### `PUT /api/settings/oauth-clients/{platform}`

**Purpose** — Save or replace OAuth client credentials for `twitter`, `linkedin`, or `threads`.

**Request body** — `{"client_id": "...", "client_secret": "..."}`. `client_secret` may be omitted for `twitter` (public client) but is required for `linkedin` and `threads`.

**Response 200** — `{"status": "ok", "storage": "..."}`.

**Errors** — `400` (unsupported platform, missing `client_id`, missing required `client_secret`).

### `DELETE /api/settings/oauth-clients/{platform}`

**Purpose** — Remove stored OAuth client credentials for a platform.

**Response 200** — `{"status": "ok"}`.

**Errors** — `400` (unsupported platform).

### `GET /api/settings/threads-oauth`

**Purpose** — Report the configured Threads OAuth redirect URL. Used by the Settings UI: Meta only redirects OAuth back to HTTPS, so the popup-based Threads flow goes through a public "bounce" page (`DYS_THREADS_REDIRECT_URL`, defaulting to `https://nuclearcyborg.com/apps/scheduler/callback-threads-redirect`) that forwards `?code&state` back to `/api/oauth/threads/callback`; bounce-page source is in `cloudflare/`.

**Response 200**:

```json
{ "redirect_url": "https://nuclearcyborg.com/apps/scheduler/callback-threads-redirect", "local_port": 8008 }
```

### `GET /api/settings/social`

**Purpose** — List every social platform with its configuration status, schema for the legacy paste form, and a redacted view of the currently active credential's bundle.

**Response 200** — Object keyed by `twitter | bluesky | mastodon | linkedin | threads`. Each value:

```json
{
  "configured": true,
  "description": "...",
  "setup_guide": ["step 1", "step 2", ...],
  "fields": [ { "key": "...", "label": "...", "type": "text" | "password", "secret": true | false, "placeholder": "..." } ],
  "stored": { "<field key>": "<masked or plain value>" },
  "storage": "keychain"
}
```

### `GET /api/settings/social/{platform}`

**Purpose** — Same as the previous endpoint, restricted to one platform.

**Response 200** — Single platform dict (no outer keying).

**Errors** — `400` (unknown platform).

### `PUT /api/settings/social/{platform}`

**Purpose** — Update credentials via the legacy paste form. Merges into the active credential's bundle, or creates a fresh credential row when none exists.

**Request body** — Object of platform-specific fields (per `PLATFORM_FIELDS`). For new credentials, `_provider_id_from_paste` derives the stable provider id and display username from the body.

**Response 200**:

```json
{ "status": "ok", "storage": "...", "social_account_id": 1, "uuid": "..." }
```

(`social_account_id` and `uuid` only present when a credential row was touched.)

**Errors**

- `400` — Unknown platform; `bluesky` (which is OAuth-only and rejects this path); body contains no resolvable provider id and no active credential exists.

### `DELETE /api/settings/social/{platform}`

**Purpose** — Soft-delete every active credential for a platform (transitional "wipe this platform" button).

**Response 200** — `{"status": "ok", "deleted": <count>}`.

**Errors** — `400` (unknown platform).

**Side effects** — Each row gets `deleted_at` set, Keychain bundle is purged, `project_social_defaults` rows pointing at it are nulled.

### `GET /api/settings/blocklist`

**Purpose** — Return the comment-moderation blocklist for the default project.

**Response 200** — Array of `{id, keyword, is_regex, project_id, created_at}`.

### `POST /api/settings/blocklist`

**Purpose** — Add a keyword.

**Request body** — `{"keyword": "...", "is_regex": false}`.

**Response 200** — `{"status": "ok"}`.

**Errors** — `400` (empty `keyword`).

### `DELETE /api/settings/blocklist/{keyword_id}`

**Purpose** — Remove a keyword.

**Response 200** — `{"status": "ok"}`.

### `GET /api/settings/moderation-log`

**Purpose** — Recent moderation actions for the default project.

**Query params** — `limit` (int, default `50`).

**Response 200** — Array of `moderation_log` rows.

### `GET /api/settings/moderation-status`

**Purpose** — Next/last run timestamps for the comment-moderation APScheduler job.

**Response 200**:

```json
{
  "next_run": "2026-04-27T13:00:00+00:00" | null,
  "last_run": "2026-04-27T12:30:00+00:00" | null,
  "interval_minutes": 30
}
```

`null` for both when the job isn't currently registered. `last_run` is computed as `next_run - interval`.

### `POST /api/settings/moderation/run`

**Purpose** — Run comment moderation right now against the default project's videos.

**Response 200**:

```json
{
  "checked": 12,
  "matched": 1,
  "actions_by_video": { "abc123": [ { "comment_id": "...", "action": "held" | "rejected", ... } ] },
  "errors": [ { "video_id": "...", "error": "..." } ]
}
```

**Errors** — `500` (moderation run raised an unexpected exception).

---

## Built-in social OAuth flows (`/api/oauth`)

Source: `src/yt_scheduler/routers/oauth_routes.py`

These routes implement OAuth start/callback for each platform. The browser pre-opens a popup, the start endpoint returns an `auth_url`, the popup is redirected, and on callback the popup `postMessage`s its result back to the opener. Pending state is held in process; a server restart between start and callback forces a fresh start.

### `POST /api/oauth/linkedin/start`

**Purpose** — Begin LinkedIn OAuth (Authorization Code).

**Request body** — `{"client_id": "...", "client_secret": "...", "origin": "http://127.0.0.1:8008", "project_slug": "..." (optional)}`. If `client_id`/`client_secret` are omitted, the stored values from `oauth_clients` are used.

**Response 200** — `{"auth_url": "https://www.linkedin.com/oauth/v2/authorization?...", "redirect_uri": "..."}`.

**Errors** — `400` (no client configured, `origin` missing).

### `GET /api/oauth/linkedin/callback`

**Purpose** — Exchange the authorization code, look up the LinkedIn person URN via `userinfo`, persist the credential bundle.

**Query params** — `code`, `state`, `error`, `error_description`.

**Response 200** — Self-contained HTML page (`HTMLResponse`) shown in the popup; postMessages success/failure back to the opener.

**Side effects** — Exchanges code for token; calls `/v2/userinfo`; upserts credential row + Keychain bundle; binds project default if `project_slug` was supplied at start time.

### `POST /api/oauth/threads/start`

**Purpose** — Begin Threads OAuth.

**Request body** — `{"client_id": "...", "client_secret": "...", "origin": "...", "project_slug": "..." (optional)}`. Falls back to stored OAuth client.

**Response 200** — `{"auth_url": "...", "redirect_uri": "..."}`.

**Errors** — `400` (client missing, origin missing, or the resolved `redirect_uri` is not HTTPS).

**Notes** — `redirect_uri` is the configured Threads bounce page — `DYS_THREADS_REDIRECT_URL`, which defaults to `https://nuclearcyborg.com/apps/scheduler/callback-threads-redirect` (a static page that forwards `?code&state` back to `/api/oauth/threads/callback`; source in `cloudflare/`). Meta only allows HTTPS redirect URIs, so this must be HTTPS. Whatever `redirect_uri` is used here is replayed verbatim in the token exchange (Meta requires an exact match). For local testing without a deployed bounce page, use `POST /api/oauth/threads/exchange` or `POST /api/oauth/threads/token` instead.

### `GET /api/oauth/threads/callback`

**Purpose** — Exchange code, upgrade short-lived → long-lived (~60d), fetch username, persist credential.

**Query params** — `code`, `state`, `error`, `error_description`.

**Response 200** — HTML result page.

### `POST /api/oauth/threads/exchange`

**Purpose** — Meta-friendly alternative to the OAuth popup: paste a short-lived token from the Graph API Explorer, mint a long-lived token, persist credentials.

**Request body** — `{"app_secret": "...", "short_lived_token": "..."}`. `app_secret` falls back to the stored Threads OAuth client when omitted.

**Response 200**:

```json
{ "ok": true, "user_id": "...", "username": "...", "social_account_id": 1, "uuid": "...", "expires_in": 5184000 }
```

**Errors** — `400` (missing token, missing app secret), `502` (network error or upstream error from Graph API).

### `POST /api/oauth/threads/token`

**Purpose** — Store a long-lived Threads access token pasted directly by the user (e.g. from the Meta app dashboard's *User Token Generator* → "Generate Access Token"). No OAuth redirect, no short→long exchange — handy for local testing.

**Request body** — `{"access_token": "..."}`.

**Response 200**:

```json
{ "ok": true, "user_id": "...", "username": "...", "social_account_id": 1, "uuid": "..." }
```

**Side effects** — Calls `https://graph.threads.net/v1.0/me`; upserts the credential row + Keychain bundle (`access_token`, `user_id`, `username`, plus the stored Threads app secret if one is configured).

**Errors** — `400` (missing `access_token`), `502` (network error or `/me` failed — status passed through).

### `POST /api/oauth/twitter/start`

**Purpose** — Begin X / Twitter OAuth 2.0 PKCE.

**Request body** — `{"client_id": "...", "client_secret": "..." (optional for public clients), "origin": "...", "project_slug": "..." (optional)}`. Falls back to stored client.

**Response 200** — `{"auth_url": "...", "redirect_uri": "..."}`.

**Errors** — `400` (client missing, origin missing).

### `GET /api/oauth/twitter/callback`

**Purpose** — Exchange code (PKCE) + Basic auth (when secret set), fetch `@handle` and numeric id via `users/me`, persist credentials.

**Response 200** — HTML result page.

### `POST /api/oauth/mastodon/start`

**Purpose** — Begin Mastodon OAuth via per-instance dynamic client registration.

**Request body** — `{"instance_url": "https://mastodon.social", "origin": "...", "project_slug": "..." (optional)}`.

**Response 200** — `{"auth_url": "...", "redirect_uri": "..."}`.

**Errors** — `400` (instance/origin missing), `502` (instance refused dynamic registration), instance HTTP status passed through on registration failure.

**Side effects** — Registers a fresh OAuth app on the user's instance via `POST /api/v1/apps`.

### `GET /api/oauth/mastodon/callback`

**Purpose** — Exchange code, call `verify_credentials`, persist credential with handle of the form `acct@host`.

**Response 200** — HTML result page.

### `POST /api/oauth/youtube/start`

**Purpose** — Begin the YouTube web OAuth flow. Two modes (mutually exclusive):

1. `re_auth` — `{"origin": "...", "project_slug": "<existing>"}` re-authenticates an existing project (token refresh; channel must match the bound channel id).
2. `pre_create` — `{"origin": "...", "pre_create": {"name": "My new project"}}` runs the new-project wizard. Pass `pre_create: {}` for the channel-first flow where the project name is derived from the resolved YouTube channel title.

**Response 200** — `{"auth_url": "..."}`.

**Errors** — `400` (origin missing, no client secret uploaded, both modes given, neither mode given, slug already exists, invalid client_secret config), `404` (re-auth slug not found).

### `GET /api/oauth/youtube/callback`

**Purpose** — Exchange the YouTube authorization code (with PKCE replay), resolve the channel, and either create the project (pre_create mode) or refresh tokens (re_auth mode).

**Response 200** — HTML result page. PostMessage payload includes `mode`, `slug`, `channel_id`, `channel_title`, `channel_handle`, and `project_id` (pre_create only).

**Side effects** — In pre_create: inserts a new `projects` row, stores credentials, calls `ensure_default_template`. In re_auth: validates channel id matches the project's bound channel; updates the bound channel id if previously empty; refreshes credentials. Always returns 200 with success/failure rendered in HTML.

### `POST /api/oauth/bluesky/start`

**Purpose** — Begin Bluesky AT-proto OAuth (handle-based; per-account dynamic key + PAR).

**Request body** — `{"handle": "alice.bsky.social", "origin": "...", "project_slug": "..." (optional)}`.

**Response 200** — `{"auth_url": "https://bsky.social/oauth/authorize?..."}`.

**Errors** — `400` (handle invalid, origin missing, identity resolution failed, PDS auth-server discovery failed, PAR push failed).

**Side effects** — Resolves `handle → DID → PDS`; discovers the auth server; generates PKCE pair + EC key; pushes a PAR request.

### `GET /api/oauth/bluesky/callback`

**Purpose** — Verify `iss`, exchange the code (DPoP-bound), persist credential bundle.

**Query params** — `code`, `state`, `iss`, `error`, `error_description`.

**Response 200** — HTML result page. Refuses callbacks missing `iss` or with mismatched `iss` (mix-up defense).

---

## Social credentials (`/api/social-credentials`)

Source: `src/yt_scheduler/routers/social_credentials_routes.py`

### `GET /api/social-credentials`

**Purpose** — List active credentials.

**Query params** — `platform` (string, optional). Must be one of the known platforms when given.

**Response 200** — Array of credential dicts:

```json
[
  {
    "id": 1, "uuid": "...", "platform": "twitter",
    "provider_account_id": "...", "username": "alice",
    "display_name": "Alice", "is_nickname": false,
    "credentials_ref": "...", "created_at": "...", "deleted_at": null,
    "needs_reauth": false, "label": "@alice @X",
    "token_acquired_at": "2026-07-28T19:24:18+00:00",
    "token_expires_at": "2026-09-26T19:13:47+00:00"
  }
]
```

`token_acquired_at` / `token_expires_at` are non-secret mirrors of the Keychain bundle's token metadata, stamped by every flow that mints or refreshes a token and mirrored by `save_bundle` / `upsert_credential`. Either can be `null`, meaning the flow that last wrote the bundle predates stamping (acquired) or the issuer never reported a lifetime (expires — e.g. Mastodon tokens don't expire, and a pasted Threads token's age is unknown until its first refresh). `null` renders as unknown, never as "doesn't expire".

**Errors** — `400` (unknown `platform`).

### `GET /api/social-credentials/{uuid}`

**Purpose** — Fetch one credential.

**Response 200** — Single credential dict (same shape as above).

**Errors** — `404` (no such credential).

### `GET /api/social-credentials/{uuid}/dependents`

**Purpose** — Return projects + template slots that point at this credential — used by the delete-confirmation dialog.

**Response 200**:

```json
{
  "projects": [ { "slug": "...", "name": "...", "platform": "..." } ],
  "slots":    [ { "slot_id": 7, "template_id": 1, "template_name": "...", "project_slug": "...", "project_name": "...", "platform": "..." } ]
}
```

**Errors** — `404` (no such credential).

### `DELETE /api/social-credentials/{uuid}`

**Purpose** — Soft-delete a credential.

**Query params** — `confirm` (bool, default `false`).

**Response 200**:

- Without `?confirm=1`: `{"would_delete": <cred>, "dependents": <deps>, "needs_confirm": true}`.
- With `?confirm=1`: `{"deleted": <cred with deleted_at set>, "needs_confirm": false}`.

**Errors** — `404` (no such credential).

**Side effects** (when confirmed) — Sets `deleted_at`; deletes Keychain bundle; deletes any rows in `project_social_defaults` referencing this credential; template slots remain pointing at the row so the UI renders "Missing credential".

### `POST /api/social-credentials/{uuid}/refresh-username`

**Purpose** — Re-run the platform's identity endpoint and update the row's `username` if it has changed.

**Response 200** — `{"changed": true, "username": "<new>"}` or `{"changed": false, "username": "<existing>"}`.

**Errors** — `404` (no such credential).

**Side effects** — Calls the platform's identity endpoint; on change, updates `social_accounts.username`.

---

## YouTube imports (`/api/projects/{slug}/imports`)

Source: `src/yt_scheduler/routers/import_routes.py`

### `GET /api/projects/{slug}/imports/available`

**Purpose** — List YouTube videos on the project's authenticated channel that aren't yet in the local DB.

**Query params** — `max_results` (int, default `50`).

**Response 200** — Array:

```json
[
  {
    "video_id": "abc",
    "title": "...",
    "description": "...",
    "published_at": "...",
    "thumbnail_url": "https://...",
    "privacy_status": "public" | "unlisted" | "private",
    "embeddable": true
  }
]
```

**Errors** — `404` (unknown slug), `500` (YouTube API error).

**Side effects** — Calls `youtube.list_channel_videos` (~1 quota each per page).

### `POST /api/projects/{slug}/imports/import`

**Purpose** — Import a specific YouTube video by id into the project.

**Request body** — `{"video_id": "abc", "parent_item_id": "optional_parent_id"}`.

When `parent_item_id` is set, the imported video lands as a promo child of that primary (hidden from Dashboard, surfaces on the parent's Promo Videos screen). The parent must exist in the same project and itself be a primary (`parent_item_id IS NULL`); only one level of parenting is supported.

**Response 200** — The newly inserted `videos` row.

**Errors** — `404` (unknown slug), `400` (`video_id` missing, video not on YouTube, video already imported, parent_item_id not found / belongs to another project / is itself a child), `500` (any other failure).

**Side effects** — Calls `youtube.get_video` (~1 quota); downloads the thumbnail to `UPLOAD_DIR`; inserts the row; records `imported`; tries to download the existing YouTube caption (50 quota) and store it as a transcript; runs `auto_actions.run_post_create_actions(... source="import")` in the background.

**Renderer (background path)** — When auto-gen-socials is enabled for imports, the background job renders each platform's slot body through the same engine as [`POST /api/expand_text`](#post-apiexpand_text). Same variables and same syntax.

## Promo Videos (`/api/projects/{slug}/videos/{parent_id}/promos`)

Source: `src/yt_scheduler/routers/promo_routes.py`

Bulk-upload promo children under a primary video. Each upload runs through the multi-step auto-action chain (title → upload → probe → transcribe → description → tags → push metadata); per-card progress is polled via either the upload-jobs endpoint (pre-INSERT) or `GET /api/videos/{id}/auto-actions` (post-INSERT).

### `GET /api/projects/{slug}/videos/{parent_id}/promos`

**Purpose** — List the parent's children, bucketed by `item_type`.

**Query params** — `include_archived` (bool, default `false`): when `true`, archived clips are included in `children` (each carries `archived: 1` and `archived_at`); when `false` they're omitted. `summary`/`readiness` always count active clips only, regardless of this flag.

**Response 200** — `{"summary": {"segment": N, "short": N, "hook": N}, "children": {"segment": [...], "short": [...], "hook": [...]}, "readiness": {"segment": {"count": N, "line": "...", "state": "..."}, "short": {...}, "hook": {...}}, "pending_jobs": [{"job_id": str, "video_id": str|null, "item_type": "segment"|"short"|"hook"|null, "state": str, "title": str|null, "last_error": str|null}, ...], "archived_count": N}`. Each child entry is the `_video_public` projection, plus per-card schedule-readiness: `ready` (bool) and `missing` (list of human-readable reasons, e.g. `["tags (need ≥ 3)"]`), computed from the raw row via the same `is_ready_for_schedule` the per-tier `readiness` summary uses — so a card's "Ready" bar can never disagree with the tier summary. This matters for imported clips, whose `auto_action_state` is `NULL` (they never ran the promo chain), so the card cannot infer readiness from the chain state alone. `archived_count` is the number of archived clips on this parent (drives the page's "Show N archived" toggle). `readiness` is a per-tier one-line summary for the parent's Promo videos card: `line` is human-readable (e.g. `"4 need thumbnail · 1 ready"`, `"all ready"`); `state` is one of `empty | ready | working | attention` (drives the card's status dot). `pending_jobs` lists in-flight promo-chain jobs for this parent that haven't reached `ready` (e.g. just-inserted Generate clips still cutting/uploading/transcribing) — surfaced from the in-memory job table so the page renders live placeholder cards before a DB row exists, on any load — PLUS persisted failed jobs from `pending_promo_jobs` (status `failed`, last 7 days, not currently live in memory), so a failure survives the in-memory TTL, page reloads, and restarts until it is retried or dismissed. `state` is one of the promo-chain states (`pending`, `cutting`, `uploading`, `transcribing`, `generating_desc`, …), `failed:<step>` (live in-memory failure), or plain `failed` (persisted row — the step isn't persisted, so none is claimed).

**Errors** — `404` (project or parent video not found in project), `400` (parent is itself a child — only one level of parenting is supported).

### `POST /api/projects/{slug}/videos/{parent_id}/promos/{video_id}/archive`

**Purpose** — Archive a promo clip: hide it from the Promo Videos page without deleting it. The `videos` row and the YouTube video both remain; sets `archived = 1` and `archived_at`. Reversible via the unarchive endpoint. Used to clear duplicates (e.g. an imported clip that duplicates a generated one) non-destructively.

**Response 200** — `{"archived": true, "video_id": str}`.

**Errors** — `404` (project/parent not found, or `video_id` is not a clip under this parent).

### `POST /api/projects/{slug}/videos/{parent_id}/promos/{video_id}/unarchive`

**Purpose** — Restore an archived promo clip back onto the page (`archived = 0`, `archived_at = NULL`).

**Response 200** — `{"archived": false, "video_id": str}`.

**Errors** — `404` (project/parent not found, or `video_id` is not a clip under this parent).

### `POST /api/projects/{slug}/videos/{parent_id}/promos/upload`

**Purpose** — Queue one or more files into the promo auto-action chain.

**Request body** — `multipart/form-data` with `files[]` (one or more video files) and optional `item_type` (one of `segment`, `short`, `hook`; when omitted, the chain derives `item_type` from the probed duration).

**Response 200** — `{"jobs": [{"job_id": "job_<hex>", "filename": "..."}, ...]}`. Each `job_id` is the polling handle.

**Errors** — `404` (project / parent missing), `400` (invalid `item_type`, no files, parent is itself a child).

**Side effects** — Saves each file under `UPLOAD_DIR`; spawns one `auto_actions.start_promo_upload(...)` task per file. The chain runs sequentially per-task: the transcription step is serialised across all in-flight promo uploads via a module-level lock so concurrent Whisper instances don't thrash the box. Each finished chain spends ≈ 150 YouTube quota (100 upload + 50 metadata update).

### `GET /api/projects/{slug}/videos/{parent_id}/promos/upload-jobs/{job_id}`

**Purpose** — Poll a single in-flight upload job.

**Response 200** — `{"job_id": "...", "filename": "...", "parent_id": "...", "video_id": "..." | null, "state": "...", "last_error": "..." | null, "title": "..." | null}`. `video_id` is `null` until the YouTube upload step succeeds; once set, the UI should switch to polling `GET /api/videos/{video_id}/auto-actions` (the job dict is dropped from memory when the chain terminates, returning 404 on a stale poll).

**Errors** — `404` (job not found / already completed).

### `POST /api/projects/{slug}/videos/{parent_id}/promos/upload-jobs/{job_id}/retry`

**Purpose** — Re-run a persisted failed promo job. Pre-videos-row failures (a cut, title, or YouTube-upload step dying — the canonical case is the YouTube "Video Uploads per day" quota 429) persist in `pending_promo_jobs` with their cut file and cut params, and surface in the promos payload's `pending_jobs` with `state == "failed"` for up to 7 days. Retry flips the row back to `pending` and re-spawns the chain: the intact cut file is reused when it still exists; otherwise the clip is re-cut from the stored parent params.

**Response 200** — the same job dict as `GET .../upload-jobs/{job_id}`, for polling.

**Errors** — `404` (no failed job with this id under this parent), `409` (job is currently running, or can't be reconstructed — e.g. already uploaded to YouTube but never inserted, or cut file gone with no re-cut params; the message says why).

### `POST /api/projects/{slug}/videos/{parent_id}/promos/upload-jobs/{job_id}/dismiss`

**Purpose** — Permanently dismiss a persisted failed promo job. Marks the row `dismissed` (it stops surfacing in `pending_jobs`, across reloads and restarts) and deletes its cut file — derived data, re-creatable from the parent since the row keeps the cut params. The file is NOT deleted when a `videos` row references it (the job was retried and succeeded in another tab) or when the stored path falls outside the uploads dir.

**Response 200** — `{"dismissed": true}`.

**Errors** — `404` (no failed job with this id under this parent), `409` (job is currently running — only failed jobs can be dismissed).

### `POST /api/projects/{slug}/videos/{parent_id}/promos/generate/preview`

**Purpose** — Kick off a Generate-from-source preview job. Claude proposes hook / short / segment ranges out of the parent's local MP4 using the parent's SRT transcript. Returns a job_id the client polls; the heavy lifting (transcription if missing + the three parallel Claude calls) runs in the background.

**Request body** — `{"kinds": [...], "crop_vertical": {"hook": bool, "short": bool, "segment": bool}, "max_per_kind": {"hook": int, "short": int, "segment": int}}`. `kinds` is a non-empty subset of `hook | short | segment`. `crop_vertical` defaults to `{hook: true, short: true, segment: false}` for any kind not explicitly set. `max_per_kind` is the per-kind cap on how many proposals Claude is asked for (and how many the server will keep after filtering); each entry must be an integer in `[1, 20]`, default `8`. Both Claude's prompt and the server-side validator honour this value.

**Response 200** — `{"job_id": "gen_...", "eligible_kinds": [...], "ineligible_kinds": [...], "parent_warnings": [...], "parent_video_file_url": str|null, "parent_browser_playable": bool|null, "parent_youtube_id": str}`. `eligible_kinds` is the subset of `kinds` whose minimum required parent length is satisfied (each kind needs `kind_max + 15 s` of parent); `ineligible_kinds` is the rest. `parent_warnings` is the same `quality_warnings` shape from `GET /file-info`. The `parent_*` fields tell the client how to render proposal previews (`<video src="…">` when `parent_browser_playable` is true, YouTube iframe with `?start=&end=` otherwise).

**Errors** — `400` (no kinds requested; `max_per_kind` value out of `[1, 20]` or not an integer; parent duration unknown; parent longer than 4 h; no requested kind eligible; parent has no local file; transcript was hand-edited and lacks timestamps), `404` (project or parent missing or parent is itself a child).

### `GET /api/projects/{slug}/videos/{parent_id}/promos/generate/jobs/{job_id}`

**Purpose** — Poll a generate preview job.

**Response 200** — `{"job_id", "parent_id", "project_id", "kinds", "crop_vertical", "state", "last_error", "progress_message", "proposals", "rejected", "raw_counts", "kind_errors", "warnings"}`. `state` transitions `pending → transcribing? → proposing → cutting_previews → done | failed`. `transcribing` only appears when the parent had no usable timestamped transcript at job start; the inline whisper run upserts back into the transcripts table when complete so future Generates on the same parent skip it. `cutting_previews` cuts one preview file per accepted proposal so the review page plays the real clip; Confirm reuses those files rather than re-cutting. Vertical-crop kinds are reframed there by the on-device `clipcrop` head-tracking recrop (the Claude-vision pass it replaced is gone). Once the proposal pass completes (so from `cutting_previews` onward), three sibling fields explain what did NOT make it, so a count on screen can never stand in for an explanation:

- `raw_counts` — `{kind: int}`, how many proposals Claude actually returned for that kind. `raw_counts` minus accepted is the number the server refused; without it, 23 proposed / 7 kept is indistinguishable from Claude finding 7. Kinds in `kind_errors` are **omitted**, not reported as 0 — their count is unknown.
- `rejected` — `{kind: [{"kind", "reason", "detail", "title", "duration_seconds"}]}`. `reason` is one of `invalid_indices | index_out_of_bounds | duration_out_of_band | duplicate_title | overlaps_existing | over_output_cap`; `detail` is the sentence shown to the user. Kinds with no rejections are omitted.
- `kind_errors` — `{kind: str}` for kinds whose Claude call failed outright. Distinct from a rejection: the count is unknown, not zero. One kind failing no longer fails the others.
- `warnings` — `[{"code": str, "message": str}]`, present (as a non-empty list) only when the run hit something the reviewer should see. Today the sole code is `unexpected_timing_grid`: the transcription's detected timing grid wasn't Apple SpeechAnalyzer's expected 60 ms (either `detect_quantum` misfired or Apple changed the grid). Clip edge math assumes 60 ms, so the warning surfaces on the review page (rule C — never a silent "fine") next to the source-quality `parent_warnings`. Absent when everything was nominal; the field is dropped from the response, not sent as `[]`.

When `state == "done"`, `proposals` is a `{kind: [{...}]}` map; each proposal carries:

- `kind`, `start_seconds`, `end_seconds`, `duration_seconds`, `title`, `reason` — straight from the proposal call. `start_seconds` / `end_seconds` are the ramp-inclusive cut bounds (they extend a hair into the inter-word gaps for the audio fades). On the word-stream (index) path, `duration_seconds` is therefore the *ramped* span and can run slightly past the kind's stated length window (up to one ramp on each edge, ≤0.5 s each): the length constraint is enforced on the spoken-content duration, while the extra is silence in the inter-word gaps.
- `rating` (int 1-4 | null) — the model's self-score from the word-stream (index) proposal path; `null` on the legacy anchor path.
- `audio_fade_in`, `audio_fade_out` (float seconds) — audio edge ramps the cut applies (cubic in / linear out). Non-zero only on the word-stream path; `0` on the anchor path.
- `vertical_crop` (bool) — mirror of the kind's crop toggle at preview time; the confirm endpoint relies on this to know whether to apply the crop filter.
- `x_shift_normalized` (float in `[-1.0, 1.0]`) — actual shift the cut step will use after the cautious-shift threshold (0.15) is applied. Always 0 on live proposals: crop geometry is owned by the on-device recrop. Retained so stored rejection rows from before that change still round-trip.
- `crop_classification` (str, historical) — one of `centered | off_center | drift | multi_face | no_face | vision_error`. Only ever present on rejection rows stored while the Claude-vision pass existed; never set on a live proposal.
- `crop_confidence` (float, historical) — model-reported 0–1 confidence, on those same stored rejection rows only.
- `crop_uncertain` (bool) — set by the on-device recrop when it could not track a subject (b-roll / screen content), so the UI can badge "center crop will be used". Also derived from `crop_classification` on restored historical rejections.

Terminal jobs (`done` / `failed`) are evicted from the in-memory job dict 30 minutes past terminal state. Long enough that a user reviewing 24 proposals at a leisurely pace doesn't have the job evict under them (the confirm endpoint's `vertical_crop` cross-check goes away when the job is gone), short enough that the dict can't grow unboundedly on a long-running install.

**Errors** — `404` (job not found / dropped from memory after a process restart).

### `POST /api/projects/{slug}/videos/{parent_id}/promos/generate/confirm`

**Purpose** — Cut and insert the user-accepted proposals. Each accepted entry spawns a promo chain job: ffmpeg cut (gated by a per-encoder semaphore in `services/clipper` — 4-wide for hardware/videotoolbox cuts, 8-wide for software/libx264), YouTube upload, then the regular probe → transcribe → describe → tag → push-metadata steps (the chain itself is gated by a 4-wide semaphore in `services/auto_actions` so a 16-clip burst queues rather than thrashes the box). The new rows have `cut_start_seconds` / `cut_end_seconds` populated so the next Generate run on the same parent can skip those ranges. Generated rows are stamped `source_file_origin='generated_clip'` (not `'uploaded'`) so Replace-source's resolution-downgrade warning skips them the way it skips `youtube_download`.

**Request body** — `{"accepted": [{"kind": "hook" | "short" | "segment", "start_seconds": float, "end_seconds": float, "title": str, "vertical_crop"?: bool, "x_shift_normalized"?: float, "audio_fade_in"?: float, "audio_fade_out"?: float}, ...], "rejected"?: [{"kind", "start_seconds", "end_seconds", "title"?, "reason"?, "x_shift_normalized"?, "crop_classification"?, "crop_confidence"?}, ...], "job_id"?: str}`. `vertical_crop` defaults to false; `x_shift_normalized` to 0. When set, the cut step pulls a 9:16 (1080×1920) column centered at `(iw - crop_width)/2 + x_shift_normalized * (iw - crop_width)/2` — i.e. shift +1.0 fully aligns the crop with the right edge, -1.0 with the left, 0 is dead-center. Both fields are clamped to their respective bounds server-side. `audio_fade_in` / `audio_fade_out` default to 0 and matter only on the word-stream proposal path — they re-apply the audio edge ramps for the fallback re-cut (when the fade-bearing preview cut isn't adopted). The optional `job_id` ties the call back to the `/generate/preview` job that produced these proposals; when supplied, the server cross-checks each entry's `vertical_crop` against the preview-time `crop_vertical[kind]` snapshot and forces it to false (with `x_shift_normalized=0`) on any kind whose preview toggle was off — defense against a tampered client body requesting a crop for a kind the user had toggled off. The optional `rejected` array persists into the `generate_rejections` table (migration 028) so a subsequent visit to the review page can show those entries in a "Previously dismissed" section with Restore buttons; rejections are *not* fed into the next Claude proposal call.

### `GET /api/projects/{slug}/videos/{parent_id}/promos/generate/rejections`

**Purpose** — List proposals the user has previously dismissed for this parent. Backs the "Previously dismissed" section of the Generate review page.

**Response 200** — `{"rejections": [{"id": int, "kind": str, "start_seconds": float, "end_seconds": float, "duration_seconds": float, "title": str|null, "reason": str|null, "x_shift_normalized": float|null, "crop_classification": str|null, "crop_confidence": float|null, "rejected_at": str}, ...]}`. Sorted newest first. Survives process restart (unlike the in-memory generate-job dict).

**Errors** — `404` (project / parent missing or parent is itself a child).

### `POST /api/projects/{slug}/videos/{parent_id}/promos/generate/rejections`

**Purpose** — Persist one or more dismissed proposals immediately, the moment the user dismisses them on the review page — independent of `/generate/confirm` (which only records rejections as a side-effect of accepting at least one clip). Keeps the "Previously dismissed" memory in sync even when the user accepts nothing.

**Request body** — `{"rejected": [{"kind": "hook" | "short" | "segment", "start_seconds": float, "end_seconds": float, "title"?: str, "reason"?: str, "x_shift_normalized"?: float, "crop_classification"?: str, "crop_confidence"?: float}, ...]}`. Each entry is upserted into `generate_rejections` keyed on `(parent_id, project_id, kind, start_seconds, end_seconds)`; malformed entries are skipped.

**Response 200** — `{"stored": int}` — the number of entries actually written.

**Errors** — `400` (missing / empty `rejected` list); `404` (project / parent missing or parent is itself a child).

### `DELETE /api/projects/{slug}/videos/{parent_id}/promos/generate/rejections/{rejection_id}`

**Purpose** — Restore a previously-dismissed proposal — drops the row from `generate_rejections`. The review page calls this when the user clicks Restore on a dismissed card; the UI then re-renders the card as an active proposal.

**Response 200** — `{"deleted": bool}`. `false` when the id doesn't exist *or* belongs to a different parent (defense against slug-confused cross-tab requests) — the caller can treat both as "the row is gone, refresh the list".

**Errors** — `404` (project / parent missing).

**Response 200** — `{"jobs": [{"job_id", "kind", "title"}, ...]}`. Each `job_id` is polled via the existing `/upload-jobs/{job_id}` endpoint; the initial state is `pending` ("Queued…"). The chain stamps `cutting` only while an actual cut runs — confirm-created jobs adopt the already-cut preview file, so they normally go straight from `pending` to `uploading` without ever showing `cutting`.

**Errors** — `400` (empty `accepted`, no usable entries after defensive filtering, parent has no local file, or supplied `job_id` is unknown / expired — the in-memory generate-job dict TTL-evicts 30 minutes past terminal), `404` (project / parent missing). Defensive filter drops entries with non-finite `start_seconds` / `end_seconds`, wrong kind, end before start, range outside parent bounds, or empty title.

### `GET /api/projects/{slug}/videos/{parent_id}/promos/update-descriptions/preview`

**Purpose** — Dry run for the "Update all descriptions" confirm dialog: which promo clips under this parent would have their description re-generated, which would be skipped and why, and what it costs.

**Query params** — `tiers` (optional): comma-separated subset of `hook|short|segment`. Omit it for every tier; supplying it empty (`?tiers=`) is a `400`, not "everything".

**Response 200** — `{"eligible": [{"id","title","item_type","status"}, ...], "ineligible": [{..., "reason": "..."}, ...], "counts": {"segment": N, "short": N, "hook": N}, "quota_units_estimate": N}`. A clip is ineligible when it has no YouTube video (`youtube_video_id IS NULL`), its YouTube video is deleted, it has no usable transcript to describe from, or a background chain currently owns the row. `quota_units_estimate` is `len(eligible) × 51` — `videos.list` (1) to read the live snippet plus `videos.update` (50).

**Errors** — `404` (project / parent missing), `400` (parent is itself a child, or an unknown tier in `tiers`).

### `POST /api/projects/{slug}/videos/{parent_id}/promos/update-descriptions`

**Purpose** — Re-generate the description for this parent's promo clips against the **current** promo description prompt template and push each result to YouTube. This is the "I edited the prompt, now bring the back catalogue in line" path; it deliberately overrides the chain's "already has a description" short-circuit.

**Request body** (all optional) — `{"tiers": ["hook", "short"], "video_ids": ["<id>", ...]}`. `tiers` is a real JSON list, never comma-split, so `["hook,short"]` is one unknown tier rather than two; omit the key for every tier, and an empty list is a `400` rather than "everything" (guessing there spends a Claude call and ~51 quota units per clip nobody asked for). `video_ids` narrows the run to specific clips (used by the per-card "Retry description update"); every id must appear in the eligible set or the whole request is rejected.

**Response 202** — `{"started": [{"id","title","item_type","status"}, ...], "skipped": [{..., "reason"}, ...], "quota_units_estimate": N}`. Work is detached; per-clip progress lands in `videos.auto_action_state` as `updating_desc` → `ready`, or `failed:updating_desc` with the real error in `auto_action_last_error`.

**Errors** — `404` (project / parent missing), `400` (parent is itself a child, unknown/empty/malformed `tiers`, empty/ineligible `video_ids`, or nothing eligible under this parent), `409` (every eligible clip was claimed by another job between the scan and the claim).

**Side effects** — Per clip, in this order: one Claude call (the `description_from_transcript_prompt_promo` variant chain), then a `youtube.update_video_metadata` call carrying **only** the description, then the write to `videos.description` + `description_generated_at` and a `metadata_updated` event. Push-before-persist matches [`apply-description`](#post-apivideosvideo_idapply-description): a failed push must not leave the row claiming YouTube holds text it never received, which for an already-published clip nothing would ever reconcile. Title and tags are left as YouTube currently holds them, so an edit made on YouTube isn't clobbered. Generation is refused (per clip) rather than falling back to keyframes when the transcript is unusable, because the frames prompt is a different template and would silently drop whatever the transcript prompt requires. Concurrency is bounded by the shared promo-chain semaphore (4). Clips are claimed as a single transaction, so a failure during the claim pass fails the whole request and leaves no clip claimed. The first clip to hit YouTube's **daily** quota wall stops the batch and the remaining clips fail with that explanation; exhaustion is identified by the server's machine-readable `error.errors[].reason` (`quotaExceeded` / `dailyLimitExceeded`), never by matching the exception text, and transient `rateLimitExceeded` throttling deliberately does not stop the batch.

### `POST /api/projects/{slug}/videos/{parent_id}/promos/schedule-all/preview`

**Purpose** — Dry-run for the review modal. Computes per-tier independent schedule chains anchored to the parent's `publish_at` (or `parent_publish_at` when the parent isn't scheduled yet, or now when the parent is already published).

**Request body** (all optional) — `{"parent_publish_at": "ISO 8601", "delays": {<per-tier {value,unit} overrides, same shape as `promo-delays`>}, "order": ["<video_id>", ...]}`. `parent_publish_at` is only honoured when the parent has no `publish_at` and isn't published. `delays` overrides the project's saved per-tier delays for this computation; when omitted, the saved delays are used. `order` is an explicit video-id sequence (from drag-reordering) — children within each tier are sequenced by their index in it.

**Response 200** — `{"parent": {"id","title","publish_at","status","already_published","ready","missing"}, "rows": [{"video_id","title","item_type","tier","target_time","ready","missing","already_scheduled"}, ...], "total_span": ISO|null, "warnings": [...], "anchor_publish_at": ISO|null}`. Rows sorted by `target_time`; each row's `ready` reflects the same readiness check used by the commit endpoint. `already_scheduled` is true for a child that already has a `publish_at` — such rows are shown at their existing time and are left untouched by the commit endpoint (so their readiness does not gate the batch and any manual publish-time pin is preserved). `parent.already_published` is true when the parent's app status is `published` **or** it is already public on YouTube (an imported episode that went live outside the app); for such a parent the readiness check and publish-time prompt are skipped and the promo chains anchor from now.

**Errors** — `404` (project / parent missing), `400` (parent is itself a child, or a malformed `delays` payload).

### `POST /api/projects/{slug}/videos/{parent_id}/promos/schedule-all`

**Purpose** — Commit the batch from the preview modal. Calls `schedule_publish` for each freshly-computed child (and for the parent when `parent_publish_at` is supplied and the parent isn't already published). Children that are already scheduled (`already_scheduled` in the preview) are left untouched.

**Request body** (all optional) — `{"parent_publish_at": "ISO 8601", "delays": {<per-tier {value,unit} overrides>}, "order": ["<video_id>", ...]}`. Same `delays` / `order` semantics as the preview. A supplied `delays` is additionally saved as the project's `promo_delays` default so the next batch keeps the same pace.

**Response 200** — `{"scheduled": [{"video_id", "publish_at"}, ...], "errors": [{"video_id", "error"}, ...], "warnings": [...]}`. `schedule_publish` commits per child, so the batch is not a single transaction; a per-child write failure is reported in `errors` rather than aborting the batch. The operation is idempotent — re-running resumes the chain from whatever was already scheduled.

**Errors** — `404` (project / parent missing), `400` (parent is itself a child / parent has no publish time / readiness gates fail / no eligible children / malformed `delays`).

**Side effects** — For each scheduled video: writes `videos.publish_at`, sets `videos.status='scheduled'`, registers the APScheduler `publish_video_job` (which also re-stages per-post social jobs), and stamps `publish_at_manual = 0` so future cascade routines may sweep these rows.


## Smart queues (`/api/projects/{slug}/smart-queues`)

Source: `src/yt_scheduler/routers/smart_queue_routes.py`

Project-wide social posting of promo clips: an ordered list of videos plus a weekly recurrence, posting one video to every enabled slot of its template at each recurrence time. See `SMART_QUEUE.md` for the design.

Eligibility is decided by a single `smart_queue.is_eligible()` used by both the Auto-select preview here and the live-transition hook, so the two cannot drift into disagreeing about which videos belong in a queue.

### `GET /api/projects/{slug}/smart-queues`

**Response 200** — `{"queues": [{...queue, "slots": [...], "counts": {"queued": N, "scheduled": N, "posted": N, ...}}]}`. `counts` buckets the queue's items. `queued` and `scheduled` are both pending. **`posted` and `failed` are derived from the item's `social_posts` rows, not from `smart_queue_items.state`** — sending updates the post and never the item, so an item stays `scheduled` for life and counting the column reported "0 posted" forever.

### `GET /api/projects/{slug}/smart-queues/{queue_id}`

**Response 200** — the queue row plus `slots`. **404** when the queue doesn't exist *or* belongs to another project — reported as not-found rather than forbidden so an id in another project isn't confirmed to exist by the error.

### `POST /api/projects/{slug}/smart-queues`

**Body** — `name`, `template_id`, `timezone` (IANA name, e.g. `America/Los_Angeles`), `slots` (`[{"weekday": 0-6 where 0=Monday, "time_of_day": "HH:MM"}]`). Optional: `min_duration_seconds` (default 0), `max_duration_seconds` (default 180), `orientations` (default `["portrait","square"]`), `exclude_already_posted` (default true), `auto_add_on_live` (default true), `missed_policy` (`post_late` | `reschedule_end` | `remove`, default `post_late`), `missed_grace_hours` (default 24, applies only to `post_late` and is stored NULL otherwise).

Keys the body omits are left to the documented creation defaults — the route forwards only what was actually sent, so an absent key can't override a default and then fail its own validation.

**Response 200** — the created queue. **400** with the specific reason for: no posting times, an inverted duration range, an unknown orientation or timezone, `post_late` without a positive grace window, or a duplicate name within the project.

`item_type` is deliberately **not** a queue field: the template's `applies_to` is the single source of truth for which types a queue can touch.

### `PATCH /api/projects/{slug}/smart-queues/{queue_id}`

Partial update. `slots`, when present, **replaces the whole set**. Switching `missed_policy` away from `post_late` clears `missed_grace_hours` rather than leaving a stale number that does nothing.

### `DELETE /api/projects/{slug}/smart-queues/{queue_id}`

**Response 200** — `{"deleted": true, "cancelled_posts": N}`. Cancels every pending per-post job the queue owns. Posting history is **kept**: `social_posts.smart_queue_item_id` is `ON DELETE SET NULL`, so posts a deleted queue sent remain readable on each video.

### `POST /api/projects/{slug}/smart-queues/{queue_id}/candidates`

**Purpose** — Preview which videos the queue would take, what it excluded and why, and when they would go out. **Writes nothing.**

**Body** (all optional) — `min_duration_seconds`, `max_duration_seconds`, `orientations`, `exclude_already_posted` override the saved filters *for this preview only*, so the config screen can show the effect of a change before saving. `shuffle` (bool) reorders the proposed batch; it never touches items already scheduled by this queue.

**Response 200** — `{"waiting": N, "eligible": [...], "excluded": [{...video, "reasons": [...]}], "unknown_dimensions": N, "summary": {"total": N, "by_type": {...}}, "forecast": [ISO, ...], "ends_at": ISO|null, "warnings": [...]}`.

`waiting` counts items auto-add already put in the queue with no posting time. They are **not** candidates — they are in the queue already — but Accept schedules them first, so the screen has to be able to say they exist.

The forecast continues after everything the queue has already stamped, matching what Accept will actually do. Computing it from *now* made every predicted date wrong as soon as the queue had anything pending.

`forecast[i]` is when `eligible[i]` would post. Occurrences are enumerated as **local** dates in the queue's timezone and converted individually, so an instant on the far side of a DST boundary still lands at its stated wall-clock time. Every excluded video carries its reasons, and `unknown_dimensions` counts those whose orientation can't be determined — a video the filters dropped is always accounted for rather than silently missing from the total. `warnings` carries e.g. "this queue has no posting times", so an empty forecast can't be mistaken for "nothing scheduled".

### `POST /api/projects/{slug}/smart-queues/{queue_id}/accept`

**Purpose** — Schedule a batch of videos onto the queue's recurrence.

**Body** — `video_ids` (optional): the batch, **in the order the user is looking at** (shuffled or not). This endpoint does not re-sort it; ordering is a UI concern and Accept honours whatever it is sent. **Omit it, or send `[]`, to give posting times to the items auto-add has been collecting** without adding anything new.

Two sources feed one plan, in this order: items already in the queue in state `queued` (what auto-add appends when a video goes live — they keep their position, so they go out in the order they arrived), then `video_ids`. **Accept is the only thing that assigns a posting time**; auto-add deliberately does not, so "when does this go out" is decided in one place.

**Response 200** — `{"scheduled": N, "items": [{"id", "video_id", "scheduled_at", "posted_to_any"}], "skipped": [{"video_id", "platform", "reason"}], "errors": [{"video_id", "post_id"?, "error"}]}`.

It does not raise once the loop has begun — a caller told nothing cannot tell a half-landed batch from a normal one, so every outcome comes back in the ledger. `skipped` reasons include `already scheduled by this queue` (re-submitting a stale selection cannot double-book), `belongs to a different project`, and `video no longer exists`. Ids repeated within one request are scheduled once.

Each video is written **whole or not at all**: its item row and all its posts commit in one transaction, and posts carry `scheduled_at` from the moment they exist, so a crash before the timer is registered is recovered at the next restart rather than stranding a post nothing can see. A **deterministic** render failure (undefined variable, malformed section) skips that slot; anything else — Anthropic overloaded, no API key, network down — abandons that **video**, writes nothing for it, and reports it in `errors`, so it stays a candidate and the retry is clean. A video that will post nothing consumes no posting time.

**Side effects** — Per video: inserts a `smart_queue_items` row stamped with a concrete UTC instant, then per enabled template slot either creates an ordinary `social_posts` row (`status='approved'`, `slot_id`, `smart_queue_item_id`) and registers its `DateTrigger`, or records a `status='skipped'` row carrying the reason.

Times continue after the last item already scheduled by this queue, so a second Accept appends rather than double-booking. Occurrences are enumerated as local dates in the queue's timezone and converted individually, so an instant past a DST boundary still lands at its stated wall-clock time.

**Text is rendered now, not at fire time**, matching the rest of the app. A later template edit does not reach posts already scheduled — use [`/re-render`](#post-apiprojectsslugsmart-queuesqueue_idre-render).

A slot is **skipped** rather than failed when nothing could make it work: the clip is longer than the platform's cap (no encode shortens a clip), the platform can't take an attachment at all (Threads), or the slot's body fails to render. Skipped means "known in advance, not attempted"; `failed` means "attempted and broke", and history has to tell them apart. Size, resolution, and codec are deliberately **not** grounds for skipping — `prepared_media` fixes those at send time.

A render failure is scoped to its own slot: the other platforms still schedule. A video for which **no** slot could carry it has its queue item marked `skipped` too, so it doesn't burn a posting slot on a no-op. A disabled slot produces no row at all — it was never in play.

### `POST /api/projects/{slug}/smart-queues/{queue_id}/re-flow`

**Purpose** — Re-stamp every pending item onto the queue's current posting times, after the recurrence changed.

**Response 200** — `{"reflowed": N}`.

Order and rendered text are preserved; only *when* each item goes out moves. The UI calls this when the user answers yes to "re-flow existing scheduled postings?" — answering no simply doesn't call it, so the new times apply to items added from then on and what is already on the books stays put. Posted and in-flight posts are never re-stamped.

### `GET /api/projects/{slug}/smart-queues/{queue_id}/missed`

**Purpose** — Posts this queue owns that didn't go out and need a decision.

**Response 200** — `{"missed": [{"post_id", "platform", "status", "scheduled_at", "error", "failed_at", "item_id", "video_id", "title", "missed_reason", "within_grace"}], "missed_policy": ..., "missed_grace_hours": ...}`.

The two time fields are mutually exclusive in practice and the UI labels them differently. An overdue-but-never-attempted post still holds `scheduled_at` and renders as "due …"; a failed one had `scheduled_at` cleared when it was marked failed and renders as "failed …" from `failed_at` (migration 044). Both NULL means a pre-migration failure — the row shows no time rather than a wrong one.

**Missed is derived, not stored** — a post whose `scheduled_at` is in the past and which hasn't been sent. There is no `missed` flag and no background sweeper, so the state can't go stale and nothing has to keep it up to date. A `failed` post is included regardless of time: from the user's point of view "it didn't go out and needs a decision" is the same situation.

`skipped` rows are excluded — a skip was a decision, not a failure, so it needs no disposition. `within_grace` reports whether the queue's post-late window still covers the row, so the UI can present "post now" as the expected action rather than an override.

### `POST /api/projects/{slug}/smart-queues/{queue_id}/missed/{post_id}`

**Body** — `action`: `post_now` | `reschedule_end` | `remove`.

Acts per **post**, not per queue item: one platform failing shouldn't drag the others with it, and the right answer can differ per platform.

* `post_now` — returns a `failed` row to `approved` (the send path's claim only takes `approved`) and sends through the **ordinary** send path, so the same duplicate check, liveness check, and media preparation apply. **Response** carries `status`/`error`/`post_url`: a send that fails again is a result, not an exception, and is reported rather than raised.
* `reschedule_end` — moves the item behind everything else at the next free posting time, and re-registers its trigger.
* `remove` — cancels the trigger and marks the item `removed`. Since `removed` is neither `scheduled` nor `posted`, the standing filters stop excluding that video, so it becomes eligible to add again — via Auto-select + Accept, never automatically (its auto-add marker is already spent).

### `GET /api/projects/{slug}/smart-queues/{queue_id}/activity`

**Query** — `limit` (default 10) per list.

**Response 200** — `{"upcoming": [...], "recent": [...]}`, each a list of `{"id", "platform", "status", "scheduled_at", "posted_at", "post_url", "error", "title", "video_id"}`.

Rows are per-platform `social_posts`, not per queue item: a video whose Mastodon slot failed while Bluesky succeeded has to read as two different outcomes rather than one ambiguous line. Drives the expandable panel on the project dashboard, which is the only place queue activity rolls up across videos.

### `POST /api/projects/{slug}/smart-queues/{queue_id}/re-render`

**Purpose** — Queue a re-render of every still-pending post this queue owns, after editing the template.

**Response 200** — `{"queued": true, "jobs": N}`. Returns immediately: one AI round-trip per post is minutes of work, so it runs on the reconcile worker rather than holding the request open. Progress is at `GET /api/reconcile-status` and in the app-wide banner; closing the page no longer stops it.

Implemented as a `slot_body_changed` job covering every enabled slot — the whole-template case of what a template edit queues, rather than a second implementation.

**Errors** — `409` while reconciliation already owns this queue.

Posts already `posted` (or mid-`sending`) are left alone — they are history. Rendering uses the project's editable `ai_block_default_system_prompt`, same as the generate path.

### `POST /api/social-credentials/{uuid}/verify`

**Purpose** — Ask the *provider* whether this credential still works. Distinct from `/refresh-username`, which reads the cached username out of the stored bundle and so reports a healthy account for a token that died weeks ago. This one makes a real call (Threads: `GET graph.threads.net/v1.0/me`).

**Response 200** — `{"ok": true, "detail": "Token is valid.", "username": "..."}`, or `{"ok": false, "detail": "Threads rejected the token (HTTP 400): ..."}`.

`ok: false` with `"unreachable": true` means we couldn't get an answer — a network failure *or* a 5xx from the provider. Neither is a verdict on the token, and both are kept distinct from a 4xx rejection so a bad day at the provider isn't read as an expired credential.

**Side effect** — the verdict is mirrored into the credential's `needs_reauth` flag: a rejection sets it (Settings then shows the badge and Reconnect button), a pass clears a stale flag, and unreachable leaves it untouched in both directions.

**Errors** — `404` (unknown credential, or no stored bundle), `501` (platform has no live check implemented).

### `GET /api/platform-capabilities`

**Purpose** — Which platforms accept an attachment, and which need media hosting configured to do it. Exists so the UI never hardcodes a platform name for this; every client-side attempt to answer it ("Threads is text-only") outlived the fact it encoded.

**Response 200** — `{"accepts_media": [...], "requires_hosted_media": ["threads"], "supports_live_check": ["threads"]}`.

`supports_live_check` lists platforms with a working `POST /api/social-credentials/{uuid}/verify`; the Settings UI shows a Verify button only for those, rather than one that always 501s.

### `GET /api/reconcile-status`

**Purpose** — Progress of smart-queue template reconciliation, for the banner every page shows. Deliberately not scoped to a project: the work rewrites real schedules, so it has to be visible wherever the user is.

**Response 200**:

```json
{
  "active": [{"id": 3, "queue_id": 1, "queue_name": "Daily Shorts",
              "kind": "slots_added", "label": "Adding posts for new slots",
              "status": "running", "done": 12, "total": 48, "error": null}],
  "failed": [],
  "busy": true,
  "locked_queue_ids": [1]
}
```

`kind` is one of `slots_added`, `slots_removed`, `slot_body_changed`, `applies_to_removed`, plus `enqueue_failed` (only ever recorded already-failed, when a saved template edit could not be turned into jobs).

A queue in `locked_queue_ids` returns `409` from every schedule mutation — `PATCH`, `DELETE`, `/accept`, `/re-flow`, `/re-render`, `/backfill-slots` — until its jobs finish. Reading stays allowed.

### `POST /api/projects/{slug}/smart-queues/{queue_id}/reconcile-jobs/{job_id}/dismiss`

**Purpose** — Acknowledge a failed reconciliation so it leaves the app-wide banner. Does not retry it.

**Response 200** — `{"status": "ok"}`.

### `GET /api/projects/{slug}/smart-queues/{queue_id}/slot-gap`

**Purpose** — Which pending items lack which of the template's enabled slots. Writes nothing.

Reconciliation now runs automatically on template change, so a gap here means items accepted *before* that existed — repair, not routine.

**Response 200** — `{"items_missing_slots": N, "missing_posts": N, "by_platform": {"twitter": N}}`.

### `POST /api/projects/{slug}/smart-queues/{queue_id}/backfill-slots`

**Purpose** — Create the posts pending items would have had, had the slots existed. Counterpart to `re-render`: that rewrites rows that exist, this adds rows that never did. Every `scheduled_at` is left alone — a backfilled post inherits its item's time, so nothing on the calendar moves.

**Response 200** — `{"queued": true, "jobs": N}`. Queued on the reconcile worker for the same reason as re-render, and reported in the same banner. Implemented as a `slots_added` job covering every enabled slot; that handler skips any slot an item already has, so repeating it is safe. A slot that cannot carry the video records a `skipped` row with its reason rather than a post that would fail at send time.

**Errors** — `409` while reconciliation already owns this queue.

### `GET /api/projects/{slug}/smart-queues/{queue_id}/items`

**Query** — `state` (optional): `queued` | `scheduled` | `posted` | `failed` | `skipped` | `removed`.

`queued` is in the queue with no posting time yet; `scheduled` has one. Note that only `queued`, `scheduled` and `removed` are ever *written* to `smart_queue_items.state` — filtering on the other three matches nothing. Use `has_posted` (below) to tell a sent item from a pending one.

**Response 200** — `{"items": [{"id", "video_id", "position", "scheduled_at", "state", "reason", "added_at", "title", "item_type", "duration_seconds", "has_posted", "has_pending"}]}`, ordered by position.

`has_posted` = at least one post has gone out. `has_pending` = at least one post is still unsent (excludes `posted` and `skipped`). They are different questions and only `has_pending` decides whether an item is upcoming — they agree until a send lands partially, after which `!has_posted` would hide an item that still holds live timers.

`has_posted` (0/1) is derived from the item's `social_posts` rows, because sending updates the post and never the item — an item that has already gone out still reads `state = 'scheduled'`. Any caller asking "what is still coming up?" must check `has_posted`, not `state` alone.

An item is an **occurrence, not a membership**: one row per time a video was added, with no uniqueness constraint on (queue, video). Recycling a previously-posted clip appends a new row and the history keeps both.


## Chunked uploads (`/api/uploads`)

All large-file domain endpoints (`POST /api/videos/upload`, `POST /api/videos/items`, `POST /api/videos/{id}/source-file`) consume an `upload_id` produced by this chunked-upload protocol rather than accepting a multipart body. The protocol exists because:

* FastAPI / Starlette's `UploadFile` buffers the body into a `SpooledTemporaryFile` in `$TMPDIR` before invoking the handler, doubling disk I/O on multi-GB sources (8 GB body → ~24 GB of total disk traffic).
* Safari/WebKit raises "request body stream exhausted" when `xhr.send(file)` is combined with custom request headers — the entire body stream is consumed once for an engine pre-flight and then can't be re-read for the actual send.

Slicing the file into chunks side-steps both: each chunk is small enough not to spill to a temp file on the server, and a sliced body read into an `ArrayBuffer` doesn't trip Safari's stream-exhaustion bug.

Every browser uses this path, including Safari. Safari was previously routed to a single multipart POST for Replace Source, because WebKit could not read File slices past 4 GiB ([Bug 272600](https://bugs.webkit.org/show_bug.cgi?id=272600)); that bug is RESOLVED FIXED (May 2024) and was in fact about reading a *whole* file ≥4 GiB via `ReadableStream`, with slicing as the documented workaround. Verified against a 10.9 GB source in Safari — 64 MiB slices at 4 GiB, 4 GiB+1 and 8 GiB all read byte-correct. `POST /api/videos/{id}/source-file` still accepts a multipart body, so the fallback can be restored client-side if WebKit regresses.

### `POST /api/uploads/init`

**Purpose** — Reserve an upload slot.

**Request body** — `{"filename": str, "size": int}`. `size` is the byte count of the source.

**Response 200** — `{"upload_id": str, "chunk_size": int}`. The client slices into chunks no larger than `chunk_size` (`config.UPLOAD_WIRE_CHUNK_BYTES`, currently 64 MiB — an 11 GB source is ~175 round trips).

**Errors** — `400` (missing fields, non-positive size), `413` (size exceeds `config.MAX_SOURCE_FILE_BYTES`, 64 GiB by default, overridable with `DYS_MAX_SOURCE_FILE_GIB`). Checked here, before any bytes are sent.

### `POST /api/uploads/{upload_id}/chunk/{offset}`

**Purpose** — Append the raw request body at `offset` (which must equal the upload's current `received_bytes` — out-of-order or overlapping chunks are rejected to avoid silent corruption).

**Request body** — Raw octet-stream bytes (no multipart, no custom headers — small chunks slide under Safari's stream-exhaustion bug).

**Response 200** — `{"received_bytes": int}` (the new running total).

**Errors** — `404` unknown / expired upload, `409` offset mismatch or upload finalized, `413` chunk would exceed declared size, `400` empty body / chunk over the per-chunk cap.

### `POST /api/uploads/{upload_id}/finalize`

**Purpose** — Declare the upload complete. Verifies received_bytes equals the declared size and renames `upload_<id>.partial` to `upload_<id>.<ext>`.

**Response 200** — `{"upload_id", "size", "filename"}`. The server-internal `path` isn't surfaced.

**Errors** — `404` unknown / expired, `409` incomplete (received_bytes ≠ declared size).

### `DELETE /api/uploads/{upload_id}`

**Purpose** — Drop an in-flight or finalized upload and unlink its on-disk file. Idempotent.

**Response 200** — `{"status": "cancelled"}` when the entry existed, `{"status": "gone"}` when it was already removed (TTL'd / consumed / cancelled).

**Lifetime** — Uploads expire 30 minutes after the last successful append. A startup sweep removes any `upload_*.partial` files from a previous process.

