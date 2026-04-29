# YouTube Publisher — Backend API Reference

This file documents every HTTP endpoint exposed by the FastAPI backend running at http://127.0.0.1:8008. The server binds to loopback only and has **no application-layer authentication** — anything that can open a TCP connection to the port can call any endpoint.

Generated from the router source. When endpoints change, update this file. (CLAUDE.md tracks this convention.)

## Routers / sections

- [Application-level routes (`app.py`)](#application-level-routes-apppy) — HTML pages, static mounts, build identity
- [Projects (`/api/projects`)](#projects-apiprojects) — `project_routes.py`
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
Backwards-compatibility redirect to `/projects/<DEFAULT_PROJECT_SLUG>/videos/{video_id}`.

### `GET /templates` → 307 Redirect
Backwards-compatibility redirect to `/projects/<DEFAULT_PROJECT_SLUG>/templates`.

### `GET /templates/{name}` → 307 Redirect
Backwards-compatibility redirect to `/projects/<DEFAULT_PROJECT_SLUG>/templates/{name}`.

### `GET /moderation` → 307 Redirect
Backwards-compatibility redirect to `/projects/<DEFAULT_PROJECT_SLUG>/moderation`.

### Static mounts

- `GET /static/*` — serves files from `src/yt_scheduler/static/`.
- `GET /uploads/*` — serves files from the configured `UPLOAD_DIR` (typically `~/.yt-scheduler/uploads/`).

---

## Projects (`/api/projects`)

Source: `src/yt_scheduler/routers/project_routes.py`

### `GET /api/projects/recent-events`

Aliases: `GET /api/projects/__recent-events` (excluded from OpenAPI schema).

**Purpose** — Newest activity log entries across all projects, for the Home page feed.

**Query params**

| Name | Type | Required | Description |
|---|---|---|---|
| `limit` | int | optional | Max rows. Default `7`. |

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

**Response 200** — Array of full video rows from `videos` (every column), ordered `created_at DESC`. `tags` is the raw JSON-encoded string from the column (the frontend `JSON.parse`s it).

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

### `GET /api/videos/{video_id}`

**Purpose** — Full details for a single video, plus a live YouTube readback.

**Response 200** — The local row (all `videos` columns) plus either `youtube_data` (the full `videos.list()` response) or `youtube_data_error` (string) if the readback failed.

**Errors** — `404` (no local row).

### `POST /api/videos/upload`

**Purpose** — Upload a video to YouTube and track it inside a project.

**Request body** — multipart/form-data:

| Field | Type | Required | Description |
|---|---|---|---|
| `video_file` | file | yes | Video binary. |
| `thumbnail_file` | file | no | Optional thumbnail image. |
| `title` | string | yes | Video title. |
| `description` | string | no | Default `""`. |
| `tags` | string | no | Comma-separated tag list. |
| `pinned_links` | string | no | Free-form text appended after generated descriptions. |
| `privacy_status` | string | no | `unlisted` (default), `private`, `public`. |
| `publish_at` | string | no | ISO 8601 future timestamp; tells YouTube to scheduled-publish at that time. |
| `project_slug` | string | no | Target project. Default `default`. |
| `item_type` | string | no | One of `episode | short | segment | hook`. Default `episode`. `standalone` is rejected here — standalone items don't go through YouTube; use a separate creation path (forthcoming). |
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

**Renderer (background path)** — When the project's auto-actions matrix has auto-gen-socials enabled, the background job renders each platform's slot body through the same engine as [`POST /api/expand_text`](#post-apiexpand_text). Same variables and same `{{var!}}` / `{{var??default}}` / `{{ai: ...}}` / `{{ai[system]: ...}}` semantics — there is no separate template engine for the auto path.

### `POST /api/videos/items`

**Purpose** — Create an item that does **not** go through YouTube. Used for `standalone` items (a GitHub-repo post with screenshots, an "AI Chess" project announcement, an image-only Bluesky post) and for `hook` items where the user wants to post the clip directly to social without also uploading to YouTube.

**Request body** — `multipart/form-data`:

| Field | Type | Required | Description |
|---|---|---|---|
| `title` | string | yes | Item title — also serves as the social-post body's `{{title}}`. |
| `description` | string | no | Default `""`. Available as `{{description}}`. |
| `tags` | string | no | Comma-separated. Available as `{{tags}}` / `{{hashtags}}`. |
| `project_slug` | string | no | Default `default`. |
| `item_type` | string | no | One of `standalone | hook`. Default `standalone`. `episode/short/segment` are rejected here — they need YouTube and go through `/api/videos/upload`. |
| `parent_item_id` | string | no | Optional parent item id (e.g. a hook attaching to its episode). Rejected for `standalone`. |
| `url` | string | no | The value behind `{{url}}` for this item. For a hook attached to a parent, omit this and `{{url}}` will resolve to the hook's own URL (NULL → empty); use `{{episode_url}}` to link to the parent. |
| `video_file` | file | no | Optional video file. When present, this is the file `{{video}}` attaches in templates and what the platform-specific Posters upload as a media asset. |
| `thumbnail_file` | file | no | Optional thumbnail image. |

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

**Purpose** — Update video metadata (title, description, tags, privacy, publish time, pinned links, status, manual tier override).

**Request body** — Object with any subset of: `title`, `description`, `tags` (list), `privacy_status`, `publish_at`, `pinned_links`, `status`, `tier`.

**Response 200** — `{"status": "ok"}`.

**Errors** — `404` (no local row), `400` (invalid `tier` value — must be one of `hook`, `short`, `segment`, `video`, `null`, or `""`), `500` (YouTube update failed).

**Side effects** — Calls `youtube.update_video_metadata` (50 quota), reads back via `youtube.get_video` to capture any silent coercion (privacy clamp, tag truncation), writes confirmed values to the DB, and records a `metadata_updated` event with a per-field `{old, new}` diff for changed tracked fields.

### `POST /api/videos/{video_id}/transcribe`

**Purpose** — Transcribe a video locally using on-device speech recognition.

**Query params** — `confirm_unlist` (bool, default `false`). See "private video" path below.

**Request body** (optional):

```json
{ "model": "large-v3", "language": "en", "backend": "mlx-whisper" }
```

`model` defaults to `large-v3`. `language` is auto-detected when omitted. `backend` forces a specific backend (`mlx-whisper`, `whisper.cpp`, `macos-speech`); otherwise the service picks the best available.

**Response 200**:

```json
{
  "status": "ok",
  "backend": "mlx-whisper",
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

**Purpose** — Generate an SEO description from the video's transcript, or from extracted keyframes when no transcript exists.

**Request body** (optional):

```json
{ "extra_instructions": "...", "mode": "auto" | "transcript" | "frames" }
```

`mode=auto` (default) uses transcript if present, falls back to frames. `transcript` is hard-fail if no transcript. `frames` forces frame-based even when a transcript exists.

**Response 200** — `{"description": "<full text incl. pinned_links>", "raw_ai_description": "<just the AI output>"}`.

**Errors**

- `404` — Video not found.
- `400` — `mode=transcript` without a transcript, or `mode=frames`/auto-frames without a local video file.
- `502` — Anthropic auth/transport failure (special-cased for 401 with a message asking the user to update their API key) or ffmpeg returning no usable keyframes.

**Side effects** — Calls Anthropic API; for frames mode also calls `ffmpeg` to extract keyframes; writes `videos.generated_description`. The applied description includes `pinned_links` appended after the AI text.

**Renderer** — `mode=transcript` (and the transcript leg of `auto`) substitutes the prompt body from `prompt_templates.description_from_transcript` through the same engine as [`POST /api/expand_text`](#post-apiexpand_text), then sends the substituted prompt to Claude in a single call. Any `{{ai: ...}}`, `{{var!}}`, or `{{var??default}}` syntax in the prompt-template body is honoured. `mode=frames` skips substitution entirely — it sends the keyframes with a hardcoded vision instruction.

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

**Purpose** — Schedule a video to flip to public (and fire its social posts) at a specific future time.

**Request body** — `{"publish_at": "2026-04-28T15:00:00-07:00"}` (ISO 8601).

**Response 200**:

```json
{ "status": "ok", "job_id": "publish_<video_id>", "publish_at": "...", "message": "..." }
```

**Errors** — `400` (missing `publish_at`, invalid format, time not in future).

**Cascades** — **Re-baselines all per-post jobs.** Any pending scheduled posts for this video (rows with `scheduler_job_id IS NOT NULL`) are cancelled via `cancel_scheduled_post()` and re-scheduled at staggered offsets driven by the project's `post_video_delay_minutes` and `inter_post_spacing_minutes`. Hand-retimed per-post jobs from a prior `POST /api/social/posts/{post_id}/schedule` call are intentionally overwritten — re-scheduling the video is the explicit "reset everything" action.

**Side effects** — Registers an APScheduler `DateTrigger` job (`publish_<video_id>`); cancels and re-attaches per-post jobs (see Cascades above); sets `videos.status='scheduled'`, `videos.publish_at=<iso>`; records `publish_scheduled` and one `social_post_scheduled` event per re-attached post.

### `DELETE /api/videos/{video_id}/schedule`

**Purpose** — Cancel a previously scheduled publish.

**Response 200** — `{"status": "ok", "message": "Schedule cancelled"}`.

**Errors** — `404` if the video has no scheduled publish.

**Cascades** — **Cancels every pending per-post job for this video.** All `social_posts` rows with `scheduler_job_id IS NOT NULL` go through `cancel_scheduled_post()`: their APScheduler `DateTrigger` is removed and `scheduled_at` / `scheduler_job_id` are nulled. Already-posted rows are unaffected (their `scheduler_job_id` is already NULL).

**Side effects** — Removes the publish APScheduler job and all per-post jobs (see Cascades); clears `videos.publish_at`; resets `videos.status` to `'ready'`.

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

**Side effects** — Saves file under `UPLOAD_DIR`; calls `youtube.set_thumbnail`; updates `videos.thumbnail_path`.

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
  "platforms": ["twitter", "bluesky"], // optional whitelist
  "user_message": "..."                // exposed to templates as {{user_message}}
}
```

**Response 200** — Array of generated post snapshots:

```json
[
  { "slot_id": 7, "platform": "twitter", "content": "...", "media": "thumbnail" | "video" | "none", "max_chars": 280, "social_account_id": 1 }
]
```

**Query params** — `confirm_overwrite_scheduled` (bool, default `false`). When false, the route refuses to regenerate if any unsent post for this video is currently scheduled (has a non-NULL `scheduler_job_id`).

**Errors**

- `404` — Video or template not found.
- `409` — One or more posts are scheduled. Body: `{"detail": {"scheduled_overwrite": true, "needs_confirm": true, "scheduled": [{"post_id": int, "platform": str, "scheduled_at": "<ISO>"}, ...]}}`. Re-issue with `?confirm_overwrite_scheduled=true` to proceed.

**Side effects** — Holds the per-video publish lock. When `confirm_overwrite_scheduled=true`, calls `cancel_scheduled_post()` on each scheduled row first (tearing down its APScheduler `DateTrigger`) so no orphan jobs remain. Then deletes existing `social_posts` for the video where `status NOT IN ('posted','sending')` and inserts one fresh `draft` row per non-disabled, matching slot. Template variables exposed: `title`, `url`, `description`, `description_short` (≤150), `description_medium` (≤500), `tags`, `hashtags`, `thumbnail_path`, `tier`, `transcript` (plain text, SRT stripped), `user_message`. Also calls `youtube.get_video` to read the duration tier.

**Renderer** — Each slot's `body` is rendered through the same engine as [`POST /api/expand_text`](#post-apiexpand_text) (`services/templates.render`). All variables, `{{var!}}` / `{{var??default}}` / `{{ai: ...}}` / `{{ai[system]: ...}}` syntax, and recursive AI-block evaluation behave identically.

### `GET /api/social/posts/{video_id}`

**Purpose** — All social posts for a video.

**Response 200** — Array of full `social_posts` rows ordered by platform.

### `PUT /api/social/posts/{post_id}`

**Purpose** — Edit a draft social post in place.

**Request body** — Any subset of `{"content": str, "status": str, "media_path": str}`. `content` is auto-trimmed of leading/trailing whitespace at write time.

**Note on media** — The `social_posts` table now has both `media_path` (legacy single-string column, kept for backwards compat) and `media_paths` (JSON array column, the canonical form). The post-generation paths and PUT endpoint write both. The send paths read `media_paths` first and fall back to `media_path`. Once all writers stop touching the legacy column it'll be dropped in a follow-up migration.

**Response 200** — `{"status": "ok"}`.

### `POST /api/social/posts/{post_id}/send`

**Purpose** — Send a single social post.

**`post_id`** — `social_posts.id` returned by `POST /api/social/generate-posts/{video_id}` (in the array of created rows) or `GET /api/social/posts/{video_id}`.

**Prerequisites** — The row must already exist; `status` is not checked, so an unedited AI-generated post can be sent directly. A prior `PUT /api/social/posts/{post_id}` is only needed if the caller wants to edit `content` or `media_path` before sending.

**Request body** — None.

**Query params** — `confirm_dup` (bool, default `false`). When false (default) the route refuses to resend duplicates of the last 30 days.

**Response 200** — `{"status": "ok", "url": "<post URL>"}`.

**Errors**

- `404` — Post not found.
- `400` — Resolved poster is misconfigured (no credentials), or platform routing yielded no usable poster.
- `401` — Resolved credential is flagged `needs_reauth=1` (pre-check), or the platform itself rejected the request as unauthorized (post-call). On post-call failure the credential's `needs_reauth` flag is set.
- `409` — A post with same `(platform, account, content, media_path)` was sent within the last 30 days. Body is `{"detail": {"duplicate": true, "platform": "...", "previous": {"id": int, "video_id": str, "posted_at": str, "post_url": str, "content_preview": str}, "needs_confirm": true}}`. Re-issue with `?confirm_dup=true`.
- `500` — Anything else from the platform.

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

**Response 200** — Object keyed by platform:

```json
{
  "twitter":  { "status": "posted",       "url": "..." },
  "bluesky":  { "status": "skipped",      "reason": "not configured" },
  "linkedin": { "status": "needs_reauth", "error": "..." },
  "mastodon": { "status": "failed",       "error": "..." }
}
```

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
  "platforms": { "twitter": { "template": "...", "media": "thumbnail", "max_chars": 280 }, ... }
}
```

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

**Errors** — `400` (no `name`, empty `applies_to`, or DB integrity error).

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

### `GET /api/templates/{name}/slots`

**Purpose** — List every slot for a template.

**Response 200** — Array of slot dicts (same shape as the `slots` array under `GET /api/templates/{name}`).

**Errors** — `404` (unknown name).

### `POST /api/templates/{name}/slots`

**Purpose** — Add a non-builtin slot.

**Request body** — `{"platform": "twitter", "body": "...", "media": "thumbnail", "max_chars": 500, "social_account_id": 1, "is_disabled": false, "order_index": 3}`. Only `platform` is required.

**Response 200** — The newly created slot dict.

**Errors** — `400` (missing `platform`, non-int `social_account_id`, service-layer validation error), `404` (unknown template name).

### `PATCH /api/templates/{name}/slots/{slot_id}`

**Purpose** — Update a slot.

**Request body** — Any subset of `body`, `media`, `is_disabled`, `order_index`, `max_chars`, `social_account_id`. Pass `null` (or `""`) to clear `social_account_id`.

**Response 200** — Updated slot dict.

**Errors** — `404` (unknown template name, slot not found in this template), `400` (non-int `social_account_id`, validation failure).

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

**Template syntax**

- `{{name}}` — substitute. Missing keys are left literal in the output so the user can see what didn't resolve.
- `{{name!}}` — required substitute. Missing key returns **400** with `{"detail": {"missing_required": "<name>"}}`. No fallback; use `??` if you want one.
- `{{name??default text}}` — optional with explicit fallback. When `name` is missing, the literal string between `??` and `}}` is rendered. Default text is **absolute** — a `{{title}}` inside the default stays literal, no recursive substitution. For empty fallback write `{{name??}}`.
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

- `{{title}}`, `{{description}}`, `{{description_short}}` (≤150), `{{description_medium}}` (≤500), `{{tags}}` (comma-joined), `{{hashtags}}` (top 5 as `#CamelCase`), `{{thumbnail_path}}`, `{{tier}}`, `{{transcript}}` (plain-text, SRT stripped), `{{user_message}}`.
- `{{url}}` — `videos.url`. Populated from the YouTube URL at upload / import for YT-backed items, NULL→empty string for standalone items unless explicitly set.
- `{{episode_url}}` — when the item has `parent_item_id` set, the parent's `url`; empty otherwise.
- `{{project_url}}` — `projects.project_url`. Auto-populated from the YouTube channel handle on OAuth bind for YT projects; set explicitly via `POST /api/projects` for non-YT projects; editable via `PATCH /api/projects/{slug}` and refreshable via `POST /api/projects/{slug}/youtube/refresh-channel-url`.

**Custom variables** are merged at every render via the four-level inheritance chain (lowest priority first): `global_variables` → `project_variables` → parent item's `item_variables` (when the item has a parent) → self item's `item_variables`. Built-ins always come from the self item — they never inherit. See "Custom variables" below for the per-scope CRUD endpoints.

`POST /api/expand_text` is the bottom of that hierarchy: it has no item context, so project / parent / item layers don't apply, but it **does** merge in `global_variables` automatically (with the caller's `variables` taking precedence on any key collision — the caller acts as the "self" level). To exercise the full chain, use `POST /api/social/generate-posts/{video_id}` or call the renderer through one of the auto-action paths.

**Response 200** — `{"rendered": "<rendered text>"}` on success, or `{"rendered": null, "error": "<message>"}` for non-required render failures (e.g., Anthropic API error).

**Errors**

- `400` if `template` is empty, or `{"detail": {"missing_required": "<name>"}}` when a `{{var!}}` placeholder isn't supplied.

---

## Item images (`/api/videos/{video_id}/images`)

Source: `src/yt_scheduler/routers/item_image_routes.py`

Multi-image attachments per item, referenced from templates as `{{image:shortname}}` or `{{image:*}}`. Each row carries a unique-per-item `shortname` ([a-z0-9-], can't start with hyphen) plus optional `alt_text` and an `order_index` that controls the order in `{{image:*}}` expansion.

### `GET /api/videos/{video_id}/images`

**Response 200** — Array of image rows in `(order_index, id)` order:

```json
[
  { "id": 1, "video_id": "abc", "shortname": "cat", "path": "/u/...", "alt_text": "a cat", "order_index": 0, "created_at": "..." }
]
```

### `POST /api/videos/{video_id}/images`

Multipart form upload.

**Form fields** — `file` (binary, required), `shortname` (required), `alt_text` (default `""`), `order_index` (int, default `0`).

**Response 200** — The created image row.

**Errors** — `404` (video not found), `400` (shortname collision, invalid shortname).

### `PATCH /api/videos/{video_id}/images/{image_id}`

**Body** — Any subset of `shortname`, `alt_text`, `order_index`. The image file is immutable; delete + re-upload to replace.

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

**Purpose** — Upsert a flat key/value blob.

**Request body** — Any object; values are stringified before storage.

**Response 200** — `{"status": "ok"}`.

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

### `GET /api/settings/ngrok`

**Purpose** — Detect whether an ngrok tunnel is forwarding to our local port. Used by the Settings UI to surface the HTTPS URL needed for OAuth flows that reject `http://`.

**Response 200**:

```json
{ "detected": true, "public_url": "https://...ngrok-free.app", "local_port": 8008 }
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

**Errors** — `400` (client missing, origin missing).

**Notes** — Threads requires HTTPS; on local `http://` origins, use `POST /api/oauth/threads/exchange` instead.

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
    "needs_reauth": false, "label": "@alice @X"
  }
]
```

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

**Request body** — `{"video_id": "abc"}`.

**Response 200** — The newly inserted `videos` row.

**Errors** — `404` (unknown slug), `400` (`video_id` missing, video not on YouTube, video already imported), `500` (any other failure).

**Side effects** — Calls `youtube.get_video` (~1 quota); downloads the thumbnail to `UPLOAD_DIR`; inserts the row; records `imported`; tries to download the existing YouTube caption (50 quota) and store it as a transcript; runs `auto_actions.run_post_create_actions(... source="import")` in the background.

**Renderer (background path)** — When auto-gen-socials is enabled for imports, the background job renders each platform's slot body through the same engine as [`POST /api/expand_text`](#post-apiexpand_text). Same variables and same syntax.
