# Roadmap

Open work that doesn't fit in `NEW_REQUIREMENTS.md` (which is the
feature-level punch list). Items here are mostly cross-cutting:
infrastructure, security, dev-experience.

## Prompts — make every LLM call site user-editable (NEW_REQUIREMENTS #18)

Today four prompt **bodies** are DB-backed and editable in Project Settings →
LLM prompt templates (`description_from_transcript`, `description_from_frames`,
`tags_from_metadata`, `tags_from_frames`). What's still hardcoded:

* The **system prompts** for the two `tags_*` seeds (duplicate string,
  `services/ai.py:238` and `:277`).
* The **whole** `shorten_post` flow — body and system both inline at
  `routers/social_routes.py:516–528`.
* `DEFAULT_AI_SYSTEM_PROMPT` (`services/ai.py:283`) — the system prompt for
  every bare `{{ai: …}}` block in social/post templates.

The two description seeds intentionally send no system prompt and stay that
way — the UI only shows a system-prompt textarea for seeds that declare one.

### Schema (migration 014)

* `ALTER TABLE prompt_templates ADD COLUMN system_body TEXT NULL` —
  `NULL` means "send no system prompt"; non-null is rendered through the
  same `{{variable}}` engine as `body`.
* `UPDATE prompt_templates SET key = key || '_prompt' WHERE key IN
  ('description_from_transcript', 'description_from_frames',
   'tags_from_metadata', 'tags_from_frames')` — so the four legacy DB keys
  match the new naming convention. Existing user edits survive intact.

### Seeds (`services/prompts.py`)

`SeedPrompt` gains a `system: str | None` field. Renames + new seeds:

| Python constant | DB key | `system` exposed in UI? |
|---|---|---|
| `SEED_DESCRIPTION_FROM_TRANSCRIPT_PROMPT` | `description_from_transcript_prompt` | no |
| `SEED_DESCRIPTION_FROM_FRAMES_PROMPT` | `description_from_frames_prompt` | no |
| `SEED_TAGS_FROM_METADATA_PROMPT` | `tags_from_metadata_prompt` | yes |
| `SEED_TAGS_FROM_FRAMES_PROMPT` | `tags_from_frames_prompt` | yes |
| `SEED_SHORTEN_POST_PROMPT` (new) | `shorten_post_prompt` | yes |
| `SEED_AI_BLOCK_DEFAULT_SYSTEM_PROMPT` (new) | `ai_block_default_system_prompt` | yes — system-only (no body textarea) |

`shorten_post_prompt` variables: `target_chars`, `post_text`.
`ai_block_default_system_prompt` variables: none (`body` is unused for this
key; only `system` is editable).

`prompts.py` exposes `get_prompt_with_fallback(key, *, project_id) ->
{"body": str, "system": str | None}`. The existing
`get_prompt_body_with_fallback` stays as a thin wrapper for the description
call sites that don't need system.

### Default-system resolution (option **c**)

`call_ai_block(prompt, *, system, ...)` loses its module-level default —
callers pass `system=` explicitly. `templates.render()` keeps its existing
`default_system_prompt` parameter; the **caller** of `render()` fetches the
project's edited `ai_block_default_system_prompt` (async) and passes it
through. Atomic per render: a mid-flight edit takes effect on the next
render, not partway through the current one. No module-level cache, no
startup-time warming.

### Call sites migrated

* `ai.py:generate_tags_from_metadata` and `generate_tags_from_frames` —
  read `system` from the seed, drop the hardcoded string.
* `routers/social_routes.py:shorten_post` — render body via
  `templates.render` with `{target_chars, post_text}` and pass system
  through.
* Any caller of `templates.render` that wants the user-editable
  `{{ai: …}}` default fetches it and supplies `default_system_prompt=`.
  Bare callers (e.g. `expand_text` with no override) fall back to
  `DEFAULT_AI_SYSTEM_PROMPT` (the seed fallback).

### API + UI

* `GET /api/projects/{slug}/prompts` returns `system` (and `default_system`)
  alongside `body`.
* `PUT /api/projects/{slug}/prompts/{key}` accepts `body` *and* `system`.
* Project Settings LLM-prompts card grows a second textarea per seed
  labelled **System prompt**, shown only when the seed declares one.
  When `body` is empty in the seed (the `ai_block_default_system_prompt`
  case), the body textarea is hidden — system-only.
* `API.md` documents both new request/response fields.

### Folded follow-ups (small but adjacent)

* **Per-tier default-template selector** — `project_settings.html:92–95`
  becomes a `<select>` populated from the project's templates filtered by
  each row's tier against `applies_to`. Today it's a free-text input.
* **Compose-flow tier filtering** — `socials_compose.html` filters the
  template picker so only templates with `applies_to.includes(video.tier)`
  appear. A template with all four tiers checked (the default for new
  templates) is "global" and always appears.

### Acceptance

1. `pytest` green (existing + new tests for `get_prompt_with_fallback`,
   the renamed `DEFAULT_AI_SYSTEM_PROMPT` fallback, and `shorten_post`).
2. Editing any of the six prompt keys in Project Settings + Preview shows
   the rendered output with the user's edits.
3. Triggering each call path (generate description, generate tags, shorten
   a post, render a template with a bare `{{ai: …}}` block) consumes the
   user-edited prompt — verified by adding a recognisable phrase to a
   system prompt and seeing it shape the output.
4. Fresh install (no migration run) still produces output via the seed
   fallbacks (regression guard, matches current behaviour).
5. Per-tier default-template selector lists real template names filtered
   by `applies_to`; compose-flow picker hides templates whose `applies_to`
   excludes the current video's tier.
6. `grep -rn "messages\.create\|call_ai_block" src/yt_scheduler/` returns
   the same call sites as today — no new hardcoded prompts.

### Completed pre-work (no longer in this plan)

* `DEFAULT_AI_SYSTEM` → `DEFAULT_AI_SYSTEM_PROMPT` (rename).
* `services/ai.py:generate_social_post` + `platform_guidance` +
  `tier_guidance` deleted as dead code (all live social-post generation
  goes through the template engine's `{{ai: …}}` blocks).
* Six sentence-case fixes in `moderation.html` + `video_detail.html`.

## Security

### Lock down the local API if it's ever exposed publicly

The local server has no auth, no CORS check, no CSRF protection — fine
when only `127.0.0.1` can reach it. But anyone who fronts it with a
reverse proxy / tunnel (Caddy, Cloudflare Tunnel, …) exposes
every endpoint: upload videos, edit metadata, list / unlink keychain
accounts, schedule posts, fetch the contents of `/uploads/*`.

Note that the *only* flow that legitimately needs a public surface is
the Threads OAuth redirect, and that's now handled by a static "bounce"
page off-box (`DYS_THREADS_REDIRECT_URL`, see `cloudflare/`) — the app
itself never has to be reachable from the internet. So this is purely
defense-in-depth for users who choose to expose it anyway.

Acceptance:

- Mutating routes reject requests whose `Host` header isn't
  `127.0.0.1` / `localhost`, unless an explicit allow-list of
  external hostnames is configured.
- `/uploads/*` is either gated behind a per-session token or moved
  off the public mount and served via an authenticated route.

Notes:

- A simple shared-secret header set by the .app shell when it spawns
  the server would also work, and keeps the browser-on-the-same-Mac
  case ergonomic.
