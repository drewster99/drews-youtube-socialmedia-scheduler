# Roadmap

Outstanding work. Items are removed once they ship.

## Tier workflow — promo videos under a primary

Full spec in [`TIER_WORKFLOW.md`](TIER_WORKFLOW.md). Adds a Promo
Videos screen per primary video, sequential bulk-upload with auto-
actions (AI title, transcribe, description, tags, YouTube metadata
push), per-tier schedule chains anchored to the parent, readiness
gates, and parent-aware template variables.

## Moderation — end-to-end retest

The implementation is in place:

- `moderation_log` table (baseline schema).
- `services/moderation.py` — blocklist match, write path at `:147`.
- `routers/settings_routes.py:512` — `run_moderation_now` endpoint.

But the original "doesn't seem to work" complaint predates the
project-scoping rewrite and was never re-confirmed against the
current code. Need a manual end-to-end test:

1. Add a blocked keyword (plain text + a regex).
2. Post a comment containing each on a test video.
3. Hit *Run check now* in the Moderation tab.
4. Confirm hits appear in the moderation log.
5. If it still doesn't fire, debug — likely candidates are the YouTube
   comment-list call, the project-scoping of the blocklist load, or
   the action-write path.

## Security — lock down the local API if it's ever exposed publicly

The local server has no auth, no CORS check, no CSRF protection — fine
when only `127.0.0.1` can reach it. But anyone who fronts it with a
reverse proxy / tunnel (Caddy, Cloudflare Tunnel, …) exposes every
endpoint: upload videos, edit metadata, list / unlink keychain
accounts, schedule posts, fetch the contents of `/uploads/*`.

The *only* flow that legitimately needs a public surface is the
Threads OAuth redirect, and that's now handled by a static "bounce"
page off-box (`DYS_THREADS_REDIRECT_URL`, see `cloudflare/`) — the
app itself never has to be reachable from the internet. So this is
purely defense-in-depth for users who choose to expose it anyway.

**Acceptance:**

- Mutating routes reject requests whose `Host` header isn't
  `127.0.0.1` / `localhost`, unless an explicit allow-list of
  external hostnames is configured.
- `/uploads/*` is either gated behind a per-session token or moved
  off the public mount and served via an authenticated route.

**Notes:**

- A simple shared-secret header set by the .app shell when it spawns
  the server would also work, and keeps the browser-on-the-same-Mac
  case ergonomic.
