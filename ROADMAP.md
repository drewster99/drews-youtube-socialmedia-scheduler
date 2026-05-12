# Roadmap

Open work that doesn't fit in `NEW_REQUIREMENTS.md` (which is the
feature-level punch list). Items here are mostly cross-cutting:
infrastructure, security, dev-experience.

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
