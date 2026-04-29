# Roadmap

Open work that doesn't fit in `NEW_REQUIREMENTS.md` (which is the
feature-level punch list). Items here are mostly cross-cutting:
infrastructure, security, dev-experience.

## Security

### Lock down the local API when an ngrok tunnel is open

The local server has no auth, no CORS check, no CSRF protection — fine
when only `127.0.0.1` can reach it, but several OAuth flows
(Threads, LinkedIn, etc.) require an HTTPS callback, which today is
satisfied by pointing `ngrok http 8008` at the running app. While
that tunnel is open, every endpoint becomes publicly reachable:
upload videos, edit metadata, list / unlink keychain accounts,
schedule posts, fetch the contents of `/uploads/*`.

Acceptance:

- Admin / mutating routes reject requests whose `Host` header isn't
  `127.0.0.1` / `localhost` *or* whose origin isn't a configured
  ngrok tunnel host.
- The OAuth callback paths (`/api/oauth/*/callback`) remain
  reachable from the tunnel, since that's the whole reason we open
  one.
- `/uploads/*` is either gated behind a per-session token or moved
  off the public mount and served via an authenticated route.
- The Settings UI surfaces the active "tunnel mode" so the user
  knows the lockdown is in effect.

Notes:

- A simple shared-secret header set by the .app shell when it spawns
  the server would also work, and keeps the browser-on-the-same-Mac
  case ergonomic.
- Worth grepping for any existing `ngrok` detection — `services/ngrok.py`
  already polls `127.0.0.1:4040/api/tunnels`, so the allow-list of
  hosts can be derived from that.
