# Cloudflare files for Threads OAuth + Meta callbacks

The local app runs at `http://127.0.0.1:8008`, but Meta will only register /
redirect to **HTTPS** URLs. These files go on your public Cloudflare-hosted site
(`https://nuclearcyborg.com`) and provide the three URLs the Threads app needs.
The paths are namespaced with `threads` so other integrations can add their own
callbacks later without colliding.

| Meta app field          | URL to register                                                       | Served by                                                          |
|-------------------------|-----------------------------------------------------------------------|--------------------------------------------------------------------|
| Redirect Callback URL   | `https://nuclearcyborg.com/apps/scheduler/callback-threads-redirect`  | static `apps/scheduler/callback-threads-redirect.html`             |
| Uninstall Callback URL  | `https://nuclearcyborg.com/apps/scheduler/callback-threads-uninstall` | Pages Function `functions/apps/scheduler/callback-threads-uninstall.js` |
| Delete Callback URL     | `https://nuclearcyborg.com/apps/scheduler/callback-threads-delete`    | Pages Function `functions/apps/scheduler/callback-threads-delete.js`    |

(There's also `apps/scheduler/threads-deletion-status.html`, the page the Delete
callback points the user at.)

## How the redirect works

1. The app opens the Threads OAuth popup with
   `redirect_uri=https://nuclearcyborg.com/apps/scheduler/callback-threads-redirect`.
2. Meta redirects the browser to that page with `?code=…&state=…`.
3. `callback-threads-redirect.html` is a tiny static page that does
   `location.replace("http://127.0.0.1:8008/api/oauth/threads/callback?code=…&state=…")`.
   The browser does the navigation, so `127.0.0.1` is reachable (loopback is a
   "secure context" even over plain http — browsers don't block this).
4. The local app exchanges the code (using the *same* `redirect_uri` string —
   Meta requires an exact match) and stores the long-lived token.

The Uninstall and Delete callbacks are **server-to-server POSTs from Meta**, so
they can't reach `127.0.0.1` and a plain static page can't handle a POST — hence
the Pages Functions. They have nothing to do server-side (the app keeps tokens
only on the user's machine), so they verify Meta's `signed_request` and return
the minimal required responses.

## Deploying

The site is wired to Cloudflare Pages via the **GitHub integration**, so a
`git push` to the configured Production branch is the deploy. The layout
mirrors the URL structure: at the **repo root**, both your static files and a
`functions/` directory — `functions/` is a *sibling* of your static content,
not inside it:

```
(repo root)/
├── apps/scheduler/...                          ← your existing static files
│   ├── callback-threads-redirect.html          ← from cloudflare/apps/scheduler/
│   └── threads-deletion-status.html            ← from cloudflare/apps/scheduler/
└── functions/apps/scheduler/
    ├── callback-threads-uninstall.js           ← from cloudflare/functions/apps/scheduler/
    └── callback-threads-delete.js              ← from cloudflare/functions/apps/scheduler/
```

So: copy the two `.html` files from `cloudflare/apps/scheduler/` into the
matching folder of your site repo, and add a `functions/` folder at the repo
root containing `apps/scheduler/callback-threads-uninstall.js` and
`callback-threads-delete.js`. (Or just run `scripts/deploy-cloudflare.sh` —
it does exactly that, then commits and pushes.) Cloudflare detects `functions/`
during the build — nothing to configure. The path *inside* `functions/` is what
maps to the URL (`functions/apps/scheduler/callback-threads-uninstall.js` →
`/apps/scheduler/callback-threads-uninstall`); it doesn't have to match where
your static files live, though mirroring it is the convention. Functions take
precedence over a static asset at the same path.

> Cloudflare Pages serves `foo.html` at `/foo` with no trailing-slash redirect,
> so keep the `.html` filenames — don't switch to `foo/index.html` or you'll get
> a 301 to `/foo/` that changes the registered redirect URI.

> Pushing to a branch *other* than the Pages project's Production branch gives
> you a Preview deploy at a `*.pages.dev` subdomain — fine for testing, but the
> registered Meta callbacks point at the production hostname and won't hit it.

Then, in the Cloudflare Pages project → **Settings → Environment variables**, add
`THREADS_APP_SECRET` = your Threads app secret (the value behind "Show" next to
*Threads app secret* in the Meta console — make sure there's no surrounding
whitespace or quotes; you can mark it **Encrypted**). It only enables
`signed_request` verification — without it the callbacks still work, just
unverified — but Meta's app review wants it.

Redeploy.

### Quick checks after deploy

```sh
# Should serve the bounce page (HTML), not a 404:
curl -sS https://nuclearcyborg.com/apps/scheduler/callback-threads-redirect | head

# GET shows route is live + whether THREADS_APP_SECRET is wired up:
curl -sS https://nuclearcyborg.com/apps/scheduler/callback-threads-uninstall
curl -sS https://nuclearcyborg.com/apps/scheduler/callback-threads-delete

# POST behaves like Meta calling it (no signed_request → unverified, still valid):
curl -sS -X POST https://nuclearcyborg.com/apps/scheduler/callback-threads-uninstall   # {"status":"ok"}
curl -sS -X POST https://nuclearcyborg.com/apps/scheduler/callback-threads-delete       # {"url":"…/threads-deletion-status?…","confirmation_code":"…"}
```

(The GET output reports whether the *secret is present*, not whether its value
matches Meta — to confirm the value, re-copy it from the Meta console and
overwrite the Cloudflare secret, or watch Pages → Functions → Real-time Logs for
the "for user …" line when Meta actually calls it.)

## Wiring up the local app

The app already defaults `DYS_THREADS_REDIRECT_URL` to
`https://nuclearcyborg.com/apps/scheduler/callback-threads-redirect`, so once the
files above are deployed there's nothing to set — **Settings → Add account →
Threads** works directly from `http://127.0.0.1:8008`. Only set the env var if
you host the bounce page somewhere else. (For quick local testing you can skip
all of this and use Settings → Threads → "Paste long-lived token".)

## Notes / gotchas

- **Port.** `callback-threads-redirect.html` hard-codes `http://127.0.0.1:8008`.
  If you run the app on a different `DYS_PORT`, edit the `LOCAL_BASE` constant in
  that file.
- **`state` lifetime.** The OAuth `state` is held in the running app's memory.
  If the app restarts between starting and finishing the flow, you'll see
  "Unknown or expired OAuth state" — just click Add account again.
- **Renaming the paths.** If you move these to different paths, keep all of
  these in sync: the file locations, the registered Meta URLs, the
  `_DEFAULT_THREADS_REDIRECT_URL` constant in the app's `config.py` (or set
  `DYS_THREADS_REDIRECT_URL` to override it), and `STATUS_PATH` in
  `callback-threads-delete.js`.
- **Not Cloudflare Pages?** If your site is a plain Worker instead, the same
  `parseSignedRequest` logic works; wrap the two handlers in
  `export default { fetch(request, env) { … route on URL pathname … } }`.
