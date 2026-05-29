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

## Vertical crop — face-follow (Generate from source, v2)

The first iteration of "Generate from source" ships with a center-crop
toggle for 9:16 hooks/shorts. Center crop misses the action when the
subject isn't framed dead-center, which is most of the time on
multi-camera podcast / interview footage.

**Acceptance:**

- macOS Vision framework face detection (built-in, no extra install)
  run on every Nth frame (e.g. every 5 frames at 30fps = every ~165ms).
- Track the dominant face position over the clip; smooth across detections
  with a 1-second moving average so the crop doesn't jitter.
- Fall back to center crop when no face is detected for a sustained run.
- New per-kind setting in the Generate modal: crop mode = center | follow.

**Notes:**

- For multi-speaker scenes the "dominant" face heuristic does best when
  one person dominates frame size. Active-speaker tracking is v3 below.
- Implement as a small Swift helper (Vision is Swift-native) called from
  Python via subprocess, or use PyObjC. Avoid mediapipe / PyTorch — the
  install cost would dwarf the feature.

## Vertical crop — active-speaker tracking (Generate from source, v3)

Multi-person podcasts cut to vertical look best when the crop follows the
person currently talking. v2's "follow dominant face" doesn't switch
between speakers.

**Acceptance:**

- Per-time-window identify which detected face is the active speaker.
  Two viable signals: audio diarization (whisperx, pyannote — requires
  GPU/MLX), or mouth-motion analysis from Vision face landmarks.
- Smooth the active-speaker switch (don't flip mid-word). Slight lead
  time (~250ms) so the switch happens just as a new speaker starts.

**Notes:**

- Significantly more complex than v2 — its own work item.

## Token-cost estimate on Generate from source

Each Generate-from-source call hits Claude with the parent's transcript +
prompt for N proposals. For a 3-hour podcast this can be 80K input tokens
+ a few thousand output tokens. The user has no visibility into the cost
before clicking Generate.

**Acceptance:**

- Estimate input tokens from transcript byte length (rough ~4 chars per
  token) plus the prompt template overhead. Show "Estimated cost: ~$0.12"
  in the modal, computed against the current `ANTHROPIC_MODEL` rate card.
- Rate card is small (3 models × 2 prices), can be hardcoded with a note.
- Recompute when counts change (more proposals = more output tokens).

**Notes:**

- Defer until the feature has been used enough to know whether users care.
  If they don't notice spend, this is wasted scope.

## Source-file backup cleanup

The "Replace source" flow (migration 026, `POST /api/videos/{id}/source-file`)
intentionally never renames or deletes the previous local file — the row
is just re-pointed at the new one. That avoids the half-renamed-row crash
window, but it means every replace leaves the old file on disk as an
orphan. After many replacements of a multi-GB master, disk usage grows
silently.

**Acceptance:**

- A janitor that finds `UPLOAD_DIR` files no longer referenced by any
  `videos.video_file_path` and older than some threshold (e.g. 7 days)
  and deletes them. Either a CLI subcommand (`yt-scheduler gc-uploads`)
  or a periodic scheduler job, or both. Dry-run by default; require an
  explicit `--apply` to actually unlink.

**Notes:**

- Be careful: ``UPLOAD_DIR`` also holds thumbnails, item images, and
  per-video transcript artifacts. The janitor must only act on files
  it can positively identify as orphaned *video* sources (extension
  set + cross-check against all rows' `video_file_path`). When in
  doubt, leave it.
