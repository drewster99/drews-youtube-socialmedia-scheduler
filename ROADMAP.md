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

## ~~Public media hosting~~ — shipped

Threads takes images and video as an `image_url` / `video_url` Meta fetches
itself, and this app serves on `127.0.0.1`. Now handled by
`services/media_hosting.py`: the file goes to a private Cloudflare R2 bucket and
Meta gets a presigned GET URL valid two hours. `services/sigv4.py` does the
signing over stdlib `hmac`/`hashlib` — no new dependency.

`ThreadsPoster.accepts_media` is `True`, with `requires_hosted_media` marking
the dependency so `smart_queue_accept` can skip a Threads slot with a useful
reason when hosting isn't set up. Credentials live under
Settings → Media hosting.

No cleanup code: the bucket enforces Object Lock with a 24-hour minimum
retention, so early deletion is impossible. The bucket's 7-day lifecycle rule
removes objects, and signed-URL expiry — not deletion — is what ends access.

Still open: **carousels**. Threads needs a `CAROUSEL` container for more than
one attachment; the poster refuses multi-media posts rather than silently
dropping the extras.

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

## Cache the Generate-from-source word-stream transcription

`clipper._run_generate_job` re-transcribes the parent on-device (Apple
SpeechAnalyzer) on **every** Generate-from-source run — the code deliberately
never persists word-level timing, on the reasoning that it's "cheap to
re-derive." The stored `transcripts` row only carries cue-level SRT timing;
the index-based proposal path needs word-level timing to build its
complete-thought units, so it re-derives every time.

That's fine for a one-off cut, but painful when iterating on the same parent
(re-tuning the check_range / propose_clips loop): every run waits through
transcription again before any proposal work starts.

**Acceptance:**

- Persist the word-stream (the `TranscriptWord` list, or the built
  `ClipUnit`s) after the first transcription, keyed by parent id + a hash of
  the source file (so a Replace-source invalidates it).
- On a subsequent Generate for the same parent+file, reuse the cached word
  stream and skip transcription entirely; the job goes straight to
  `proposing`.
- Invalidate on source-file change (migration 026 `source_file_origin` /
  Replace-source) and when the transcript is re-derived for any other reason.

**Notes:**

- Word timing is larger than the SRT but still small; a table
  (`parent_id`, `source_hash`, `words_json`, `created_at`) or a sidecar
  file next to the source both work. A table is consistent with the rest of
  the persistence.
- Until this lands, `scripts/clip_dev.py` is the fast iteration path — it
  reads the stored SRT (cue-level, one unit per cue) and never transcribes,
  so loop mechanics can be tuned in seconds without a rebuild.

## A video published on YouTube directly is never considered for a smart queue

`smart_queue_live.on_video_became_live` is the single funnel from "this went
live" to "should a queue take it", and it has three call sites: a privacy flip
through our own UI (`routers/video_routes.py:918`, keyed on the transition
`before != 'public' && now == 'public'`), import (`services/imports.py:262`),
and the publish timer (`services/scheduler.py:277,337`).

Publishing in YouTube Studio hits none of them. What *does* happen is the drift
sync in `GET /api/videos/{id}` (`routers/video_routes.py:298-304`), which takes
YouTube's value as truth for title / description / tags / **privacy_status** and
writes it straight to the row. So the local privacy is eventually corrected —
lazily, only when someone opens that video's detail page, since there is no
background poll — while `auto_add_considered_at` stays NULL forever and no queue
ever looks at the video.

The shape of the bug is that the drift sync writes the very column the manual
path watches for a transition, but writes it directly, so the event is swallowed.

**Acceptance:**

- A video whose `privacy_status` becomes `'public'` via the drift sync is
  considered exactly once, the same as one flipped through our UI.
- The transition test stays where it is — an already-public video re-synced is
  not a fresh event, and `on_video_became_live` is idempotent on
  `auto_add_considered_at` anyway.
- Best-effort like the existing call site: a queue problem must not make a GET
  fail.

**Notes:**

- The lazy trigger is a second, separate question: with no background poll, a
  video published in Studio and never opened here is invisible until someone
  visits it. Decide whether that is acceptable or whether the sweep should be a
  job — the answer probably differs for a 4-hourly poll's quota cost versus how
  often publishing happens out of band.

## Failed YouTube publishes have no confirmed surface

The app-wide failed-sends banner reads `social_posts` only
(`routers/social_routes.py:628`), so every row in it is a social post. A failed
*YouTube publish* — `scheduler.publish_video_job` erroring — is a different
table and a different path, and it was not traced during the work that added
`failed_at` and the banner ordering.

**Acceptance:**

- Establish where a failed publish currently surfaces, if anywhere: the video's
  `status`, a `video_events` row, `server.log` only, or nothing.
- If it is log-only, it falls under the same working-agreement rule the social
  banner was built for — the publish timer fires with no page open, so a failure
  reaches nobody. Give it a surface.

**Notes:**

- Worth checking at the same time whether a publish that fails leaves the video
  in a state the scheduled-publish path will retry, or whether it is stranded
  like a failed social send was.
