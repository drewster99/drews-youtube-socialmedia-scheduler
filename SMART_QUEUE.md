# Smart queue — project-wide social posting of promo clips

A project can have one or more **smart queues**. A queue holds an ordered
list of videos and a weekly recurrence; at each recurrence slot it posts
the next video to every enabled slot of its template, attaching the video
file itself. This doc captures the full design.

## Concepts

* **Smart queue** — belongs to a project. Has a template, a set of
  selection filters, a weekly recurrence, a timezone, and an ordered list
  of items. Multiple queues per project are supported.
* **Recurrence slot** — a (weekday, time-of-day) pair. Any number per
  day, including zero. Weekly only: no every-other-week, no blackout
  dates, no specific calendar dates.
* **Queue item** — an *occurrence*, not a membership: one row per time a
  video is added to a queue. A video can legitimately appear more than
  once over the queue's life (see *Recycling*).
* **Eligibility** — the AND of: template `applies_to` ∩ `item_type`,
  duration within [min, max], orientation in the selected set, video is
  live, and not archived.

## Selection filters

All conditions AND together:

| filter | default |
|---|---|
| `item_type` | from the template's `applies_to` — not separately editable |
| duration | min 0:00, max 3:00 |
| orientation | portrait + square |
| exclude already posted by this queue | on |

Always applied, not user-editable: video is live/published, not archived,
and not currently scheduled by this queue.

The template's `applies_to` is the source of truth for which item types a
queue can touch. A template limited to hook/short/segment yields a queue
that can never pick up an episode.

### Orientation needs new data

Nothing in the schema stores video dimensions today. `youtube_kind` is a
duration-derived guess (`≤60s → short`), written only on the import path,
and is NULL on 112 of 113 promos — it is **not** an orientation signal and
must not be used as one.

Migration adds `videos.width` / `videos.height` (nullable INTEGER), probed
via the existing `media.probe_video_file`, populated at upload / cut /
attach time, with a one-time backfill for rows that have a local file.
Orientation is *derived*, never stored: `height > width` → portrait,
`width > height` → landscape, equal → square.

Rows with no dimensions (nothing local to probe) are **excluded and
counted**, never silently dropped: the config screen shows "N videos
skipped — dimensions unknown" so the number is always accounted for.

Measured against the current DB (project *Drew and Dan in The Morning*),
the defaults select **52 clips — 28 hooks, 24 shorts, 0 segments**. All 42
segments are landscape, and only 10 are ≤3:00, so they cannot qualify
under the default filters even though the template admits them.

## Platform capability metadata

`PLATFORM_MEDIA_LIMITS` in `services/social.py`, beside `ALL_PLATFORMS` /
`PLATFORM_FIELDS` — the established registry location. Same idea as the
existing `DEFAULT_MAX_CHARS_BY_PLATFORM` in `services/templates.py`, which
already encodes a per-platform limit and lets a slot override it.

Verified limits:

| platform | max size | max duration | max resolution | codecs |
|---|---|---|---|---|
| Bluesky | 300 MB | 3:00 | not published | mp4, mpeg, webm, mov |
| X (no Premium) | 512 MB | **2:20** | 1920×1200 | mp4 h264/aac |
| Mastodon | **read live** | — | **read live** | mp4, webm, mov, ogg |
| Threads | 1 GB | 5:00 | 1920 horizontal | mov/mp4, h264/HEVC, AAC ≤48 kHz, 23–60 fps |
| LinkedIn | 5 GB | 15 min | 4096×2304 | mp4 h264/aac |

Mastodon's limits are **per instance** and must be read from
`/api/v2/instance` (cached), not hardcoded. mastodon.social reports
103,809,024 bytes / 8,294,400 px / 120 fps; Mastodon's built-in defaults
are 40 MB / 2,304,000 px. Hardcoding the defaults would wrongly exclude
most clips.

One function, `slots_accepting(video)`, is the single source of truth for
"can this video go to this slot". Used at Accept to decide which slots an
item posts to, and again at send as a pre-flight.

## Media preparation

Today **nothing transcodes**. `{{video}}` puts the raw `video_file_path`
into `social_posts.media_paths` at generation time and the poster uploads
it as-is — no size, duration, or dimension check. For the current 4K
vertical clips (2160×3840, ~110 MB) that silently fails on Mastodon, X,
and Threads. The Bluesky embed also omits `aspectRatio`, so clients guess
the layout.

`prepare_media_for_platform()` goes in the **shared send path**, so the
queue, publish fan-out, and manual Send all get it — and the existing
`{{video}}` flow stops failing on three of five platforms.

Rules:

* **Late-bound.** Transcode immediately before upload, not at Accept. A
  9:00 post may go out at 9:01–9:03. Temp file is deleted after upload,
  so there is no retention policy, janitor, or TTL.
* **Key by profile, not platform.** Compute what the source actually
  violates for each platform. Nothing violated → upload the original
  untouched. Otherwise encode to the minimum profile that fixes it, and
  dedupe encodes by profile within one fan-out. Typically one encode for
  a five-platform send, often zero.
* **Preserve aspect ratio.** Square stays square, portrait portrait,
  landscape landscape. Fit inside a 1920 long edge × 1080 short edge
  envelope, never upscaling, even dimensions.
* **Verify before sending.** A byte ceiling (Mastodon) is not achievable
  with CRF, whose output size is unpredictable. Compute a target bitrate
  from duration, encode, then **stat the result**; if it is still over,
  that is a hard error on the post. Never hand a platform something we
  already know it will reject.
* **Encoder choice.** `h264_videotoolbox` for plain downscales;
  `libx264` when a byte ceiling must be hit, since videotoolbox honors an
  exact bitrate target poorly. Both are present, along with `aac`, on the
  ffmpeg already used for clip cutting. Reuse `clipper.py`'s hardware(2) /
  software(8) semaphores.
* **Send `aspectRatio`** on the Bluesky embed, from the probed dimensions.

When no encoding can fix it — a 168 s clip against X's 140 s cap —
that slot is **skipped**, not failed. Skipped means "known in advance,
not attempted"; failed means "attempted and broke". They must stay
distinguishable in history.

## Configuration flow

1. Dashboard gains a **Create smart schedule** button near "Compose
   standalone post" / "Upload new video". Existing queues appear on the
   same screen above the episode videos, each with **Edit**.
2. Config screen: pick the template, set the recurrence slots, set the
   filters.
3. **Auto-select videos** walks every video in the project, applies the
   filters, and presents a summary by type plus a schedule forecast
   ("runs through September 28, 2026"), listed in creation order — same
   presentation as the promo mass-schedule review.
4. **Shuffle** reorders the *pending selection only*. It never touches
   items already scheduled by this queue.
5. **Accept** schedules the selection. Accept is optional: a queue can be
   created with auto-add enabled and no back-catalogue.
6. After Accept, a checkbox — *Automatically add eligible videos to this
   queue as they go live* (default **checked**) — then **Finish**.

## Scheduling

Times are **pre-determined at Accept**, not derived at fire time. Each
item is stamped with a concrete target so the user can see exactly which
video posts when.

Storage is **UTC ISO**, identical to `videos.publish_at` and
`social_posts.scheduled_at`. No new convention.

DST is handled at *computation* time, not storage time. Enumerate the
occurrences as local dates — "Mon Nov 9 at 09:00" in the queue's zone —
and convert each one individually. Every stamped UTC instant is then
correct for its own date. Drift only occurs if you resolve one UTC offset
and then add 7-day increments in UTC; enumerating locally avoids it
entirely. `smart_queues.timezone` exists because "9:00am" has to be
interpreted against something, but items carry a plain UTC instant.

At Accept, each item's text is **rendered per slot** and written as
ordinary `social_posts` rows (`status='approved'`, `scheduled_at`,
`slot_id`). The queue owns ordering and state; `social_posts` owns the
posting. Everything downstream — send, retry, duplicate detection,
history, the missed-backlog guard — is reused rather than reimplemented.

A **re-render scheduled posts from current template** action regenerates
the text of still-pending items after a template edit.

Changing a queue's recurrence or filters while it has scheduled items
prompts: **re-flow existing scheduled postings?** No → new settings apply
only to items added from now on. Yes → cancel everything pending and
re-flow it onto the new schedule.

### Recycling

Because items are occurrences, recycling needs no special case. Unchecking
*exclude already posted by this queue* resurfaces previously-posted clips
as candidates; Accept appends **new** item rows for them. History keeps
both. `removed` and `skipped` are neither `posted` nor `scheduled`, so a
removed item is eligible again automatically.

## Auto-add on live

Every transition of a video to live — a promo whose publish timer fired,
an imported episode discovered already public, a manual publish — checks
**all** smart queues, and for each with auto-add enabled, applies the same
`is_eligible()` used by the config screen. Eligible → appended to the tail.

**Liveness is `privacy_status == 'public'`, never `status == 'published'.**
`status` drifts off `published` whenever privacy is flipped via the
metadata dropdown, which is why the existing send-time check keys on
privacy alone (see the comment in `_send_scheduled_post`).

The write sites that make a video live, which the funnel must cover:

| site | covers |
|---|---|
| `scheduler.py:263` | publish timer fires (scheduled-later, batch-scheduled promos) |
| `scheduler.py:303` | publish path for a video already public on YouTube |
| `video_routes.py:841` | manual edit → privacy public + save (upload-and-flag-live, import-and-flag-live, generated promo edited live) |
| `imports.py:241` | importing a video that is already public |

`video_routes.py:841` already carries a miniature of this idea —
`if new_privacy == "public" and before_status != "published"` — so the
manual path has precedent for a transition guard.

### Firing once, not twice

Public → unlisted → public must **not** re-trigger. A per-video marker
records that the auto-add decision has already been made.

Name it for the decision, not the fact: the fact "is live" is already
derivable from `privacy_status`, so a second column asserting it would be
redundant state that can drift. What is *not* derivable is "we already
considered this video for auto-add."

Two things this must get right:

* **Only mark it when the check was decidable.** If a video goes live
  before its dimensions or duration are known, eligibility evaluates to
  "no" — and if the marker is set anyway, the video is never reconsidered
  once the data arrives. Set it only when the answer was real.
* **Backfill on migration.** Set the marker for every video that is
  already public. Creating a queue never auto-adds anything on its own —
  the trigger is a transition, and the back-catalogue enters a queue only
  through Auto-select + Accept. What the backfill prevents is this: an
  existing public video is flipped to unlisted and back months from now,
  which *is* a real non-public → public transition, and with the marker
  NULL it would read as "first time ever live" and auto-add.

Its distinct effect is narrower than it first appears. The always-applied
filters already exclude items currently scheduled by this queue, and by
default those already posted by it. So the marker only changes the outcome
when the previous occurrence ended `failed`, `skipped`, or `removed` —
those would otherwise be re-added on a second live transition.

That is deliberate, and it means **removed is eligible for manual re-add
only** (via Auto-select + Accept), never automatic re-add.

Two implementations of eligibility would eventually disagree, so the
config screen and this hook must call exactly one `is_eligible()`.

## Missed slots

Per queue, a picker with a user-configurable **N**:

* **A** — post late if within N hours
* **B** — reschedule to the end of the queue
* **C** — do not reschedule; remove from the queue (which makes the video
  eligible to be added again later)

Note the interaction with the existing global guard: past 10 overdue posts
at startup, `restore_scheduled_posts` marks everything failed and sends
nothing, before any queue policy runs. The queue's policy therefore
governs the ordinary case; the global guard is the backstop against a
week-long outage.

## Failure and manual disposition

A failure is per (item, slot). One platform failing does not stop the
other four. A failed item sits as `failed` with the real error; the user
inspects it, fixes the cause (slot config, a missing variable, expired
credentials), then chooses **Post now**, **Reschedule to end of queue**,
or **Remove from queue**.

No governor, no sweeper, no new spacing machinery, no automatic retry.
Recovery is manual and user-initiated.

"Missed" is a **derived** state — `scheduled_at` in the past and not
posted — computed when the screen loads. No background job, no stored
flag.

At fire time, verify the video is **still live**. This already exists:
`_send_scheduled_post` checks `privacy_status != 'public'`, marks the post
failed with the real reason, and records a
`social_post_failed_video_not_public` event that the Log panel already
renders. Because queue items are ordinary `social_posts` rows on the
shared send path, the queue inherits it with no new code.

## Display

* **Project dashboard** — each queue shows upcoming and recent history,
  expandable. This is the only roll-up: it shows postings for promos as
  well as episodes.
* **Promo video log** — an entry when that promo is posted to social.
* **Episode video log** — an entry only when the episode itself was
  posted. Promo postings do **not** roll up into the parent's event log.
* Deleting a queue requires confirmation, cancels everything pending, and
  **keeps all history**.

## Schema

```sql
-- dimensions, for the orientation filter
ALTER TABLE videos ADD COLUMN width INTEGER;
ALTER TABLE videos ADD COLUMN height INTEGER;

CREATE TABLE smart_queues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    template_id INTEGER NOT NULL REFERENCES templates(id),
    timezone TEXT NOT NULL,              -- IANA, e.g. America/Los_Angeles
    min_duration_seconds REAL NOT NULL DEFAULT 0,
    max_duration_seconds REAL NOT NULL DEFAULT 180,
    orientations TEXT NOT NULL DEFAULT '["portrait","square"]',
    exclude_already_posted INTEGER NOT NULL DEFAULT 1,
    auto_add_on_live INTEGER NOT NULL DEFAULT 1,
    missed_policy TEXT NOT NULL,         -- post_late | reschedule_end | remove
    missed_grace_hours INTEGER,          -- only for post_late
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE smart_queue_slots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER NOT NULL REFERENCES smart_queues(id) ON DELETE CASCADE,
    weekday INTEGER NOT NULL,            -- 0=Mon
    time_of_day TEXT NOT NULL            -- 'HH:MM' local to the queue
);

CREATE TABLE smart_queue_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id INTEGER NOT NULL REFERENCES smart_queues(id) ON DELETE CASCADE,
    video_id TEXT NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    scheduled_at TEXT,                   -- UTC ISO, same as social_posts
    state TEXT NOT NULL,                 -- scheduled|posted|failed|skipped|removed
    reason TEXT,                         -- why skipped/failed
    added_at TEXT NOT NULL DEFAULT (datetime('now'))
);
```

`social_posts` gains `smart_queue_item_id INTEGER` (nullable, FK, ON
DELETE SET NULL) so a posting can be traced back without duplicating the
posting machinery.

## Phases

1. **Dimensions + capability metadata + transcode in the shared send
   path.** Independently valuable: it fixes `{{video}}` posting, which
   silently fails on three of five platforms today, and adds the missing
   Bluesky `aspectRatio`. No queue involved.
2. **Schema + queue CRUD + config screen with auto-select preview.** No
   scheduling yet — the screen can show you the 52 clips and the forecast
   without committing anything.
3. **Accept** — render per slot, create posts, stamp targets, register jobs.
4. **Auto-add on live + re-flow + dashboard upcoming/history.**
5. **Failure and missed disposition UI.**

## Open

* Queue **name** — auto-derived from the template, or user-entered?
* Where does "Reschedule to end of queue" land when the queue's schedule
  is already fully stamped — append after the last stamped target, using
  the next free recurrence slot?
* Reordering is explicitly deferred ("later we might add a re-ordering
  tool"); only Shuffle-before-Accept is in scope.
