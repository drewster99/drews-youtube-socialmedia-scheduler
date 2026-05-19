# Tier workflow — promo videos under a primary

A primary video can have child "promo" videos (segments, shorts, hooks)
that exist to promote the primary. This doc captures the full design.

## Concepts

* **Primary** — `parent_item_id IS NULL`. Appears on Dashboard. Has its
  own detail view; can have children.
* **Child / promo** — `parent_item_id` points at a primary. Hidden from
  Dashboard. Reached from the parent's "Promo Videos" screen.
* **Tier** — auto-derived from `duration_seconds`, read-only.
  `hook` < 50s, `short` < 180s, `segment` < 720s, `video` ≥ 720s.
* **`item_type`** — user-editable bucket label. Defaults to tier on
  upload but can be overridden (so a 75-second clip can be treated as a
  segment if the user wants). Possible values stay
  `episode | short | segment | hook | standalone` — we keep
  `episode`/`standalone` even though current UX collapses them.
* One level only. A child cannot itself have children.

## Visibility rules

* Dashboard "Your Videos" filters on `parent_item_id IS NULL`.
* "YouTube videos available to import" shows everything; the user
  decides at import time whether something becomes a child (see
  *Import*).
* Children are reached only through their parent's Promo Videos screen.

## Video detail — primary

* Near the thumbnail: a "Promo Videos" badge.
  * 0 children → label reads `Promo Videos`.
  * ≥1 children → `<N> Promo Videos`. Count includes all states
    (draft / scheduled / published).
  * Clicking opens `/projects/{slug}/videos/{id}/promos`.
* Right-column Status panel gets a "Promo Videos" section listing the
  same count with a link into the Promo Videos screen.

## Video detail — child

* Top of metadata area: a small "Promo of: <parent title> →" link.
* Otherwise identical to a primary's detail view (shared code path:
  edit description, transcript, tags, privacy; schedule publish; etc.).

## Promo Videos screen (`/projects/{slug}/videos/{id}/promos`)

* Header includes a global "Add videos" button (top-of-screen) and a
  short summary (count by tier).
* Three sections, in order: **Segments**, **Shorts**, **Hooks**.
* Each section has its own "Add videos" button.
* Both buttons accept file picker AND drag-and-drop. Multi-select.
* **Section-scoped add** forces `item_type` to that section's value
  for every uploaded file (the user picked the bucket, not duration).
* **Top-level add** auto-assigns `item_type = tier` based on probed
  duration.

### Auto-actions on add

For each newly uploaded file (sequential, one at a time):

1. **Generate title** — Anthropic call using `title_from_filename_prompt`
   with `{{filename}}`, `{{parent_title??}}`,
   `{{parent_description??}}`. Fallback on API failure: strip extension,
   replace `_` with space, title-case, drop common prefixes
   (`riverside_`, etc.).
2. **Upload to YouTube** as unlisted with the generated title + a
   placeholder description.
3. **Probe local duration** to stamp `duration_seconds` + `tier`.
4. **Transcribe locally** (Apple Speech or mlx-whisper per project
   settings).
5. **Description + tags:**
   * If transcript (after trim) ≥ 10 chars: `description_from_transcript_prompt`
     and `tags_from_metadata_prompt` (which sees the transcript).
   * Else: `description_from_frames_prompt` and `tags_from_frames_prompt`.
6. **Metadata update to YouTube** — single `videos.update` call with
   title, description, tags. Never a re-upload.

Sequence honors "skip steps we already have" — if a transcript is
already present (e.g. user-edited), step 4 is skipped; step 5 still
runs against the existing transcript.

### Progress and state

* New column: `videos.auto_action_state TEXT` — one of `pending`,
  `generating_title`, `uploading`, `transcribing`, `generating_desc`,
  `generating_tags`, `pushing_metadata`, `ready`, `failed:<step>`.
* Per-card progress strip on the Promo Videos screen reads from this
  column (polled).
* Card cannot be edited or scheduled until `ready`.

### Failure UX

* When a step fails, `auto_action_state` becomes `failed:<step>` and
  completed steps before it stay intact.
* Per-card "Retry <step>" button reruns only that step (and any
  downstream steps that depended on it).

## Item type override

The user can change `item_type` on any child after upload.

* The card moves visually to the new section.
* `publish_at`, if set, is **never** rewritten — only the bucket label
  changes. (Manual edits don't silently reschedule.)

## Readiness check

A video is ready to schedule when **all** of:

* Transcript present and not all whitespace (any source).
* Tags ≥ 3.
* Description present and not all whitespace.
* Thumbnail present (YouTube's auto-generated frame counts).

Schedule-all blocks if **any** of: parent or any child being included
is not ready. The dialog shows a red chip per not-ready item with a
hint.

## Schedule-all flow

Entered from the Promo Videos screen. Includes all children that
aren't already published.

### Delays (global project defaults)

| Tier    | Initial (after parent publish) | Subsequent (between siblings) |
|---------|--------------------------------|-------------------------------|
| hook    | 4 hours                        | 99 hours                      |
| short   | 18 hours                       | 6 days                        |
| segment | 3 days                         | 9 days                        |

Each tier has its own independent chain, each anchored to the parent's
publish time:

> Parent at 4/1 5:30pm, 2 hooks + 1 short + 1 segment:
> * Hook 1: 4/1 9:30pm (parent + 4h)
> * Hook 2: 4/5 11:30pm (hook1 + 99h)
> * Short 1: 4/2 11:30am (parent + 18h)
> * Segment 1: 4/4 5:30pm (parent + 3d)

### Adding new children later

A new child added to a tier that already has scheduled siblings goes
**after the chronologically-last scheduled time in that tier**, using
the tier's subsequent-delay. (This naturally handles both auto-cadence
and previously manually-rescheduled siblings.)

### Including the parent

If the parent has no `publish_at` yet, the schedule-all dialog
includes a slot for the parent at the top with its own time picker.
Children's slots cascade from that. If the parent is already
scheduled or published, the dialog just shows children with their
times relative to the existing parent time.

### Cascade on parent reschedule

When the parent's `publish_at` shifts by Δ:

* Children whose schedule was auto-computed move by the same Δ.
* Children that were manually overridden stay put.
  (Manual override tracked by a flag on the schedule entry.)

### Cascade on sibling reschedule

When a child at chain position N is rescheduled by Δ, all subsequent
siblings in the same tier (N+1, N+2, …) shift by Δ too — except those
flagged as manually overridden, which stay put.

### Review screen

The schedule-all confirm step shows, in order:

1. **Parent row** at top:
   * If the parent is being scheduled in this batch: shows its target
     time.
   * If already scheduled: shows the existing time.
   * If already published: shows "Published <date>".
2. **Per-child rows**, grouped by tier section (Segments → Shorts →
   Hooks), each showing the title, target time, readiness chip.
3. Total batch span (e.g. "ends 5/4 7:30pm").
4. Confirm + Cancel.

Confirm is disabled if any readiness chip is red. v1 doesn't allow
per-row time editing in the review.

## Time-string parser

Used for the global default delays and any future per-batch override.

* Accepts: `24h`, `1.5h`, `30s`, `90m`, `1w`, `1d`, plus long forms
  `"1 hour"`, `"90 minutes"`, `"3 days"`, etc.
* Single unit only (no `1d 2h` compounds).
* No negatives, no zero.
* Maximum 90 days.

## Imports

* "YouTube videos available to import" cards gain a **"Parent
  (optional)"** dropdown listing existing primaries in the project,
  default `(none / standalone)`.
* If a parent is chosen at import, the video is created with
  `parent_item_id` already set and immediately routed to the Promo
  Videos screen for that parent.

## Template variables (new)

Resolved at render time from the parent video row when
`parent_item_id IS NOT NULL`; empty string otherwise.

* `{{parent_url}}`
* `{{parent_title}}`
* `{{parent_description}}`
* `{{parent_tags}}`

The bare form returns empty for primaries; use `{{parent_url??}}` if
you want fallback control.

## Prompt seeds

### New seed

* `title_from_filename_prompt` — generates a clean YouTube title from
  filename, with optional parent context. Body editable, system
  editable (same UI shape as other editable prompts).

### Seed updates (default body changes only — user-edited DB rows
untouched)

* `description_from_transcript_prompt` — gains a parent-context block
  that runs only when `{{parent_title}}` is non-empty: "If this is a
  promo for a parent video, mention/link the parent (`{{parent_url}}`,
  `{{parent_title}}`)."
* `description_from_frames_prompt` — same.
* `tags_from_metadata_prompt` — include `{{parent_tags??}}` as
  suggested seed tags.
* `tags_from_frames_prompt` — same.

## Schema changes

* `ALTER TABLE videos ADD COLUMN auto_action_state TEXT` — nullable;
  used only by the Promo flow. NULL on existing rows = no flow active.
* `ALTER TABLE scheduled_posts ADD COLUMN manually_overridden INTEGER
  NOT NULL DEFAULT 0` — set to 1 when the user reschedules a child
  directly; cascade logic skips these. (Or equivalent flag on whatever
  table holds per-child publish times — needs verification of which
  table actually carries this for video publishes.)

## Quota budget

YouTube cost for one promo batch:

* Per video: 100 (upload) + 50 (metadata update) + 50 (thumbnail set,
  if any) + caption-list polls.
* A 9-promo batch ≈ 1,800 units, ~18 % of the 10,000-unit daily quota.

The Promo Videos screen surfaces a warning before kicking off auto-
actions if the batch would consume > 25 % of the remaining daily
budget for the bound channel.

## Out of scope (v2)

* Posting hooks as native social videos (currently socials only post
  thumbnails / text, not the video file itself).
* Attaching an existing standalone to a parent after-the-fact (this
  v1 only supports designating parent at upload or import time).
