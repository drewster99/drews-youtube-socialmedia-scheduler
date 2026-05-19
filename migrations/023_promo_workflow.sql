-- Migration 023: Promo Videos workflow — auto-action state tracking +
-- per-video "this publish_at was set by the user, don't move it on cascade"
-- flag.
--
-- The Promo Videos screen lets the user bulk-upload child clips under a
-- primary. Each clip runs through a multi-step auto-action chain
-- (generate title → upload to YouTube → probe duration → transcribe →
-- generate description + tags → push metadata back to YouTube). The UI
-- polls per-card progress, so each step transition is persisted.
--
-- ``auto_action_state`` is one of: 'pending', 'generating_title',
-- 'uploading', 'probing', 'transcribing', 'generating_desc',
-- 'generating_tags', 'pushing_metadata', 'ready', 'failed:<step>'.
-- NULL means "no Promo flow has ever touched this row" — the existing
-- ``status`` column continues to drive the standard upload/import
-- lifecycle independently.
--
-- ``auto_action_last_error`` carries the human-readable error message
-- when the state hits 'failed:<step>', so the retry button can show
-- *what* went wrong.
--
-- ``publish_at_manual`` = 1 when the user directly set / changed the
-- video's publish_at (via the detail-page scheduler or the schedule-all
-- review modal's parent-time picker). Schedule-all batches and
-- cascade-on-parent-shift writes set it to 0. The cascade routines
-- skip any row where this flag is 1.

ALTER TABLE videos ADD COLUMN auto_action_state TEXT;
ALTER TABLE videos ADD COLUMN auto_action_last_error TEXT;
ALTER TABLE videos ADD COLUMN publish_at_manual INTEGER NOT NULL DEFAULT 0;

CREATE INDEX idx_videos_auto_state ON videos(auto_action_state)
  WHERE auto_action_state IS NOT NULL;
