-- Migration 027: derived-clip provenance on the videos row.
--
-- The Generate-from-source flow cuts new promo videos out of a parent's
-- local MP4 by passing ffmpeg a start/end timestamp. When the user
-- subsequently re-runs Generate on the same parent, the second pass
-- should know which ranges are already taken so it doesn't propose
-- overlapping clips and so the defensive overlap filter can drop any
-- that Claude proposes anyway.
--
-- Stored in seconds (float). NULL for everything not produced by
-- Generate-from-source — manual uploads, imports, the parent itself,
-- and existing promo children predating this migration. Independent of
-- parent_item_id: a row may have a parent but no cut range (manual
-- upload as a promo child), and we deliberately don't backfill
-- pre-027 rows because we have no record of where they came from.

ALTER TABLE videos ADD COLUMN cut_start_seconds REAL;
ALTER TABLE videos ADD COLUMN cut_end_seconds REAL;

-- Generate queries by (parent_item_id, item_type) and reads the two
-- columns to assemble the "ranges already taken" list per kind. The
-- index covers that lookup; rows with NULL cut_start_seconds are
-- harmless in the scan and the index stays small in practice (only
-- generate-produced rows are non-NULL).
CREATE INDEX IF NOT EXISTS idx_videos_cut_range
    ON videos(parent_item_id, item_type, cut_start_seconds);
