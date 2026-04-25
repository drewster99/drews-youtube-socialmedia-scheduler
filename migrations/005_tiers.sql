-- Tier classification + cached duration.
--
-- Tiers (per spec):
--   hook    : duration < 50s
--   short   : 50 <= duration < 180s
--   segment : 180 <= duration < 720s
--   video   : duration >= 720s
--
-- duration_seconds caches the value so we don't have to refetch from YouTube
-- every page load. tier may be null when duration is unknown; the UI shows
-- a manual override picker.

ALTER TABLE videos ADD COLUMN duration_seconds REAL;
ALTER TABLE videos ADD COLUMN tier TEXT;
