-- Migration 034: cache each video's encoded frame dimensions.
--
-- Orientation (portrait / landscape / square) is needed as a *filter* — the
-- smart queue selects across every video in a project, so the value has to be
-- available to SQL, not computed per-row by shelling out to ffprobe.
--
-- Deliberately NOT storing an `orientation` column: it would be a second
-- source of truth that could disagree with the dimensions it was derived
-- from. Orientation is derived at read time (height > width -> portrait,
-- width > height -> landscape, equal -> square).
--
-- Both columns are nullable and mean "unknown", never zero: a row with no
-- local file has nothing to probe. Callers must surface unknown as its own
-- state rather than silently treating it as excluded.
--
-- videos.youtube_kind is NOT an orientation signal despite the name — it is
-- a duration-derived guess (<=60s -> 'short'), written only on the import
-- path, and NULL on nearly every row.

ALTER TABLE videos ADD COLUMN width INTEGER;
ALTER TABLE videos ADD COLUMN height INTEGER;
