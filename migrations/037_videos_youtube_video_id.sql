-- Migration 037: store the YouTube video id instead of inferring it from the
-- primary key's length.
--
-- videos.id has been doing two jobs. For a row uploaded to or imported from
-- YouTube it *is* the 11-character YouTube video id; for an item created
-- outside YouTube it is a generated 22-character token. Five places across
-- app.py, video_routes, promo_routes and smart_queue asked "does this row have
-- a YouTube video behind it?" by measuring `len(id) == 11`.
--
-- That is a record's *type* inferred from the *shape* of its key. Nothing
-- enforces that a generated token can't be 11 characters, and the inference is
-- invisible at the call site: test fixtures using two-letter ids silently ran
-- the non-YouTube path, so tests that claimed to cover YouTube behaviour were
-- not covering it at all and nothing said so.
--
-- The id keeps its identity job. The YouTube video id becomes an ordinary
-- nullable attribute, and NULL is the answer to "no YouTube video" — a fact
-- the row states rather than one the caller derives.
--
-- The length rule is used here, once, against the data in front of us, and
-- never again. It is sound as a backfill because it is exactly how every
-- existing row was written: the YouTube upload and import paths key the row by
-- the id YouTube returned, and the non-YouTube path mints
-- secrets.token_urlsafe(16)[:22]. Verified against the live database before
-- writing this: 128 rows at 11 characters, every one of them matching the
-- YouTube id alphabet, and 1 row at 22.

ALTER TABLE videos ADD COLUMN youtube_video_id TEXT;

UPDATE videos SET youtube_video_id = id WHERE length(id) = 11;

-- Promo/clip lookups join parents by their YouTube id, and the caption and
-- comment pollers scan for rows that have one.
CREATE INDEX IF NOT EXISTS idx_videos_youtube_video_id
    ON videos(youtube_video_id);
