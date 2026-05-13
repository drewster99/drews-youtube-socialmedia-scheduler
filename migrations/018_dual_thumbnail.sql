-- Migration 018: dual-thumbnail tracking with Claude-vision compare.
--
-- The detail page used to show a single thumbnail. If the user
-- uploaded one and then edited it externally on YouTube (or YouTube
-- re-encoded it on upload), the local copy went stale silently. New
-- columns let us track both sides separately and flag when they
-- visually diverge.
--
-- Columns:
--   thumbnail_source TEXT       'user' | 'youtube'. Who supplied the
--                                 currently-active thumbnail_path?
--   youtube_thumbnail_path TEXT  Local copy of the last YouTube
--                                 thumbnail we fetched (NULL until we
--                                 fetch one).
--   youtube_thumbnail_url TEXT   The URL we last downloaded from — when
--                                 YouTube's URL changes we know to
--                                 re-fetch + re-compare.
--   thumbnail_compare_verdict TEXT  'same' | 'different' | NULL.
--                                    NULL means "haven't asked Claude
--                                    yet" (or the inputs changed since
--                                    last verdict).
--   thumbnail_compared_at TEXT   When the verdict was computed.
--
-- Backfill: every existing row with a thumbnail_path gets a source.
-- imported_from_youtube=1 rows pull their thumbnail from YouTube at
-- import time, so we know both sides were equal then — copy the path
-- into youtube_thumbnail_path so the next GET has something to
-- compare against. Non-import rows are 'user'.

ALTER TABLE videos ADD COLUMN thumbnail_source TEXT;
ALTER TABLE videos ADD COLUMN youtube_thumbnail_path TEXT;
ALTER TABLE videos ADD COLUMN youtube_thumbnail_url TEXT;
ALTER TABLE videos ADD COLUMN thumbnail_compare_verdict TEXT;
ALTER TABLE videos ADD COLUMN thumbnail_compared_at TEXT;

UPDATE videos
   SET thumbnail_source = 'youtube',
       youtube_thumbnail_path = thumbnail_path
 WHERE thumbnail_path IS NOT NULL
   AND imported_from_youtube = 1;

UPDATE videos
   SET thumbnail_source = 'user'
 WHERE thumbnail_path IS NOT NULL
   AND (imported_from_youtube = 0 OR imported_from_youtube IS NULL);
