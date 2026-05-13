-- Migration 019: surface "YouTube transcript unavailable" vs.
-- "haven't checked" on the detail page.
--
-- imports.py already calls youtube.list_captions() at import time —
-- when it comes back empty we log "No YouTube transcript available"
-- and leave the row's transcript column NULL. From the UI side that
-- looks identical to "we haven't tried yet," so the user can't tell
-- whether to wait or write one manually.
--
-- This column tracks the three states:
--   NULL          haven't checked (legacy rows or non-import sources)
--   'fetched'     YouTube had captions and we pulled them
--   'unavailable' YouTube has no captions for this video
--
-- Backfill: rows whose transcript_source='youtube' AND transcript is
-- non-empty came from a successful caption fetch → 'fetched'. Other
-- rows stay NULL (we can't tell from the existing columns whether a
-- past import found nothing on YouTube or simply never tried).

ALTER TABLE videos ADD COLUMN youtube_transcript_state TEXT;

UPDATE videos
   SET youtube_transcript_state = 'fetched'
 WHERE transcript_source = 'youtube'
   AND transcript IS NOT NULL
   AND transcript <> '';
