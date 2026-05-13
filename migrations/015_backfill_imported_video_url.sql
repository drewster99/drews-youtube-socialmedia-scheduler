-- Migration 015: backfill videos.url for any imported rows that lost it
-- between migration 010 (which seeded existing rows) and the fix that
-- makes services/imports.py set url on INSERT.
--
-- Imported rows always come from YouTube and the id IS the YouTube video
-- id, so the canonical URL is deterministic. Standalone / non-YT items
-- are left alone — their url is user-supplied.

UPDATE videos
   SET url = 'https://youtu.be/' || id
 WHERE url IS NULL
   AND imported_from_youtube = 1;
