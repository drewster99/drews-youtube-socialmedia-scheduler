-- Migration 026: track the provenance of a video's local file.
--
-- Used by the "Replace source" feature on the video detail page so the
-- UI can show what the current local file is (Original upload /
-- Re-downloaded from YouTube / User-attached master) and so the
-- replace-source endpoint can skip the fidelity-downgrade warning when
-- the current file is a known-lossy YouTube re-download.
--
-- Three values are defined; NULL means "unknown" (pre-026 rows where
-- we can't reliably classify the existing file).
--
--   'uploaded'         — uploaded through this app's normal upload
--                        flow; this is the user's own source file.
--   'youtube_download' — fetched from YouTube via
--                        youtube.download_video_file; lossy transcode.
--   'user_attached'    — replaced via POST /api/videos/{id}/source-file
--                        with a user-supplied higher-fidelity master.

ALTER TABLE videos ADD COLUMN source_file_origin TEXT;

-- Backfill what we can infer. Rows imported from YouTube whose local
-- file came from the download path are youtube_download; everything
-- else with a local file is treated as uploaded. Rows without a local
-- file stay NULL.
UPDATE videos
SET source_file_origin = 'youtube_download'
WHERE video_file_path IS NOT NULL
  AND video_file_path != ''
  AND imported_from_youtube = 1;

UPDATE videos
SET source_file_origin = 'uploaded'
WHERE video_file_path IS NOT NULL
  AND video_file_path != ''
  AND source_file_origin IS NULL;
