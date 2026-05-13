-- Migration 017: track YouTube video-file download progress.
--
-- The detail page needs to know when a background download (kicked off
-- by an import or the explicit Transcribe button) is in progress so it
-- can show a "Downloading…" indicator and poll for completion without
-- the user having to leave and re-open the page.
--
-- NULL means "not started or completed" — completion is the natural
-- state once video_file_path is set, so we don't have a separate
-- "done" value. The three live values are:
--
--   'in_progress' — a download is currently fetching the mp4.
--   'failed'      — the last attempt errored out (network/HTTP/parse).
--   'unavailable' — YouTube refused the request (private video,
--                   age-gated, etc.) — retry won't help without the
--                   user flipping privacy on YouTube.

ALTER TABLE videos ADD COLUMN video_file_download_state TEXT;
