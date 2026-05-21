-- Migration 024: remember the filename a video file was uploaded with.
--
-- On-disk upload filenames are now chosen by the app (canonical
-- <id>.<ext>) rather than the raw client-supplied filename. The old
-- behaviour built the path straight from UploadFile.filename with no
-- basename(), which allowed path traversal (../../...) and silent
-- same-name overwrites between unrelated uploads.
--
-- The original filename is still useful to the user and for debugging,
-- so it's recorded here — sanitized to a basename and truncated —
-- separate from the on-disk path.
--
-- NULL means unknown: pre-024 rows, or videos with no local file.

ALTER TABLE videos ADD COLUMN video_file_original_name TEXT;
