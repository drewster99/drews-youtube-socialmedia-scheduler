-- Migration 022: cache the Claude-vision thumbnail-compare verdict by
-- (local_sha, youtube_sha) so identical bytes are never re-asked.
--
-- C3 originally fired ai.compare_thumbnails every time the YouTube
-- thumbnail URL changed, even when the bytes were ultimately
-- identical to last time (Google re-cached / re-CDN'd the same
-- image). Each compare is a Claude vision API unit; for users who
-- open videos often that adds up. Storing the (local_sha, youtube_sha,
-- verdict) tuple short-circuits the call whenever the inputs match
-- the last computed pair.
--
-- New columns are nullable — a row with no cached sha-pair falls
-- through to a fresh compare on the next open.
--
-- ``youtube_thumbnail_etag`` is the HTTP ETag the i.ytimg.com CDN
-- returned on the last fetch. The G3 HEAD precheck compares it against
-- whatever ETag the CDN returns now: when they match, the bytes
-- haven't changed on the server and we can skip the GET entirely (a
-- second optimization stacked on top of the SHA cache).

ALTER TABLE videos ADD COLUMN thumbnail_compare_local_sha TEXT;
ALTER TABLE videos ADD COLUMN thumbnail_compare_youtube_sha TEXT;
ALTER TABLE videos ADD COLUMN youtube_thumbnail_etag TEXT;
