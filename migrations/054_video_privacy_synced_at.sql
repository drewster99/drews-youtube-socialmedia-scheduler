-- When we last READ this video's privacy back from YouTube.
--
-- `videos.privacy_status` was write-only with respect to YouTube: every path
-- that set it was one WE initiated (upload, the publish timer, the metadata
-- dropdown, import). Nothing ever read it back. A video published directly in
-- YouTube Studio therefore stayed 'unlisted' here forever, with two effects:
--
--   * `smart_queue.is_eligible` calls a YouTube-backed video live from this
--     column, so auto-add never considered it — the video simply never entered
--     any queue.
--   * the send gate refuses to post a link to a non-public video from this same
--     column, so social posts for it were marked failed with "YouTube video is
--     still 'unlisted'" about a video that was, in fact, public.
--
-- The reverse is just as bad and is why the sweep checks every YouTube-backed
-- video rather than only the non-public ones: a video pulled back to unlisted on
-- YouTube leaves a stale 'public' here, and the send gate — the one guard
-- against announcing a link nobody can open — reads it and waves the post
-- through.
--
-- NULL means never checked, which is what every pre-migration row is. It is not
-- "checked and found unchanged": a sweep that cannot reach YouTube must leave
-- these stamps to go stale rather than write a confirmation it did not earn.
-- The column is therefore the record of what was actually VERIFIED, as against
-- privacy_status, which is merely what we last believed.

ALTER TABLE videos ADD COLUMN privacy_synced_at TEXT;

-- The sweep orders by this column (least-recently-verified first) so a capped
-- run rotates through the library instead of re-checking the same head of the
-- list every time and starving the tail.
CREATE INDEX IF NOT EXISTS idx_videos_privacy_synced_at
    ON videos (privacy_synced_at);
