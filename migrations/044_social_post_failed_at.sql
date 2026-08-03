-- When a social post's most recent send attempt failed.
--
-- The exact mirror of posted_at, which has always recorded when the last
-- attempt succeeded. Without it a failure had no time at all: created_at is
-- when the post row was written (hours or weeks before the send), and
-- scheduled_at is cleared by the same update that marks the row failed. The
-- failed-sends banner could therefore say what broke but never when, which is
-- the difference between "this is happening now" and "this is five days old".
--
-- NULL means unknown — a row that failed before this column existed. Those
-- rows are the only legitimate NULLs; every failure written from here on is
-- stamped by models.social_post.mark_failed, the single writer of the
-- 'failed' state.

ALTER TABLE social_posts ADD COLUMN failed_at TEXT;
