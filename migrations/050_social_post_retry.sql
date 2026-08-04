-- Automatic retry of transient social send failures, and the anchor the
-- post-late window is measured from.
--
-- Replaces an unapplied `dismissed_at` migration. Dismissing was the wrong
-- model: a post the user is never sending is not a failed post that happens to
-- be hidden, it is a post that has been given up on — which `status = 'skipped'`
-- already says, and which smart_queue_disposition's `remove` already sets. One
-- state, one meaning.
--
-- `intended_at` — when this post was MEANT to go out, set when it is scheduled
-- and never cleared.
--
--   `mark_failed` clears `scheduled_at` on purpose: a row still holding its
--   scheduling columns is resurrected and re-sent by the restore pass on the
--   next restart. But `smart_queue_disposition.within_grace` measures the
--   post-late window from `scheduled_at`, so every FAILED post reported as
--   outside its window no matter how recent the failure — a post that failed
--   three hours into a 24-hour window read as expired. The window needs an
--   anchor that survives the failure, which is this.
--
-- `retryable` — whether the recorded failure is one a blind retry could fix.
--   Stamped at failure time from the exception type, never inferred later by
--   matching the message text.
--
--   ONLY connect-phase failures qualify: the request provably never arrived, so
--   re-sending cannot duplicate. A read/write timeout is equally "transient" and
--   is emphatically NOT retryable — the request may have been delivered and only
--   the response lost, which is exactly how a retry mints a second post. That
--   distinction already exists for Threads publishes
--   (`_PUBLISH_AMBIGUOUS_TRANSPORT_ERRORS`); this generalises it.
--
-- `retry_count` / `next_retry_at` / `retry_until` — the backoff schedule.
--   `retry_until` is computed once from the anchor plus the grace window, so
--   repeated failures cannot walk the deadline forward forever. NULL in all
--   three means this failure is not on the retry path at all.

ALTER TABLE social_posts ADD COLUMN intended_at TEXT;
ALTER TABLE social_posts ADD COLUMN retryable INTEGER;
ALTER TABLE social_posts ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE social_posts ADD COLUMN next_retry_at TEXT;
ALTER TABLE social_posts ADD COLUMN retry_until TEXT;

-- Existing scheduled rows have an intent we can still recover; failed ones have
-- had it erased already and stay NULL, which reads as "unknown", never as now.
UPDATE social_posts SET intended_at = scheduled_at WHERE scheduled_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_social_posts_retry
    ON social_posts(status, next_retry_at);
