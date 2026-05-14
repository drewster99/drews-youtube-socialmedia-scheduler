-- Migration 021: link every social_posts row to the template_slots row
-- that produced it.
--
-- Pre-G1, social_posts had only (video_id, platform) — fine when a
-- template had at most one slot per platform. The E1 picker was
-- already capable of expressing "two Mastodon slots with different
-- accounts" (two rows), but the backend filter / DELETE-before-regen /
-- and the "which platform is this post for" UI all keyed on
-- ``platform`` alone, so a partial regenerate of one Mastodon row
-- wiped both, and the user-facing checkbox state couldn't actually
-- route to a specific account.
--
-- ``slot_id`` is nullable for two reasons:
--   * Legacy rows generated before this migration didn't track it —
--     they fall back to platform-based filtering at read time.
--   * Standalone-post / non-template flows still pass through this
--     table without a template-slot context.
--
-- ON DELETE SET NULL on the FK so deleting a slot template doesn't
-- cascade-delete the posts (which may already be 'posted' and part of
-- the audit trail). The row keeps its platform/account info and just
-- becomes "no longer tied to a live slot."

ALTER TABLE social_posts ADD COLUMN slot_id INTEGER
    REFERENCES template_slots(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_social_posts_slot_id
    ON social_posts(slot_id);
