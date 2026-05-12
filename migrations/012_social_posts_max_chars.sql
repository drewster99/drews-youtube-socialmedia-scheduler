-- Migration 012: social_posts.max_chars
--
-- Carries the slot's "Max characters" value onto each generated post so the
-- review UI can flag over-limit posts without re-deriving the limit from the
-- template. NULL on rows generated before this migration — the UI falls back
-- to a per-platform default for those.

ALTER TABLE social_posts ADD COLUMN max_chars INTEGER;
