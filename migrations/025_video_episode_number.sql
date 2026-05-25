-- Migration 025: optional per-video episode number.
--
-- A user-supplied label that appears as a chip next to the title on the
-- video detail page and the dashboard card. Local-only metadata — never
-- pushed to YouTube. NULL means "no episode number set" (the chip is
-- hidden).
--
-- Stored as INTEGER (rather than TEXT) so we get cheap ordering "for
-- free" when something later wants to sort episodes numerically.

ALTER TABLE videos ADD COLUMN episode_number INTEGER;
