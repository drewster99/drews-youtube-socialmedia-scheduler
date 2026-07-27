-- Migration 036: record that a video's smart-queue auto-add decision was made.
--
-- Auto-add fires on a video *becoming* live. Public -> unlisted -> public is a
-- real transition and would fire again, so something has to record that the
-- decision has already been taken once.
--
-- Named for the decision, not the fact. "Is live" is already derivable from
-- privacy_status; a second column asserting it would be redundant state that
-- can drift. What is NOT derivable is "we already considered this one".
--
-- Only set when the eligibility check was actually decidable. A video that
-- goes live before its dimensions or duration are known evaluates to "not
-- eligible" — marking it then would mean never reconsidering it once the data
-- arrives.
--
-- The backfill is the important half. Creating a queue never auto-adds
-- anything on its own; the back catalogue enters a queue only through
-- Auto-select + Accept. What this prevents is an existing public video being
-- flipped to unlisted and back months from now — a genuine transition that,
-- with the marker NULL, would read as "first time ever live" and auto-add.
--
-- privacy_status, not status: status drifts off 'published' whenever privacy
-- is flipped via the metadata dropdown, so it is not the authority on
-- liveness.

ALTER TABLE videos ADD COLUMN auto_add_considered_at TEXT;

UPDATE videos
   SET auto_add_considered_at = datetime('now')
 WHERE privacy_status = 'public';
