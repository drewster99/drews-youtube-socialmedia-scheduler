-- When each social token dies, so the Settings screen can say so.
--
-- The expiry itself lives in the Keychain bundle, which is the right place for
-- it: it travels with the token it describes. But rendering a credential list
-- shouldn't mean a Keychain read per row, and an expiry timestamp is not a
-- secret — it says nothing about the token beyond when to replace it.
--
-- Motivating case: a Threads token issued 2026-05-11 lapsed at 60 days and
-- every post after that failed with an opaque HTTP 500 for weeks. Nothing in
-- the UI could have shown it, because nothing recorded it.
--
-- NULL means unknown, not "never expires" — a credential stored before this
-- column existed, or a pasted token whose issuer told us nothing.

ALTER TABLE social_accounts ADD COLUMN token_expires_at TEXT;
