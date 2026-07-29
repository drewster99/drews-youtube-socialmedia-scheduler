-- When each social token was obtained, so the Settings screen can show a
-- token's age and lifetime alongside its expiry (migration 039's column).
--
-- Acquisition time lives in the Keychain bundle next to the token it
-- describes; this column is the same kind of non-secret mirror as
-- token_expires_at, so the credential list never needs a Keychain read.
--
-- NULL means unknown — a credential whose bundle was last written before
-- acquisition stamping existed. It backfills naturally: the next token
-- refresh or re-auth stamps the bundle and the mirror follows.

ALTER TABLE social_accounts ADD COLUMN token_acquired_at TEXT;
