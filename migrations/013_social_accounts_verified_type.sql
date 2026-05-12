-- Migration 013: social_accounts.verified_type
--
-- For X (Twitter) accounts, the value of `verified_type` from /2/users/me
-- ('none' | 'blue' | 'business' | 'government'). Anything other than 'none'/NULL
-- means the account is on X Premium / a verified org — which raises the post
-- character limit from 280 to 25,000. Captured on connect / re-auth. NULL for
-- non-X platforms and for X accounts connected before this column existed.

ALTER TABLE social_accounts ADD COLUMN verified_type TEXT;
