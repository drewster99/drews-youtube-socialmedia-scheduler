-- needs_reauth flag on social credentials.
--
-- When a poster hits a terminal auth failure (401 after a refresh attempt,
-- or refresh itself rejected), we set this so the UI can render a
-- 'Needs re-auth' badge and prompt the user before the next post fails.
-- It is cleared automatically by ``upsert_credential`` whenever a row
-- is recreated/refreshed via OAuth.

ALTER TABLE social_accounts
    ADD COLUMN needs_reauth INTEGER NOT NULL DEFAULT 0;
