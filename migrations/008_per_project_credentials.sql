-- Per-project credentials + multi-account socials.
--
-- Reshape `social_accounts` to identify credentials by a stable provider id
-- (Twitter user_id, Mastodon acct@host, LinkedIn sub, Threads user_id, etc.)
-- plus our own immutable UUID. Add soft-delete so that template slots and
-- project defaults can show "Missing credential" after a delete instead of
-- silently becoming "use whatever default applies".
--
-- Replace `project_social_accounts` (many-to-many ownership, never consulted
-- at send time) with `project_social_defaults` (per-(project, platform)
-- default credential).
--
-- Replace `templates.platforms` JSON with a real `template_slots` table so
-- each slot can carry its own credential override, disabled flag, and
-- ordering — enabling "two LinkedIn accounts in the same template", and
-- the disable-without-delete behaviour the redesign requires.
--
-- The Python migration `services/keychain_migration.py` runs immediately
-- after this and resolves the `'pending:...'` placeholders into real
-- provider account ids by hitting each platform's identity endpoint.

PRAGMA foreign_keys = OFF;

-- 1) social_accounts -- rebuild with uuid + provider_account_id + soft delete.
CREATE TABLE _new_social_accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid TEXT NOT NULL UNIQUE,
    platform TEXT NOT NULL,
    provider_account_id TEXT NOT NULL,
    username TEXT NOT NULL,
    display_name TEXT,
    is_nickname INTEGER NOT NULL DEFAULT 0,
    credentials_ref TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    deleted_at TEXT,
    UNIQUE (platform, provider_account_id)
);

INSERT INTO _new_social_accounts
    (id, uuid, platform, provider_account_id, username, display_name,
     credentials_ref, created_at, deleted_at)
SELECT id,
       '__pending__:' || id,
       platform,
       'pending:' || platform || ':' || username,
       username,
       display_name,
       'cred.__pending__:' || id,
       created_at,
       NULL
FROM social_accounts;

DROP TABLE social_accounts;
ALTER TABLE _new_social_accounts RENAME TO social_accounts;
CREATE INDEX idx_social_accounts_platform_active
    ON social_accounts(platform) WHERE deleted_at IS NULL;

-- 2) project_social_accounts is replaced by project_social_defaults.
DROP TABLE IF EXISTS project_social_accounts;

-- 3) Per-project default credential per platform.
CREATE TABLE project_social_defaults (
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    platform   TEXT    NOT NULL,
    social_account_id INTEGER REFERENCES social_accounts(id) ON DELETE SET NULL,
    PRIMARY KEY (project_id, platform)
);

-- 4) Each project owns at most one YouTube channel; each channel at most one
-- project. SQLite treats NULLs as distinct so a partial UNIQUE index is fine.
CREATE UNIQUE INDEX idx_projects_youtube_channel_unique
    ON projects(youtube_channel_id) WHERE youtube_channel_id IS NOT NULL;

-- 5) template_slots replaces templates.platforms JSON.
CREATE TABLE template_slots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    template_id INTEGER NOT NULL REFERENCES templates(id) ON DELETE CASCADE,
    platform    TEXT    NOT NULL,
    -- NULL on a built-in slot means "track project default for this
    -- platform". NULL on a non-built-in slot (after a credential is
    -- soft-deleted) means "missing credential".
    social_account_id INTEGER REFERENCES social_accounts(id) ON DELETE SET NULL,
    is_builtin  INTEGER NOT NULL DEFAULT 0,
    is_disabled INTEGER NOT NULL DEFAULT 0,
    order_index INTEGER NOT NULL DEFAULT 0,
    body        TEXT    NOT NULL DEFAULT '',
    media       TEXT    NOT NULL DEFAULT 'thumbnail',
    max_chars   INTEGER NOT NULL DEFAULT 500,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX idx_template_slots_template ON template_slots(template_id);

-- 6) Backfill slots from each template's existing `platforms` JSON BEFORE
-- the column is dropped. The two seeded built-ins each have one entry per
-- platform, so each becomes one built-in slot per platform.
INSERT INTO template_slots
    (template_id, platform, is_builtin, is_disabled, order_index, body, media, max_chars)
SELECT t.id,
       je.key,
       CASE WHEN t.name IN ('new_video', 'new_message', 'announce_video', 'send_message')
            THEN 1 ELSE 0 END,
       0,
       0,
       COALESCE(json_extract(je.value, '$.template'), ''),
       COALESCE(json_extract(je.value, '$.media'), 'thumbnail'),
       COALESCE(json_extract(je.value, '$.max_chars'), 500)
FROM templates t, json_each(t.platforms) je;

-- 7) Drop the platforms column from templates by rebuild.
CREATE TABLE _new_templates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE DEFAULT 1,
    name TEXT NOT NULL,
    description TEXT DEFAULT '',
    applies_to TEXT NOT NULL DEFAULT '["hook","short","segment","video"]',
    is_builtin INTEGER NOT NULL DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE (project_id, name)
);

INSERT INTO _new_templates
    (id, project_id, name, description, applies_to, is_builtin, created_at, updated_at)
SELECT id, project_id, name, description, applies_to, is_builtin, created_at, updated_at
FROM templates;

DROP TABLE templates;
ALTER TABLE _new_templates RENAME TO templates;
CREATE INDEX idx_templates_project ON templates(project_id);

-- 8) Rename the seeded built-ins to the new names. Guard against a user
-- having already created a custom template at the new name within the same
-- project (would violate UNIQUE(project_id, name)). When a conflict exists
-- the legacy row is left for a human to resolve manually.
UPDATE templates SET name = 'announce_video', is_builtin = 1
WHERE name = 'new_video'
  AND NOT EXISTS (
      SELECT 1 FROM templates t2
      WHERE t2.project_id = templates.project_id AND t2.name = 'announce_video'
  );

UPDATE templates SET name = 'send_message', is_builtin = 1
WHERE name = 'new_message'
  AND NOT EXISTS (
      SELECT 1 FROM templates t2
      WHERE t2.project_id = templates.project_id AND t2.name = 'send_message'
  );

-- Mark the renamed-from rows as built-in too (in case the migration above
-- couldn't rename them due to a conflict).
UPDATE templates SET is_builtin = 1
WHERE name IN ('announce_video', 'send_message', 'new_video', 'new_message');

PRAGMA foreign_keys = ON;
