-- Migration 016: persisted test variables on templates.
--
-- The template-edit page has a Preview pane with editable test values
-- (title, url, tags, description, user_message). Those values were
-- ephemeral browser-state until now — close the tab and you lose
-- everything you typed. This column saves them with the template so
-- the next session opens with the same fixtures, plus any custom
-- key/value pairs the user added via "+ Add test variable".
--
-- Shape: JSON object {key: value} where keys match the {{name}} they
-- substitute. NULL on existing rows means "use the page's seeded
-- defaults," which is the previous behaviour.

ALTER TABLE templates ADD COLUMN test_variables TEXT;
