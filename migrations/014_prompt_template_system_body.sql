-- Prompt templates: add the system-prompt column and normalise legacy keys.
--
-- The four seeds that shipped with migration 006 used bare snake-case keys
-- (description_from_transcript, tags_from_metadata, …). The new prompts
-- added alongside this migration (shorten_post_prompt,
-- ai_block_default_system_prompt) use a "_prompt" suffix; rename the four
-- legacy keys to match so the naming convention is uniform across the
-- table. User-edited rows survive intact — UPDATE preserves body/name.
--
-- The new system_body column is rendered through the same {{variable}}
-- engine as body; NULL means "send no system prompt", which matches today's
-- behaviour for the description seeds.

ALTER TABLE prompt_templates ADD COLUMN system_body TEXT NULL;

UPDATE prompt_templates
SET key = key || '_prompt'
WHERE key IN (
    'description_from_transcript',
    'description_from_frames',
    'tags_from_metadata',
    'tags_from_frames'
);
