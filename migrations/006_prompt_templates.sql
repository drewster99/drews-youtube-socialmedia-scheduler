-- LLM prompt templates: same {{variable}} engine as social templates.
-- Stored separately because their shape differs (single body, no per-platform
-- variants). Seeded with the SEO description prompt so the existing
-- generate-description flow keeps working unchanged.

CREATE TABLE prompt_templates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE DEFAULT 1,
    key TEXT NOT NULL,
    name TEXT NOT NULL,
    body TEXT NOT NULL,
    applies_to TEXT NOT NULL DEFAULT '["hook","short","segment","video"]',
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (project_id, key)
);

CREATE INDEX idx_prompt_templates_project ON prompt_templates(project_id);

-- Seed the description-from-transcript prompt for the default project. The
-- body uses the existing template engine variables; missing values render as
-- empty strings.
INSERT INTO prompt_templates (project_id, key, name, body) VALUES (
    1,
    'description_from_transcript',
    'Description from transcript',
    'Generate an SEO-friendly YouTube video description.

Video title: {{title}}
{{channel_name_block}}
Transcript:
{{transcript_truncated}}

Instructions:
- Write a compelling description that summarizes the video content
- Include relevant keywords naturally
- Use short paragraphs for readability
- Include timestamps if the transcript suggests distinct sections
- Do NOT include links (those will be added separately)
- Do NOT include hashtags (those will be added separately)
- Keep it under 2000 characters
{{extra_instructions}}

Return ONLY the description text, no preamble.'
);

INSERT INTO prompt_templates (project_id, key, name, body) VALUES (
    1,
    'tags_from_metadata',
    'Tags from metadata',
    'Generate 8–15 YouTube tags that maximise discoverability for this video.

Title: {{title}}
Description: {{description}}
Transcript (first 4000 chars): {{transcript_truncated}}

Instructions:
- Output a comma-separated list, no numbering, no quotes.
- Use lowercase except for proper nouns.
- Include both broad terms and specific phrases.
- Avoid duplicates and near-duplicates.

Return ONLY the comma-separated list.'
);
