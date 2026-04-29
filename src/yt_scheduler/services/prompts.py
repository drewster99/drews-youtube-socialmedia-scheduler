"""Prompt template CRUD + render-with-fallback.

Both prompt templates and social templates flow through the same renderer
in ``services/templates.py``. The variable set passed to ``render_template``
is generous on purpose: any LLM prompt can pull in transcript, project name,
channel name, etc., regardless of the calling context.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable

from yt_scheduler.database import get_db


@dataclass(frozen=True)
class SeedPrompt:
    key: str
    name: str
    body: str


# Fallback bodies if a row is missing — kept in code so existing installs that
# haven't run migration 006 yet still produce something useful.
SEED_DESCRIPTION_FROM_TRANSCRIPT = SeedPrompt(
    key="description_from_transcript",
    name="Description from transcript",
    body=(
        "Generate an SEO-friendly YouTube video description.\n\n"
        "Video title: {{title}}\n"
        "{{channel_name_block}}\n"
        "Transcript:\n{{transcript_truncated}}\n\n"
        "Instructions:\n"
        "- Write a compelling description that summarizes the video content\n"
        "- Include relevant keywords naturally\n"
        "- Use short paragraphs for readability\n"
        "- Include timestamps if the transcript suggests distinct sections\n"
        "- Do NOT include links (those will be added separately)\n"
        "- Do NOT include hashtags (those will be added separately)\n"
        "- Keep it under 2000 characters\n"
        "{{extra_instructions}}\n\n"
        "Return ONLY the description text, no preamble."
    ),
)

SEED_TAGS_FROM_METADATA = SeedPrompt(
    key="tags_from_metadata",
    name="Tags from metadata",
    body=(
        "Generate 8–15 YouTube tags that maximise discoverability for this video.\n\n"
        "Title: {{title}}\n"
        "Description: {{description}}\n"
        "Transcript (first 4000 chars): {{transcript_truncated}}\n\n"
        "Instructions:\n"
        "- Output a comma-separated list, no numbering, no quotes.\n"
        "- Use lowercase except for proper nouns.\n"
        "- Include both broad terms and specific phrases.\n"
        "- Avoid duplicates and near-duplicates.\n\n"
        "Return ONLY the comma-separated list."
    ),
)

SEED_DESCRIPTION_FROM_FRAMES = SeedPrompt(
    key="description_from_frames",
    name="Description from keyframes (vision)",
    body=(
        "{{channel_name_block}}"
        "Title: {{title}}\n\n"
        "Below are keyframes sampled in order from a short YouTube video.\n"
        "Write a YouTube SEO description (3-5 short paragraphs) that "
        "describes what happens in the video and would help viewers find "
        "it via search. Open with a strong hook in the first sentence — "
        "that's the only line shown in YouTube's collapsed description. "
        "Do not invent dialogue or audio; describe only what's visible. "
        "Do not output any preamble, tags list, or markdown headings — "
        "just the description text."
        "{{extra_instructions_block}}"
    ),
)

SEED_TAGS_FROM_FRAMES = SeedPrompt(
    key="tags_from_frames",
    name="Tags from keyframes (vision)",
    body=(
        "Title: {{title}}\n"
        "Description: {{description_or_none}}\n\n"
        "Below are keyframes from the video. Generate 8-12 YouTube search "
        "tags as a comma-separated list. Each tag 1-3 words, lowercase, "
        "no quotes, no '#'. Return ONLY the comma-separated tags."
    ),
)

_SEEDS_BY_KEY: dict[str, SeedPrompt] = {
    SEED_DESCRIPTION_FROM_TRANSCRIPT.key: SEED_DESCRIPTION_FROM_TRANSCRIPT,
    SEED_TAGS_FROM_METADATA.key: SEED_TAGS_FROM_METADATA,
    SEED_DESCRIPTION_FROM_FRAMES.key: SEED_DESCRIPTION_FROM_FRAMES,
    SEED_TAGS_FROM_FRAMES.key: SEED_TAGS_FROM_FRAMES,
}


def _row_to_dict(row) -> dict:
    data = dict(row)
    applies_to = data.pop("applies_to", None)
    if applies_to:
        try:
            data["applies_to"] = json.loads(applies_to)
        except json.JSONDecodeError:
            data["applies_to"] = ["hook", "short", "segment", "video"]
    else:
        data["applies_to"] = ["hook", "short", "segment", "video"]
    return data


async def list_prompt_templates(project_id: int) -> list[dict]:
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, project_id, key, name, body, applies_to, updated_at "
        "FROM prompt_templates WHERE project_id = ? ORDER BY key",
        (project_id,),
    )
    return [_row_to_dict(r) for r in rows]


async def get_prompt_template(key: str, *, project_id: int) -> dict | None:
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, project_id, key, name, body, applies_to, updated_at "
        "FROM prompt_templates WHERE project_id = ? AND key = ?",
        (project_id, key),
    )
    return _row_to_dict(rows[0]) if rows else None


async def get_prompt_body_with_fallback(key: str, *, project_id: int) -> str:
    """Return the prompt body for ``key`` from the DB, falling back to the seed.

    Routes call this so a missing row doesn't break generation in existing
    installs that haven't applied the migration yet.
    """
    record = await get_prompt_template(key, project_id=project_id)
    if record is not None:
        return record["body"]
    seed = _SEEDS_BY_KEY.get(key)
    if seed is not None:
        return seed.body
    raise KeyError(f"No prompt template for key '{key}'")


async def upsert_prompt_template(
    *,
    key: str,
    name: str,
    body: str,
    project_id: int,
    applies_to: Iterable[str] = ("hook", "short", "segment", "video"),
) -> int:
    db = await get_db()
    cursor = await db.execute(
        """
        INSERT INTO prompt_templates (project_id, key, name, body, applies_to)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(project_id, key) DO UPDATE SET
            name = excluded.name,
            body = excluded.body,
            applies_to = excluded.applies_to,
            updated_at = datetime('now')
        """,
        (project_id, key, name, body, json.dumps(list(applies_to))),
    )
    await db.commit()
    return int(cursor.lastrowid or 0)
