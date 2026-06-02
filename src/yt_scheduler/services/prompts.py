"""Prompt template CRUD + render-with-fallback.

Both prompt templates and social templates flow through the same renderer
in ``services/templates.py``. The variable set passed to ``render_template``
is generous on purpose: any LLM prompt can pull in transcript, project name,
channel name, etc., regardless of the calling context.

Each seed defines a ``body`` (user prompt) and optionally a ``system``
prompt. ``system=None`` means "send no system prompt", which is the right
default for the description seeds — they instruct Claude entirely through
the user-message body. Seeds that *do* declare a system prompt expose it
in the Project Settings UI as a second textarea.
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
    # Variables the AI service substitutes into this prompt at render time.
    # Surfaced in the prompt-editor UI so users editing the template know
    # what placeholders they have available — referencing any name not
    # listed here will render literally (the bare ``{{name}}`` form falls
    # through; only the ``{{name!}}`` required form raises).
    variables: tuple[str, ...] = ()
    # Optional system prompt shipped with the seed. ``None`` means "the
    # call site sends no system message" — the description seeds rely on
    # that. When non-None it's surfaced as a second textarea in the UI and
    # rendered through the same {{variable}} engine as ``body``.
    system: str | None = None
    # Names of variables available inside ``system`` (for the variable
    # hints chip-row above the system textarea in the UI). Most seeds
    # don't reference variables in the system prompt, so this defaults
    # to empty.
    system_variables: tuple[str, ...] = ()


# Fallback bodies if a row is missing — kept in code so existing installs that
# haven't run migration 006 / 014 yet still produce something useful.
SEED_DESCRIPTION_FROM_TRANSCRIPT_PROMPT = SeedPrompt(
    key="description_from_transcript_prompt",
    name="Description from transcript",
    body=(
        "Generate an SEO-friendly YouTube video description.\n\n"
        "Video title: {{title}}\n"
        "{{channel_name_block}}\n"
        "Transcript:\n{{transcript_truncated}}\n"
        "{{parent_context_block??}}\n\n"
        "Instructions:\n"
        "- Write a compelling description that summarizes the video content\n"
        "- Include relevant keywords naturally\n"
        "- Use short paragraphs for readability\n"
        "- Include timestamps if the transcript suggests distinct sections\n"
        "- Do NOT include links (those will be added separately)\n"
        "- Do NOT include hashtags (those will be added separately)\n"
        "- Do NOT use the literal characters '<' or '>' anywhere — "
        "YouTube rejects descriptions containing them. When referencing "
        "code symbols (e.g. UIView, NSObject) write them in backticks "
        "or quotes instead.\n"
        "- Keep it under 2000 characters\n"
        "{{extra_instructions}}\n\n"
        "Return ONLY the description text, no preamble."
    ),
    variables=(
        "title", "channel_name", "channel_name_block",
        "transcript", "transcript_truncated", "extra_instructions",
        "parent_url", "parent_title", "parent_description", "parent_tags",
        "parent_context_block",
    ),
    # No system prompt — instructions live in the user message body.
    system=None,
)

SEED_TAGS_FROM_METADATA_PROMPT = SeedPrompt(
    key="tags_from_metadata_prompt",
    name="Tags from metadata",
    body=(
        "Generate 8–15 YouTube tags that maximise discoverability for this video.\n\n"
        "Title: {{title}}\n"
        "Description: {{description}}\n"
        "Transcript (first 4000 chars): {{transcript_truncated}}\n"
        "{{parent_context_block??}}\n\n"
        "Instructions:\n"
        "- Output a comma-separated list, no numbering, no quotes.\n"
        "- Each tag must be 1–2 words. NEVER a sentence or a phrase.\n"
        "- Each tag must be at most 24 characters long.\n"
        "- Use lowercase except for proper nouns.\n"
        "- Include both broad terms and specific phrases.\n"
        "- Avoid duplicates and near-duplicates.\n"
        "- When parent tags are listed above, feel free to reuse the most "
        "relevant ones so the promo is discoverable alongside the parent.\n\n"
        "Return ONLY the comma-separated list."
    ),
    variables=(
        "title", "description", "transcript", "transcript_truncated",
        "parent_url", "parent_title", "parent_description", "parent_tags",
        "parent_context_block",
    ),
    system=(
        "You return ONLY a comma-separated list of tags, no preamble. "
        "Each tag is 1–2 words and at most 24 characters."
    ),
)

SEED_DESCRIPTION_FROM_FRAMES_PROMPT = SeedPrompt(
    key="description_from_frames_prompt",
    name="Description from keyframes (vision)",
    body=(
        "{{channel_name_block}}"
        "Title: {{title}}\n"
        "{{parent_context_block??}}\n\n"
        "Below are keyframes sampled in order from a short YouTube video.\n"
        "Write a YouTube SEO description (3-5 short paragraphs) that "
        "describes what happens in the video and would help viewers find "
        "it via search. Open with a strong hook in the first sentence — "
        "that's the only line shown in YouTube's collapsed description. "
        "Do not invent dialogue or audio; describe only what's visible. "
        "Do not use the literal characters '<' or '>' anywhere — "
        "YouTube rejects descriptions containing them. When referencing "
        "code symbols, write them in backticks or quotes instead. "
        "Do not output any preamble, tags list, or markdown headings — "
        "just the description text."
        "{{extra_instructions_block}}"
    ),
    variables=(
        "title", "channel_name", "channel_name_block",
        "extra_instructions", "extra_instructions_block",
        "parent_url", "parent_title", "parent_description", "parent_tags",
        "parent_context_block",
    ),
    system=None,
)

SEED_TAGS_FROM_FRAMES_PROMPT = SeedPrompt(
    key="tags_from_frames_prompt",
    name="Tags from keyframes (vision)",
    body=(
        "Title: {{title}}\n"
        "Description: {{description_or_none}}\n"
        "{{parent_context_block??}}\n\n"
        "Below are keyframes from the video. Generate 8-12 YouTube search "
        "tags as a comma-separated list. Each tag MUST be 1–2 words and at "
        "most 24 characters long — never a sentence or phrase. Lowercase, "
        "no quotes, no '#'. When parent tags are listed above, feel free "
        "to reuse the most relevant ones so the promo is discoverable "
        "alongside the parent. Return ONLY the comma-separated tags."
    ),
    variables=(
        "title", "description", "description_or_none",
        "parent_url", "parent_title", "parent_description", "parent_tags",
        "parent_context_block",
    ),
    system=(
        "You return ONLY a comma-separated list of tags, no preamble. "
        "Each tag is 1–2 words and at most 24 characters."
    ),
)

SEED_TITLE_FROM_FILENAME_PROMPT = SeedPrompt(
    key="title_from_filename_prompt",
    name="Title from filename (promo upload)",
    body=(
        "Generate a concise, SEO-friendly YouTube title for this promo clip.\n\n"
        "Filename: {{filename}}\n"
        "{{parent_context_block??}}\n\n"
        "Rules:\n"
        "- ≤ 70 characters.\n"
        "- Title case.\n"
        "- No quotes, no preamble, no trailing punctuation.\n"
        "- Strip out recording-software prefixes (riverside_, recording_, "
        "screen_recording_, untitled_, etc.) and file extensions.\n"
        "- When parent title is provided, the title should read naturally "
        "as a clip from that parent — not a copy of the parent's title.\n\n"
        "Return ONLY the title text."
    ),
    variables=(
        "filename",
        "parent_url", "parent_title", "parent_description", "parent_tags",
        "parent_context_block",
    ),
    system=(
        "You return ONLY a single YouTube title under 70 characters. "
        "No quotes, no preamble, no explanation."
    ),
)


SEED_SHORTEN_POST_PROMPT = SeedPrompt(
    key="shorten_post_prompt",
    name="Shorten a social post",
    body=(
        "Shorten this social post to at most {{target_chars}} characters "
        "without losing its meaning, and keep every URL/link in it exactly "
        "as written:\n\n{{post_text}}"
    ),
    variables=("target_chars", "post_text"),
    system=(
        "You rewrite social media posts to be shorter. Return ONLY the "
        "shortened post text — no quotes, no preamble, no explanation. "
        "Preserve every URL/link exactly. Keep the original meaning and tone."
    ),
)

# System-only seed: ``body`` is unused (the body of an ``{{ai: …}}`` block
# in a template *is* the user-message prompt). The UI hides the body
# textarea for this key and only exposes the system editor.
SEED_AI_BLOCK_DEFAULT_SYSTEM_PROMPT = SeedPrompt(
    key="ai_block_default_system_prompt",
    name="Default system prompt for {{ai: …}} blocks",
    body="",
    variables=(),
    system=(
        "You are a social media copywriter. Return ONLY the requested text, "
        "no preamble, no quotes, no explanation. "
        "All URLs and links must include 'https://' at the beginning. "
        "Do not use markdown link syntax — write URLs as plain text. "
        "None of the supported platforms (X, Bluesky, Mastodon, LinkedIn, "
        "Threads) reliably render markdown."
    ),
)

# Insertion order here is the order the Project Settings page renders
# the prompt cards. Group related concerns: shorten (tiny utility) →
# description (transcript-driven, then vision) → tags (metadata-driven,
# then vision) → default system prompt (catch-all, last because it
# --- Generate-from-source clip proposals ----------------------------------
# One per kind. The clipper service prepends the parent's SRT transcript as
# a separate cached message block, so the body here is the per-kind tail
# (instructions + parent context + existing-ranges + optional crop block).
#
# Output is structured: Claude is forced to respond via the
# ``propose_clips`` tool; the body says so explicitly to keep behavior
# stable if the prompt is edited.

_CLIP_PROPOSAL_VARIABLES = (
    "parent_title",
    "parent_duration_human",
    # The full SRT transcript of the parent video. Placed inline in the
    # rendered body where the author writes ``{{parent_transcript}}``
    # (or ``{{transcript}}`` — both are recognised aliases). The
    # renderer leaves the literal placeholder in the output (because
    # the variable is intentionally not in the substitution dict — it's
    # a system-managed input), and the caller splits there so the SRT
    # lands in its own cache-controlled block. Authors who omit the
    # placeholder still get the transcript prepended as a separate
    # block (backwards-compat fallback in ``clipper`` itself).
    "parent_transcript",
    "transcript",
    "existing_ranges_block",
    "crop_constraints",
    # Per-kind length bounds — sourced from clipper._PER_KIND_BOUNDS so
    # editing the prompt template can't accidentally drift the numbers
    # away from what the server-side validator enforces.
    "min_seconds",
    "max_seconds",
    # Output cap — sourced from clipper._OUTPUT_CAP_PER_KIND so a future
    # cap change updates the prompt instruction at the same time.
    "max_proposals",
)

SEED_CLIP_PROPOSALS_HOOK_PROMPT = SeedPrompt(
    key="promo_clip_proposals_hook",
    name="Promo clip proposals — Hook",
    body=(
        "Parent video: {{parent_title}}\n"
        "Duration: {{parent_duration_human}}\n\n"
        "SRT transcript of the parent video — use the timestamps as the "
        "anchor points for every range you propose:\n"
        "{{parent_transcript}}\n\n"
        "You are looking for HOOKS — standalone clips that grab attention in "
        "under 30 seconds. Each proposal must:\n"
        "- Be between {{min_seconds}} and {{max_seconds}} seconds long.\n"
        "- Have a self-contained payoff inside the clip itself "
        "(no cliffhangers, no 'wait for it' that pays off after the cut).\n"
        "- Snap to natural sentence boundaries visible in the transcript "
        "timestamps — never start or end mid-word or mid-sentence.\n"
        "- Stand alone without context from elsewhere in the video.\n\n"
        "{{existing_ranges_block??}}"
        "{{crop_constraints??}}"
        "\nPropose up to {{max_proposals}} hooks that genuinely stand alone. Returning fewer "
        "(or zero) is much better than padding with mediocre ones.\n\n"
        "For each proposal:\n"
        "- start_seconds and end_seconds: exact timestamps from the "
        "transcript above; do not round.\n"
        "- title: a punchy 4-8 word working title for the hook.\n"
        "- reason: one sentence on what makes this work as a hook.\n\n"
        "Return your proposals via the propose_clips tool."
    ),
    variables=_CLIP_PROPOSAL_VARIABLES,
    system=(
        "You find compelling hook clips inside a longer video. A hook is a "
        "short standalone clip that grabs attention immediately: a strong "
        "cold open, a surprising statement, a one-line story or punchline "
        "that pays off entirely within the clip. You output your proposals "
        "via the propose_clips tool, never as prose."
    ),
)

SEED_CLIP_PROPOSALS_SHORT_PROMPT = SeedPrompt(
    key="promo_clip_proposals_short",
    name="Promo clip proposals — Short",
    body=(
        "Parent video: {{parent_title}}\n"
        "Duration: {{parent_duration_human}}\n\n"
        "SRT transcript of the parent video — use the timestamps as the "
        "anchor points for every range you propose:\n"
        "{{parent_transcript}}\n\n"
        "You are looking for SHORTS — complete bits with a setup→payoff arc "
        "that fit between {{min_seconds}} and {{max_seconds}} seconds. "
        "Each proposal must:\n"
        "- Be between {{min_seconds}} and {{max_seconds}} seconds long.\n"
        "- Contain a complete idea, story, or answer to a question.\n"
        "- Have enough setup that a viewer landing cold can follow it.\n"
        "- Snap to natural sentence boundaries in the transcript timestamps.\n"
        "- Stand alone — no required context from elsewhere in the video.\n\n"
        "{{existing_ranges_block??}}"
        "{{crop_constraints??}}"
        "\nPropose up to {{max_proposals}} shorts that genuinely have a complete arc. "
        "Returning fewer (or zero) is much better than padding.\n\n"
        "For each proposal:\n"
        "- start_seconds and end_seconds: exact timestamps from the "
        "transcript above; do not round.\n"
        "- title: a 4-8 word working title for the short.\n"
        "- reason: one sentence describing the setup→payoff arc.\n\n"
        "Return your proposals via the propose_clips tool."
    ),
    variables=_CLIP_PROPOSAL_VARIABLES,
    system=(
        "You find compelling short clips inside a longer video. A short is a "
        "clip with a complete setup→payoff arc — a bit, a story, an answer "
        "to a question — that stands on its own. The exact length band is "
        "given per-call in the user message. You output your proposals via "
        "the propose_clips tool, never as prose."
    ),
)

SEED_CLIP_CROP_REFINEMENT_PROMPT = SeedPrompt(
    key="promo_clip_crop_refinement",
    name="Promo clip crop refinement",
    body=(
        "You're judging whether a proposed clip range will crop well to 9:16 "
        "vertical by taking a column from the source frame. The frames "
        "below are sampled evenly across the proposed range.\n\n"
        "Decide one of:\n"
        "* 'centered' — the subject sits in the center third throughout; a "
        "plain center crop works.\n"
        "* 'off_center' — the subject is consistently in one side third (not "
        "moving back and forth). Estimate ``x_shift_normalized`` in "
        "[-1.0, 1.0] where -1.0 fully shifts the crop column left and +1.0 "
        "fully shifts it right. Be conservative — small offsets should map "
        "to small shifts.\n"
        "* 'drift' — the subject moves between thirds across the frames; "
        "no single crop window will follow them. Return 0 for shift.\n"
        "* 'multi_face' — multiple distinct subjects in different thirds; "
        "no single crop holds them all. Return 0 for shift.\n"
        "* 'no_face' — no person/subject is clearly visible (b-roll, "
        "graphics, screen recording). Return 0 for shift.\n\n"
        "Return your judgment via the assess_crop tool. Be conservative "
        "with both shift magnitude and 'off_center' classification: a "
        "false positive forces a bad reframe, a false negative just gives "
        "the user a center crop they were probably going to accept."
    ),
    variables=(),
    system=(
        "You assess whether a video clip frames its subject well for a 9:16 "
        "vertical crop, and how much to shift the crop window if needed. "
        "You output your assessment via the assess_crop tool, never as prose."
    ),
)

SEED_CLIP_PROPOSALS_SEGMENT_PROMPT = SeedPrompt(
    key="promo_clip_proposals_segment",
    name="Promo clip proposals — Segment",
    body=(
        "Parent video: {{parent_title}}\n"
        "Duration: {{parent_duration_human}}\n\n"
        "SRT transcript of the parent video — use the timestamps as the "
        "anchor points for every range you propose:\n"
        "{{parent_transcript}}\n\n"
        "You are looking for SEGMENTS — coherent topic blocks that pull a "
        "whole sub-topic out of the parent. Each proposal must:\n"
        "- Be at least {{min_seconds}} seconds long. No fixed maximum — as "
        "long as the topic naturally runs.\n"
        "- Cover one coherent subject, with the discussion ending naturally "
        "rather than mid-thought.\n"
        "- Snap to natural sentence and topic boundaries in the transcript.\n"
        "- Stand on its own as a discussion of that subject.\n\n"
        "{{existing_ranges_block??}}"
        "{{crop_constraints??}}"
        "\nPropose up to {{max_proposals}} segments that pull out coherent topic blocks. "
        "Returning fewer (or zero) is much better than padding with weakly-"
        "bounded slices.\n\n"
        "For each proposal:\n"
        "- start_seconds and end_seconds: exact timestamps from the "
        "transcript above; do not round.\n"
        "- title: a 4-8 word working title describing the topic.\n"
        "- reason: one sentence on why this is a coherent standalone segment.\n\n"
        "Return your proposals via the propose_clips tool."
    ),
    variables=_CLIP_PROPOSAL_VARIABLES,
    system=(
        "You find coherent topic segments inside a longer video. A segment is "
        "a self-contained discussion of one subject — there is no length "
        "cap, but the segment must end naturally rather than mid-thought. "
        "You output your proposals via the propose_clips tool, never as prose."
    ),
)

# applies to every {{ai: ...}} block elsewhere).
_SEEDS_BY_KEY: dict[str, SeedPrompt] = {
    SEED_SHORTEN_POST_PROMPT.key: SEED_SHORTEN_POST_PROMPT,
    SEED_TITLE_FROM_FILENAME_PROMPT.key: SEED_TITLE_FROM_FILENAME_PROMPT,
    SEED_DESCRIPTION_FROM_TRANSCRIPT_PROMPT.key: SEED_DESCRIPTION_FROM_TRANSCRIPT_PROMPT,
    SEED_DESCRIPTION_FROM_FRAMES_PROMPT.key: SEED_DESCRIPTION_FROM_FRAMES_PROMPT,
    SEED_TAGS_FROM_METADATA_PROMPT.key: SEED_TAGS_FROM_METADATA_PROMPT,
    SEED_TAGS_FROM_FRAMES_PROMPT.key: SEED_TAGS_FROM_FRAMES_PROMPT,
    SEED_AI_BLOCK_DEFAULT_SYSTEM_PROMPT.key: SEED_AI_BLOCK_DEFAULT_SYSTEM_PROMPT,
    SEED_CLIP_PROPOSALS_HOOK_PROMPT.key: SEED_CLIP_PROPOSALS_HOOK_PROMPT,
    SEED_CLIP_PROPOSALS_SHORT_PROMPT.key: SEED_CLIP_PROPOSALS_SHORT_PROMPT,
    SEED_CLIP_PROPOSALS_SEGMENT_PROMPT.key: SEED_CLIP_PROPOSALS_SEGMENT_PROMPT,
    SEED_CLIP_CROP_REFINEMENT_PROMPT.key: SEED_CLIP_CROP_REFINEMENT_PROMPT,
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
        "SELECT id, project_id, key, name, body, system_body, applies_to, updated_at "
        "FROM prompt_templates WHERE project_id = ? ORDER BY key",
        (project_id,),
    )
    return [_row_to_dict(r) for r in rows]


async def get_prompt_template(key: str, *, project_id: int) -> dict | None:
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, project_id, key, name, body, system_body, applies_to, updated_at "
        "FROM prompt_templates WHERE project_id = ? AND key = ?",
        (project_id, key),
    )
    return _row_to_dict(rows[0]) if rows else None


async def get_prompt_with_fallback(key: str, *, project_id: int) -> dict:
    """Return ``{"body": str, "system": str | None}`` for ``key``,
    merging the saved row with the seed.

    Resolution order per field:

    1. The user-edited row (if any).
    2. The seed default.

    The two fields fall back independently — a user who has saved a custom
    body but left the system prompt untouched gets their body + the seed
    system. Raises ``KeyError`` for unknown keys.
    """
    record = await get_prompt_template(key, project_id=project_id)
    seed = _SEEDS_BY_KEY.get(key)
    if record is None and seed is None:
        raise KeyError(f"No prompt template for key '{key}'")

    body = (record or {}).get("body") if record is not None else None
    if body is None:
        body = seed.body if seed is not None else ""

    if record is not None and record.get("system_body") is not None:
        system: str | None = record["system_body"]
    elif seed is not None:
        system = seed.system
    else:
        system = None

    return {"body": body, "system": system}


async def get_prompt_body_with_fallback(key: str, *, project_id: int) -> str:
    """Return the prompt body for ``key`` from the DB, falling back to the seed.

    Routes call this so a missing row doesn't break generation in existing
    installs that haven't applied the migration yet. Kept as a thin wrapper
    over ``get_prompt_with_fallback`` for call sites that don't need the
    system prompt.
    """
    record = await get_prompt_with_fallback(key, project_id=project_id)
    return record["body"]


async def upsert_prompt_template(
    *,
    key: str,
    name: str,
    body: str,
    project_id: int,
    system: str | None = None,
    applies_to: Iterable[str] = ("hook", "short", "segment", "video"),
) -> int:
    """Insert or update a prompt template row.

    ``system=None`` writes a SQL NULL for the system column, which the
    fallback resolver treats as "use the seed default". Pass an empty
    string only when you want to suppress the system prompt entirely
    (which the UI offers as "Clear system prompt" — distinct from "reset").
    """
    db = await get_db()
    cursor = await db.execute(
        """
        INSERT INTO prompt_templates (
            project_id, key, name, body, system_body, applies_to
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(project_id, key) DO UPDATE SET
            name        = excluded.name,
            body        = excluded.body,
            system_body = excluded.system_body,
            applies_to  = excluded.applies_to,
            updated_at  = datetime('now')
        """,
        (project_id, key, name, body, system, json.dumps(list(applies_to))),
    )
    await db.commit()
    return int(cursor.lastrowid or 0)
