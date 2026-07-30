"""Prompt template CRUD + render-with-fallback.

Both prompt templates and social templates flow through the same renderer
in ``services/templates.py``. The variable set passed to ``render_template``
is generous on purpose: any LLM prompt can pull in transcript, project name,
channel name, etc., regardless of the calling context.

Each seed defines a ``body`` and optionally a ``system`` prompt.
``system=None`` means "send no system prompt", which is the right default
for the description seeds — they instruct Claude entirely through the
user-message body. Seeds that *do* declare a system prompt expose it in
the Project Settings UI as a second textarea.

One exception to "body = user prompt": the ``promo_clip_proposals_*``
seeds are editorial fragments, not whole prompts. ``clipper`` splices each
one into the middle of a system prompt it builds itself, so the tool
contract around it can't be edited away. See the comment above those
seeds.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable

from yt_scheduler.database import get_db, write_transaction


class EmptyPromptBodyError(ValueError):
    """Raised when a saved prompt row has an empty/whitespace-only body.

    A blank saved body means the user cleared the editor. We refuse to fall
    back to the seed silently — that would let a blank prompt quietly drive
    generation. Surfacing the error lets the user fix their prompt instead of
    getting weird/empty output.
    """

    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__(
            f"Prompt '{key}' has an empty body. Restore the default or enter "
            "prompt text in Project Settings — generation won't run on a blank "
            "prompt."
        )


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
# Shared by the description seeds: everything build_prompt_variables +
# the generator's explicit arguments supply to the render.
_DESCRIPTION_PROMPT_VARIABLES: tuple[str, ...] = (
    "title", "channel_name", "channel_name_block",
    "transcript", "transcript_truncated",
    "transcript_srt", "transcript_srt_truncated",
    "extra_instructions",
    "url", "episode_url", "project_url",
    "parent_url", "parent_title", "parent_description", "parent_tags",
    "parent_context_block",
)

SEED_DESCRIPTION_FROM_TRANSCRIPT_PROMPT = SeedPrompt(
    key="description_from_transcript_prompt",
    name="Description from transcript",
    body=(
        "Generate an SEO-friendly YouTube video description.\n\n"
        "Video title: {{title}}\n"
        "{{channel_name_block}}\n"
        "Transcript (SRT — cue timestamps preserved; may be truncated):\n"
        "{{transcript_srt_truncated}}\n"
        "{{parent_context_block??}}\n\n"
        "Instructions:\n"
        "- Write a compelling description that summarizes the video content\n"
        "- Include relevant keywords naturally\n"
        "- Use short paragraphs for readability\n"
        "- If the transcript suggests distinct sections, include a chapter "
        "list: one 'M:SS Title' line per section, first line 0:00, "
        "converting the SRT cue times (HH:MM:SS,mmm) to M:SS. If the "
        "transcript looks truncated, only list chapters for the part you "
        "can see.\n"
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
    variables=_DESCRIPTION_PROMPT_VARIABLES,
    # No system prompt — instructions live in the user message body.
    system=None,
)

SEED_DESCRIPTION_FROM_TRANSCRIPT_PROMPT_PROMO = SeedPrompt(
    key="description_from_transcript_prompt_promo",
    name="Description from transcript (promo clips)",
    body=(
        "Generate a YouTube description for a short promo clip cut from a "
        "longer parent video.\n\n"
        "Clip title: {{title}}\n"
        "{{channel_name_block}}\n"
        "Clip transcript:\n{{transcript_truncated}}\n"
        "{{parent_context_block??}}\n\n"
        "Instructions:\n"
        "- Write 1-3 short, punchy paragraphs that hook the viewer and "
        "make them want to watch the full video.\n"
        "- Include relevant keywords naturally.\n"
        "- Do NOT include hashtags (those will be added separately).\n"
        "- Do NOT use the literal characters '<' or '>' anywhere — "
        "YouTube rejects descriptions containing them. When referencing "
        "code symbols (e.g. UIView, NSObject) write them in backticks "
        "or quotes instead.\n"
        "- Keep the prose under 1000 characters.\n"
        "{{extra_instructions}}\n"
        "{{#episode_url}}\n"
        "End the description with this line exactly as written, on its "
        "own line:\n"
        "Full episode: {{episode_url}}\n"
        "{{/episode_url}}\n\n"
        "Return ONLY the description text, no preamble."
    ),
    variables=_DESCRIPTION_PROMPT_VARIABLES,
    system=None,
)

SEED_TAGS_FROM_METADATA_PROMPT = SeedPrompt(
    key="tags_from_metadata_prompt",
    name="Tags from metadata",
    body=(
        "Generate 8–15 YouTube tags that maximise discoverability for this video.\n\n"
        "Title: {{title}}\n"
        "Description: {{description}}\n"
        "Transcript (truncated): {{transcript_truncated}}\n"
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
        "- Do NOT use the literal characters '<' or '>' anywhere — YouTube "
        "rejects titles containing them. When referencing code symbols "
        "(e.g. UIView, NSObject) write them in backticks or quotes instead.\n"
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
#
# These are EDITORIAL blocks, not whole prompts. ``clipper`` builds the
# system prompt for a proposal call and splices the body below into the
# middle of it; the surrounding sections — the transcript's line format,
# the index-only rule, the tool contract — stay in code so a prompt edit
# can never break the output format or desync from the check tool.
#
# Write plain prose with the two headings shown. ``{{kind}}`` is the only
# variable; any other name raises at generate time rather than rendering
# empty.

_CLIP_EDITORIAL_VARIABLES = ("kind",)

_CLIP_SHARED_BULLETS = (
    "- Self-contained: it makes sense with no other context. Starts and "
    "ends on a complete thought.\n"
    "- AUDIO ONLY: never pick a clip that depends on something visual (a "
    "chart, code on screen, a demo, \"look at this\", \"right here\"). If the "
    "words only make sense with a picture, skip it."
)

SEED_CLIP_PROPOSALS_HOOK_PROMPT = SeedPrompt(
    key="promo_clip_proposals_hook",
    name="Promo clip proposals — Hook (editorial)",
    body=(
        "## What makes a good hook\n"
        "- A hook is a single surprising, opinionated, useful, or candid "
        "moment with an immediate payoff — one clear point, no setup. Since "
        "hooks are very short, include only minimal lead-in to the main point "
        "or punchline: a couple of seconds at most. DO include reactions "
        "afterward, but very little beyond that. The topic should begin right "
        "away.\n"
        f"{_CLIP_SHARED_BULLETS}\n\n"
        "## Title\n"
        "- The title IS the hook: 3-4 words ideally, but max 8 words, punchy "
        "and a little opinionated/divisive or questioning (never clickbait "
        "like \"You won\u2019t believe\"), clearly supported by or discussed in "
        "the content; state the point, keep it short — few words, short words."
    ),
    variables=_CLIP_EDITORIAL_VARIABLES,
)

SEED_CLIP_PROPOSALS_SHORT_PROMPT = SeedPrompt(
    key="promo_clip_proposals_short",
    name="Promo clip proposals — Short (editorial)",
    body=(
        "## What makes a good short\n"
        "- A short is ONE complete mini-story or explanation: a brief setup "
        "and a satisfying payoff, understandable on its own — one coherent "
        "idea, not a grab-bag. Include minimal lead-in — a few seconds at "
        "most. DO include reactions afterward, but not much beyond that.\n"
        f"{_CLIP_SHARED_BULLETS}\n\n"
        "## Title\n"
        "- 4-9 words, punchy and clear, opinionated or questioning — but "
        "always supported by or resolved in the content; never clickbait."
    ),
    variables=_CLIP_EDITORIAL_VARIABLES,
)

SEED_CLIP_PROPOSALS_SEGMENT_PROMPT = SeedPrompt(
    key="promo_clip_proposals_segment",
    name="Promo clip proposals — Segment (editorial)",
    body=(
        "## What makes a good segment\n"
        "- A segment is a full, self-contained DISCUSSION of ONE topic, from "
        "where it is introduced to where it wraps up, before the next topic. "
        "These are usually at least several minutes long, but the sentence "
        "that starts the topic should begin within 5 seconds of the start of "
        "your selection.\n"
        f"{_CLIP_SHARED_BULLETS}\n\n"
        "## Title\n"
        "- 5-10 words, clear, descriptive and informative — NOT divisive and "
        "NOT clickbait; name the topic, clear and brief."
    ),
    variables=_CLIP_EDITORIAL_VARIABLES,
)

# applies to every {{ai: ...}} block elsewhere).
_SEEDS_BY_KEY: dict[str, SeedPrompt] = {
    SEED_SHORTEN_POST_PROMPT.key: SEED_SHORTEN_POST_PROMPT,
    SEED_TITLE_FROM_FILENAME_PROMPT.key: SEED_TITLE_FROM_FILENAME_PROMPT,
    SEED_DESCRIPTION_FROM_TRANSCRIPT_PROMPT.key: SEED_DESCRIPTION_FROM_TRANSCRIPT_PROMPT,
    SEED_DESCRIPTION_FROM_TRANSCRIPT_PROMPT_PROMO.key: SEED_DESCRIPTION_FROM_TRANSCRIPT_PROMPT_PROMO,
    SEED_DESCRIPTION_FROM_FRAMES_PROMPT.key: SEED_DESCRIPTION_FROM_FRAMES_PROMPT,
    SEED_TAGS_FROM_METADATA_PROMPT.key: SEED_TAGS_FROM_METADATA_PROMPT,
    SEED_TAGS_FROM_FRAMES_PROMPT.key: SEED_TAGS_FROM_FRAMES_PROMPT,
    SEED_AI_BLOCK_DEFAULT_SYSTEM_PROMPT.key: SEED_AI_BLOCK_DEFAULT_SYSTEM_PROMPT,
    SEED_CLIP_PROPOSALS_HOOK_PROMPT.key: SEED_CLIP_PROPOSALS_HOOK_PROMPT,
    SEED_CLIP_PROPOSALS_SHORT_PROMPT.key: SEED_CLIP_PROPOSALS_SHORT_PROMPT,
    SEED_CLIP_PROPOSALS_SEGMENT_PROMPT.key: SEED_CLIP_PROPOSALS_SEGMENT_PROMPT,
}


class RetiredPromptKey(KeyError):
    """Asked for a prompt key that was deliberately retired."""


# Keys that once shipped a seed and no longer do. Retiring is a declared act,
# not just a deletion: without this, a fresh install raises KeyError while an
# install with a stale saved row silently keeps generating from retired text —
# behaviour that differs per machine. It also makes re-using a name a test
# failure rather than a silent stale-row-beats-new-seed swap.
#
# Saved rows for these keys are NEVER deleted; they are the user's text.
# If a prompt is being RENAMED rather than retired, migrate the row's key
# instead, or the user's customisation is stranded.
_RETIRED_KEYS: dict[str, str] = {
    "promo_clip_crop_refinement":
        "The Claude-vision crop pass was replaced by the on-device Swift "
        "clipcrop (YOLO head-tracking) in 7aa3012.",
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


def _merge_record_and_seed(
    key: str, record: dict | None, seed: SeedPrompt | None
) -> dict:
    """Per-field merge of one saved row with one seed — the body and the
    system prompt fall back independently, so a user who saved a custom
    body but left the system prompt untouched gets their body + the seed
    system."""
    seed_body = seed.body if seed is not None else ""
    if record is not None:
        body = record.get("body")
        # A saved row with a blank body means the user cleared the editor.
        # Do NOT silently fall back to the seed — fail loudly so a blank prompt
        # can't quietly drive generation. The exception is the system-only seeds
        # whose seed body is itself intentionally empty (e.g. the default
        # {{ai:}} system prompt): a blank body there matches the seed and is
        # fine. (A *missing* row, below, legitimately uses the shipped seed.)
        if body is None or not body.strip():
            if seed_body.strip():
                raise EmptyPromptBodyError(key)
            body = seed_body
    else:
        body = seed_body

    if record is not None and record.get("system_body") is not None:
        system: str | None = record["system_body"]
    elif seed is not None:
        system = seed.system
    else:
        system = None

    return {"body": body, "system": system}


async def get_prompt_with_fallback(
    key: str, *, project_id: int, prefer_promo_variant: bool = False
) -> dict:
    """Return ``{"body": str, "system": str | None}`` for ``key``.

    With ``prefer_promo_variant=False`` (default), resolution per field is:

    1. The user-edited row (if any).
    2. The seed default.

    With ``prefer_promo_variant=True`` (the video being generated for is a
    promo child), the ``<key>_promo`` variant is a *distinct prompt* that
    wins outright when it exists in any form:

    1. Saved ``<key>_promo`` row (paired with the promo seed for
       field-level fallback).
    2. The ``<key>_promo`` seed.
    3. Saved ``<key>`` row (paired with the base seed).
    4. The ``<key>`` seed.

    The promo seed deliberately beats a saved base row: migration 006
    seeds base ROWS into ``prompt_templates`` on every install, so "a base
    row exists" cannot distinguish a user customisation from install
    defaults. The resulting mental model is simple — promos use the promo
    prompt, everything else uses the base prompt — and each is separately
    editable in Project Settings.

    Raises ``KeyError`` for unknown keys.
    """
    # Checked before the promo-variant branch so nothing can route around it.
    if key in _RETIRED_KEYS:
        raise RetiredPromptKey(
            f"Prompt key '{key}' was retired: {_RETIRED_KEYS[key]}"
        )
    base_seed = _SEEDS_BY_KEY.get(key)

    if prefer_promo_variant:
        promo_key = f"{key}_promo"
        promo_seed = _SEEDS_BY_KEY.get(promo_key)
        promo_record = await get_prompt_template(promo_key, project_id=project_id)
        if promo_record is not None:
            return _merge_record_and_seed(
                promo_key, promo_record, promo_seed or base_seed
            )
        if promo_seed is not None:
            return _merge_record_and_seed(promo_key, None, promo_seed)
        base_record = await get_prompt_template(key, project_id=project_id)
        if base_record is not None:
            return _merge_record_and_seed(key, base_record, base_seed)
        if base_seed is None:
            raise KeyError(f"No prompt template for key '{key}'")
        return _merge_record_and_seed(key, None, base_seed)

    record = await get_prompt_template(key, project_id=project_id)
    if record is None and base_seed is None:
        raise KeyError(f"No prompt template for key '{key}'")
    return _merge_record_and_seed(key, record, base_seed)


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
    async with write_transaction() as db:
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
    return int(cursor.lastrowid or 0)
