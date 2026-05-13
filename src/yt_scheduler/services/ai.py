"""AI service — Claude API for generating descriptions and social posts.

Prompt bodies for the description / tags flows are stored in the
``prompt_templates`` table so they can be edited from the UI. The seed body
ships with the migration; ``services/prompts`` falls back to it when a row is
missing (e.g. install hasn't applied the migration yet).
"""

from __future__ import annotations

import asyncio

import anthropic

from yt_scheduler.config import ANTHROPIC_MODEL, get_anthropic_api_key


def get_client() -> anthropic.Anthropic:
    """Get an Anthropic client."""
    api_key = get_anthropic_api_key()
    if not api_key:
        raise RuntimeError("Anthropic API key not configured. Set it in Settings.")
    return anthropic.Anthropic(api_key=api_key)


# Module-level cache for the Anthropic model name. Both async (description /
# tags) and sync (template ``{{ai: ...}}`` blocks) code paths read through
# this so the user's Settings → Model selection applies everywhere. Settings
# save handler calls ``invalidate_model_cache()``.
_active_model_cache: str | None = None


def _resolve_model_sync() -> str:
    """Synchronous read of the active model with cache + DB fallback to env var.

    Uses a short-lived sqlite3 connection to dodge the async aiosqlite layer.
    Concurrent reads are safe in default journal mode; the catch-all silently
    falls back to ``ANTHROPIC_MODEL`` if the DB is unavailable or busy.
    """
    global _active_model_cache
    if _active_model_cache is not None:
        return _active_model_cache
    import sqlite3

    from yt_scheduler.config import DB_PATH

    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            row = conn.execute(
                "SELECT value FROM settings WHERE key = 'anthropic_model'"
            ).fetchone()
        if row and row[0]:
            _active_model_cache = row[0]
            return _active_model_cache
    except Exception:
        pass
    _active_model_cache = ANTHROPIC_MODEL
    return _active_model_cache


def invalidate_model_cache() -> None:
    """Clear the cached model name so the next call re-reads from the DB."""
    global _active_model_cache
    _active_model_cache = None


async def _resolve_model() -> str:
    """Async wrapper that delegates to the sync resolver."""
    return _resolve_model_sync()


def _render_template_body(body: str, variables: dict[str, str]) -> str:
    """Substitute placeholders in a prompt body using the unified renderer
    (`services/templates.render`). Prompt-template authors who want a
    silent fallback for an optional variable should write
    ``{{user_message??}}`` (or ``{{user_message??default text}}``) so the
    fallback is explicit at the template site rather than implicit in
    the renderer."""
    from yt_scheduler.services import templates  # avoid import cycle at module load

    return templates.render(body, variables)


# Tag cleaning shared by generate_tags_from_metadata and
# generate_tags_from_frames. The LLM is told (in both seed body + system)
# that tags must be 1–2 words and ≤24 characters, but the model
# occasionally slips out a longer phrase ("a hands-on tutorial about X")
# or repeats an existing tag with a synonym; we enforce both invariants
# server-side so a misbehaving model can't push garbage into YouTube's
# tag field. ``max_tags`` caps the final list (seed says 8–15, so 15 is
# generous).
_TAG_MAX_LEN = 24
_TAG_MAX_WORDS = 2
_TAG_MAX_COUNT = 15


def _clean_tags(raw: str) -> list[str]:
    """Parse the LLM's comma-separated tag list, lowercasing, stripping
    quotes/whitespace, then enforcing length and word-count caps. Drops
    anything that violates the rules (rather than truncating — a
    truncated tag is usually a worse search term than the next-best
    short one)."""
    cleaned: list[str] = []
    seen: set[str] = set()
    for piece in raw.split(","):
        t = piece.strip().strip('"\'').lower()
        if not t:
            continue
        if len(t) > _TAG_MAX_LEN:
            continue
        if len(t.split()) > _TAG_MAX_WORDS:
            continue
        if t in seen:
            continue
        seen.add(t)
        cleaned.append(t)
        if len(cleaned) >= _TAG_MAX_COUNT:
            break
    return cleaned


async def generate_seo_description(
    title: str,
    transcript: str,
    *,
    project_id: int,
    channel_name: str = "",
    extra_instructions: str = "",
) -> str:
    """Generate an SEO-friendly video description from a transcript.

    Prompt body comes from the ``description_from_transcript_prompt`` row
    in ``prompt_templates``; the seed default kicks in when the row is
    missing. No system prompt — instructions live in the user message.
    """
    from yt_scheduler.services import prompts as prompt_service
    from yt_scheduler.services.transcripts import srt_to_plain_text

    # Transcripts are stored canonically as SRT (preserves timestamps for
    # YouTube round-trip). For Claude we strip to plain text so cue numbers
    # and timestamp lines don't eat the context budget.
    plain = srt_to_plain_text(transcript) if transcript else ""

    prompt = await prompt_service.get_prompt_with_fallback(
        "description_from_transcript_prompt", project_id=project_id
    )
    rendered = _render_template_body(
        prompt["body"],
        {
            "title": title,
            "channel_name": channel_name,
            "channel_name_block": f"Channel: {channel_name}" if channel_name else "",
            "transcript": plain,
            "transcript_truncated": plain[:8000],
            "extra_instructions": extra_instructions,
        },
    )

    kwargs: dict[str, object] = {
        "model": await _resolve_model(),
        "max_tokens": 1024,
        "messages": [{"role": "user", "content": rendered}],
    }
    if prompt["system"]:
        kwargs["system"] = prompt["system"]

    client = get_client()
    message = await asyncio.to_thread(client.messages.create, **kwargs)
    return message.content[0].text.strip()


async def generate_seo_description_from_frames(
    title: str,
    frames: list[bytes],
    *,
    project_id: int,
    channel_name: str = "",
    extra_instructions: str = "",
) -> str:
    """Generate an SEO description from a list of JPEG keyframes.

    Prompt body comes from the ``description_from_frames_prompt`` row in
    ``prompt_templates``; the seed default kicks in when the row is missing.
    Frames are attached after the rendered prompt text in the same user turn.
    """
    import base64
    from yt_scheduler.services import prompts as prompt_service

    if not frames:
        raise ValueError("generate_seo_description_from_frames called with no frames")

    prompt = await prompt_service.get_prompt_with_fallback(
        "description_from_frames_prompt", project_id=project_id
    )
    extra_block = (
        f"\n\nAdditional instructions:\n{extra_instructions}\n"
        if extra_instructions else ""
    )
    instructions = _render_template_body(
        prompt["body"],
        {
            "title": title,
            "channel_name": channel_name,
            "channel_name_block": f"Channel: {channel_name}\n" if channel_name else "",
            "extra_instructions": extra_instructions,
            "extra_instructions_block": extra_block,
        },
    )

    content: list[dict] = [{"type": "text", "text": instructions}]
    for frame in frames:
        content.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/jpeg",
                "data": base64.b64encode(frame).decode("ascii"),
            },
        })

    kwargs: dict[str, object] = {
        "model": await _resolve_model(),
        "max_tokens": 1024,
        "messages": [{"role": "user", "content": content}],
    }
    if prompt["system"]:
        kwargs["system"] = prompt["system"]

    client = get_client()
    message = await asyncio.to_thread(client.messages.create, **kwargs)
    return message.content[0].text.strip()


async def generate_tags_from_frames(
    title: str,
    description: str,
    frames: list[bytes],
    *,
    project_id: int,
) -> list[str]:
    """Generate YouTube tags using the title + (optionally generated)
    description + the keyframes, when there's no transcript to feed
    ``generate_tags_from_metadata``.

    Prompt body and system come from the ``tags_from_frames_prompt`` row
    in ``prompt_templates``; the seed defaults kick in when fields are
    missing.
    """
    import base64
    from yt_scheduler.services import prompts as prompt_service

    if not frames:
        raise ValueError("generate_tags_from_frames called with no frames")

    prompt = await prompt_service.get_prompt_with_fallback(
        "tags_from_frames_prompt", project_id=project_id
    )
    instructions = _render_template_body(
        prompt["body"],
        {
            "title": title,
            "description": description,
            "description_or_none": description or "(none yet)",
        },
    )

    content: list[dict] = [{"type": "text", "text": instructions}]
    for frame in frames:
        content.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/jpeg",
                "data": base64.b64encode(frame).decode("ascii"),
            },
        })

    kwargs: dict[str, object] = {
        "model": await _resolve_model(),
        "max_tokens": 256,
        "messages": [{"role": "user", "content": content}],
    }
    if prompt["system"]:
        kwargs["system"] = prompt["system"]

    client = get_client()
    message = await asyncio.to_thread(client.messages.create, **kwargs)
    return _clean_tags(message.content[0].text.strip())


async def generate_tags_from_metadata(
    title: str,
    description: str,
    transcript: str = "",
    *,
    project_id: int,
) -> list[str]:
    """Generate YouTube tags based on title/description/transcript using the
    user-editable prompt template."""
    from yt_scheduler.services import prompts as prompt_service
    from yt_scheduler.services.transcripts import srt_to_plain_text

    plain = srt_to_plain_text(transcript) if transcript else ""

    prompt = await prompt_service.get_prompt_with_fallback(
        "tags_from_metadata_prompt", project_id=project_id
    )
    rendered = _render_template_body(
        prompt["body"],
        {
            "title": title,
            "description": description,
            "transcript": plain,
            "transcript_truncated": plain[:4000],
        },
    )

    kwargs: dict[str, object] = {
        "model": await _resolve_model(),
        "max_tokens": 256,
        "messages": [{"role": "user", "content": rendered}],
    }
    if prompt["system"]:
        kwargs["system"] = prompt["system"]

    client = get_client()
    message = await asyncio.to_thread(client.messages.create, **kwargs)
    return _clean_tags(message.content[0].text.strip())


# Seed fallback for ``ai_block_default_system_prompt`` — kept here so an
# install whose migrations haven't run yet still has a sensible default for
# bare ``{{ai: …}}`` blocks. The user-editable copy lives in
# ``services/prompts.SEED_AI_BLOCK_DEFAULT_SYSTEM_PROMPT`` and is mirrored
# from there to keep this constant in lockstep.
DEFAULT_AI_SYSTEM_PROMPT = (
    "You are a social media copywriter. Return ONLY the requested text, "
    "no preamble, no quotes, no explanation."
)


def call_ai_block(
    prompt: str,
    *,
    system: str | None,
    model: str | None = None,
    max_tokens: int = 512,
) -> str:
    """One Claude round-trip for a single template ``{{ai: ...}}`` block.

    ``system`` is keyword-only and required — callers must be explicit
    about what system message (if any) to send. ``None`` or empty string
    sends no system prompt. ``model=None`` falls back to
    ``settings.anthropic_model`` (or env). Used as the unified leaf call;
    the walker in `services/templates.py` handles nesting itself.
    """
    client = get_client()
    kwargs: dict[str, object] = {
        "model": model or _resolve_model_sync(),
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system:
        kwargs["system"] = system
    message = client.messages.create(**kwargs)
    return message.content[0].text.strip()
