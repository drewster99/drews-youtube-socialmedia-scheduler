"""AI service — Claude API for generating descriptions and social posts.

Prompt bodies for the description / tags flows are stored in the
``prompt_templates`` table so they can be edited from the UI. The seed body
ships with the migration; ``services/prompts`` falls back to it when a row is
missing (e.g. install hasn't applied the migration yet).
"""

from __future__ import annotations

import asyncio
import re

import anthropic

from yt_scheduler.config import ANTHROPIC_MODEL, get_anthropic_api_key


def get_client() -> anthropic.Anthropic:
    """Get an Anthropic client."""
    api_key = get_anthropic_api_key()
    if not api_key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY not configured. Set it in Settings or in your .env file."
        )
    return anthropic.Anthropic(api_key=api_key)


# Module-level cache for the Anthropic model name. Both async (description /
# tags) and sync (template ``{{ai: ...}}`` blocks, social_post generation)
# code paths read through this so the user's Settings → Model selection
# applies everywhere. Settings save handler calls ``invalidate_model_cache()``.
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
    """Substitute ``{{var}}`` placeholders in a prompt body. Missing variables
    render as empty strings so a prompt referencing ``{{user_message}}`` from
    a context that doesn't supply one doesn't leak the literal token."""
    def repl(match: re.Match) -> str:
        key = match.group(1).strip()
        value = variables.get(key)
        return "" if value is None else str(value)

    return re.sub(r"\{\{(\w+)\}\}", repl, body)


async def generate_seo_description(
    title: str,
    transcript: str,
    channel_name: str = "",
    extra_instructions: str = "",
    project_id: int = 1,
) -> str:
    """Generate an SEO-friendly video description from a transcript.

    Prompt body comes from the ``description_from_transcript`` row in
    ``prompt_templates``; the seed default kicks in when the row is missing.
    """
    from yt_scheduler.services import prompts as prompt_service
    from yt_scheduler.services.transcripts import srt_to_plain_text

    # Transcripts are stored canonically as SRT (preserves timestamps for
    # YouTube round-trip). For Claude we strip to plain text so cue numbers
    # and timestamp lines don't eat the context budget.
    plain = srt_to_plain_text(transcript) if transcript else ""

    body = await prompt_service.get_prompt_body_with_fallback(
        "description_from_transcript", project_id=project_id
    )
    rendered = _render_template_body(
        body,
        {
            "title": title,
            "channel_name": channel_name,
            "channel_name_block": f"Channel: {channel_name}" if channel_name else "",
            "transcript": plain,
            "transcript_truncated": plain[:8000],
            "extra_instructions": extra_instructions,
        },
    )

    client = get_client()
    message = await asyncio.to_thread(
        client.messages.create,
        model=await _resolve_model(),
        max_tokens=1024,
        messages=[{"role": "user", "content": rendered}],
    )
    return message.content[0].text.strip()


async def generate_tags_from_metadata(
    title: str,
    description: str,
    transcript: str = "",
    project_id: int = 1,
) -> list[str]:
    """Generate YouTube tags based on title/description/transcript using the
    user-editable prompt template."""
    from yt_scheduler.services import prompts as prompt_service
    from yt_scheduler.services.transcripts import srt_to_plain_text

    plain = srt_to_plain_text(transcript) if transcript else ""

    body = await prompt_service.get_prompt_body_with_fallback(
        "tags_from_metadata", project_id=project_id
    )
    rendered = _render_template_body(
        body,
        {
            "title": title,
            "description": description,
            "transcript": plain,
            "transcript_truncated": plain[:4000],
        },
    )

    client = get_client()
    message = await asyncio.to_thread(
        client.messages.create,
        model=await _resolve_model(),
        max_tokens=256,
        messages=[{"role": "user", "content": rendered}],
        system="You return ONLY a comma-separated list of tags, no preamble.",
    )
    raw = message.content[0].text.strip()
    return [t.strip().strip('"\'').lower() for t in raw.split(",") if t.strip()]


def render_ai_blocks(text: str, variables: dict[str, str] | None = None) -> str:
    """Process {{ai: ...}} blocks in a template string.

    Variables inside AI blocks should already be substituted before calling this.
    """
    client = get_client()

    def replace_ai_block(match: re.Match) -> str:
        prompt = match.group(1).strip()
        message = client.messages.create(
            model=_resolve_model_sync(),
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
            system="You are a social media copywriter. Return ONLY the requested text, no preamble, no quotes, no explanation.",
        )
        return message.content[0].text.strip()

    return re.sub(r"\{\{ai:\s*(.*?)\s*\}\}", replace_ai_block, text, flags=re.DOTALL)


def generate_social_post(
    platform: str,
    title: str,
    url: str,
    description: str = "",
    tags: list[str] | None = None,
    max_chars: int = 280,
    custom_prompt: str = "",
    tier: str | None = None,
) -> str:
    """Generate a social media post for a specific platform."""
    client = get_client()

    platform_guidance = {
        "twitter": "Keep it punchy and under 280 chars. Include the URL. Use 2-3 hashtags.",
        "bluesky": "Conversational tone. Under 300 chars. Include the URL.",
        "mastodon": "Friendly, community-oriented. Under 500 chars. Use CamelCase hashtags.",
        "linkedin": "Professional but approachable. 2-3 short paragraphs. End with a question.",
        "threads": "Casual, engaging. Under 500 chars.",
    }

    tier_guidance = {
        "hook":    "Tier: Hook (teaser clip under 50s) — tease, don't spoil.",
        "short":   "Tier: Short (50s–3 min) — punchy, self-contained highlight.",
        "segment": "Tier: Segment (3–12 min) — deeper dive, invite viewers to explore.",
        "video":   "Tier: Video (12+ min) — the main piece, full context.",
    }

    prompt = custom_prompt or f"""Write a {platform} post announcing a new YouTube video.

Title: {title}
Description: {description}
URL: {url}
Tags: {', '.join(tags or [])}
{tier_guidance.get((tier or '').lower(), '')}

Platform guidance: {platform_guidance.get(platform, '')}
Max characters: {max_chars}

Return ONLY the post text."""

    message = client.messages.create(
        model=_resolve_model_sync(),
        max_tokens=512,
        messages=[{"role": "user", "content": prompt}],
        system="You are a social media copywriter. Return ONLY the post text, no preamble.",
    )

    return message.content[0].text.strip()
