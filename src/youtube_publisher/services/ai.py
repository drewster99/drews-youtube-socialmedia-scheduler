"""AI service — Claude API for generating descriptions and social posts."""

from __future__ import annotations

import re

import anthropic

from youtube_publisher.config import ANTHROPIC_MODEL, get_anthropic_api_key


def get_client() -> anthropic.Anthropic:
    """Get an Anthropic client."""
    api_key = get_anthropic_api_key()
    if not api_key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY not configured. Set it in Settings or in your .env file."
        )
    return anthropic.Anthropic(api_key=api_key)


def generate_seo_description(
    title: str,
    transcript: str,
    channel_name: str = "",
    extra_instructions: str = "",
) -> str:
    """Generate an SEO-friendly video description from a transcript."""
    client = get_client()

    prompt = f"""Generate an SEO-friendly YouTube video description based on the following.

Video title: {title}
{"Channel: " + channel_name if channel_name else ""}

Transcript:
{transcript[:8000]}

Instructions:
- Write a compelling description that summarizes the video content
- Include relevant keywords naturally
- Use short paragraphs for readability
- Include timestamps if the transcript suggests distinct sections
- Do NOT include links (those will be added separately)
- Do NOT include hashtags (those will be added separately)
- Keep it under 2000 characters
{extra_instructions}

Return ONLY the description text, no preamble."""

    message = client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )

    return message.content[0].text.strip()


def render_ai_blocks(text: str, variables: dict[str, str] | None = None) -> str:
    """Process {{ai: ...}} blocks in a template string.

    Variables inside AI blocks should already be substituted before calling this.
    """
    client = get_client()

    def replace_ai_block(match: re.Match) -> str:
        prompt = match.group(1).strip()
        message = client.messages.create(
            model=ANTHROPIC_MODEL,
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
        "short":   "Tier: Short (50-75s) — punchy, self-contained highlight.",
        "segment": "Tier: Segment (3-5 min) — deeper dive, invite viewers to explore.",
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
        model=ANTHROPIC_MODEL,
        max_tokens=512,
        messages=[{"role": "user", "content": prompt}],
        system="You are a social media copywriter. Return ONLY the post text, no preamble.",
    )

    return message.content[0].text.strip()
