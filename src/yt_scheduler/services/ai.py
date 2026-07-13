"""AI service — Claude API for generating descriptions and social posts.

Prompt bodies for the description / tags flows are stored in the
``prompt_templates`` table so they can be edited from the UI. The seed body
ships with the migration; ``services/prompts`` falls back to it when a row is
missing (e.g. install hasn't applied the migration yet).
"""

from __future__ import annotations

import asyncio
import logging

import anthropic

from yt_scheduler.config import ANTHROPIC_MODEL, get_anthropic_api_key

logger = logging.getLogger(__name__)


def get_client() -> anthropic.Anthropic:
    """Get an Anthropic client."""
    api_key = get_anthropic_api_key()
    if not api_key:
        raise RuntimeError("Anthropic API key not configured. Set it in Settings.")
    return anthropic.Anthropic(api_key=api_key)


class ClaudeEmptyResponseError(RuntimeError):
    """Raised when a Claude response carried no usable text block.

    Surfaces ``stop_reason`` (so a refusal looks different from a
    tool-use turn looks different from an empty completion) and the
    sequence of content types we did see — invaluable for grepping logs
    when the model starts misbehaving.
    """

    def __init__(self, message) -> None:
        self.stop_reason = getattr(message, "stop_reason", None)
        self.content_types = [
            getattr(b, "type", type(b).__name__) for b in (message.content or [])
        ]
        self.usage = getattr(message, "usage", None)
        if self.stop_reason == "refusal":
            super().__init__(
                f"Claude declined to respond (refusal; blocks={self.content_types})"
            )
        else:
            super().__init__(
                f"Claude returned no text block "
                f"(stop_reason={self.stop_reason!r}, blocks={self.content_types})"
            )


def _extract_text(message) -> str:
    """Return the first text block on a Claude response, or raise.

    Walks ``message.content`` instead of assuming index 0: the SDK can
    legitimately put a ``thinking`` / ``tool_use`` block first, and
    neither has a ``.text`` attribute. ``IndexError`` and
    ``AttributeError`` from blind indexing were a recurring crash
    vector before this helper existed.

    Prefers ``type == "text"`` for the modern SDK, but falls back to
    "any block with a ``.text`` string attribute" so test fixtures that
    use plain mock objects (no ``type`` discriminator) keep working.
    """
    blocks = list(message.content or ())
    # Modern SDK: collect text from ALL type=="text" blocks — a response can
    # legitimately interleave thinking/tool_use blocks with multiple text
    # blocks, and we want the full combined output, not just the first shard.
    texts = [
        getattr(block, "text", "") or ""
        for block in blocks
        if getattr(block, "type", None) == "text"
    ]
    combined = "".join(texts)
    if combined.strip():
        # Return the unstripped value so callers control their own whitespace
        # handling (most callers already do .strip()).
        return combined
    # Fallback: plain mock objects (no type discriminator) used in tests.
    # Skips empty-string .text so mocks that return "" trigger the same error.
    for block in blocks:
        text = getattr(block, "text", None)
        if isinstance(text, str) and text.strip():
            return text
    raise ClaudeEmptyResponseError(message)


# Module-level cache for the Anthropic model name. Both async (description /
# tags) and sync (template ``{{ai: ...}}`` blocks) code paths read through
# this so the user's Settings → Model selection applies everywhere. Settings
# save handler calls ``invalidate_model_cache()``.
_active_model_cache: str | None = None


def _resolve_model_sync() -> str:
    """Synchronous read of the active model for the sync template-render path.

    ``templates.render`` → ``call_ai_block`` runs on a worker thread with no
    event loop, so it can't use the async connection. Uses a short-lived
    sqlite3 connection; WAL (set on the file) lets this read without blocking
    the live writer, and the busy_timeout waits out a writer lock instead of
    failing instantly. A genuinely-absent setting falls back to the configured
    ``ANTHROPIC_MODEL`` default; a real DB error is logged (not silently
    swallowed) before falling back.
    """
    global _active_model_cache
    if _active_model_cache is not None:
        return _active_model_cache
    import sqlite3
    from contextlib import closing

    from yt_scheduler.config import DB_PATH

    try:
        with closing(sqlite3.connect(str(DB_PATH), timeout=5.0)) as conn:
            conn.execute("PRAGMA busy_timeout = 5000")
            row = conn.execute(
                "SELECT value FROM settings WHERE key = 'anthropic_model'"
            ).fetchone()
    except sqlite3.Error as exc:
        logger.warning(
            "Could not read anthropic_model setting (%s); falling back to %s",
            exc,
            ANTHROPIC_MODEL,
        )
        _active_model_cache = ANTHROPIC_MODEL
        return _active_model_cache
    _active_model_cache = row[0] if row and row[0] else ANTHROPIC_MODEL
    return _active_model_cache


def invalidate_model_cache() -> None:
    """Clear the cached model name so the next call re-reads from the DB."""
    global _active_model_cache
    _active_model_cache = None


async def _resolve_model() -> str:
    """Async read of the active model: cache → settings table → env default.

    Reads through the shared async connection rather than opening a second
    sqlite3 connection, so it can't contend with the live writer. A DB error
    propagates (no silent fallback) — only a genuinely-absent setting falls
    back to the configured ``ANTHROPIC_MODEL`` default.
    """
    global _active_model_cache
    if _active_model_cache is not None:
        return _active_model_cache
    from yt_scheduler.database import get_db

    db = await get_db()
    async with db.execute(
        "SELECT value FROM settings WHERE key = 'anthropic_model'"
    ) as cursor:
        row = await cursor.fetchone()
    _active_model_cache = row[0] if row and row[0] else ANTHROPIC_MODEL
    return _active_model_cache


async def _render_template_body(body: str, variables: dict[str, str]) -> str:
    """Substitute placeholders in a prompt body using the unified renderer
    (`services/templates.render`). Prompt-template authors who want a
    silent fallback for an optional variable should write
    ``{{user_message??}}`` (or ``{{user_message??default text}}``) so the
    fallback is explicit at the template site rather than implicit in
    the renderer.

    Async because ``templates.render`` is synchronous but can fire a
    blocking ``{{ai: ...}}`` round-trip — let the worker thread own
    that wait instead of the event loop.
    """
    from yt_scheduler.services import templates  # avoid import cycle at module load

    return await templates.async_render(body, variables)


async def compare_thumbnails(a_bytes: bytes, b_bytes: bytes) -> str:
    """Ask Claude whether two thumbnail images are visually the same.

    Returns ``"same"`` when the model judges them equivalent up to
    encoding/resolution/compression (i.e. the same picture YouTube
    re-encoded), ``"different"`` when content has actually changed. Any
    other response also counts as ``"different"`` — false positives
    just mean we surface a "may have changed" callout the user can
    dismiss, which is far less harmful than missing a real change.
    """
    import base64

    instructions = (
        "I will show you two thumbnail images. The first is the one we "
        "uploaded; the second is what's currently on YouTube. Answer with "
        "exactly one word: 'same' if they appear to be the same image "
        "(differences in encoding, compression, resolution, or color "
        "profile are fine — those happen automatically when YouTube "
        "re-encodes an uploaded thumbnail), or 'different' if the actual "
        "subject of the image has changed. No explanation."
    )
    content: list[dict] = [
        {"type": "text", "text": instructions},
        {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/jpeg",
                "data": base64.b64encode(a_bytes).decode("ascii"),
            },
        },
        {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/jpeg",
                "data": base64.b64encode(b_bytes).decode("ascii"),
            },
        },
    ]

    client = get_client()
    message = await asyncio.to_thread(
        client.messages.create,
        model=await _resolve_model(),
        max_tokens=8,
        messages=[{"role": "user", "content": content}],
        system=(
            "You answer with exactly one word — 'same' or 'different' — "
            "and nothing else."
        ),
    )
    raw = _extract_text(message).strip().lower()
    return "same" if raw.startswith("same") else "different"


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
    prompt_variables: dict[str, object] | None = None,
) -> str:
    """Generate an SEO-friendly video description from a transcript.

    Prompt body comes from the ``description_from_transcript_prompt`` row
    in ``prompt_templates``; the seed default kicks in when the row is
    missing. No system prompt — instructions live in the user message.

    ``prompt_variables`` is the merged dict from
    ``templates.build_prompt_variables`` (parent fields, custom variables,
    URL family) so prompt bodies can reference ``{{parent_context_block}}``,
    ``{{episode_url}}``, or any inherited item variable. The explicit
    arguments below win on key collision.
    """
    from yt_scheduler.services import prompts as prompt_service
    from yt_scheduler.services.transcripts import transcript_prompt_variables

    prompt = await prompt_service.get_prompt_with_fallback(
        "description_from_transcript_prompt", project_id=project_id
    )
    rendered = await _render_template_body(
        prompt["body"],
        {
            **(prompt_variables or {}),
            "title": title,
            "channel_name": channel_name,
            "channel_name_block": f"Channel: {channel_name}" if channel_name else "",
            **transcript_prompt_variables(transcript),
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
    return _extract_text(message).strip()


async def generate_seo_description_from_frames(
    title: str,
    frames: list[bytes],
    *,
    project_id: int,
    channel_name: str = "",
    extra_instructions: str = "",
    prompt_variables: dict[str, object] | None = None,
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
    instructions = await _render_template_body(
        prompt["body"],
        {
            **(prompt_variables or {}),
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
    return _extract_text(message).strip()


async def generate_tags_from_frames(
    title: str,
    description: str,
    frames: list[bytes],
    *,
    project_id: int,
    prompt_variables: dict[str, object] | None = None,
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
    instructions = await _render_template_body(
        prompt["body"],
        {
            **(prompt_variables or {}),
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
    return _clean_tags(_extract_text(message).strip())


async def generate_tags_from_metadata(
    title: str,
    description: str,
    transcript: str = "",
    *,
    project_id: int,
    prompt_variables: dict[str, object] | None = None,
) -> list[str]:
    """Generate YouTube tags based on title/description/transcript using the
    user-editable prompt template.

    ``prompt_variables`` is the merged dict from
    ``templates.build_prompt_variables`` so the prompt body's parent
    references (``{{parent_tags}}``, ``{{parent_context_block}}``) resolve
    for promo children. Explicit arguments win on key collision.
    """
    from yt_scheduler.services import prompts as prompt_service
    from yt_scheduler.services.transcripts import transcript_prompt_variables

    prompt = await prompt_service.get_prompt_with_fallback(
        "tags_from_metadata_prompt", project_id=project_id
    )
    rendered = await _render_template_body(
        prompt["body"],
        {
            **(prompt_variables or {}),
            "title": title,
            "description": description,
            **transcript_prompt_variables(transcript),
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
    return _clean_tags(_extract_text(message).strip())


def _build_parent_context_block(
    parent_title: str,
    parent_url: str,
    parent_description: str,
    parent_tags: str,
) -> str:
    """Collapse the four parent fields into a one-paragraph block for
    prompt bodies that reference ``{{parent_context_block??}}``. Empty
    string when there's no parent (caller passes empty fields), which
    lets the ??-default form in the prompt body swallow it cleanly."""
    if not parent_title:
        return ""
    parts = ["This is a promo clip from a parent video:",
             f"Parent title: {parent_title}"]
    if parent_url:
        parts.append(f"Parent URL: {parent_url}")
    if parent_description:
        parts.append(f"Parent description: {parent_description[:500]}")
    if parent_tags:
        parts.append(f"Parent tags: {parent_tags}")
    parts.append(
        "Where natural, reference or link back to the parent so the "
        "promo helps viewers find it."
    )
    return "\n".join(parts)


async def generate_title_from_filename(
    filename: str,
    *,
    project_id: int,
    parent_url: str = "",
    parent_title: str = "",
    parent_description: str = "",
    parent_tags: str = "",
) -> str:
    """Generate a YouTube title from a raw filename via the
    ``title_from_filename_prompt`` seed.

    Used by the Promo Videos auto-action chain to pick a sensible title
    before the YouTube upload step (so the YT video is born with a real
    title instead of a placeholder).

    Callers should wrap this in their own try/except and fall back to
    :func:`fallback_title_from_filename` on failure — the deterministic
    fallback covers the no-API-key and Claude-down cases so the upload
    chain can still make progress.
    """
    from yt_scheduler.services import prompts as prompt_service

    prompt = await prompt_service.get_prompt_with_fallback(
        "title_from_filename_prompt", project_id=project_id
    )
    parent_context_block = _build_parent_context_block(
        parent_title, parent_url, parent_description, parent_tags
    )
    rendered = await _render_template_body(
        prompt["body"],
        {
            "filename": filename,
            "parent_url": parent_url,
            "parent_title": parent_title,
            "parent_description": parent_description,
            "parent_tags": parent_tags,
            "parent_context_block": parent_context_block,
        },
    )

    kwargs: dict[str, object] = {
        "model": await _resolve_model(),
        "max_tokens": 128,
        "messages": [{"role": "user", "content": rendered}],
    }
    if prompt["system"]:
        kwargs["system"] = prompt["system"]

    client = get_client()
    message = await asyncio.to_thread(client.messages.create, **kwargs)
    raw = _extract_text(message).strip()
    # Some models still wrap output in quotes despite the system prompt.
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in {'"', "'"}:
        raw = raw[1:-1].strip()
    return raw[:100]


_FILENAME_PREFIX_STRIPS = (
    "riverside_",
    "recording_",
    "screen_recording_",
    "screenrecording_",
    "untitled_",
    "new_recording_",
)


def fallback_title_from_filename(filename: str) -> str:
    """Deterministic title from a filename, used when the AI call fails.

    Strips known recording-software prefixes, drops the extension,
    converts separators to spaces, collapses whitespace, and title-cases
    the result. Always returns a non-empty string; falls back to
    ``"Untitled video"`` if the filename collapses to nothing.
    """
    import os
    import re

    base = os.path.basename(filename)
    # ``os.path.splitext`` keeps a leading dot (treats ".mp4" as a hidden
    # file with no extension), so strip a known video extension manually.
    stem, ext = os.path.splitext(base)
    if not stem and ext:
        stem = ext.lstrip(".")
        ext = ""
    base = stem
    if base.lower().endswith((".mp4", ".mov", ".m4v", ".webm", ".mkv")):
        base = base.rsplit(".", 1)[0]
    lower = base.lower()
    for prefix in _FILENAME_PREFIX_STRIPS:
        if lower.startswith(prefix):
            base = base[len(prefix):]
            break
    base = re.sub(r"[_\-]+", " ", base)
    base = re.sub(r"\s+", " ", base).strip()
    if not base:
        return "Untitled video"
    return base.title()[:100]


# Seed fallback for ``ai_block_default_system_prompt`` — kept here so an
# install whose migrations haven't run yet still has a sensible default for
# bare ``{{ai: …}}`` blocks. The user-editable copy lives in
# ``services/prompts.SEED_AI_BLOCK_DEFAULT_SYSTEM_PROMPT`` and is mirrored
# from there to keep this constant in lockstep.
DEFAULT_AI_SYSTEM_PROMPT = (
    "You are a social media copywriter. Return ONLY the requested text, "
    "no preamble, no quotes, no explanation. "
    "All URLs and links must include 'https://' at the beginning. "
    "Do not use markdown link syntax — write URLs as plain text. "
    "None of the supported platforms (X, Bluesky, Mastodon, LinkedIn, "
    "Threads) reliably render markdown."
)


def call_ai_block(
    prompt: str,
    *,
    system: str | None,
    model: str | None = None,
    max_tokens: int = 512,
    trace: list[dict] | None = None,
) -> str:
    """One Claude round-trip for a single template ``{{ai: ...}}`` block.

    ``system`` is keyword-only and required — callers must be explicit
    about what system message (if any) to send. ``None`` or empty string
    sends no system prompt. ``model=None`` falls back to
    ``settings.anthropic_model`` (or env). Used as the unified leaf call;
    the walker in `services/templates.py` handles nesting itself.

    When ``trace`` is a list, one entry per call is appended with the
    prompt, system text, resolved model, response text, and elapsed
    ms. The collector is the F-series debug-log machinery — leave it
    None for callers that don't care (no overhead either way).
    """
    import time

    client = get_client()
    resolved_model = model or _resolve_model_sync()
    kwargs: dict[str, object] = {
        "model": resolved_model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system:
        kwargs["system"] = system
    start = time.monotonic()
    message = client.messages.create(**kwargs)
    elapsed_ms = int((time.monotonic() - start) * 1000)
    result = _extract_text(message).strip()
    if trace is not None:
        trace.append({
            "kind": "ai_call",
            "prompt": prompt,
            "system": system,
            "model": resolved_model,
            "response": result,
            "elapsed_ms": elapsed_ms,
        })
    return result
