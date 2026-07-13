"""Template engine with variable substitution and AI generation.

Templates are stored as a parent ``templates`` row plus one or more
``template_slots`` rows. Each slot binds a platform to a credential
(or, for built-in slots, defers to the project's default for that
platform). The ``platforms`` shape returned by :func:`get_template` is
a compatibility view assembled from the built-in slots so callers that
were written against the pre-slot API keep working.
"""

from __future__ import annotations

import json
import re

import aiosqlite

from yt_scheduler.database import get_db, write_transaction
from yt_scheduler.services.ai import DEFAULT_AI_SYSTEM_PROMPT, call_ai_block
from yt_scheduler.services.social import ALL_PLATFORMS

# Per-platform default character limits — the single source of truth used
# by the API (default for a new slot), the template-edit page (initial
# max_chars on "+ Add slot"), the default-template seeds below, and the
# preview UI on the video-detail page. The numbers come from each
# platform's stated post-length cap for a non-premium account; X-Premium
# accounts get the 25,000-char limit applied dynamically based on the
# connected account's tier (see social_routes / settings_routes).
DEFAULT_MAX_CHARS_BY_PLATFORM: dict[str, int] = {
    "twitter":  280,
    "bluesky":  300,
    "mastodon": 500,
    "linkedin": 3000,
    "threads":  500,
}
# Generic fallback when a platform name doesn't match any known key.
# Picked to be conservative enough to fit X/Bluesky and most niche
# Mastodon instances; callers should pass an explicit platform when one
# is available.
GENERIC_MAX_CHARS_FALLBACK = 500


def default_max_chars(platform: str | None) -> int:
    """Return the default max_chars for a platform, or the generic
    fallback when the platform isn't one of the five we ship support
    for. Used by the API, the seed templates, and any callsite that
    needs to seed a slot's character limit."""
    if platform is None:
        return GENERIC_MAX_CHARS_FALLBACK
    return DEFAULT_MAX_CHARS_BY_PLATFORM.get(
        platform.lower(), GENERIC_MAX_CHARS_FALLBACK
    )


# Default templates shipped with the app
DEFAULT_NEW_MESSAGE_TEMPLATE = {
    "name": "send_message",
    "description": "Plain user-message template — useful for one-off posts",
    "platforms": {
        "twitter":  {"template": "{{user_message}}", "media": "none", "max_chars": DEFAULT_MAX_CHARS_BY_PLATFORM["twitter"]},
        "bluesky":  {"template": "{{user_message}}", "media": "none", "max_chars": DEFAULT_MAX_CHARS_BY_PLATFORM["bluesky"]},
        "mastodon": {"template": "{{user_message}}", "media": "none", "max_chars": DEFAULT_MAX_CHARS_BY_PLATFORM["mastodon"]},
        "linkedin": {"template": "{{user_message}}", "media": "none", "max_chars": DEFAULT_MAX_CHARS_BY_PLATFORM["linkedin"]},
        "threads":  {"template": "{{user_message}}", "media": "none", "max_chars": DEFAULT_MAX_CHARS_BY_PLATFORM["threads"]},
    },
}


DEFAULT_TEMPLATE = {
    "name": "announce_video",
    "description": "Standard template for announcing a new video upload",
    "platforms": {
        "twitter": {
            "template": '{{ai: Write a punchy tweet announcing a YouTube video titled "{{title}}" about {{tags}}. Include the URL {{url}}. Under 280 chars. 2-3 hashtags.}}',
            "media": "thumbnail",
            "max_chars": DEFAULT_MAX_CHARS_BY_PLATFORM["twitter"],
        },
        "bluesky": {
            "template": '{{ai: Write a Bluesky post announcing my new video "{{title}}". Conversational tone, under 300 chars.}}\n\n{{url}}',
            "media": "thumbnail",
            "max_chars": DEFAULT_MAX_CHARS_BY_PLATFORM["bluesky"],
        },
        "mastodon": {
            "template": 'New video is live!\n\n"{{title}}"\n\n{{url}}\n\n{{ai: Generate 3-5 CamelCase hashtags for: {{tags}}}}',
            "media": "thumbnail",
            "max_chars": DEFAULT_MAX_CHARS_BY_PLATFORM["mastodon"],
        },
        "linkedin": {
            "template": '{{ai: Write a LinkedIn post (2-3 paragraphs, professional but approachable) about my new video "{{title}}". Description: {{description_short}}. End with a question.}}\n\nWatch here: {{url}}',
            "media": "thumbnail",
            "max_chars": DEFAULT_MAX_CHARS_BY_PLATFORM["linkedin"],
        },
        "threads": {
            "template": '{{ai: Write a casual Threads post announcing "{{title}}". Keep it engaging, under 500 chars.}}\n\n{{url}}',
            "media": "thumbnail",
            "max_chars": DEFAULT_MAX_CHARS_BY_PLATFORM["threads"],
        },
    },
}


BUILTIN_TEMPLATE_NAMES = {"announce_video", "send_message"}


class MissingRequiredVariable(KeyError):
    """Raised when a ``{{name!}}`` placeholder has no usable value: the name
    is absent from the variables dict, or present but blank (empty or
    whitespace-only). The required marker declares "this must not render
    without content" — a blank value violates that intent just as much as
    a missing key."""

    def __init__(self, name: str):
        self.name = name
        super().__init__(name)

    def __str__(self) -> str:
        return (
            f"Required template variable missing or empty: {{{{{self.name}!}}}}"
        )


class UndefinedTemplateVariables(KeyError):
    """Raised when bare ``{{name}}`` placeholders reference names that are
    absent from the variables dict. Every undefined name in the pass is
    collected before raising so callers can report all of them at once.

    A *defined but blank* value does NOT raise — bare placeholders render
    blank values as empty text. Only a name that isn't supplied at all is
    an authoring error (a typo, or a variable that doesn't exist in this
    render context)."""

    def __init__(self, names: list[str]):
        self.names = sorted(set(names))
        super().__init__(", ".join(self.names))

    def __str__(self) -> str:
        placeholders = ", ".join("{{" + name + "}}" for name in self.names)
        return f"Undefined template variable(s): {placeholders}"


class SectionTagError(ValueError):
    """Raised when ``{{#name}}`` / ``{{^name}}`` / ``{{/name}}`` section tags
    don't pair up: an opener that is never closed, a closer with no opener,
    or a closer whose name doesn't match the innermost open section. A
    malformed section silently rendering (or silently vanishing) is exactly
    the quiet wrong output this engine refuses to produce, so it raises."""


class UnknownImageShortname(KeyError):
    """Raised when an ``{{image:shortname}}`` directive references a shortname
    with no matching ``item_images`` row. The wildcard form ``{{image:*}}``
    and the bare ``{{video}}`` / ``{{thumbnail}}`` directives never raise —
    they silently skip when the file isn't present."""

    def __init__(self, shortname: str):
        self.shortname = shortname
        super().__init__(shortname)

    def __str__(self) -> str:
        return f"Image shortname not found: {{{{image:{self.shortname}}}}}"


def merge_variables(
    *,
    global_vars: dict[str, str] | None = None,
    project_vars: dict[str, str] | None = None,
    parent_item_vars: dict[str, str] | None = None,
    self_builtins: dict[str, object] | None = None,
    self_item_vars: dict[str, str] | None = None,
) -> dict[str, object]:
    """Combine the four custom-variable scopes with self's built-ins per the
    inheritance rule (later wins on key collision):

        1. ``global_vars``       (lowest priority)
        2. ``project_vars``
        3. ``parent_item_vars``  (only applies when self has a parent)
        4. ``self_builtins``     (title, url, episode_url, project_url, …)
        5. ``self_item_vars``    (highest priority)

    Built-in keys never inherit from a parent — that would let ``{{title}}``
    accidentally reflect the parent's title. Only custom k/v pairs inherit.
    The named cross-scope accessors (``{{episode_url}}``, ``{{project_url}}``)
    live in ``self_builtins`` because they're computed at the self-level
    rather than literally pulled from the parent's variables.
    """
    merged: dict[str, object] = {}
    if global_vars:
        merged.update(global_vars)
    if project_vars:
        merged.update(project_vars)
    if parent_item_vars:
        merged.update(parent_item_vars)
    if self_builtins:
        merged.update(self_builtins)
    if self_item_vars:
        merged.update(self_item_vars)
    return merged


async def build_prompt_variables(video: dict) -> dict[str, object]:
    """Merged variable dict for AI prompt-template bodies (description /
    tags generation): custom variables at every scope (global → project →
    parent item → self item), the URL family, the parent-video fields with
    ``parent_context_block``, the ``{{transcript*}}`` family, and the
    video's own title/description.

    The social-post path has its own richer context builder in
    ``social_routes._build_render_context`` (images, media paths, a
    YouTube tier lookup); this one is deliberately lighter — prompt
    bodies never attach media, and a YouTube API round-trip per
    description generation would be wasted quota.
    """
    # Lazy import: ai.py imports nothing from this module at load time,
    # but _build_parent_context_block lives there next to its other
    # prompt-assembly helpers, and this module is imported BY ai.py's
    # render helper at call time — mirror that lazy style to keep the
    # import graph acyclic.
    from yt_scheduler.services.ai import _build_parent_context_block
    from yt_scheduler.services.transcripts import transcript_prompt_variables

    project_id = video.get("project_id")
    if project_id in (None, 0):
        raise ValueError(
            f"Video {video.get('id')} has no project_id (data integrity error)."
        )
    project_id = int(project_id)

    db = await get_db()

    project_rows = await db.execute_fetchall(
        "SELECT project_url FROM projects WHERE id = ?", (project_id,)
    )
    project_url = (
        dict(project_rows[0]).get("project_url") or "" if project_rows else ""
    )

    parent: dict | None = None
    if video.get("parent_item_id"):
        parent_rows = await db.execute_fetchall(
            "SELECT * FROM videos WHERE id = ?", (video["parent_item_id"],)
        )
        if parent_rows:
            parent = dict(parent_rows[0])

    global_rows = await db.execute_fetchall(
        "SELECT key, value FROM global_variables"
    )
    global_vars = {r["key"]: r["value"] for r in global_rows}

    project_var_rows = await db.execute_fetchall(
        "SELECT key, value FROM project_variables WHERE project_id = ?",
        (project_id,),
    )
    project_vars = {r["key"]: r["value"] for r in project_var_rows}

    parent_item_vars: dict[str, str] = {}
    if parent is not None:
        parent_var_rows = await db.execute_fetchall(
            "SELECT key, value FROM item_variables WHERE video_id = ?",
            (parent["id"],),
        )
        parent_item_vars = {r["key"]: r["value"] for r in parent_var_rows}

    self_var_rows = await db.execute_fetchall(
        "SELECT key, value FROM item_variables WHERE video_id = ?",
        (video["id"],),
    )
    self_item_vars = {r["key"]: r["value"] for r in self_var_rows}

    parent_url = (parent or {}).get("url") or ""
    parent_title = (parent or {}).get("title") or ""
    parent_description = (parent or {}).get("description") or ""
    parent_tags = ""
    if parent is not None:
        try:
            parent_tags = ", ".join(json.loads(parent.get("tags") or "[]"))
        except json.JSONDecodeError:
            parent_tags = ""

    self_builtins: dict[str, object] = {
        "title": video.get("title") or "",
        "description": video.get("description") or "",
        "url": video.get("url") or "",
        "episode_url": parent_url,
        "project_url": project_url,
        "parent_url": parent_url,
        "parent_title": parent_title,
        "parent_description": parent_description,
        "parent_tags": parent_tags,
        "parent_context_block": _build_parent_context_block(
            parent_title, parent_url, parent_description, parent_tags
        ),
        **transcript_prompt_variables(video.get("transcript")),
    }

    return merge_variables(
        global_vars=global_vars,
        project_vars=project_vars,
        parent_item_vars=parent_item_vars,
        self_builtins=self_builtins,
        self_item_vars=self_item_vars,
    )


def _is_blank(value: object) -> bool:
    """True when a variable value counts as "no content" for the section,
    required, and default placeholder forms: ``None`` or a string that is
    empty / whitespace-only after coercion."""
    return value is None or str(value).strip() == ""


# {{#name}} (keep content when name has content), {{^name}} (inverted: keep
# when name is missing or blank), {{/name}} (close). Resolved as the FIRST
# render pass, so content inside a dropped section is discarded before
# variable substitution, media-directive extraction, or any {{ai:}} call
# can act on it.
_SECTION_TAG_PATTERN = re.compile(r"\{\{([#^/])(\w+)\}\}")

# Anything that *looks* like a section tag ({{ followed by #, ^, or /) but
# doesn't parse as one — e.g. {{#bad-name}}, {{# name}}, {{ /url}}. These
# raise instead of passing through: a section tag that silently renders as
# literal text is a template bug the author needs to see.
_SECTION_TAG_CANDIDATE_PATTERN = re.compile(r"\{\{\s*[#^/][^}]*\}\}")


def resolve_sections(
    text: str, variables: dict[str, object] | None = None
) -> str:
    """Resolve ``{{#name}}…{{/name}}`` / ``{{^name}}…{{/name}}`` blocks.

    A ``#`` section's content is kept when its variable has content
    (present in ``variables`` and non-blank after string coercion); a
    ``^`` (inverted) section's content is kept when it does NOT. Sections
    nest; close tags match by name against the innermost open section, so
    the parser never counts raw braces and is indifferent to ``{{ai: …}}``
    nesting inside section content. Raises :class:`SectionTagError` on an
    unclosed opener, a stray closer, or a close-tag name mismatch.

    Public (not just a :func:`render` internal) so callers that pre-process
    a body — e.g. media-directive extraction before the main render — can
    resolve sections first and never attach media from a section that
    isn't rendering.
    """
    variables = variables or {}

    valid_tag_spans = {m.span() for m in _SECTION_TAG_PATTERN.finditer(text)}
    for candidate in _SECTION_TAG_CANDIDATE_PATTERN.finditer(text):
        if candidate.span() not in valid_tag_spans:
            raise SectionTagError(
                f"Malformed section tag {candidate.group(0)!r} — expected "
                "{{#name}}, {{^name}}, or {{/name}} with a name of letters, "
                "digits, and underscores"
            )

    def section_has_content(name: str) -> bool:
        return name in variables and not _is_blank(variables[name])

    root_parts: list[str] = []
    # Each frame: (marker, name, parts collected inside that section).
    open_sections: list[tuple[str, str, list[str]]] = []
    cursor = 0
    for match in _SECTION_TAG_PATTERN.finditer(text):
        current_parts = open_sections[-1][2] if open_sections else root_parts
        current_parts.append(text[cursor:match.start()])
        cursor = match.end()
        marker, name = match.group(1), match.group(2)
        if marker in "#^":
            open_sections.append((marker, name, []))
            continue
        if not open_sections:
            raise SectionTagError(
                f"Section close tag {{{{/{name}}}}} has no matching opener"
            )
        open_marker, open_name, inner_parts = open_sections.pop()
        if open_name != name:
            raise SectionTagError(
                f"Section close tag {{{{/{name}}}}} does not match the "
                f"innermost open section {{{{{open_marker}{open_name}}}}}"
            )
        keep = (
            section_has_content(name)
            if open_marker == "#"
            else not section_has_content(name)
        )
        if keep:
            parent_parts = open_sections[-1][2] if open_sections else root_parts
            parent_parts.append("".join(inner_parts))
    if open_sections:
        open_marker, open_name, _ = open_sections[-1]
        raise SectionTagError(
            f"Section {{{{{open_marker}{open_name}}}}} is never closed "
            f"(expected {{{{/{open_name}}}}})"
        )
    root_parts.append(text[cursor:])
    return "".join(root_parts)


# {{video}}, {{thumbnail}}, {{image:shortname}}, {{image:*}}.
# `image:*` is the wildcard; `image:<shortname>` requires lowercase
# alphanumerics + hyphens (matches the validation rule in `item_images`).
_MEDIA_DIRECTIVE_PATTERN = re.compile(
    r"\{\{(video|thumbnail|image:(?:\*|[a-z0-9-]+))\}\}"
)


def body_declares_media(body: str) -> bool:
    """True when the body contains any media directive (``{{video}}``,
    ``{{thumbnail}}``, ``{{image:…}}``).

    Callers check this on the PRE-section-resolution body to decide
    whether the slot author took manual control of media: a directive
    that a dropped ``{{#…}}`` section removed must not be resurrected by
    the legacy per-slot media fallback — the author said "media only under
    this condition," and the condition is false."""
    return bool(_MEDIA_DIRECTIVE_PATTERN.search(body))


def extract_media_directives(
    body: str,
    *,
    video_path: str | None = None,
    thumbnail_path: str | None = None,
    images: list[dict] | None = None,
) -> tuple[str, list[str], list[str]]:
    """Pre-pass over the template body that pulls media directives out.

    Returns ``(cleaned_body, media_paths, alt_texts)``. Each directive is
    replaced by an empty string in the body and the corresponding file
    path + alt-text are appended to the returned lists, in the order
    directives appear.

    Directive semantics:

    - ``{{video}}`` — append ``video_path`` if non-empty, else skip silently.
    - ``{{thumbnail}}`` — append ``thumbnail_path`` if non-empty, else skip.
    - ``{{image:*}}`` — append every image's path, in the order of the
      ``images`` list (caller pre-sorts by ``order_index``).
    - ``{{image:shortname}}`` — append the matching image's path; raises
      :class:`UnknownImageShortname` when no row matches (the user named a
      specific image, so a miss is a real bug, not a silent fallback).

    ``images`` is a list of dicts with at least ``shortname``, ``path``,
    ``alt_text``. Pass ``None`` (or an empty list) when no images exist.
    """
    images = images or []
    images_by_name = {img["shortname"]: img for img in images}

    media_paths: list[str] = []
    alt_texts: list[str] = []

    def replace(match: re.Match) -> str:
        directive = match.group(1)
        if directive == "video":
            if video_path:
                media_paths.append(video_path)
                alt_texts.append("")
        elif directive == "thumbnail":
            if thumbnail_path:
                media_paths.append(thumbnail_path)
                alt_texts.append("")
        elif directive == "image:*":
            for img in images:
                media_paths.append(img["path"])
                alt_texts.append(img.get("alt_text") or "")
        elif directive.startswith("image:"):
            shortname = directive[len("image:"):]
            img = images_by_name.get(shortname)
            if img is None:
                raise UnknownImageShortname(shortname)
            media_paths.append(img["path"])
            alt_texts.append(img.get("alt_text") or "")
        return ""

    cleaned = _MEDIA_DIRECTIVE_PATTERN.sub(replace, body)
    return cleaned, media_paths, alt_texts


async def async_render(
    template_text: str,
    variables: dict[str, object] | None = None,
    *,
    default_system_prompt: str | None = DEFAULT_AI_SYSTEM_PROMPT,
    model: str | None = None,
    max_tokens: int = 512,
    trace: list[dict] | None = None,
    on_undefined: str = "error",
) -> str:
    """Async-safe wrapper around :func:`render`.

    Runs the synchronous renderer (which can fire blocking Anthropic
    SDK calls via ``{{ai: ...}}`` blocks) in a worker thread so the
    event loop doesn't stall. Use this from every async caller; sync
    ``render`` is kept for tests / CLI / true off-loop paths.
    """
    import asyncio as _asyncio
    return await _asyncio.to_thread(
        render, template_text, variables,
        default_system_prompt=default_system_prompt,
        model=model, max_tokens=max_tokens, trace=trace,
        on_undefined=on_undefined,
    )


def render(
    template_text: str,
    variables: dict[str, object] | None = None,
    *,
    default_system_prompt: str | None = DEFAULT_AI_SYSTEM_PROMPT,
    model: str | None = None,
    max_tokens: int = 512,
    trace: list[dict] | None = None,
    on_undefined: str = "error",
) -> str:
    """Single rendering primitive. Three passes:

    1. **Section resolution.** ``{{#name}}…{{/name}}`` keeps its content
       when ``name`` has content (present and non-blank); the inverted
       form ``{{^name}}…{{/name}}`` keeps its content when it does NOT.
       Content inside a dropped section is discarded before any later
       pass sees it — no variable errors, no ``{{ai:}}`` calls, no media.
       Malformed section tags raise :class:`SectionTagError`.

    2. **Variable substitution.** Three placeholder forms:

       * ``{{name}}`` — plain. An undefined name raises
         :class:`UndefinedTemplateVariables` (every undefined name in the
         template is collected into one error). A defined-but-blank value
         renders as empty text. ``on_undefined="literal"`` restores the
         old leave-it-literal behavior for callers where the user has
         explicitly acknowledged unresolved names.
       * ``{{name!}}`` — required. Missing OR blank raises
         :class:`MissingRequiredVariable`.
       * ``{{name??default text}}`` — fallback. Missing OR blank renders
         ``default text`` (which may be empty for ``{{name??}}``).
         Default text is taken as a literal string — no recursive
         substitution inside it.

       ``ai:`` and ``ai[...]:`` openers survive this pass because ``\\w+``
       can't cross the colon or bracket.

    3. **AI block evaluation.** ``{{ai: prompt}}`` and the system-override
       form ``{{ai[system text]: prompt}}`` are matched with a balanced-
       brace walker (Python ``re`` can't handle balanced delimiters).
       Inner blocks resolve first and their output is spliced into the
       parent's prompt before the parent is sent. Sibling blocks are
       independent.

    Per-block ``[system]`` overrides only that block's call; nested
    blocks that don't specify their own override inherit
    ``default_system_prompt``. Unbalanced ``{{ai`` openers are emitted
    verbatim so the broken syntax surfaces in the output instead of
    silently shipping a half-template to Claude.
    """
    variables = variables or {}
    if trace is not None:
        # Record the raw template, the variables we're about to
        # substitute, and the result of the substitution pass — the
        # F3 debug modal renders these in order so the user can see
        # exactly how their template body became the prompt(s) sent
        # to Claude. Variable values are coerced to strings for
        # JSON-stable serialization.
        trace.append({
            "kind": "template_body",
            "text": template_text,
        })
        trace.append({
            "kind": "variables",
            "values": {k: ("" if v is None else str(v)) for k, v in variables.items()},
        })
    text = resolve_sections(template_text, variables)
    if trace is not None and text != template_text:
        # Only traced when section tags actually changed the body, so
        # section-free templates keep the familiar three-step trace.
        trace.append({"kind": "sections", "text": text})
    text = _substitute_variables(text, variables, on_undefined=on_undefined)
    if trace is not None:
        trace.append({"kind": "substituted", "text": _restore_braces(text)})
    # Output boundary: everything below this line is what the user actually sees,
    # so sentinels become real braces again.
    return _restore_braces(_resolve_ai_blocks(
        text,
        default_system_prompt=default_system_prompt,
        model=model,
        max_tokens=max_tokens,
        trace=trace,
    ))


# Backwards-compatible alias kept so older callers don't need a churn pass.
def render_template(template_text: str, variables: dict[str, object]) -> str:
    return render(template_text, variables)


# {{name}}, {{name!}}, or {{name??default text}}. The trailing alternation
# captures '!' (required) OR '??<default>' — never both, never neither-and-
# bare-suffix. Default text is non-greedy so it stops at the next '}}'.
_VAR_PATTERN = re.compile(r"\{\{(\w+)(?:(!)|\?\?(.*?))?\}\}")

# The AI-block walker finds blocks by the literal doublets ``{{`` / ``}}`` and
# counts brace depth. Substitution runs BEFORE that walk, so any ``{{`` or ``}}``
# inside a substituted value would be miscounted as template structure: an
# unmatched ``{{`` leaves the walk at depth != 0 and emits the whole
# ``{{ai: ...}}`` directive verbatim into the post, while a stray ``}}`` closes
# the block early and truncates the prompt sent to Claude.
#
# Values carry sentinels through the walk instead of real braces, so user data
# can never alter block structure. Real braces are restored at the two output
# boundaries — the Claude prompt, and the final rendered string — which keeps
# the rendered output byte-exact for values that legitimately contain braces.
_SENTINEL_OPEN = "\x00AIB_OPEN\x00"
_SENTINEL_CLOSE = "\x00AIB_CLOSE\x00"


def _sanitize_value(value: str) -> str:
    """Hide ``{{``/``}}`` in a substituted variable value from the AI-block walker."""
    if "\x00" in value:
        # A NUL can only reach here from corrupted data, and would let a value
        # forge a sentinel and therefore forge block structure.
        raise ValueError(f"Variable value contains a NUL byte: {value[:40]!r}")
    return value.replace("{{", _SENTINEL_OPEN).replace("}}", _SENTINEL_CLOSE)


def _restore_braces(text: str) -> str:
    """Turn sentinels back into the real braces the user's value contained."""
    return text.replace(_SENTINEL_OPEN, "{{").replace(_SENTINEL_CLOSE, "}}")


def _substitute_variables(
    text: str,
    variables: dict[str, object],
    *,
    on_undefined: str = "error",
) -> str:
    """Substitute ``{{name}}`` / ``{{name!}}`` / ``{{name??default}}``.

    ``on_undefined`` controls what a bare ``{{name}}`` does when the name
    is absent from ``variables``:

    * ``"error"`` (default) — collect every undefined name across the whole
      pass, then raise :class:`UndefinedTemplateVariables` naming all of
      them. Raising happens before any ``{{ai: …}}`` block can fire, so a
      typo never costs a Claude call.
    * ``"literal"`` — leave the placeholder text in the output. Only for
      callers where the user has explicitly acknowledged unresolved names
      (the generate-posts 409 → ack flow).
    """
    undefined_names: list[str] = []

    def replace(match: re.Match) -> str:
        name = match.group(1)
        required = match.group(2) is not None
        default_text = match.group(3)  # None when no `??...` was present
        value = variables.get(name)
        has_content = name in variables and not _is_blank(value)
        if required:
            # {{name!}} means "must render with content" — blank violates
            # that intent just as much as a missing key.
            if not has_content:
                raise MissingRequiredVariable(name)
            return _sanitize_value(str(value))
        if default_text is not None:
            # {{name??default}} covers "nothing to say" (missing OR blank),
            # not merely missing keys — a blank value with a dangling label
            # in front of it is the bug this form exists to prevent.
            if not has_content:
                return default_text
            return _sanitize_value(str(value))
        if name not in variables:
            if on_undefined == "literal":
                return match.group(0)
            undefined_names.append(name)
            return ""
        if value is None:
            return ""
        return _sanitize_value(str(value))

    substituted = _VAR_PATTERN.sub(replace, text)
    if undefined_names:
        raise UndefinedTemplateVariables(undefined_names)
    return substituted


# Hard limits applied per render() call to prevent runaway cost and recursion.
_MAX_AI_BLOCK_DEPTH = 5   # nesting levels of {{ai:}} inside {{ai:}}
_MAX_AI_BLOCKS_PER_RENDER = 20  # total Claude calls across the whole template


class _RenderLimits:
    """Mutable call-count bucket shared across a single render() invocation."""

    __slots__ = ("blocks_fired",)

    def __init__(self) -> None:
        self.blocks_fired = 0


class TooManyAIBlocksError(RuntimeError):
    """Raised when a template exceeds the per-render AI-block budget."""

    def __init__(self, limit: int) -> None:
        super().__init__(
            f"Template exceeded the {limit}-block AI call limit per render. "
            "Reduce the number of {{ai:}} blocks in the template."
        )


class AIBlockDepthError(RuntimeError):
    """Raised when {{ai:}} nesting exceeds the recursion depth cap."""

    def __init__(self, limit: int) -> None:
        super().__init__(
            f"{{{{ai:}}}} blocks nested more than {limit} levels deep. "
            "Flatten the template."
        )


def _resolve_ai_blocks(
    text: str,
    *,
    default_system_prompt: str | None,
    model: str | None,
    max_tokens: int,
    trace: list[dict] | None = None,
    _depth: int = 0,
    _limits: _RenderLimits | None = None,
) -> str:
    if _limits is None:
        _limits = _RenderLimits()
    if _depth > _MAX_AI_BLOCK_DEPTH:
        raise AIBlockDepthError(_MAX_AI_BLOCK_DEPTH)

    out: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        idx = text.find("{{ai", i)
        if idx < 0:
            out.append(text[i:])
            break

        cursor = idx + 4  # past "{{ai"
        if cursor >= n or text[cursor] not in ":[":
            # Not actually an ai opener (e.g. ``{{aitch}}`` or stray ``{{ai``).
            out.append(text[i:cursor])
            i = cursor
            continue

        out.append(text[i:idx])

        system_override: str | None = None
        if text[cursor] == "[":
            close = text.find("]", cursor + 1)
            if close < 0 or close + 1 >= n or text[close + 1] != ":":
                # Unbalanced [ or missing ':' after ']' — surface the
                # broken syntax instead of silently absorbing it.
                out.append(text[idx:])
                break
            system_override = text[cursor + 1 : close]
            cursor = close + 1  # now points at ':'

        # cursor points at ':'; body starts after.
        body_start = cursor + 1
        depth = 1
        j = body_start
        while j < n and depth > 0:
            pair = text[j:j + 2]
            if pair == "{{":
                depth += 1
                j += 2
            elif pair == "}}":
                depth -= 1
                j += 2
            else:
                j += 1
        if depth != 0:
            out.append(text[idx:])
            break

        _limits.blocks_fired += 1
        if _limits.blocks_fired > _MAX_AI_BLOCKS_PER_RENDER:
            raise TooManyAIBlocksError(_MAX_AI_BLOCKS_PER_RENDER)

        inner_text = text[body_start : j - 2].strip()
        prompt = _resolve_ai_blocks(
            inner_text,
            default_system_prompt=default_system_prompt,
            model=model,
            max_tokens=max_tokens,
            trace=trace,
            _depth=_depth + 1,
            _limits=_limits,
        )
        effective_system = (
            system_override if system_override is not None else default_system_prompt
        )
        # Output boundary: Claude must see the user's real braces, not sentinels.
        out.append(call_ai_block(
            _restore_braces(prompt),
            system=(
                _restore_braces(effective_system)
                if effective_system is not None
                else None
            ),
            model=model,
            max_tokens=max_tokens,
            trace=trace,
        ))
        i = j

    return "".join(out)


_DEFAULT_APPLIES_TO = ["hook", "short", "segment", "video"]


def _decode_applies_to(raw: str | None) -> list[str]:
    if not raw:
        return list(_DEFAULT_APPLIES_TO)
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        return list(_DEFAULT_APPLIES_TO)
    if isinstance(decoded, list):
        return [str(t) for t in decoded if t in _DEFAULT_APPLIES_TO]
    return list(_DEFAULT_APPLIES_TO)


# --- Slot helpers ----------------------------------------------------------


def _slot_to_dict(row) -> dict:
    return {
        "id": int(row["id"]),
        "template_id": int(row["template_id"]),
        "platform": row["platform"],
        "social_account_id": (
            int(row["social_account_id"])
            if row["social_account_id"] is not None
            else None
        ),
        "is_builtin": bool(row["is_builtin"]),
        "is_disabled": bool(row["is_disabled"]),
        "order_index": int(row["order_index"]),
        "body": row["body"] or "",
        "media": row["media"] or "thumbnail",
        "max_chars": int(row["max_chars"] or 500),
    }


async def _list_slots(template_id: int) -> list[dict]:
    db = await get_db()
    cursor = await db.execute(
        "SELECT s.*, a.uuid AS account_uuid, a.username AS account_username, "
        "       a.platform AS account_platform, a.deleted_at AS account_deleted_at "
        "FROM template_slots s "
        "LEFT JOIN social_accounts a ON a.id = s.social_account_id "
        "WHERE s.template_id = ? "
        "ORDER BY s.order_index, s.id",
        (template_id,),
    )
    rows = await cursor.fetchall()
    out: list[dict] = []
    for row in rows:
        slot = _slot_to_dict(row)
        if row["account_uuid"] is not None:
            slot["resolved_account"] = {
                "uuid": row["account_uuid"],
                "username": row["account_username"],
                "platform": row["account_platform"],
                "deleted": row["account_deleted_at"] is not None,
            }
        else:
            slot["resolved_account"] = None
        out.append(slot)
    return out


def _platforms_view_from_slots(slots: list[dict]) -> dict:
    """Compatibility view: ``{platform: {template, media, max_chars}}`` from
    the built-in slots only. Used by callers that haven't been updated to
    speak the slot model."""
    view: dict[str, dict] = {}
    for slot in slots:
        if not slot["is_builtin"]:
            continue
        view[slot["platform"]] = {
            "template": slot["body"],
            "media": slot["media"],
            "max_chars": slot["max_chars"],
        }
    return view


# --- Public API ------------------------------------------------------------


def _decode_test_variables(raw: str | None) -> dict[str, str]:
    """Parse the JSON-encoded test_variables column. Returns {} when the
    column is NULL, empty, or malformed — never raises. Templates
    created before migration 016 will have NULL; the front-end falls
    back to its seeded defaults in that case."""
    if not raw:
        return {}
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(decoded, dict):
        return {}
    # Coerce values to strings so the template engine doesn't receive a
    # numeric/bool/list it doesn't know how to substitute.
    return {str(k): "" if v is None else str(v) for k, v in decoded.items()}


async def get_template(name: str, *, project_id: int) -> dict | None:
    """Get a template by name within a project."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT * FROM templates WHERE project_id = ? AND name = ?",
        (project_id, name),
    )
    row = await cursor.fetchone()
    if row is None:
        return None

    slots = await _list_slots(int(row["id"]))
    return {
        "id": int(row["id"]),
        "name": row["name"],
        "description": row["description"] or "",
        "applies_to": _decode_applies_to(row["applies_to"]),
        "is_builtin": bool(row["is_builtin"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "slots": slots,
        "platforms": _platforms_view_from_slots(slots),
        # Preview-pane fixtures so editing the template re-opens with
        # the same test inputs the user last saved. Empty dict = "use
        # the page's seeded defaults" (pre-016 rows, or never saved).
        "test_variables": _decode_test_variables(row["test_variables"]),
    }


async def set_template_test_variables(
    name: str, variables: dict[str, str], *, project_id: int
) -> None:
    """Persist the preview-pane test fixtures for a template. Stored as
    a JSON object on ``templates.test_variables``. Passing an empty
    dict clears the column back to NULL so the front-end falls back to
    its seeded defaults."""
    async with write_transaction() as db:
        cursor = await db.execute(
            "SELECT id FROM templates WHERE project_id = ? AND name = ?",
            (project_id, name),
        )
        row = await cursor.fetchone()
        if row is None:
            raise ValueError(f"Template '{name}' not found")
        encoded = json.dumps(variables) if variables else None
        await db.execute(
            "UPDATE templates SET test_variables = ?, updated_at = datetime('now') "
            "WHERE id = ?",
            (encoded, int(row["id"])),
        )


async def list_templates(project_id: int) -> list[dict]:
    """List all templates within a project."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT * FROM templates WHERE project_id = ? ORDER BY name",
        (project_id,),
    )
    rows = await cursor.fetchall()
    out: list[dict] = []
    for row in rows:
        slots = await _list_slots(int(row["id"]))
        out.append({
            "id": int(row["id"]),
            "name": row["name"],
            "description": row["description"] or "",
            "applies_to": _decode_applies_to(row["applies_to"]),
            "is_builtin": bool(row["is_builtin"]),
            "platforms": _platforms_view_from_slots(slots),
            "slot_count": len(slots),
        })
    return out


async def _set_builtin_slot(
    template_id: int,
    platform: str,
    body: str,
    media: str,
    max_chars: int,
) -> None:
    """Upsert a single built-in slot. Built-in slots are unique per
    (template_id, platform)."""
    # Reentrant: when called inside save_template's transaction this joins it
    # (one atomic write across all slots); called standalone it's its own.
    async with write_transaction() as db:
        cursor = await db.execute(
            "SELECT id FROM template_slots "
            "WHERE template_id = ? AND platform = ? AND is_builtin = 1",
            (template_id, platform),
        )
        row = await cursor.fetchone()
        if row is not None:
            await db.execute(
                "UPDATE template_slots "
                "SET body = ?, media = ?, max_chars = ?, updated_at = datetime('now') "
                "WHERE id = ?",
                (body, media, int(max_chars), int(row["id"])),
            )
        else:
            await db.execute(
                "INSERT INTO template_slots "
                "(template_id, platform, social_account_id, is_builtin, is_disabled, "
                " order_index, body, media, max_chars) "
                "VALUES (?, ?, NULL, 1, 0, 0, ?, ?, ?)",
                (template_id, platform, body, media, int(max_chars)),
            )


async def save_template(
    name: str,
    description: str,
    platforms: dict,
    *,
    project_id: int,
    applies_to: list[str] | None = None,
) -> dict:
    """Create or update a template within a project (compatibility-shape API).

    Each entry in ``platforms`` becomes a built-in slot (one per platform).
    Non-built-in slots are not touched here — those are managed via the
    slot CRUD endpoints in Phase D's UI.
    """
    tiers = applies_to if applies_to is not None else _DEFAULT_APPLIES_TO
    if not tiers:
        raise ValueError("applies_to must include at least one tier")

    is_builtin_flag = 1 if name in BUILTIN_TEMPLATE_NAMES else 0
    # The template row, its read-back id, and all built-in slots are ONE atomic
    # transaction so a crash can't leave a template row with stale/no slots. Each
    # _set_builtin_slot's own write_transaction joins this one (reentrant).
    try:
        async with write_transaction() as db:
            await db.execute(
                "INSERT INTO templates (project_id, name, description, applies_to, is_builtin) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(project_id, name) DO UPDATE SET "
                "  description = excluded.description, "
                "  applies_to  = excluded.applies_to, "
                "  is_builtin  = excluded.is_builtin OR templates.is_builtin, "
                "  updated_at  = datetime('now')",
                (project_id, name, description, json.dumps(tiers), is_builtin_flag),
            )
            cursor = await db.execute(
                "SELECT id FROM templates WHERE project_id = ? AND name = ?",
                (project_id, name),
            )
            row = await cursor.fetchone()
            if row is None:
                raise RuntimeError("Failed to read back saved template row")
            template_id = int(row["id"])

            for platform_name, config in (platforms or {}).items():
                if platform_name not in ALL_PLATFORMS:
                    continue
                await _set_builtin_slot(
                    template_id,
                    platform_name,
                    body=str(config.get("template", "")),
                    media=str(config.get("media", "thumbnail")),
                    max_chars=int(config.get("max_chars", 500)),
                )
    except aiosqlite.IntegrityError as exc:
        raise ValueError(str(exc)) from exc

    saved = await get_template(name, project_id=project_id)
    if saved is None:
        raise RuntimeError("Saved template disappeared between write and read")
    return saved


async def delete_template(name: str, *, project_id: int) -> None:
    """Delete a template within a project. Refuses to delete built-in
    templates by name."""
    if name in BUILTIN_TEMPLATE_NAMES:
        raise ValueError(f"Cannot delete built-in template '{name}'")
    async with write_transaction() as db:
        await db.execute(
            "DELETE FROM templates WHERE project_id = ? AND name = ?",
            (project_id, name),
        )


async def duplicate_template(
    source_name: str, new_name: str, *, project_id: int
) -> dict:
    """Create ``new_name`` as a deep copy of ``source_name`` within a project.

    Every slot is copied verbatim — built-in slots stay built-in, disabled
    slots stay disabled, account bindings and order are preserved — but the
    new *template* is never marked built-in (that flag is reserved for the
    two protected names, and a copy is always deletable). Raises
    :class:`ValueError` if the source is missing, the target name is taken,
    or the target name collides with a reserved built-in name.
    """
    new_name = (new_name or "").strip()
    if not new_name:
        raise ValueError("New template name is required")
    if new_name in BUILTIN_TEMPLATE_NAMES:
        raise ValueError(f"'{new_name}' is a reserved built-in template name")

    source = await get_template(source_name, project_id=project_id)
    if source is None:
        raise ValueError(f"Template '{source_name}' not found")

    # Whole copy (name-collision check + template INSERT + every slot) is one
    # atomic critical section — no network awaits here.
    async with write_transaction() as db:
        cursor = await db.execute(
            "SELECT id FROM templates WHERE project_id = ? AND name = ?",
            (project_id, new_name),
        )
        if await cursor.fetchone() is not None:
            raise ValueError(f"A template named '{new_name}' already exists")

        # Copy test_variables along with the rest of the template so the
        # duplicate opens in the editor with the same Preview fixtures the
        # source had. An empty dict from the source stays NULL on the copy.
        source_test_vars = source.get("test_variables") or {}
        test_vars_json = json.dumps(source_test_vars) if source_test_vars else None
        try:
            cursor = await db.execute(
                "INSERT INTO templates "
                "(project_id, name, description, applies_to, is_builtin, test_variables) "
                "VALUES (?, ?, ?, ?, 0, ?)",
                (
                    project_id, new_name, source["description"],
                    json.dumps(source["applies_to"]), test_vars_json,
                ),
            )
        except aiosqlite.IntegrityError as exc:
            raise ValueError(str(exc)) from exc
        new_template_id = int(cursor.lastrowid)

        for slot in source["slots"]:
            await db.execute(
                "INSERT INTO template_slots "
                "(template_id, platform, social_account_id, is_builtin, is_disabled, "
                " order_index, body, media, max_chars) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    new_template_id,
                    slot["platform"],
                    slot["social_account_id"],
                    1 if slot["is_builtin"] else 0,
                    1 if slot["is_disabled"] else 0,
                    int(slot["order_index"]),
                    slot["body"],
                    slot["media"],
                    int(slot["max_chars"]),
                ),
            )

    copied = await get_template(new_name, project_id=project_id)
    if copied is None:
        raise RuntimeError("Duplicated template disappeared between write and read")
    return copied


# --- Slot CRUD --------------------------------------------------------------


_UNCHANGED = object()


async def get_slot(slot_id: int) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT s.*, a.uuid AS account_uuid, a.username AS account_username, "
        "       a.platform AS account_platform, a.deleted_at AS account_deleted_at "
        "FROM template_slots s "
        "LEFT JOIN social_accounts a ON a.id = s.social_account_id "
        "WHERE s.id = ?",
        (slot_id,),
    )
    row = await cursor.fetchone()
    if row is None:
        return None
    slot = _slot_to_dict(row)
    if row["account_uuid"] is not None:
        slot["resolved_account"] = {
            "uuid": row["account_uuid"],
            "username": row["account_username"],
            "platform": row["account_platform"],
            "deleted": row["account_deleted_at"] is not None,
        }
    else:
        slot["resolved_account"] = None
    return slot


async def list_slots(template_id: int) -> list[dict]:
    """Public wrapper around the internal _list_slots helper."""
    return await _list_slots(template_id)


async def add_slot(
    template_id: int,
    platform: str,
    *,
    body: str = "",
    media: str = "thumbnail",
    max_chars: int = 500,
    social_account_id: int | None = None,
    is_disabled: bool = False,
    order_index: int | None = None,
) -> dict:
    """Add a non-builtin slot to an existing template.

    Slots created here are always ``is_builtin = 0``; built-ins are
    created exclusively by :func:`save_template` /
    :func:`ensure_default_template` for the seeded templates.
    """
    if platform not in ALL_PLATFORMS:
        raise ValueError(f"Unknown platform: {platform}")
    if max_chars < 1:
        raise ValueError("max_chars must be positive")

    # Existence check + next-order-index read + INSERT are one atomic section
    # so two concurrent add_slot calls can't pick the same order_index.
    async with write_transaction() as db:
        cursor = await db.execute(
            "SELECT id FROM templates WHERE id = ?", (template_id,)
        )
        if await cursor.fetchone() is None:
            raise ValueError(f"Template {template_id} not found")

        if order_index is None:
            cursor = await db.execute(
                "SELECT COALESCE(MAX(order_index), -1) + 1 AS next "
                "FROM template_slots WHERE template_id = ?",
                (template_id,),
            )
            next_row = await cursor.fetchone()
            order_index = int(next_row["next"]) if next_row else 0

        cursor = await db.execute(
            "INSERT INTO template_slots "
            "(template_id, platform, social_account_id, is_builtin, is_disabled, "
            " order_index, body, media, max_chars) "
            "VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?)",
            (
                template_id, platform, social_account_id,
                1 if is_disabled else 0, int(order_index),
                body, media, int(max_chars),
            ),
        )
    slot = await get_slot(int(cursor.lastrowid))
    if slot is None:
        raise RuntimeError("Saved slot disappeared between write and read")
    return slot


async def update_slot(
    slot_id: int,
    *,
    body: object = _UNCHANGED,
    media: object = _UNCHANGED,
    max_chars: object = _UNCHANGED,
    social_account_id: object = _UNCHANGED,
    is_disabled: object = _UNCHANGED,
    order_index: object = _UNCHANGED,
) -> dict:
    """Update one or more fields on an existing slot.

    Pass the sentinel ``_UNCHANGED`` (or omit the kwarg) to leave a field
    untouched. ``social_account_id`` can be set to ``None`` to clear the
    binding without removing the slot.

    The slot's ``platform`` and ``is_builtin`` flag are immutable.
    """
    existing = await get_slot(slot_id)
    if existing is None:
        raise ValueError(f"Slot {slot_id} not found")

    updates: list[str] = []
    params: list = []
    if body is not _UNCHANGED:
        updates.append("body = ?")
        params.append(str(body))
    if media is not _UNCHANGED:
        updates.append("media = ?")
        params.append(str(media))
    if max_chars is not _UNCHANGED:
        try:
            n = int(max_chars)  # type: ignore[arg-type]
        except (TypeError, ValueError) as exc:
            raise ValueError("max_chars must be an integer") from exc
        if n < 1:
            raise ValueError("max_chars must be positive")
        updates.append("max_chars = ?")
        params.append(n)
    if social_account_id is not _UNCHANGED:
        updates.append("social_account_id = ?")
        params.append(
            int(social_account_id) if social_account_id is not None else None
        )
    if is_disabled is not _UNCHANGED:
        updates.append("is_disabled = ?")
        params.append(1 if is_disabled else 0)
    if order_index is not _UNCHANGED:
        updates.append("order_index = ?")
        params.append(int(order_index))  # type: ignore[arg-type]

    if not updates:
        return existing

    updates.append("updated_at = datetime('now')")
    params.append(slot_id)

    async with write_transaction() as db:
        await db.execute(
            f"UPDATE template_slots SET {', '.join(updates)} WHERE id = ?",
            params,
        )

    refreshed = await get_slot(slot_id)
    if refreshed is None:
        raise RuntimeError("Updated slot disappeared after write")
    return refreshed


async def delete_slot(slot_id: int) -> None:
    """Delete a non-builtin slot. Built-in slots are protected — disable
    them via :func:`update_slot` with ``is_disabled=True`` instead."""
    existing = await get_slot(slot_id)
    if existing is None:
        raise ValueError(f"Slot {slot_id} not found")
    if existing["is_builtin"]:
        raise ValueError("Cannot delete a built-in slot — disable it instead")
    async with write_transaction() as db:
        await db.execute("DELETE FROM template_slots WHERE id = ?", (slot_id,))


async def ensure_default_template(project_id: int = 1) -> None:
    # NOTE: project_id retains a default of 1 because this is invoked at
    # app startup before any project context exists; the bootstrap path
    # always seeds the default project.
    """Create the two built-in templates within a project if they don't
    already exist."""
    for tpl in (DEFAULT_TEMPLATE, DEFAULT_NEW_MESSAGE_TEMPLATE):
        existing = await get_template(tpl["name"], project_id=project_id)
        if existing is not None:
            continue
        await save_template(
            tpl["name"],
            tpl["description"],
            tpl["platforms"],
            project_id=project_id,
        )
