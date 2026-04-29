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

from yt_scheduler.database import get_db
from yt_scheduler.services.ai import DEFAULT_AI_SYSTEM, call_ai_block
from yt_scheduler.services.social import ALL_PLATFORMS

# Default templates shipped with the app
DEFAULT_NEW_MESSAGE_TEMPLATE = {
    "name": "send_message",
    "description": "Plain user-message template — useful for one-off posts",
    "platforms": {
        "twitter":  {"template": "{{user_message}}", "media": "none", "max_chars": 280},
        "bluesky":  {"template": "{{user_message}}", "media": "none", "max_chars": 300},
        "mastodon": {"template": "{{user_message}}", "media": "none", "max_chars": 500},
        "linkedin": {"template": "{{user_message}}", "media": "none", "max_chars": 3000},
        "threads":  {"template": "{{user_message}}", "media": "none", "max_chars": 500},
    },
}


DEFAULT_TEMPLATE = {
    "name": "announce_video",
    "description": "Standard template for announcing a new video upload",
    "platforms": {
        "twitter": {
            "template": '{{ai: Write a punchy tweet announcing a YouTube video titled "{{title}}" about {{tags}}. Include the URL {{url}}. Under 280 chars. 2-3 hashtags.}}',
            "media": "thumbnail",
            "max_chars": 280,
        },
        "bluesky": {
            "template": '{{ai: Write a Bluesky post announcing my new video "{{title}}". Conversational tone, under 300 chars.}}\n\n{{url}}',
            "media": "thumbnail",
            "max_chars": 300,
        },
        "mastodon": {
            "template": 'New video is live!\n\n"{{title}}"\n\n{{url}}\n\n{{ai: Generate 3-5 CamelCase hashtags for: {{tags}}}}',
            "media": "thumbnail",
            "max_chars": 500,
        },
        "linkedin": {
            "template": '{{ai: Write a LinkedIn post (2-3 paragraphs, professional but approachable) about my new video "{{title}}". Description: {{description_short}}. End with a question.}}\n\nWatch here: {{url}}',
            "media": "thumbnail",
            "max_chars": 3000,
        },
        "threads": {
            "template": '{{ai: Write a casual Threads post announcing "{{title}}". Keep it engaging, under 500 chars.}}\n\n{{url}}',
            "media": "thumbnail",
            "max_chars": 500,
        },
    },
}


BUILTIN_TEMPLATE_NAMES = {"announce_video", "send_message"}


class MissingRequiredVariable(KeyError):
    """Raised when a ``{{name!}}`` placeholder has no value in the variables
    dict. The required marker is opt-in: plain ``{{name}}`` misses still
    render literally."""

    def __init__(self, name: str):
        self.name = name
        super().__init__(name)

    def __str__(self) -> str:
        return f"Required template variable not provided: {{{{{self.name}!}}}}"


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


# {{video}}, {{thumbnail}}, {{image:shortname}}, {{image:*}}.
# `image:*` is the wildcard; `image:<shortname>` requires lowercase
# alphanumerics + hyphens (matches the validation rule in `item_images`).
_MEDIA_DIRECTIVE_PATTERN = re.compile(
    r"\{\{(video|thumbnail|image:(?:\*|[a-z0-9-]+))\}\}"
)


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


def render(
    template_text: str,
    variables: dict[str, object] | None = None,
    *,
    default_system_prompt: str | None = DEFAULT_AI_SYSTEM,
    model: str | None = None,
    max_tokens: int = 512,
) -> str:
    """Single rendering primitive. Two passes:

    1. **Variable substitution.** Three placeholder forms:

       * ``{{name}}`` — optional. Missing key stays literal in the output
         so the user can spot typos.
       * ``{{name!}}`` — required. Missing key raises
         :class:`MissingRequiredVariable`.
       * ``{{name??default text}}`` — optional with fallback. Missing key
         renders ``default text`` (which may be empty for ``{{name??}}``).
         Default text is taken as a literal string — no recursive
         substitution inside it.

       ``ai:`` and ``ai[...]:`` openers survive this pass because ``\\w+``
       can't cross the colon or bracket.

    2. **AI block evaluation.** ``{{ai: prompt}}`` and the system-override
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
    text = _substitute_variables(template_text, variables)
    return _resolve_ai_blocks(
        text,
        default_system_prompt=default_system_prompt,
        model=model,
        max_tokens=max_tokens,
    )


# Backwards-compatible alias kept so older callers don't need a churn pass.
def render_template(template_text: str, variables: dict[str, object]) -> str:
    return render(template_text, variables)


# {{name}}, {{name!}}, or {{name??default text}}. The trailing alternation
# captures '!' (required) OR '??<default>' — never both, never neither-and-
# bare-suffix. Default text is non-greedy so it stops at the next '}}'.
_VAR_PATTERN = re.compile(r"\{\{(\w+)(?:(!)|\?\?(.*?))?\}\}")


def _substitute_variables(text: str, variables: dict[str, object]) -> str:
    def replace(match: re.Match) -> str:
        name = match.group(1)
        required = match.group(2) is not None
        default_text = match.group(3)  # None when no `??...` was present
        if name in variables:
            value = variables[name]
            return "" if value is None else str(value)
        if required:
            raise MissingRequiredVariable(name)
        if default_text is not None:
            return default_text
        return match.group(0)

    return _VAR_PATTERN.sub(replace, text)


def _resolve_ai_blocks(
    text: str,
    *,
    default_system_prompt: str | None,
    model: str | None,
    max_tokens: int,
) -> str:
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

        inner_text = text[body_start : j - 2].strip()
        prompt = _resolve_ai_blocks(
            inner_text,
            default_system_prompt=default_system_prompt,
            model=model,
            max_tokens=max_tokens,
        )
        effective_system = (
            system_override if system_override is not None else default_system_prompt
        )
        out.append(call_ai_block(
            prompt,
            system=effective_system,
            model=model,
            max_tokens=max_tokens,
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


async def get_template(name: str, project_id: int = 1) -> dict | None:
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
    }


async def list_templates(project_id: int = 1) -> list[dict]:
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
    db = await get_db()
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
    project_id: int = 1,
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

    db = await get_db()
    is_builtin_flag = 1 if name in BUILTIN_TEMPLATE_NAMES else 0
    try:
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
    except aiosqlite.IntegrityError as exc:
        raise ValueError(str(exc)) from exc
    await db.commit()

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
    await db.commit()

    saved = await get_template(name, project_id=project_id)
    if saved is None:
        raise RuntimeError("Saved template disappeared between write and read")
    return saved


async def delete_template(name: str, project_id: int = 1) -> None:
    """Delete a template within a project. Refuses to delete built-in
    templates by name."""
    if name in BUILTIN_TEMPLATE_NAMES:
        raise ValueError(f"Cannot delete built-in template '{name}'")
    db = await get_db()
    await db.execute(
        "DELETE FROM templates WHERE project_id = ? AND name = ?",
        (project_id, name),
    )
    await db.commit()


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

    db = await get_db()
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
    await db.commit()
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

    db = await get_db()
    await db.execute(
        f"UPDATE template_slots SET {', '.join(updates)} WHERE id = ?",
        params,
    )
    await db.commit()

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
    db = await get_db()
    await db.execute("DELETE FROM template_slots WHERE id = ?", (slot_id,))
    await db.commit()


async def ensure_default_template(project_id: int = 1) -> None:
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
