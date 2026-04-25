"""Per-video event log helpers.

Every state transition the user can observe in the right-sidebar Log panel goes
through ``record_event``. The payload shape is event-type specific:

* ``created``               — ``{"tier": str | null}``
* ``imported``              — ``{"source": "youtube", "tier": str | null}``
* ``uploaded``              — ``{"platform": "youtube", "url": str}``
* ``metadata_updated``      — ``{<field>: {"old": Any, "new": Any}, ...}`` only
                              for changed fields. Field names are user-visible
                              (title, description, tags, privacy, transcript).
* ``publish_scheduled``     — ``{"platform": "youtube", "publish_at": iso, "url": str}``
* ``published``             — ``{"platform": "youtube", "url": str}``
* ``social_post_scheduled`` — ``{"platform": str, "social_account_id": int | null,
                                 "scheduled_at": iso, "text": str}``
* ``social_post_published`` — ``{"platform": str, "social_account_id": int | null,
                                 "post_url": str, "posted_at": iso}``
"""

from __future__ import annotations

import json
from typing import Any, Iterable

from yt_scheduler.database import get_db

EventType = str  # See module docstring for the canonical set.


async def record_event(video_id: str, type: EventType, payload: dict | None = None) -> int:
    """Record a single event row; returns its id."""
    db = await get_db()
    cursor = await db.execute(
        "INSERT INTO video_events (video_id, type, payload_json) VALUES (?, ?, ?)",
        (video_id, type, json.dumps(payload or {})),
    )
    await db.commit()
    return int(cursor.lastrowid)


async def list_events_for_video(video_id: str, limit: int = 200) -> list[dict]:
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, video_id, type, payload_json, created_at FROM video_events "
        "WHERE video_id = ? ORDER BY created_at DESC, id DESC LIMIT ?",
        (video_id, limit),
    )
    return [_row_to_dict(row) for row in rows]


async def list_recent_events(limit: int = 7) -> list[dict]:
    """Newest-first events across all videos/projects, joined with the project."""
    db = await get_db()
    rows = await db.execute_fetchall(
        """
        SELECT e.id, e.video_id, e.type, e.payload_json, e.created_at,
               v.title AS video_title, v.project_id, p.name AS project_name,
               p.slug AS project_slug
        FROM video_events e
        JOIN videos v ON v.id = e.video_id
        JOIN projects p ON p.id = v.project_id
        ORDER BY e.created_at DESC, e.id DESC
        LIMIT ?
        """,
        (limit,),
    )
    return [_row_to_dict(row) for row in rows]


def diff_payload(old: dict, new: dict, fields: Iterable[str]) -> dict[str, dict[str, Any]]:
    """Build a ``metadata_updated`` payload covering only changed fields.

    Both sides are compared with ``!=`` after JSON-normalising lists/dicts so
    callers don't have to think about list-vs-tuple or dict-key ordering.
    """
    payload: dict[str, dict[str, Any]] = {}
    for field in fields:
        before = _normalise(old.get(field))
        after = _normalise(new.get(field))
        if before != after:
            payload[field] = {"old": before, "new": after}
    return payload


def _normalise(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        return [_normalise(v) for v in value]
    if isinstance(value, dict):
        return {k: _normalise(value[k]) for k in sorted(value)}
    return value


def _row_to_dict(row) -> dict:
    data = dict(row)
    payload = data.pop("payload_json", None) or "{}"
    try:
        data["payload"] = json.loads(payload)
    except json.JSONDecodeError:
        data["payload"] = {}
    return data
