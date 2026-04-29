"""Custom item variables — per-item key/value pairs.

Highest-priority layer of the four-level inheritance: a child item with
its own key always wins over project / global / parent values for that
key. Empty list when the item has none.
"""

from __future__ import annotations

import re

from fastapi import APIRouter, HTTPException

from yt_scheduler.database import get_db

router = APIRouter(
    prefix="/api/videos/{video_id}/variables", tags=["item-variables"]
)

_KEY_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def _validate_key(key: str) -> str:
    if not isinstance(key, str) or not _KEY_PATTERN.match(key):
        raise HTTPException(
            400,
            "key must match [a-z][a-z0-9_]* (lowercase letter, then letters / "
            "digits / underscores).",
        )
    return key


async def _ensure_video_exists(video_id: str) -> None:
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT 1 FROM videos WHERE id = ?", (video_id,)
    )
    if not rows:
        raise HTTPException(404, f"Video '{video_id}' not found")


@router.get("")
async def list_item_variables(video_id: str) -> list[dict]:
    await _ensure_video_exists(video_id)
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, key, value, created_at, updated_at FROM item_variables "
        "WHERE video_id = ? ORDER BY key",
        (video_id,),
    )
    return [dict(r) for r in rows]


@router.put("/{key}")
async def upsert_item_variable(video_id: str, key: str, payload: dict) -> dict:
    await _ensure_video_exists(video_id)
    _validate_key(key)
    if not isinstance(payload, dict) or "value" not in payload:
        raise HTTPException(400, "Body must include 'value'.")
    value = payload.get("value")
    if not isinstance(value, str):
        raise HTTPException(400, "'value' must be a string.")
    db = await get_db()
    await db.execute(
        "INSERT INTO item_variables (video_id, key, value) VALUES (?, ?, ?) "
        "ON CONFLICT(video_id, key) DO UPDATE SET "
        "  value = excluded.value, updated_at = datetime('now')",
        (video_id, key, value),
    )
    await db.commit()
    rows = await db.execute_fetchall(
        "SELECT id, key, value, created_at, updated_at FROM item_variables "
        "WHERE video_id = ? AND key = ?",
        (video_id, key),
    )
    return dict(rows[0])


@router.delete("/{key}")
async def delete_item_variable(video_id: str, key: str) -> dict:
    await _ensure_video_exists(video_id)
    db = await get_db()
    await db.execute(
        "DELETE FROM item_variables WHERE video_id = ? AND key = ?",
        (video_id, key),
    )
    await db.commit()
    return {"status": "ok"}
