"""Custom global variables — install-wide key/value pairs.

These are the lowest-priority layer of the four-level inheritance:
``global -> project -> parent -> self``. A typical use case is a
default sign-off line (``{{signoff}}``) that gets reused across every
project unless overridden.
"""

from __future__ import annotations

import re

from fastapi import APIRouter, HTTPException

from yt_scheduler.database import get_db

router = APIRouter(prefix="/api/global-variables", tags=["global-variables"])

_KEY_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def _validate_key(key: str) -> str:
    """Enforce ``[a-z][a-z0-9_]*`` so template authors can't write keys that
    won't match the renderer's variable pattern."""
    if not isinstance(key, str) or not _KEY_PATTERN.match(key):
        raise HTTPException(
            400,
            "key must match [a-z][a-z0-9_]* (lowercase letter, then letters / "
            "digits / underscores).",
        )
    return key


@router.get("")
async def list_globals() -> list[dict]:
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, key, value, created_at, updated_at FROM global_variables "
        "ORDER BY key"
    )
    return [dict(r) for r in rows]


@router.put("/{key}")
async def upsert_global(key: str, payload: dict) -> dict:
    """Set the value for ``key``. Body: ``{"value": "..."}``."""
    _validate_key(key)
    if not isinstance(payload, dict) or "value" not in payload:
        raise HTTPException(400, "Body must include 'value'.")
    value = payload.get("value")
    if not isinstance(value, str):
        raise HTTPException(400, "'value' must be a string.")
    db = await get_db()
    await db.execute(
        "INSERT INTO global_variables (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET "
        "  value = excluded.value, updated_at = datetime('now')",
        (key, value),
    )
    await db.commit()
    rows = await db.execute_fetchall(
        "SELECT id, key, value, created_at, updated_at FROM global_variables "
        "WHERE key = ?",
        (key,),
    )
    return dict(rows[0])


@router.delete("/{key}")
async def delete_global(key: str) -> dict:
    db = await get_db()
    await db.execute("DELETE FROM global_variables WHERE key = ?", (key,))
    await db.commit()
    return {"status": "ok"}
