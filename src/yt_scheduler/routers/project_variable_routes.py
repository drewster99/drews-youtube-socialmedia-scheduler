"""Custom project variables — per-project key/value pairs.

Sits between ``global_variables`` and item-level overrides in the
inheritance chain.
"""

from __future__ import annotations

import re

from fastapi import APIRouter, HTTPException

from yt_scheduler.database import get_db
from yt_scheduler.services import projects as project_service

router = APIRouter(
    prefix="/api/projects/{slug}/variables", tags=["project-variables"]
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


async def _resolve_project_id(slug: str) -> int:
    project = await project_service.get_project_by_slug(slug)
    if project is None:
        raise HTTPException(404, f"Project '{slug}' not found")
    return int(project["id"])


@router.get("")
async def list_project_variables(slug: str) -> list[dict]:
    project_id = await _resolve_project_id(slug)
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, key, value, created_at, updated_at FROM project_variables "
        "WHERE project_id = ? ORDER BY key",
        (project_id,),
    )
    return [dict(r) for r in rows]


@router.put("/{key}")
async def upsert_project_variable(slug: str, key: str, payload: dict) -> dict:
    _validate_key(key)
    if not isinstance(payload, dict) or "value" not in payload:
        raise HTTPException(400, "Body must include 'value'.")
    value = payload.get("value")
    if not isinstance(value, str):
        raise HTTPException(400, "'value' must be a string.")
    project_id = await _resolve_project_id(slug)
    db = await get_db()
    await db.execute(
        "INSERT INTO project_variables (project_id, key, value) VALUES (?, ?, ?) "
        "ON CONFLICT(project_id, key) DO UPDATE SET "
        "  value = excluded.value, updated_at = datetime('now')",
        (project_id, key, value),
    )
    await db.commit()
    rows = await db.execute_fetchall(
        "SELECT id, key, value, created_at, updated_at FROM project_variables "
        "WHERE project_id = ? AND key = ?",
        (project_id, key),
    )
    return dict(rows[0])


@router.delete("/{key}")
async def delete_project_variable(slug: str, key: str) -> dict:
    project_id = await _resolve_project_id(slug)
    db = await get_db()
    await db.execute(
        "DELETE FROM project_variables WHERE project_id = ? AND key = ?",
        (project_id, key),
    )
    await db.commit()
    return {"status": "ok"}
