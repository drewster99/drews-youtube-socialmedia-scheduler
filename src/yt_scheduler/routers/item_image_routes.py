"""Item images — additional images attached to an item.

These are referenced from template bodies via the media directives:
``{{image:shortname}}`` (specific image) or ``{{image:*}}`` (all of them,
in ``order_index`` order). Each upload requires a unique shortname per
item plus an optional alt text. Validated app-side.
"""

from __future__ import annotations

import re
import shutil

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from yt_scheduler.config import UPLOAD_DIR
from yt_scheduler.database import get_db

router = APIRouter(prefix="/api/videos/{video_id}/images", tags=["item-images"])

_SHORTNAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]*$")


def _validate_shortname(value: str) -> str:
    if not isinstance(value, str) or not _SHORTNAME_PATTERN.match(value):
        raise HTTPException(
            400,
            "shortname must match [a-z0-9][a-z0-9-]* (lowercase letters, "
            "digits, hyphens; can't start with a hyphen).",
        )
    return value


async def _ensure_video_exists(video_id: str) -> None:
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT 1 FROM videos WHERE id = ?", (video_id,)
    )
    if not rows:
        raise HTTPException(404, f"Video '{video_id}' not found")


@router.get("")
async def list_item_images(video_id: str) -> list[dict]:
    await _ensure_video_exists(video_id)
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, video_id, shortname, path, alt_text, order_index, created_at "
        "FROM item_images WHERE video_id = ? ORDER BY order_index, id",
        (video_id,),
    )
    return [dict(r) for r in rows]


@router.post("")
async def upload_item_image(
    video_id: str,
    file: UploadFile = File(...),
    shortname: str = Form(...),
    alt_text: str = Form(""),
    order_index: int = Form(0),
) -> dict:
    """Attach an image to an item. ``shortname`` must be unique per item."""
    await _ensure_video_exists(video_id)
    _validate_shortname(shortname)

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    # Filename collision protection: prefix with the video id and
    # shortname so two uploads with identical filenames don't clobber.
    safe_name = f"{video_id}__{shortname}__{file.filename or 'image'}"
    dest = UPLOAD_DIR / safe_name
    with open(dest, "wb") as out:
        shutil.copyfileobj(file.file, out)

    db = await get_db()
    try:
        cursor = await db.execute(
            """INSERT INTO item_images
                   (video_id, shortname, path, alt_text, order_index)
            VALUES (?, ?, ?, ?, ?)""",
            (video_id, shortname, str(dest), alt_text, int(order_index)),
        )
        await db.commit()
    except Exception as exc:
        # Most likely a UNIQUE(video_id, shortname) violation; surface as 400
        # so the UI can prompt the user to pick a different shortname.
        # Roll back the file too — no point keeping an orphan bytestream.
        try:
            dest.unlink(missing_ok=True)
        except Exception:
            pass
        raise HTTPException(400, f"Could not save image: {exc}") from exc

    rows = await db.execute_fetchall(
        "SELECT id, video_id, shortname, path, alt_text, order_index, created_at "
        "FROM item_images WHERE id = ?",
        (cursor.lastrowid,),
    )
    return dict(rows[0])


@router.patch("/{image_id}")
async def update_item_image(video_id: str, image_id: int, payload: dict) -> dict:
    """Update mutable fields on an existing image: ``shortname``, ``alt_text``,
    ``order_index``. Pass any subset; missing keys keep their current value.
    The image file itself is immutable — delete + re-upload to replace."""
    await _ensure_video_exists(video_id)
    if not isinstance(payload, dict) or not payload:
        raise HTTPException(400, "Body must include at least one field.")

    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id FROM item_images WHERE id = ? AND video_id = ?",
        (image_id, video_id),
    )
    if not rows:
        raise HTTPException(404, f"Image {image_id} not found on video {video_id}")

    updates: list[str] = []
    params: list = []
    if "shortname" in payload:
        new_short = _validate_shortname(payload["shortname"])
        updates.append("shortname = ?")
        params.append(new_short)
    if "alt_text" in payload:
        if not isinstance(payload["alt_text"], str):
            raise HTTPException(400, "alt_text must be a string.")
        updates.append("alt_text = ?")
        params.append(payload["alt_text"])
    if "order_index" in payload:
        try:
            params.append(int(payload["order_index"]))
        except (TypeError, ValueError) as exc:
            raise HTTPException(400, "order_index must be an integer.") from exc
        updates.append("order_index = ?")
    if not updates:
        raise HTTPException(400, "No recognised fields to update.")

    params.append(image_id)
    try:
        await db.execute(
            f"UPDATE item_images SET {', '.join(updates)} WHERE id = ?", params
        )
        await db.commit()
    except Exception as exc:
        raise HTTPException(400, f"Could not update image: {exc}") from exc

    rows = await db.execute_fetchall(
        "SELECT id, video_id, shortname, path, alt_text, order_index, created_at "
        "FROM item_images WHERE id = ?",
        (image_id,),
    )
    return dict(rows[0])


@router.delete("/{image_id}")
async def delete_item_image(video_id: str, image_id: int) -> dict:
    await _ensure_video_exists(video_id)
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT path FROM item_images WHERE id = ? AND video_id = ?",
        (image_id, video_id),
    )
    if not rows:
        raise HTTPException(404, f"Image {image_id} not found on video {video_id}")

    await db.execute(
        "DELETE FROM item_images WHERE id = ? AND video_id = ?",
        (image_id, video_id),
    )
    await db.commit()
    return {"status": "ok"}
