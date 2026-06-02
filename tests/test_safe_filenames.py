"""Safe canonical upload filenames + original-name recording.

On-disk upload names are app-chosen; the raw client filename never
reaches the filesystem (path traversal / silent overwrites). The
original is recorded, sanitized, in videos.video_file_original_name.
"""

from __future__ import annotations

import importlib
import sqlite3
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DYS_HOST", "127.0.0.1")
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    (tmp_path / "templates").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    app_module = importlib.import_module("yt_scheduler.app")
    with TestClient(app_module.app) as c:
        yield c


def test_safe_upload_ext() -> None:
    from yt_scheduler.config import safe_upload_ext

    assert safe_upload_ext("video.mp4") == ".mp4"
    assert safe_upload_ext("CLIP.MOV") == ".mov"
    assert safe_upload_ext("a.b.c.webm") == ".webm"
    assert safe_upload_ext("a.jpg", default=".png") == ".jpg"
    # Nothing usable -> default.
    assert safe_upload_ext(None) == ".mp4"
    assert safe_upload_ext("noext") == ".mp4"
    assert safe_upload_ext("../../../../etc/passwd") == ".mp4"
    assert safe_upload_ext("x.verylongext") == ".mp4"
    assert safe_upload_ext("weird.m p4") == ".mp4"
    assert safe_upload_ext("trailingdot.") == ".mp4"


def test_sanitized_original_filename() -> None:
    from yt_scheduler.config import sanitized_original_filename

    assert sanitized_original_filename("clip.mp4") == "clip.mp4"
    # Path components stripped — both separators.
    assert sanitized_original_filename("../../../../etc/evil.mp4") == "evil.mp4"
    assert sanitized_original_filename("C:\\Users\\me\\v.mp4") == "v.mp4"
    # Nothing usable -> None.
    assert sanitized_original_filename(None) is None
    assert sanitized_original_filename("") is None
    assert sanitized_original_filename("../../") is None
    assert sanitized_original_filename("foo/..") is None
    # Absurdly long input is truncated, never stored whole.
    out = sanitized_original_filename("a" * 5000 + ".mp4", limit=120)
    assert out is not None and len(out) == 120


def test_items_upload_uses_canonical_name_and_records_original(
    client: TestClient,
) -> None:
    """A malicious filename has zero influence on the on-disk name; the
    original is recorded sanitized to a basename."""
    from yt_scheduler.config import DB_PATH, UPLOAD_DIR

    evil = "../../../../tmp/pwned_by_test.mp4"
    # Upload bytes via the chunked-upload protocol. The evil filename
    # is the one the chunked-upload service records as the "original
    # name" — the on-disk name is server-chosen.
    init = client.post(
        "/api/uploads/init", json={"filename": evil, "size": 64},
    )
    assert init.status_code == 200, init.text
    uid = init.json()["upload_id"]
    chunk = client.post(
        f"/api/uploads/{uid}/chunk/0", content=b"\x00" * 64,
        headers={"Content-Type": "application/octet-stream"},
    )
    assert chunk.status_code == 200, chunk.text
    fin = client.post(f"/api/uploads/{uid}/finalize")
    assert fin.status_code == 200, fin.text

    resp = client.post(
        "/api/videos/items",
        json={
            "title": "safe-name test", "item_type": "standalone",
            "upload_id": uid,
        },
    )
    assert resp.status_code == 200, resp.text
    video_id = resp.json()["video_id"]

    with sqlite3.connect(str(DB_PATH)) as conn:
        disk_path, original = conn.execute(
            "SELECT video_file_path, video_file_original_name "
            "FROM videos WHERE id = ?",
            (video_id,),
        ).fetchone()

    # On-disk file lives inside UPLOAD_DIR, named for the id we control —
    # the client filename contributed nothing.
    p = Path(disk_path).resolve()
    assert p.parent == UPLOAD_DIR.resolve()
    assert p.name == f"{video_id}.mp4"
    assert p.exists()
    # Original filename recorded, reduced to a basename.
    assert original == "pwned_by_test.mp4"
