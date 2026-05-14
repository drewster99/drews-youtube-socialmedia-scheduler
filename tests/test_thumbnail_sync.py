"""Dual-thumbnail tracking + Claude-vision compare.

Verifies the migration backfill, the URL-change → fetch → compare
flow inside ``thumbnail_sync.maybe_refresh_youtube_thumbnail``, and
the two user-choice endpoints (``use-youtube`` / ``push-to-youtube``).
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import aiosqlite
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


@pytest.mark.asyncio
async def test_migration_018_backfills_thumbnail_source(tmp_path: Path) -> None:
    """Imported rows → source='youtube' AND youtube_thumbnail_path =
    thumbnail_path; non-imported rows → source='user'.

    The ALTER TABLE columns are added by apply_migrations earlier in
    the chain, so we run migrations through 017, seed pre-018 rows,
    then run only the 018 SQL (the migrations runner records its
    checksum, so a second apply_migrations would skip it — we drive
    the SQL directly instead)."""
    db_path = tmp_path / "p.db"
    async with aiosqlite.connect(str(db_path)) as conn:
        from yt_scheduler.migrations import MIGRATIONS_DIR, discover_migrations

        # Apply every migration BEFORE 018 by hand so the table exists
        # without the new columns yet.
        for mig in discover_migrations(MIGRATIONS_DIR):
            if mig.version >= 18:
                break
            await conn.executescript(mig.path.read_text())
        await conn.commit()

        await conn.execute(
            "INSERT INTO videos (id, title, status, thumbnail_path, imported_from_youtube) "
            "VALUES ('imp1', 't', 'uploaded', '/tmp/imp1.jpg', 1)"
        )
        await conn.execute(
            "INSERT INTO videos (id, title, status, thumbnail_path, imported_from_youtube) "
            "VALUES ('up1', 't', 'uploaded', '/tmp/up1.jpg', 0)"
        )
        await conn.commit()

        sql_018 = (MIGRATIONS_DIR / "018_dual_thumbnail.sql").read_text()
        await conn.executescript(sql_018)
        await conn.commit()

        cur = await conn.execute(
            "SELECT id, thumbnail_source, youtube_thumbnail_path FROM videos ORDER BY id"
        )
        rows = await cur.fetchall()
        assert rows[0][0] == "imp1"
        assert rows[0][1] == "youtube"
        assert rows[0][2] == "/tmp/imp1.jpg"
        assert rows[1][0] == "up1"
        assert rows[1][1] == "user"
        assert rows[1][2] is None


@pytest.mark.asyncio
async def test_refresh_downloads_new_thumbnail_and_runs_compare(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """End-to-end: user has local thumbnail, YouTube URL changes, we
    download it, run compare, and the verdict lands on the row."""
    from yt_scheduler.database import get_db
    db = await get_db()

    # Seed a row with a 'user' thumbnail and no YouTube-side knowledge.
    user_thumb = tmp_path / "uploads" / "user_thumb.jpg"
    user_thumb.write_bytes(b"USER_THUMB_BYTES")
    await db.execute(
        "INSERT INTO videos (id, title, status, thumbnail_path, thumbnail_source, "
        " imported_from_youtube) VALUES (?, ?, 'uploaded', ?, 'user', 1)",
        ("THUMBT00001", "t", str(user_thumb)),
    )
    await db.commit()

    fake_yt_url = "https://i.ytimg.com/vi/THUMBT00001/maxres.jpg"
    fake_yt_bytes = b"YOUTUBE_NEW_BYTES"

    # httpx client patch — never hit the network.
    class FakeResp:
        content = fake_yt_bytes
        # G3: the GET branch now reads resp.headers for the ETag.
        # Empty dict means "no ETag from this CDN response," which the
        # code handles by storing NULL — fine for this test.
        headers: dict = {}
        def raise_for_status(self):
            pass
    class FakeAsyncClient:
        def __init__(self, *a, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *exc): return False
        async def get(self, url, **kwargs):
            # G3: the production code now passes follow_redirects=True.
            assert url == fake_yt_url
            return FakeResp()
        async def head(self, url, **kwargs):
            # G3: HEAD precheck. This test seeds no etag and a brand-new
            # row, so url_changed=True drives the GET branch directly —
            # head() shouldn't actually be reached here, but we declare
            # it so the mock class is a complete httpx.AsyncClient stand-in.
            class _HeadResp:
                status_code = 200
                headers: dict = {}
            return _HeadResp()

    from yt_scheduler.services import thumbnail_sync, ai as ai_service
    monkeypatch.setattr(thumbnail_sync.httpx, "AsyncClient", FakeAsyncClient)

    async def fake_compare(a, b):
        assert a == b"USER_THUMB_BYTES"
        assert b == fake_yt_bytes
        return "different"
    monkeypatch.setattr(ai_service, "compare_thumbnails", fake_compare)

    yt_payload = {
        "snippet": {
            "thumbnails": {
                "maxres": {"url": fake_yt_url},
                "high": {"url": "https://i.ytimg.com/vi/THUMBT00001/hq.jpg"},
            }
        },
    }
    await thumbnail_sync.maybe_refresh_youtube_thumbnail("THUMBT00001", yt_payload)

    rows = await db.execute_fetchall(
        "SELECT youtube_thumbnail_path, youtube_thumbnail_url, "
        "thumbnail_compare_verdict, thumbnail_compared_at "
        "FROM videos WHERE id = ?",
        ("THUMBT00001",),
    )
    yt_path, yt_url, verdict, compared_at = rows[0]
    assert yt_path is not None and Path(yt_path).exists()
    assert Path(yt_path).read_bytes() == fake_yt_bytes
    assert yt_url == fake_yt_url
    assert verdict == "different"
    assert compared_at is not None


@pytest.mark.asyncio
async def test_use_youtube_endpoint_promotes_path(
    client: TestClient, tmp_path: Path,
) -> None:
    """POST /api/videos/{id}/thumbnail/use-youtube promotes the cached
    YouTube path to be the active thumbnail."""
    from yt_scheduler.database import get_db
    db = await get_db()
    user_thumb = tmp_path / "uploads" / "user.jpg"
    yt_thumb = tmp_path / "uploads" / "yt.jpg"
    user_thumb.write_bytes(b"u")
    yt_thumb.write_bytes(b"y")
    await db.execute(
        "INSERT INTO videos (id, title, status, thumbnail_path, thumbnail_source, "
        " youtube_thumbnail_path, thumbnail_compare_verdict, imported_from_youtube) "
        "VALUES (?, ?, 'uploaded', ?, 'user', ?, 'different', 1)",
        ("PROMO00001", "t", str(user_thumb), str(yt_thumb)),
    )
    await db.commit()

    resp = client.post("/api/videos/PROMO00001/thumbnail/use-youtube")
    assert resp.status_code == 200

    rows = await db.execute_fetchall(
        "SELECT thumbnail_path, thumbnail_source, thumbnail_compare_verdict "
        "FROM videos WHERE id = ?", ("PROMO00001",),
    )
    path, source, verdict = rows[0]
    assert path == str(yt_thumb)
    assert source == "youtube"
    assert verdict == "same"


@pytest.mark.asyncio
async def test_use_youtube_404s_when_no_cached_thumbnail(
    client: TestClient, tmp_path: Path,
) -> None:
    from yt_scheduler.database import get_db
    db = await get_db()
    await db.execute(
        "INSERT INTO videos (id, title, status, thumbnail_source, imported_from_youtube) "
        "VALUES ('NOTHM00001', 't', 'uploaded', 'user', 1)"
    )
    await db.commit()

    resp = client.post("/api/videos/NOTHM00001/thumbnail/use-youtube")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_no_youtube_url_in_payload_noops(
    client: TestClient, tmp_path: Path,
) -> None:
    """A video with no thumbnails on YouTube (snippet.thumbnails empty)
    short-circuits without touching the DB."""
    from yt_scheduler.database import get_db
    db = await get_db()
    await db.execute(
        "INSERT INTO videos (id, title, status, imported_from_youtube) "
        "VALUES ('NOTHM00002', 't', 'uploaded', 1)"
    )
    await db.commit()

    from yt_scheduler.services import thumbnail_sync
    await thumbnail_sync.maybe_refresh_youtube_thumbnail("NOTHM00002", {"snippet": {}})

    rows = await db.execute_fetchall(
        "SELECT youtube_thumbnail_path, youtube_thumbnail_url FROM videos WHERE id = ?",
        ("NOTHM00002",),
    )
    assert rows[0][0] is None
    assert rows[0][1] is None


@pytest.mark.asyncio
async def test_extract_youtube_thumbnail_url_prefers_maxres() -> None:
    """maxres > high > medium > default fallback order."""
    from yt_scheduler.services.thumbnail_sync import _extract_youtube_thumbnail_url
    assert _extract_youtube_thumbnail_url({
        "snippet": {"thumbnails": {
            "maxres":  {"url": "MAX"},
            "high":    {"url": "HIGH"},
            "medium":  {"url": "MED"},
            "default": {"url": "DEF"},
        }}
    }) == "MAX"
    assert _extract_youtube_thumbnail_url({
        "snippet": {"thumbnails": {"medium": {"url": "MED"}}}
    }) == "MED"
    assert _extract_youtube_thumbnail_url({"snippet": {"thumbnails": {}}}) is None
    assert _extract_youtube_thumbnail_url(None) is None
