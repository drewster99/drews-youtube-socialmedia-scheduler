"""G3 — thumbnail-compare verdict cache + HEAD precheck.

* Same (local_sha, youtube_sha) pair as last time → skip the Claude
  vision call entirely.
* HEAD precheck: when the recorded ETag matches what i.ytimg.com
  reports now, skip the GET (and the SHA cache short-circuits the
  AI call once the bytes line up).
* New bytes invalidate the cached verdict so the next compare runs.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
async def db_setup(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    db_module = importlib.import_module("yt_scheduler.database")
    db = await db_module.get_db()
    yield db, tmp_path
    await db_module.close_db()


async def _seed_video_with_thumbs(db, tmp_path: Path, user_bytes: bytes, yt_bytes: bytes) -> str:
    """Create a row with thumbnail_source='user' and both files on disk."""
    user_path = tmp_path / "uploads" / "user_thumb.jpg"
    yt_path = tmp_path / "uploads" / "yt_thumb.jpg"
    user_path.write_bytes(user_bytes)
    yt_path.write_bytes(yt_bytes)
    await db.execute(
        "INSERT INTO videos (id, project_id, title, status, thumbnail_path, "
        "thumbnail_source, youtube_thumbnail_path, youtube_thumbnail_url, "
        "imported_from_youtube) "
        "VALUES (?, 1, 't', 'uploaded', ?, 'user', ?, ?, 1)",
        ("CACHE0001", str(user_path), str(yt_path), "https://i.ytimg.com/vi/CACHE0001/maxres.jpg"),
    )
    await db.commit()
    return "CACHE0001"


@pytest.mark.asyncio
async def test_sha_cache_skips_ai_when_bytes_unchanged(
    db_setup, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If both shas match the stored pair AND verdict is non-NULL,
    ai.compare_thumbnails must not be called."""
    db, tmp_path = db_setup
    video_id = await _seed_video_with_thumbs(
        db, tmp_path,
        user_bytes=b"USER_BYTES_FIXED",
        yt_bytes=b"YT_BYTES_FIXED",
    )

    from yt_scheduler.services import thumbnail_sync, ai
    # Pre-seed the cache: same shas as the bytes on disk + verdict.
    import hashlib
    user_sha = hashlib.sha256(b"USER_BYTES_FIXED").hexdigest()
    yt_sha = hashlib.sha256(b"YT_BYTES_FIXED").hexdigest()
    await db.execute(
        "UPDATE videos SET thumbnail_compare_verdict = 'different', "
        "thumbnail_compare_local_sha = ?, "
        "thumbnail_compare_youtube_sha = ? WHERE id = ?",
        (user_sha, yt_sha, video_id),
    )
    await db.commit()

    call_count = {"n": 0}

    async def fake_compare(a, b):
        call_count["n"] += 1
        return "different"

    monkeypatch.setattr(ai, "compare_thumbnails", fake_compare)

    yt_payload = {"snippet": {"thumbnails": {
        "maxres": {"url": "https://i.ytimg.com/vi/CACHE0001/maxres.jpg"},
    }}}
    await thumbnail_sync.maybe_refresh_youtube_thumbnail(video_id, yt_payload)

    assert call_count["n"] == 0, "SHA cache must short-circuit the AI call"


@pytest.mark.asyncio
async def test_sha_cache_runs_ai_when_local_bytes_changed(
    db_setup, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The user uploaded a new thumbnail (different local sha) — the
    AI call must run to compute a fresh verdict.

    Seed an ETag matching what the (mocked) CDN returns on HEAD so
    the precheck confirms "bytes haven't changed on YouTube" and we
    skip the GET, falling straight through to the SHA compare which
    is the path under test."""
    db, tmp_path = db_setup
    video_id = await _seed_video_with_thumbs(
        db, tmp_path,
        user_bytes=b"USER_BYTES_NEW",      # ← differs from cached sha
        yt_bytes=b"YT_BYTES_FIXED",
    )

    import hashlib
    stale_user_sha = hashlib.sha256(b"USER_BYTES_OLD").hexdigest()
    yt_sha = hashlib.sha256(b"YT_BYTES_FIXED").hexdigest()
    await db.execute(
        "UPDATE videos SET thumbnail_compare_verdict = 'same', "
        "thumbnail_compare_local_sha = ?, "
        "thumbnail_compare_youtube_sha = ?, "
        "youtube_thumbnail_etag = 'etag-stable' WHERE id = ?",
        (stale_user_sha, yt_sha, video_id),
    )
    await db.commit()

    class FakeHeadResp:
        status_code = 200
        headers = {"etag": "etag-stable"}

    class FakeAsyncClient:
        def __init__(self, *a, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def head(self, url, **kwargs):
            return FakeHeadResp()
        async def get(self, url, **kwargs):
            raise AssertionError("HEAD should have prevented this GET")

    from yt_scheduler.services import thumbnail_sync, ai
    monkeypatch.setattr(thumbnail_sync.httpx, "AsyncClient", FakeAsyncClient)
    call_count = {"n": 0}

    async def fake_compare(a, b):
        call_count["n"] += 1
        return "different"

    monkeypatch.setattr(ai, "compare_thumbnails", fake_compare)

    yt_payload = {"snippet": {"thumbnails": {
        "maxres": {"url": "https://i.ytimg.com/vi/CACHE0001/maxres.jpg"},
    }}}
    await thumbnail_sync.maybe_refresh_youtube_thumbnail(video_id, yt_payload)

    assert call_count["n"] == 1, "Stale local sha must force a re-compare"

    # And the new verdict + shas are persisted.
    rows = await db.execute_fetchall(
        "SELECT thumbnail_compare_verdict, thumbnail_compare_local_sha "
        "FROM videos WHERE id = ?", (video_id,),
    )
    new_user_sha = hashlib.sha256(b"USER_BYTES_NEW").hexdigest()
    assert rows[0][0] == "different"
    assert rows[0][1] == new_user_sha


@pytest.mark.asyncio
async def test_head_precheck_skips_download_when_etag_matches(
    db_setup, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the recorded ETag still matches what HEAD returns, the
    GET is skipped — no new bytes downloaded, no AI call triggered
    (the SHA cache already covers the verdict)."""
    db, tmp_path = db_setup
    video_id = await _seed_video_with_thumbs(
        db, tmp_path,
        user_bytes=b"U",
        yt_bytes=b"Y",
    )
    await db.execute(
        "UPDATE videos SET youtube_thumbnail_etag = 'etag-fixed-1' WHERE id = ?",
        (video_id,),
    )
    # Pre-cache an AI verdict so we know the AI path won't run after the
    # HEAD path confirms unchanged bytes.
    import hashlib
    await db.execute(
        "UPDATE videos SET thumbnail_compare_verdict = 'same', "
        "thumbnail_compare_local_sha = ?, "
        "thumbnail_compare_youtube_sha = ? WHERE id = ?",
        (
            hashlib.sha256(b"U").hexdigest(),
            hashlib.sha256(b"Y").hexdigest(),
            video_id,
        ),
    )
    await db.commit()

    head_calls = {"n": 0}
    get_calls = {"n": 0}

    class FakeHeadResp:
        status_code = 200
        headers = {"etag": "etag-fixed-1"}

    class FakeAsyncClient:
        def __init__(self, *a, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def head(self, url, **kwargs):
            head_calls["n"] += 1
            return FakeHeadResp()
        async def get(self, url, **kwargs):
            get_calls["n"] += 1
            raise AssertionError("HEAD path should have prevented this GET")

    from yt_scheduler.services import thumbnail_sync, ai
    monkeypatch.setattr(thumbnail_sync.httpx, "AsyncClient", FakeAsyncClient)

    async def fake_compare(a, b):
        raise AssertionError("SHA cache should have short-circuited the AI call")
    monkeypatch.setattr(ai, "compare_thumbnails", fake_compare)

    yt_payload = {"snippet": {"thumbnails": {
        "maxres": {"url": "https://i.ytimg.com/vi/CACHE0001/maxres.jpg"},
    }}}
    await thumbnail_sync.maybe_refresh_youtube_thumbnail(video_id, yt_payload)

    assert head_calls["n"] == 1
    assert get_calls["n"] == 0
