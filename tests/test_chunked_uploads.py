"""Chunked-upload service + HTTP endpoints.

Tests run against an isolated tmp_path data dir (DYS_DATA_DIR override +
module reload) so the user's real ~/.yt-scheduler/ is never touched.
"""

from __future__ import annotations

import importlib
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


def _init(client: TestClient, *, filename: str = "x.mp4", size: int = 32) -> dict:
    resp = client.post(
        "/api/uploads/init", json={"filename": filename, "size": size},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def _chunk(
    client: TestClient, upload_id: str, offset: int, body: bytes,
):
    return client.post(
        f"/api/uploads/{upload_id}/chunk/{offset}",
        content=body,
        headers={"Content-Type": "application/octet-stream"},
    )


# --- service-level (no HTTP layer) ----------------------------------


async def test_init_returns_upload_id_and_chunk_size(client: TestClient):
    """init returns a fresh upload_id + the server's per-chunk cap so
    the client knows how to slice the file."""
    from yt_scheduler.services import chunked_uploads as cu

    info = await cu.init_upload("master.mov", size=1024)
    assert info["upload_id"]
    assert info["chunk_size"] == cu.CHUNK_SIZE_BYTES


async def test_init_rejects_oversized_declaration(client: TestClient):
    """A declared size past the global cap is rejected up-front
    (UploadTooLarge → 413 at the HTTP layer)."""
    from yt_scheduler.services import chunked_uploads as cu

    with pytest.raises(cu.UploadTooLarge):
        await cu.init_upload("huge.mov", size=11 * 1024**3)


async def test_init_rejects_zero_size(client: TestClient):
    from yt_scheduler.services import chunked_uploads as cu

    with pytest.raises(ValueError):
        await cu.init_upload("x.mov", size=0)


async def test_append_and_finalize_round_trip(client: TestClient):
    """Append two chunks at the expected offsets, finalize, then the
    on-disk file should be the concatenation."""
    from yt_scheduler.services import chunked_uploads as cu

    info = await cu.init_upload("x.bin", size=20)
    uid = info["upload_id"]
    n = await cu.append_chunk(uid, 0, b"AAAAAAAAAA")  # 10 bytes
    assert n == 10
    n = await cu.append_chunk(uid, 10, b"BBBBBBBBBB")  # 10 bytes
    assert n == 20
    finalized = await cu.finalize_upload(uid)
    assert Path(finalized["path"]).read_bytes() == b"A" * 10 + b"B" * 10
    assert finalized["size"] == 20
    assert finalized["filename"] == "x.bin"


async def test_append_rejects_out_of_order_offset(client: TestClient):
    from yt_scheduler.services import chunked_uploads as cu

    info = await cu.init_upload("x.bin", size=10)
    uid = info["upload_id"]
    await cu.append_chunk(uid, 0, b"AAAAA")
    with pytest.raises(cu.UploadConflict):
        # offset should be 5, not 0 — would overlap.
        await cu.append_chunk(uid, 0, b"BBBBB")


async def test_append_rejects_overflow(client: TestClient):
    from yt_scheduler.services import chunked_uploads as cu

    info = await cu.init_upload("x.bin", size=10)
    uid = info["upload_id"]
    with pytest.raises(cu.UploadTooLarge):
        await cu.append_chunk(uid, 0, b"A" * 11)


async def test_finalize_refuses_incomplete(client: TestClient):
    from yt_scheduler.services import chunked_uploads as cu

    info = await cu.init_upload("x.bin", size=10)
    uid = info["upload_id"]
    await cu.append_chunk(uid, 0, b"AAA")
    with pytest.raises(cu.UploadConflict):
        await cu.finalize_upload(uid)


async def test_finalize_is_idempotent(client: TestClient):
    from yt_scheduler.services import chunked_uploads as cu

    info = await cu.init_upload("x.bin", size=4)
    uid = info["upload_id"]
    await cu.append_chunk(uid, 0, b"ABCD")
    first = await cu.finalize_upload(uid)
    second = await cu.finalize_upload(uid)
    # Path + filename are stable across repeat finalizes.
    assert first == second


async def test_append_blocked_after_finalize(client: TestClient):
    from yt_scheduler.services import chunked_uploads as cu

    info = await cu.init_upload("x.bin", size=4)
    uid = info["upload_id"]
    await cu.append_chunk(uid, 0, b"ABCD")
    await cu.finalize_upload(uid)
    with pytest.raises(cu.UploadConflict):
        await cu.append_chunk(uid, 4, b"E")


async def test_consume_returns_entry_and_removes_it(client: TestClient):
    """consume is the bridge between the upload protocol and a domain
    endpoint — pops the entry so the same upload can't be claimed
    twice."""
    from yt_scheduler.services import chunked_uploads as cu

    info = await cu.init_upload("x.bin", size=4)
    uid = info["upload_id"]
    await cu.append_chunk(uid, 0, b"ABCD")
    await cu.finalize_upload(uid)
    entry = await cu.consume_upload(uid)
    assert entry["filename"] == "x.bin"
    with pytest.raises(cu.UploadNotFound):
        await cu.consume_upload(uid)


async def test_consume_refuses_unfinalized(client: TestClient):
    from yt_scheduler.services import chunked_uploads as cu

    info = await cu.init_upload("x.bin", size=4)
    uid = info["upload_id"]
    await cu.append_chunk(uid, 0, b"AB")
    with pytest.raises(cu.UploadConflict):
        await cu.consume_upload(uid)


async def test_cancel_removes_disk_file(client: TestClient):
    from yt_scheduler.services import chunked_uploads as cu

    info = await cu.init_upload("x.bin", size=10)
    uid = info["upload_id"]
    await cu.append_chunk(uid, 0, b"AAA")
    table = cu._UPLOADS_FOR_TESTS()
    path = Path(table[uid]["path"])
    assert path.exists()
    assert await cu.cancel_upload(uid) is True
    assert not path.exists()
    # Idempotent.
    assert await cu.cancel_upload(uid) is False


async def test_cleanup_orphan_partial_uploads(client: TestClient, tmp_path: Path):
    """The startup sweep nukes upload_*.partial regardless of in-memory
    state — partials from a previous process are unreachable."""
    from yt_scheduler.services import chunked_uploads as cu
    from yt_scheduler.config import UPLOAD_DIR

    (UPLOAD_DIR / "upload_abc.partial").write_bytes(b"x")
    (UPLOAD_DIR / "upload_def.partial").write_bytes(b"y")
    # A non-partial file in the same dir must NOT be touched.
    (UPLOAD_DIR / "source_safe.mp4").write_bytes(b"z")
    removed = cu.cleanup_orphan_partial_uploads()
    assert removed == 2
    assert not (UPLOAD_DIR / "upload_abc.partial").exists()
    assert (UPLOAD_DIR / "source_safe.mp4").exists()


# --- HTTP layer ----------------------------------------------------


def test_http_init_chunk_finalize_round_trip(client: TestClient):
    """End-to-end: init → 2 chunk POSTs → finalize. The on-disk file
    after finalize is the concatenation of both chunk bodies."""
    info = _init(client, filename="x.bin", size=16)
    uid = info["upload_id"]

    r1 = _chunk(client, uid, 0, b"AAAA" * 2)  # 8 bytes
    assert r1.status_code == 200
    assert r1.json()["received_bytes"] == 8

    r2 = _chunk(client, uid, 8, b"BBBB" * 2)  # 8 bytes
    assert r2.status_code == 200
    assert r2.json()["received_bytes"] == 16

    r3 = client.post(f"/api/uploads/{uid}/finalize")
    assert r3.status_code == 200, r3.text
    assert r3.json()["upload_id"] == uid
    assert r3.json()["size"] == 16
    # Path isn't surfaced over the wire — peek at the table.
    from yt_scheduler.services import chunked_uploads as cu

    path = cu._UPLOADS_FOR_TESTS()[uid]["path"]
    assert Path(path).read_bytes() == b"A" * 8 + b"B" * 8


def test_http_init_rejects_bad_payload(client: TestClient):
    resp = client.post("/api/uploads/init", json={"size": 10})
    assert resp.status_code == 400
    resp = client.post("/api/uploads/init", json={"filename": "x", "size": -1})
    assert resp.status_code == 400


def test_http_chunk_404_on_unknown_id(client: TestClient):
    r = _chunk(client, "nope", 0, b"AAAA")
    assert r.status_code == 404


def test_http_chunk_409_on_offset_mismatch(client: TestClient):
    info = _init(client, size=10)
    uid = info["upload_id"]
    _chunk(client, uid, 0, b"AAAAA")
    r = _chunk(client, uid, 0, b"BBBBB")
    assert r.status_code == 409


def test_http_chunk_413_on_overflow(client: TestClient):
    info = _init(client, size=4)
    uid = info["upload_id"]
    r = _chunk(client, uid, 0, b"AAAAAA")  # 6 bytes > 4
    assert r.status_code == 413


def test_http_finalize_409_on_incomplete(client: TestClient):
    info = _init(client, size=10)
    uid = info["upload_id"]
    _chunk(client, uid, 0, b"AAA")  # only 3 of 10
    r = client.post(f"/api/uploads/{uid}/finalize")
    assert r.status_code == 409


def test_http_cancel_idempotent(client: TestClient):
    info = _init(client, size=4)
    uid = info["upload_id"]
    r1 = client.delete(f"/api/uploads/{uid}")
    assert r1.status_code == 200
    assert r1.json()["status"] == "cancelled"
    r2 = client.delete(f"/api/uploads/{uid}")
    assert r2.status_code == 200
    assert r2.json()["status"] == "gone"


def test_http_empty_chunk_rejected(client: TestClient):
    """Zero-byte chunk would silently advance nothing; refuse so the
    client doesn't accidentally finalize a partial upload."""
    info = _init(client, size=4)
    uid = info["upload_id"]
    r = _chunk(client, uid, 0, b"")
    assert r.status_code == 400
