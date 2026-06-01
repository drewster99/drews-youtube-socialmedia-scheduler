"""Local-file info endpoints: file-info (name + server path) and
reveal-file (open in Finder)."""

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


def _insert(video_id: str, **cols: object) -> None:
    from yt_scheduler.config import DB_PATH

    keys = ["id", "title", "status", *cols]
    vals = [video_id, "t", "ready", *cols.values()]
    placeholders = ", ".join("?" for _ in keys)
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute(
            f"INSERT INTO videos ({', '.join(keys)}) VALUES ({placeholders})",
            vals,
        )
        conn.commit()


def test_file_info_returns_original_and_path(client: TestClient) -> None:
    from yt_scheduler.config import UPLOAD_DIR

    f = UPLOAD_DIR / "VID11char01.mp4"
    f.write_bytes(b"\x00" * 32)
    _insert(
        "VID11char01",
        video_file_path=str(f),
        video_file_original_name="My Original Clip.mov",
    )
    resp = client.get("/api/videos/VID11char01/file-info")
    assert resp.status_code == 200
    data = resp.json()
    assert data["has_file"] is True
    assert data["original_name"] == "My Original Clip.mov"
    assert data["disk_name"] == "VID11char01.mp4"
    assert "server_path" not in data
    assert data["exists"] is True


def test_file_info_404_for_unknown_video(client: TestClient) -> None:
    assert client.get("/api/videos/nope/file-info").status_code == 404


def test_reveal_file_404_without_local_file(client: TestClient) -> None:
    _insert("NOFILE00001")
    assert client.post("/api/videos/NOFILE00001/reveal-file").status_code == 404


def test_file_info_surfaces_codec_playable_and_warnings(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The /file-info response carries codec_name, container,
    browser_playable, and quality_warnings derived from probe + origin."""
    from yt_scheduler.config import UPLOAD_DIR
    from yt_scheduler.services import media
    from yt_scheduler.routers import video_routes

    f = UPLOAD_DIR / "PLAY1234567.mp4"
    f.write_bytes(b"\x00")
    _insert(
        "PLAY1234567",
        video_file_path=str(f),
        source_file_origin="youtube_download",
    )

    fake = media.VideoProbe(
        duration_seconds=60.0, width=1280, height=720,
        bitrate_bps=5_000_000, size_bytes=100_000,
        codec_name="h264", container="mp4",
    )
    monkeypatch.setattr(media, "probe_video_file", lambda _p: fake)
    monkeypatch.setattr(video_routes.media_service, "probe_video_file", lambda _p: fake)

    resp = client.get("/api/videos/PLAY1234567/file-info")
    assert resp.status_code == 200
    data = resp.json()
    assert data["codec_name"] == "h264"
    assert data["container"] == "mp4"
    assert data["browser_playable"] is True
    codes = [w["code"] for w in data["quality_warnings"]]
    assert codes == ["low_resolution", "youtube_download_lossy"]


def test_file_info_unplayable_codec(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from yt_scheduler.config import UPLOAD_DIR
    from yt_scheduler.services import media
    from yt_scheduler.routers import video_routes

    f = UPLOAD_DIR / "PRORES12345.mov"
    f.write_bytes(b"\x00")
    _insert("PRORES12345", video_file_path=str(f), source_file_origin="uploaded")

    fake = media.VideoProbe(
        duration_seconds=60.0, width=3840, height=2160,
        bitrate_bps=200_000_000, size_bytes=1_500_000_000,
        codec_name="prores", container="mov",
    )
    monkeypatch.setattr(media, "probe_video_file", lambda _p: fake)
    monkeypatch.setattr(video_routes.media_service, "probe_video_file", lambda _p: fake)

    resp = client.get("/api/videos/PRORES12345/file-info")
    data = resp.json()
    assert data["codec_name"] == "prores"
    assert data["browser_playable"] is False
    assert data["quality_warnings"] == []  # 4K uploaded master, nothing to warn about


def test_reveal_file_runs_open_dash_r(
    client: TestClient, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from yt_scheduler.config import UPLOAD_DIR
    from yt_scheduler.routers import video_routes

    f = UPLOAD_DIR / "REVEALvid01.mp4"
    f.write_bytes(b"\x00")
    _insert("REVEALvid01", video_file_path=str(f))

    calls: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)

        class _Result:
            returncode = 0

        return _Result()

    monkeypatch.setattr(video_routes.subprocess, "run", fake_run)
    monkeypatch.setattr(video_routes.sys, "platform", "darwin")

    resp = client.post("/api/videos/REVEALvid01/reveal-file")
    assert resp.status_code == 200
    assert resp.json() == {"revealed": True}
    assert calls and calls[0][:2] == ["open", "-R"]
    assert calls[0][2] == str(f.resolve())
