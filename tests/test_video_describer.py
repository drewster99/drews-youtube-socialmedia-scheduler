"""Tests for the keyframe-based fallback description path.

Frame extraction itself uses ffmpeg + a real video, which is too heavy
for a unit test. We mock ``services.media.extract_keyframes`` and assert
the AI service builds the right multi-image Claude request and the
route falls back to it when the transcript is empty.
"""

from __future__ import annotations

import base64
import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
async def app_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-fake")
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)
    importlib.import_module("yt_scheduler.config")
    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    database = importlib.import_module("yt_scheduler.database")
    projects = importlib.import_module("yt_scheduler.services.projects")

    db = await database.get_db()
    await projects.ensure_default_project()
    yield db
    await database.close_db()


# --- ai.generate_seo_description_from_frames -------------------------------


async def test_frames_prompt_shape(app_db) -> None:
    """The Claude request must include the title in text + every frame as
    a base64 image part, in order."""
    from yt_scheduler.services import ai

    # Three fake frames — content doesn't matter, only the encoding does.
    frames = [b"FAKE_FRAME_1", b"FAKE_FRAME_2", b"FAKE_FRAME_3"]

    captured: dict = {}

    def _fake_create(**kwargs):
        captured.update(kwargs)
        result = MagicMock()
        result.content = [MagicMock(text="A description.")]
        return result

    fake_client = MagicMock()
    fake_client.messages.create = _fake_create

    with patch.object(ai, "get_client", return_value=fake_client):
        with patch.object(ai, "_resolve_model", new=_async_return("claude-fake")):
            out = await ai.generate_seo_description_from_frames(
                title="My Test Video",
                frames=frames,
                channel_name="Test Channel",
                extra_instructions="No emoji.",
                project_id=1,
            )

    assert out == "A description."
    msg = captured["messages"][0]
    assert msg["role"] == "user"
    parts = msg["content"]
    # First part is the instruction text; remaining N parts are images.
    assert parts[0]["type"] == "text"
    assert "My Test Video" in parts[0]["text"]
    assert "Test Channel" in parts[0]["text"]
    assert "No emoji." in parts[0]["text"]
    assert len(parts) == 1 + len(frames)

    for i, frame in enumerate(frames):
        image_part = parts[1 + i]
        assert image_part["type"] == "image"
        assert image_part["source"]["type"] == "base64"
        assert image_part["source"]["media_type"] == "image/jpeg"
        assert image_part["source"]["data"] == base64.b64encode(frame).decode("ascii")


async def test_frames_empty_raises() -> None:
    from yt_scheduler.services import ai

    with pytest.raises(ValueError, match="no frames"):
        await ai.generate_seo_description_from_frames(title="x", frames=[], project_id=1)


async def test_tags_from_frames_returns_lowercase_list(app_db) -> None:
    from yt_scheduler.services import ai

    frames = [b"FAKE_FRAME"]

    def _fake_create(**kwargs):
        result = MagicMock()
        result.content = [MagicMock(text="Foo, Bar Baz, QUX, 'tag-five'")]
        return result

    fake_client = MagicMock()
    fake_client.messages.create = _fake_create

    with patch.object(ai, "get_client", return_value=fake_client):
        with patch.object(ai, "_resolve_model", new=_async_return("claude-fake")):
            tags = await ai.generate_tags_from_frames(
                title="t", description="d", frames=frames, project_id=1,
            )

    assert tags == ["foo", "bar baz", "qux", "tag-five"]


# --- /api/videos/{id}/generate-description fallback ------------------------


async def test_generate_description_falls_back_to_frames(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DYS_HOST", "127.0.0.1")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-fake")
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    (tmp_path / "templates").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)

    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    app_module = importlib.import_module("yt_scheduler.app")
    media = importlib.import_module("yt_scheduler.services.media")
    ai = importlib.import_module("yt_scheduler.services.ai")

    # Fake a local video file so the route's existence check passes.
    fake_video = tmp_path / "uploads" / "fake.mp4"
    fake_video.write_bytes(b"\x00" * 32)

    monkeypatch.setattr(media, "extract_keyframes", lambda path, count, **kw: [b"F1", b"F2"])

    async def _fake_from_frames(*, title, frames, channel_name="", extra_instructions="", project_id=1):
        # Verify the route handed us the mocked frames, not a transcript.
        assert frames == [b"F1", b"F2"]
        return "Visually-described summary."

    monkeypatch.setattr(ai, "generate_seo_description_from_frames", _fake_from_frames)

    from fastapi.testclient import TestClient
    with TestClient(app_module.app) as c:
        from yt_scheduler.database import get_db
        db = await get_db()
        await db.execute(
            "INSERT INTO videos (id, project_id, title, status, video_file_path, transcript) "
            "VALUES ('vidF', 1, 'Test', 'uploaded', ?, '')",
            (str(fake_video),),
        )
        await db.commit()

        resp = c.post("/api/videos/vidF/generate-description", json={})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["description"] == "Visually-described summary."

    from yt_scheduler.database import close_db
    await close_db()


async def test_generate_description_400s_when_no_transcript_and_no_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setenv("DYS_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DYS_HOST", "127.0.0.1")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-fake")
    (tmp_path / "uploads").mkdir(parents=True, exist_ok=True)
    (tmp_path / "templates").mkdir(parents=True, exist_ok=True)
    for mod in list(sys.modules.keys()):
        if mod.startswith("yt_scheduler"):
            sys.modules.pop(mod, None)

    keychain = importlib.import_module("yt_scheduler.services.keychain")
    monkeypatch.setattr(keychain, "_is_macos", lambda: False)
    app_module = importlib.import_module("yt_scheduler.app")

    from fastapi.testclient import TestClient
    with TestClient(app_module.app) as c:
        from yt_scheduler.database import get_db
        db = await get_db()
        # Imported-from-YouTube case: no local file, no transcript yet.
        await db.execute(
            "INSERT INTO videos (id, project_id, title, status, video_file_path, transcript) "
            "VALUES ('vidG', 1, 'Imported', 'imported', '', '')"
        )
        await db.commit()

        resp = c.post("/api/videos/vidG/generate-description", json={})
        assert resp.status_code == 400
        assert "no local video file" in resp.json()["detail"].lower()

    from yt_scheduler.database import close_db
    await close_db()


# --- helpers ---------------------------------------------------------------


def _async_return(value):
    async def _f(*a, **kw):
        return value
    return _f
