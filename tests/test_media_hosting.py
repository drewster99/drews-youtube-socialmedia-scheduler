"""Media hosting uploads through a mocked transport — no network, no bucket.

The regression these exist for: ``httpx.AsyncClient`` refuses a plain file
handle as ``content=``, and an async generator without an explicit
``Content-Length`` is sent as ``Transfer-Encoding: chunked``, which an S3 PUT
rejects. Both failures happen only on a real upload, so they are asserted here.
"""

from __future__ import annotations

import importlib

import httpx
import pytest

ACCOUNT_ID = "64102e7e8155344538065755e9cd8a7a"
SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"


@pytest.fixture
def media_hosting(isolated_data_dir):
    """The live module, with the suite's isolation already in place.

    Depends on ``isolated_data_dir`` for two reasons, both mandatory: it
    installs the in-memory Keychain (without it, anything reaching
    ``load_secret`` hits the real login Keychain and blocks on a password
    prompt), and it re-freezes config so this binds to the tmp data dir.
    Resolved by name because that fixture purges ``yt_scheduler.*``.
    """
    return importlib.import_module("yt_scheduler.services.media_hosting")


@pytest.fixture
def config(media_hosting):
    return media_hosting.MediaHostingConfig(
        account_id=ACCOUNT_ID,
        bucket="test-bucket",
        access_key_id="AKIAIOSFODNN7EXAMPLE",
        secret_access_key=SECRET,
    )


@pytest.fixture
def captured(monkeypatch):
    """Route every httpx.AsyncClient through a transport that records requests."""
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        await request.aread()
        seen.append(request)
        return httpx.Response(200)

    original = httpx.AsyncClient.__init__

    def patched(self, *args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        original(self, *args, **kwargs)

    monkeypatch.setattr(httpx.AsyncClient, "__init__", patched)
    return seen


@pytest.fixture
def video(tmp_path):
    path = tmp_path / "clip.mp4"
    # Larger than one chunk so the streaming path is actually exercised.
    path.write_bytes(bytes(i % 256 for i in range(200_000)))
    return path


async def test_upload_sends_content_length_and_never_chunked(captured, video, media_hosting, config):
    """The bug this file exists for: chunked encoding is rejected by S3."""
    await media_hosting.host_file(video, config=config)

    put = captured[0]
    assert put.method == "PUT"
    assert put.headers["content-length"] == str(video.stat().st_size)
    assert "transfer-encoding" not in put.headers


async def test_upload_body_is_the_whole_file_intact(captured, video, media_hosting, config):
    await media_hosting.host_file(video, config=config)
    assert captured[0].content == video.read_bytes()


async def test_file_larger_than_one_chunk_still_arrives_whole(captured, tmp_path, monkeypatch, media_hosting, config):
    """Force several chunks so a boundary bug can't hide behind a big default."""
    monkeypatch.setattr(media_hosting, "_UPLOAD_CHUNK_BYTES", 1024)
    path = tmp_path / "big.mp4"
    payload = bytes(i % 256 for i in range(10_000))
    path.write_bytes(payload)

    await media_hosting.host_file(path, config=config)

    assert captured[0].content == payload
    assert captured[0].headers["content-length"] == str(len(payload))


@pytest.mark.parametrize("name,expected", [
    ("clip.mp4", "video/mp4"),
    ("frame.jpg", "image/jpeg"),
    ("frame.png", "image/png"),
])
async def test_content_type_follows_the_file_so_images_work_like_video(captured, tmp_path, name, expected, media_hosting, config):
    """Threads fetches images and video the same way; nothing here is
    video-specific. An unset type would arrive as application/octet-stream."""
    path = tmp_path / name
    path.write_bytes(b"x" * 128)

    hosted = await media_hosting.host_file(path, config=config)

    assert captured[0].headers["content-type"] == expected
    assert hosted.content_type == expected
    assert hosted.key.endswith(path.suffix)


async def test_download_url_is_signed_and_short_lived(captured, video, media_hosting, config):
    hosted = await media_hosting.host_file(video, config=config)

    assert "X-Amz-Signature=" in hosted.url
    assert f"X-Amz-Expires={media_hosting.DOWNLOAD_URL_TTL_SECONDS}" in hosted.url
    assert hosted.key in hosted.url
    assert config.bucket in hosted.url
    # Short enough that the object is unreachable long before the bucket's
    # 7-day lifecycle rule removes it — expiry is the access control here,
    # because Object Lock makes early deletion impossible.
    assert media_hosting.DOWNLOAD_URL_TTL_SECONDS <= 6 * 60 * 60


async def test_upload_and_download_urls_authorize_different_verbs(captured, video, media_hosting, config):
    """A GET URL must not double as an upload credential."""
    await media_hosting.host_file(video, config=config)
    assert "X-Amz-Signature=" in str(captured[0].url)


async def test_object_keys_are_unguessable_and_unique(captured, video, media_hosting, config):
    first = await media_hosting.host_file(video, config=config)
    second = await media_hosting.host_file(video, config=config)

    assert first.key != second.key
    assert len(first.key.split(".")[0]) == 32  # uuid4 hex


async def test_server_error_is_surfaced_with_its_body(monkeypatch, video, media_hosting, config):
    """Rule C: a failed upload must not look like a success."""
    async def handler(request):
        return httpx.Response(403, text="SignatureDoesNotMatch")

    original = httpx.AsyncClient.__init__
    monkeypatch.setattr(httpx.AsyncClient, "__init__",
                        lambda self, *a, **kw: original(
                            self, *a, **{**kw, "transport": httpx.MockTransport(handler)}))

    with pytest.raises(media_hosting.MediaHostingError, match="SignatureDoesNotMatch"):
        await media_hosting.host_file(video, config=config)


async def test_missing_file_raises_before_any_request(captured, tmp_path, media_hosting, config):
    with pytest.raises(media_hosting.MediaHostingError):
        await media_hosting.host_file(tmp_path / "nope.mp4", config=config)
    assert captured == []


async def test_empty_file_raises_before_any_request(captured, tmp_path, media_hosting, config):
    """Zero bytes would upload "successfully" and then fail at the platform."""
    path = tmp_path / "empty.mp4"
    path.write_bytes(b"")
    with pytest.raises(media_hosting.MediaHostingError):
        await media_hosting.host_file(path, config=config)
    assert captured == []


@pytest.mark.parametrize("missing", ["account_id", "bucket", "access_key_id",
                                     "secret_access_key"])
async def test_partial_config_is_not_configured(monkeypatch, missing, media_hosting, config):
    """Any one value absent means no working URL can be produced, so this must
    report unconfigured rather than defer the failure to send time."""
    values = {
        "account_id": "acct", "bucket": "buck",
        "access_key_id": "akid", "secret_access_key": "secret",
    }
    values[missing] = ""

    async def fake_settings():
        return values["account_id"], values["bucket"]

    async def fake_secret(_namespace, field):
        return values["access_key_id"] if field == "access_key_id" else values["secret_access_key"]

    monkeypatch.setattr(media_hosting, "_load_settings_values", fake_settings)
    monkeypatch.setattr("yt_scheduler.services.keychain.load_secret_async", fake_secret)

    assert await media_hosting.load_config() is None
    assert await media_hosting.is_configured() is False
    with pytest.raises(media_hosting.MediaHostingNotConfigured):
        await media_hosting.require_config()
