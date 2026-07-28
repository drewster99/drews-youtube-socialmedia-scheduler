"""ThreadsPoster's media container flow.

Threads takes images and video as a URL Meta fetches, not as an upload, so the
poster hosts the file first and sends `image_url` / `video_url`. These assert
the container is built correctly for each kind and that every failure is loud
rather than degrading to a text-only post the user never composed.
"""

from __future__ import annotations

import importlib

import httpx
import pytest

CREDS = {"access_token": "tok", "user_id": "42", "username": "drew", "uuid": "u-1"}
HOSTED_URL = "https://acct.r2.cloudflarestorage.com/bucket/abc.mp4?X-Amz-Signature=xyz"


@pytest.fixture
def media_hosting():
    """Resolve the live module, never a module-scope reference.

    Other tests purge ``sys.modules`` to re-freeze config, which orphans any
    reference captured at import time: we would patch a dead object while the
    code under test talks to a fresh one, and its exception classes would no
    longer compare equal either.
    """
    return importlib.import_module("yt_scheduler.services.media_hosting")


@pytest.fixture
def social():
    return importlib.import_module("yt_scheduler.services.social")


@pytest.fixture
def graph(monkeypatch):
    """Record Threads Graph calls and answer them the way Meta would."""
    calls: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        path = request.url.path
        if path.endswith("/threads"):
            return httpx.Response(200, json={"id": "container-1"})
        if path.endswith("/threads_publish"):
            return httpx.Response(200, json={"id": "post-9"})
        return httpx.Response(200, json={"status": "FINISHED"})

    original = httpx.AsyncClient.__init__
    monkeypatch.setattr(
        httpx.AsyncClient, "__init__",
        lambda self, *a, **kw: original(
            self, *a, **{**kw, "transport": httpx.MockTransport(handler)}),
    )
    return calls


@pytest.fixture
def hosted(monkeypatch, media_hosting):
    """Stand in for the R2 upload; records what it was asked to host."""
    asked: list[str] = []

    async def _host_file(path, **_kw):
        asked.append(str(path))
        return media_hosting.HostedObject(
            key="abc.mp4", url=HOSTED_URL, size_bytes=10, content_type="video/mp4")

    monkeypatch.setattr(media_hosting, "host_file", _host_file)
    return asked


def _params(request: httpx.Request) -> dict[str, str]:
    return dict(request.url.params)


async def test_video_builds_a_video_container_with_the_signed_url(
    graph, hosted, tmp_path, social
):
    path = tmp_path / "clip.mp4"
    path.write_bytes(b"v" * 64)
    poster = social.ThreadsPoster(bundle=CREDS)

    result = await poster._post_prepared("hello", media_paths=[str(path)])

    create = _params(graph[0])
    assert create["media_type"] == "VIDEO"
    assert create["video_url"] == HOSTED_URL
    assert create["text"] == "hello"
    assert "image_url" not in create
    assert hosted == [str(path)]
    assert result["id"] == "post-9"


async def test_image_builds_an_image_container(graph, hosted, tmp_path, social):
    """Images have the identical requirement — the only difference is which
    field the URL goes in."""
    path = tmp_path / "frame.jpg"
    path.write_bytes(b"i" * 64)
    poster = social.ThreadsPoster(bundle=CREDS)

    await poster._post_prepared("caption", media_paths=[str(path)])

    create = _params(graph[0])
    assert create["media_type"] == "IMAGE"
    assert create["image_url"] == HOSTED_URL
    assert "video_url" not in create


async def test_text_only_post_hosts_nothing(graph, hosted, social):
    poster = social.ThreadsPoster(bundle=CREDS)

    await poster._post_prepared("just words")

    assert _params(graph[0])["media_type"] == "TEXT"
    assert hosted == [], "a text post must not touch media hosting"


async def test_publish_uses_the_container_id(graph, hosted, tmp_path, social):
    path = tmp_path / "clip.mp4"
    path.write_bytes(b"v" * 64)

    await social.ThreadsPoster(bundle=CREDS)._post_prepared("x", media_paths=[str(path)])

    publish = next(r for r in graph if r.url.path.endswith("/threads_publish"))
    assert _params(publish)["creation_id"] == "container-1"


async def test_unconfigured_hosting_raises_instead_of_posting_text_only(
    graph, monkeypatch, tmp_path, media_hosting, social
):
    """The failure that matters most: silently dropping the attachment would
    publish something the user did not write."""
    async def _unconfigured(path, **_kw):
        raise media_hosting.MediaHostingNotConfigured("Media hosting isn't configured.")

    monkeypatch.setattr(media_hosting, "host_file", _unconfigured)
    path = tmp_path / "clip.mp4"
    path.write_bytes(b"v" * 64)

    with pytest.raises(social.MediaUploadError, match="hosting"):
        await social.ThreadsPoster(bundle=CREDS)._post_prepared(
            "x", media_paths=[str(path)])

    assert graph == [], "nothing may be posted when hosting is unavailable"


async def test_hosting_failure_is_surfaced_not_swallowed(
    graph, monkeypatch, tmp_path, media_hosting, social
):
    async def _boom(path, **_kw):
        raise media_hosting.MediaHostingError("HTTP 403 SignatureDoesNotMatch")

    monkeypatch.setattr(media_hosting, "host_file", _boom)
    path = tmp_path / "clip.mp4"
    path.write_bytes(b"v" * 64)

    with pytest.raises(social.MediaUploadError, match="SignatureDoesNotMatch"):
        await social.ThreadsPoster(bundle=CREDS)._post_prepared(
            "x", media_paths=[str(path)])
    assert graph == []


async def test_oversized_image_is_refused_before_upload(graph, hosted, tmp_path, social):
    """PLATFORM_MEDIA_LIMITS covers video only, so the image cap is checked
    here rather than letting Meta reject it opaquely."""
    path = tmp_path / "huge.png"
    path.write_bytes(b"p" * (social.ThreadsPoster.MAX_IMAGE_BYTES + 1))

    with pytest.raises(social.MediaUploadError, match="image limit"):
        await social.ThreadsPoster(bundle=CREDS)._post_prepared(
            "x", media_paths=[str(path)])

    assert hosted == [] and graph == []


async def test_multiple_attachments_are_refused_rather_than_truncated(
    graph, hosted, tmp_path, social
):
    """Threads needs a CAROUSEL container for more than one, which we don't
    build. Posting only the first would drop content silently."""
    paths = []
    for name in ("a.mp4", "b.mp4"):
        p = tmp_path / name
        p.write_bytes(b"v" * 32)
        paths.append(str(p))

    with pytest.raises(social.MediaUploadError, match="one attachment"):
        await social.ThreadsPoster(bundle=CREDS)._post_prepared("x", media_paths=paths)

    assert hosted == [] and graph == []


async def test_unknown_media_kind_is_refused(graph, hosted, tmp_path, social):
    path = tmp_path / "notes.txt"
    path.write_bytes(b"hello")

    with pytest.raises(social.MediaUploadError, match="neither image nor video"):
        await social.ThreadsPoster(bundle=CREDS)._post_prepared(
            "x", media_paths=[str(path)])

    assert hosted == [] and graph == []


async def test_media_containers_get_a_longer_poll_budget_than_text(social):
    """Meta downloads the file during the container window and asks for ~30s;
    the text budget would time out before a video ever finished."""
    assert (social.ThreadsPoster._MEDIA_CONTAINER_POLL_ATTEMPTS
            > social.ThreadsPoster._TEXT_CONTAINER_POLL_ATTEMPTS)
    assert (social.ThreadsPoster._MEDIA_CONTAINER_POLL_ATTEMPTS
            * social.ThreadsPoster._CONTAINER_POLL_DELAY_SECONDS) >= 120


async def test_post_flow_client_sets_an_explicit_timeout(monkeypatch, social):
    """Regression: the post-flow client once relied on httpx's 5-second
    default, which container create exceeds whenever Meta fetches the hosted
    media during the call — a real image post died on ReadTimeout the first
    day a working token met this path."""
    timeouts_passed: list[object] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/threads"):
            return httpx.Response(200, json={"id": "container-1"})
        if path.endswith("/threads_publish"):
            return httpx.Response(200, json={"id": "post-9"})
        return httpx.Response(200, json={"status": "FINISHED"})

    original = httpx.AsyncClient.__init__

    def patched(self, *a, **kw):
        timeouts_passed.append(kw.get("timeout"))
        original(self, *a, **{**kw, "transport": httpx.MockTransport(handler)})

    monkeypatch.setattr(httpx.AsyncClient, "__init__", patched)

    await social.ThreadsPoster(bundle=CREDS)._post_prepared("just words")

    assert timeouts_passed, "expected the post flow to construct an httpx client"
    assert all(t is not None for t in timeouts_passed), (
        "the post-flow client must set an explicit timeout, not httpx's default"
    )
    # The budget has to comfortably cover Meta fetching and validating the
    # hosted media inside the container-create call.
    assert all(t >= 30 for t in timeouts_passed)


async def test_platform_capability_flags_are_the_single_source_of_truth(social):
    assert social.platform_accepts_attached_media("threads") is True
    assert social.platform_requires_hosted_media("threads") is True
    # Everyone else takes a direct upload and must not gain a hosting dependency.
    for platform in ("twitter", "bluesky", "mastodon", "linkedin"):
        assert social.platform_requires_hosted_media(platform) is False
