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


# --- Ambiguous publish resolution -------------------------------------------
#
# A ReadTimeout on /threads_publish means the request was sent and the
# response was lost — Meta may have published (post 365 did exactly this in
# production, and the stored error told the user to Send again, i.e. to
# double-post). The resolver settles the outcome with read-only status
# checks and never issues a second publish.


@pytest.fixture
def ambiguous_graph(monkeypatch, social):
    """Graph where the publish leg is scriptable, everything else canned."""
    monkeypatch.setattr(social.ThreadsPoster, "_PUBLISH_RESOLVE_DELAY_SECONDS", 0.0)
    calls: list[httpx.Request] = []
    state = {
        "publish_exception": None,
        "publish_response": (200, {"id": "post-9"}),
        "publish_attempted": False,
        "status_sequence": [],
        "status_default": (200, {"status": "FINISHED"}),
        "container_lookup": (400, {"error": {"message": "unsupported"}}),
        "threads_listing": (200, {"data": []}),
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        path = request.url.path
        params = dict(request.url.params)
        if path.endswith("/threads_publish"):
            state["publish_attempted"] = True
            if state["publish_exception"] is not None:
                raise state["publish_exception"]
            code, payload = state["publish_response"]
            return httpx.Response(code, json=payload)
        if path.endswith("/threads"):
            if request.method == "POST":
                return httpx.Response(200, json={"id": "container-1"})
            code, payload = state["threads_listing"]
            return httpx.Response(code, json=payload)
        if "status" in params.get("fields", ""):
            # The pre-publish poll uses this endpoint too; the scripted
            # sequence is only for the post-publish resolver.
            if not state["publish_attempted"]:
                return httpx.Response(200, json={"status": "FINISHED"})
            if state["status_sequence"]:
                item = state["status_sequence"].pop(0)
                if item == "raise":
                    raise httpx.ReadTimeout("status check timed out", request=request)
                code, payload = item
                return httpx.Response(code, json=payload)
            code, payload = state["status_default"]
            return httpx.Response(code, json=payload)
        code, payload = state["container_lookup"]
        return httpx.Response(code, json=payload)

    original = httpx.AsyncClient.__init__
    monkeypatch.setattr(
        httpx.AsyncClient, "__init__",
        lambda self, *a, **kw: original(
            self, *a, **{**kw, "transport": httpx.MockTransport(handler)}),
    )
    return calls, state


def _publish_requests(calls) -> list:
    return [r for r in calls if r.url.path.endswith("/threads_publish")]


async def test_lost_response_with_published_container_is_a_success(
    ambiguous_graph, social,
):
    calls, state = ambiguous_graph
    state["publish_exception"] = httpx.ReadTimeout("response never arrived")
    state["status_sequence"] = [(200, {"status": "PUBLISHED"})]
    state["container_lookup"] = (
        200, {"id": "post-9", "permalink": "https://threads.net/t/AbCdE"})

    result = await social.ThreadsPoster(bundle=CREDS)._post_prepared("hello")

    assert result["url"] == "https://threads.net/t/AbCdE"
    assert "lost publish response" in result["warning"]
    assert len(_publish_requests(calls)) == 1, "never publish twice"


async def test_lost_response_with_error_container_is_retry_safe(
    ambiguous_graph, social,
):
    calls, state = ambiguous_graph
    state["publish_exception"] = httpx.ReadTimeout("response never arrived")
    state["status_sequence"] = [(200, {"status": "ERROR", "error_message": "boom"})]

    with pytest.raises(RuntimeError) as exc_info:
        await social.ThreadsPoster(bundle=CREDS)._post_prepared("hello")
    message = str(exc_info.value)
    assert "Nothing was published" in message
    assert "boom" in message
    assert "Check your network connection" not in message


async def test_lost_response_with_persistent_finished_is_unknown(
    ambiguous_graph, social, monkeypatch,
):
    """FINISHED is the expected pre-publish state, so seeing it again proves
    only 'not yet' — a confident 'retry is safe' here rebuilds the duplicate
    with extra steps."""
    monkeypatch.setattr(social.ThreadsPoster, "_PUBLISH_RESOLVE_ATTEMPTS", 3)
    calls, state = ambiguous_graph
    state["publish_exception"] = httpx.ReadTimeout("response never arrived")

    with pytest.raises(social.ThreadsPublishOutcomeUnknown) as exc_info:
        await social.ThreadsPoster(bundle=CREDS)._post_prepared("hello")
    message = str(exc_info.value)
    assert "Check your Threads profile" in message
    assert "container-1" in message
    assert "probably safe" in message


async def test_lost_response_with_unreachable_checks_is_unknown_and_bounded(
    ambiguous_graph, social,
):
    calls, state = ambiguous_graph
    state["publish_exception"] = httpx.ReadTimeout("response never arrived")
    state["status_sequence"] = ["raise"] * 30

    with pytest.raises(social.ThreadsPublishOutcomeUnknown) as exc_info:
        await social.ThreadsPoster(bundle=CREDS)._post_prepared("hello")
    message = str(exc_info.value)
    assert message.index("Check your Threads profile") < 50, \
        "the advice must survive the banner's 200-char truncation"
    cap = social.ThreadsPoster._PUBLISH_RESOLVE_MAX_CONSECUTIVE_CHECK_FAILURES
    assert len(state["status_sequence"]) == 30 - cap, \
        "consecutive check failures must stop the polling early"


async def test_connect_error_on_publish_stays_a_plain_failure(
    ambiguous_graph, social,
):
    """No connection means the request never arrived — no ambiguity, retry is
    genuinely safe, and the resolver must not run."""
    calls, state = ambiguous_graph
    state["publish_exception"] = httpx.ConnectError("no route to host")

    with pytest.raises(RuntimeError) as exc_info:
        await social.ThreadsPoster(bundle=CREDS)._post_prepared("hello")
    assert not isinstance(exc_info.value, social.ThreadsPublishOutcomeUnknown)
    assert "use Send to retry" in str(exc_info.value)
    post_publish_status_reads = [
        r for r in calls
        if "status" in dict(r.url.params).get("fields", "")
        and calls.index(r) > calls.index(_publish_requests(calls)[0])
    ]
    assert post_publish_status_reads == []


async def test_permalink_fallback_fences_by_text_and_timestamp(
    ambiguous_graph, social,
):
    from datetime import datetime, timezone

    calls, state = ambiguous_graph
    state["publish_exception"] = httpx.ReadTimeout("response never arrived")
    state["status_sequence"] = [(200, {"status": "PUBLISHED"})]
    fresh_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")
    state["threads_listing"] = (200, {"data": [
        {"id": "old-1", "permalink": "https://threads.net/t/OLD",
         "text": "hello", "timestamp": "2020-01-01T00:00:00+0000"},
        {"id": "new-1", "permalink": "https://threads.net/t/NEW",
         "text": "hello", "timestamp": fresh_stamp},
    ]})

    result = await social.ThreadsPoster(bundle=CREDS)._post_prepared("hello")

    assert result["url"] == "https://threads.net/t/NEW"


async def test_permalink_fallback_degrades_when_ambiguous(
    ambiguous_graph, social,
):
    """Two candidates pass the fence: picking either risks the wrong
    permalink, and liveness is already proven — degrade honestly."""
    from datetime import datetime, timezone

    calls, state = ambiguous_graph
    state["publish_exception"] = httpx.ReadTimeout("response never arrived")
    state["status_sequence"] = [(200, {"status": "PUBLISHED"})]
    fresh_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")
    state["threads_listing"] = (200, {"data": [
        {"id": "a", "permalink": "https://threads.net/t/A",
         "text": "hello", "timestamp": fresh_stamp},
        {"id": "b", "permalink": "https://threads.net/t/B",
         "text": "hello", "timestamp": fresh_stamp},
    ]})

    result = await social.ThreadsPoster(bundle=CREDS)._post_prepared("hello")

    assert result["url"] == ""
    assert "permalink could not be recovered" in result["warning"]


async def test_a_200_publish_without_an_id_uses_the_resolver(
    ambiguous_graph, social,
):
    """Meta said 200, so the post almost certainly landed — a bare KeyError
    would invite the same blind retry as a lost response."""
    calls, state = ambiguous_graph
    state["publish_response"] = (200, {"unexpected": "shape"})
    state["status_sequence"] = [(200, {"status": "PUBLISHED"})]
    state["container_lookup"] = (
        200, {"id": "post-9", "permalink": "https://threads.net/t/AbCdE"})

    result = await social.ThreadsPoster(bundle=CREDS)._post_prepared("hello")

    assert result["url"] == "https://threads.net/t/AbCdE"


async def test_a_401_during_resolution_flags_reauth_but_warns(
    ambiguous_graph, social,
):
    calls, state = ambiguous_graph
    state["publish_exception"] = httpx.ReadTimeout("response never arrived")
    state["status_sequence"] = [(401, {"error": {"message": "bad token"}})]

    with pytest.raises(social.CredentialAuthError) as exc_info:
        await social.ThreadsPoster(bundle=CREDS)._post_prepared("hello")
    assert "before resending" in str(exc_info.value)


async def test_a_clean_publish_never_touches_the_resolver(
    ambiguous_graph, social,
):
    calls, _state = ambiguous_graph

    result = await social.ThreadsPoster(bundle=CREDS)._post_prepared("hello")

    assert result["id"] == "post-9"
    assert "warning" not in result
    lookups = [
        r for r in calls if "permalink" in dict(r.url.params).get("fields", "")
    ]
    assert lookups == []
