"""A failed post must always tell the user what happened.

Several exceptions the posting paths actually raise stringify to '' — so
``f"Threads post failed: {e}"`` stored a bare colon and nothing else. Post 102
in the production database is exactly that: ``"Threads post failed: "``.
"""

from __future__ import annotations

import httpx
import pytest

from yt_scheduler.services.social import _exception_detail

REQUEST = httpx.Request("POST", "https://graph.threads.net/v1.0/me/threads")


def _detail(exc: BaseException) -> str:
    return _exception_detail(exc)


@pytest.mark.parametrize(
    "exc",
    [
        httpx.ConnectError(""),
        httpx.ReadTimeout(""),
        httpx.ConnectTimeout(""),
        RuntimeError(),
        ValueError(""),
    ],
)
def test_detail_free_exceptions_still_produce_a_message(exc: BaseException) -> None:
    """The bug: str(exc) == '' left the user with nothing after the colon."""
    assert str(exc) == ""
    detail = _detail(exc)
    assert detail.strip(), "empty detail — user is told a post failed and nothing else"
    assert type(exc).__name__ in detail


def test_http_status_error_surfaces_the_provider_body() -> None:
    response = httpx.Response(
        400,
        request=REQUEST,
        json={"error": {"message": "Invalid media", "code": 100}},
    )
    exc = httpx.HTTPStatusError("", request=REQUEST, response=response)

    detail = _detail(exc)

    assert "HTTP 400" in detail
    assert "Invalid media" in detail
    assert "code=100" in detail


def test_timeout_names_the_host_and_says_what_to_do() -> None:
    exc = httpx.ReadTimeout("", request=REQUEST)

    detail = _detail(exc)

    assert "graph.threads.net" in detail
    assert "retry" in detail.lower()


def test_transport_error_without_a_request_does_not_raise() -> None:
    """httpx.HTTPError.request RAISES when unset — reading it bare would blow up
    inside the error handler itself."""
    exc = httpx.ConnectError("connection refused")  # no request= given

    detail = _detail(exc)

    assert "ConnectError" in detail
    assert "connection refused" in detail


def test_ordinary_exception_keeps_its_message_and_gains_its_type() -> None:
    detail = _detail(ValueError("bad slot id"))

    assert detail == "ValueError: bad slot id"


def test_no_poster_can_emit_a_bare_trailing_colon() -> None:
    """Guards the literal shape of the production bug."""
    for exc in (httpx.ConnectError(""), RuntimeError()):
        for platform in ("Twitter", "Mastodon", "LinkedIn", "Threads"):
            message = f"{platform} post failed: {_exception_detail(exc)}"
            assert not message.endswith(": "), message
            assert message.split(": ", 1)[1].strip()
