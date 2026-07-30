"""The Claude call wrapper must log everything and break nothing.

Every Anthropic round-trip goes through ``ai.create_message``. Two invariants
matter and neither is obvious from reading the happy path:

1. Logging cannot fail the call. On the request side a formatting error would
   kill a call that would have succeeded; on the response side it would throw
   away an answer already paid for.
2. Base64 media never reaches the log verbatim — a dozen keyframes would bury
   the record the wrapper exists to produce.
"""

from __future__ import annotations

import logging

import pytest

from yt_scheduler.services import ai


class _Usage:
    def __str__(self) -> str:  # pragma: no cover - exercised via the wrapper
        raise RuntimeError("usage repr is broken")


class _Message:
    """Stand-in for an SDK response: no ``model_dump``, so the wrapper takes
    its attribute-reading fallback — the same path the other tests' mocks use."""

    def __init__(self, *, stop_reason: str = "end_turn", usage: object = None) -> None:
        self.id = "msg_test"
        self.model = "claude-test"
        self.stop_reason = stop_reason
        self.content = []
        self.usage = usage


class _Client:
    def __init__(self, message: object) -> None:
        self._message = message
        self.calls = 0

    class _Messages:
        def __init__(self, outer: "_Client") -> None:
            self._outer = outer

        def create(self, **kwargs):
            self._outer.calls += 1
            return self._outer._message

    @property
    def messages(self) -> "_Client._Messages":
        return _Client._Messages(self)


def test_base64_is_elided_from_list_content():
    payload = [{"type": "image", "media_type": "image/jpeg", "data": "A" * 512}]
    rendered = ai._as_json(payload)
    assert "A" * 512 not in rendered
    assert "elided" in rendered


def test_base64_is_elided_from_tuple_content():
    """Regression: the walk descended into list but not tuple, so a
    tuple-wrapped content block dumped its full base64 payload."""
    payload = ({"type": "image", "media_type": "image/png", "data": "B" * 512},)
    rendered = ai._as_json(payload)
    assert "B" * 512 not in rendered
    assert "elided" in rendered


@pytest.mark.parametrize(
    "poison, expected_error",
    [
        pytest.param("self_ref_dict", "RecursionError", id="self-referencing-dict"),
        pytest.param("self_ref_list", "RecursionError", id="self-referencing-list"),
        pytest.param("tuple_key", "TypeError", id="non-str-dict-key"),
    ],
)
def test_as_json_never_raises(poison: str, expected_error: str):
    """Both halves can blow up: the elide walk recurses before json.dumps is
    reached, and json.dumps rejects non-str keys (``default=`` skips keys)."""
    if poison == "self_ref_dict":
        value: object = {"a": 1}
        value["self"] = value  # type: ignore[index]
    elif poison == "self_ref_list":
        inner: list = [1]
        inner.append(inner)
        value = inner
    else:
        value = {("a", "b"): 1}

    rendered = ai._as_json(value)
    assert rendered.startswith("<unloggable ")
    assert expected_error in rendered


def test_request_log_failure_does_not_kill_the_call(monkeypatch, caplog):
    message = _Message()
    client = _Client(message)

    def boom(label, kwargs):
        raise RuntimeError("request logging exploded")

    monkeypatch.setattr(ai, "_log_request", boom)
    with caplog.at_level(logging.INFO, logger="yt_scheduler.services.ai"):
        result = ai.create_message(client, label="unit-test", messages=[])

    assert result is message
    assert client.calls == 1, "the call must still have been made"
    assert "LOG FAILURE (request)" in caplog.text


def test_response_log_failure_does_not_discard_a_paid_response(monkeypatch, caplog):
    """Worse than the request side: the tokens are already spent."""
    message = _Message()
    client = _Client(message)

    def boom(label, msg, elapsed_ms, max_tokens):
        raise RuntimeError("response logging exploded")

    monkeypatch.setattr(ai, "_log_response", boom)
    with caplog.at_level(logging.INFO, logger="yt_scheduler.services.ai"):
        result = ai.create_message(client, label="unit-test", messages=[])

    assert result is message
    assert "LOG FAILURE (response)" in caplog.text


def test_truncation_warning_survives_a_broken_response_payload(caplog):
    """The max_tokens warning is emitted before the payload dump, so a
    formatting failure in the dump cannot take it down with it."""
    message = _Message(stop_reason="max_tokens", usage=_Usage())
    client = _Client(message)

    with caplog.at_level(logging.INFO, logger="yt_scheduler.services.ai"):
        result = ai.create_message(
            client, label="unit-test", messages=[], max_tokens=1234,
        )

    assert result is message
    assert "TRUNCATED" in caplog.text
    assert "1234" in caplog.text


def test_happy_path_still_logs_both_blocks(caplog):
    """Anti-masking: the guards must not turn a broken formatter into silence."""
    message = _Message()
    client = _Client(message)

    with caplog.at_level(logging.INFO, logger="yt_scheduler.services.ai"):
        ai.create_message(client, label="unit-test", messages=[{"role": "user", "content": "hi"}])

    assert "Claude REQUEST [unit-test]" in caplog.text
    assert "Claude RESPONSE [unit-test]" in caplog.text
    assert "LOG FAILURE" not in caplog.text


def test_sdk_failure_still_propagates(caplog):
    """The guards are for logging only — a real API error must still raise."""
    class _Boom(_Client):
        class _Messages:
            def create(self, **kwargs):
                raise RuntimeError("api is down")

        @property
        def messages(self):
            return _Boom._Messages()

    with caplog.at_level(logging.INFO, logger="yt_scheduler.services.ai"):
        with pytest.raises(RuntimeError, match="api is down"):
            ai.create_message(_Boom(None), label="unit-test", messages=[])
    assert "Claude FAILED" in caplog.text


def test_base64_bytes_are_elided_too():
    """`data` arrives as str today, but the SDK accepts bytes — the invariant
    is "no media payload in the log", not "no str media payload"."""
    payload = [{"type": "image", "media_type": "image/jpeg", "data": b"C" * 512}]
    rendered = ai._as_json(payload)
    assert "CCCC" not in rendered
    assert "elided" in rendered


def test_truncation_warning_survives_info_being_disabled(caplog):
    """The whole point of hoisting it above the isEnabledFor(INFO) return:
    WARNING outranks INFO, so a quieter logger must not silence it."""
    message = _Message(stop_reason="max_tokens")
    client = _Client(message)
    with caplog.at_level(logging.WARNING, logger="yt_scheduler.services.ai"):
        ai.create_message(client, label="unit-test", messages=[], max_tokens=99)
    assert "TRUNCATED" in caplog.text
    assert "Claude REQUEST" not in caplog.text, "INFO blocks must be skipped"


def test_payloads_are_not_serialized_when_info_is_disabled(monkeypatch, caplog):
    """A clip-proposal request is ~46 KB of JSON built as a call argument, so
    logging's own level check comes too late to save the work."""
    calls = []
    monkeypatch.setattr(ai, "_as_json", lambda v: calls.append(v) or "{}")
    client = _Client(_Message())
    with caplog.at_level(logging.WARNING, logger="yt_scheduler.services.ai"):
        ai.create_message(client, label="unit-test", messages=[{"a": 1}])
    assert calls == [], "payload was serialized despite INFO being off"
