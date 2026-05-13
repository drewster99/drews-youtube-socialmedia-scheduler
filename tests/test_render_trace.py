"""When ``trace`` is passed through ``services.templates.render()``,
each step of the substitution and every ``{{ai: ...}}`` round-trip
appends an entry. The F2 layer persists these per-post; the F3 UI
turns them back into a debug modal — but this test pins the shape
the rest of the F-series depends on."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from yt_scheduler.services import ai as ai_service
from yt_scheduler.services import templates


def test_trace_records_substitution_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    """A template with no AI block emits template_body / variables /
    substituted entries and nothing else."""
    trace: list[dict] = []
    out = templates.render(
        "Hello {{name}}, the URL is {{url}}",
        {"name": "Drew", "url": "https://example.com"},
        trace=trace,
    )
    assert out == "Hello Drew, the URL is https://example.com"
    kinds = [e["kind"] for e in trace]
    assert kinds == ["template_body", "variables", "substituted"]
    assert trace[0]["text"] == "Hello {{name}}, the URL is {{url}}"
    assert trace[1]["values"] == {"name": "Drew", "url": "https://example.com"}
    assert trace[2]["text"] == "Hello Drew, the URL is https://example.com"


def test_trace_records_ai_call(monkeypatch: pytest.MonkeyPatch) -> None:
    """An {{ai: ...}} block in the template emits one ai_call trace
    entry with the rendered prompt, the effective system, the model,
    the response, and a non-negative elapsed_ms."""
    captured: dict = {}

    class FakeMessage:
        content = [type("C", (), {"text": "  hello world  "})()]

    fake_client = MagicMock()
    fake_client.messages.create.return_value = FakeMessage()
    monkeypatch.setattr(ai_service, "get_client", lambda: fake_client)
    monkeypatch.setattr(ai_service, "_resolve_model_sync", lambda: "claude-test")

    def fake_create(**kwargs):
        captured.update(kwargs)
        time.sleep(0.001)
        return FakeMessage()
    fake_client.messages.create.side_effect = fake_create

    trace: list[dict] = []
    out = templates.render(
        "Result: {{ai: Say hello to {{name}}}}",
        {"name": "Drew"},
        default_system_prompt="Be brief.",
        trace=trace,
    )
    assert out == "Result: hello world"
    ai_entries = [e for e in trace if e["kind"] == "ai_call"]
    assert len(ai_entries) == 1
    e = ai_entries[0]
    assert e["prompt"] == "Say hello to Drew"
    assert e["system"] == "Be brief."
    assert e["model"] == "claude-test"
    assert e["response"] == "hello world"
    assert e["elapsed_ms"] >= 0


def test_trace_records_nested_ai_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    """Nested {{ai: ...{{ai: ...}}...}} fires two API calls, inner
    first, and both land in the trace in execution order."""
    call_order: list[str] = []

    def fake_create(**kwargs):
        # The 'messages[0].content' is the prompt at this level.
        prompt = kwargs["messages"][0]["content"]
        call_order.append(prompt)
        # Echo a deterministic transform so we can see which call
        # produced which output later.
        rv = MagicMock()
        rv.content = [type("C", (), {"text": f"[{prompt}-result]"})()]
        return rv

    fake_client = MagicMock()
    fake_client.messages.create.side_effect = fake_create
    monkeypatch.setattr(ai_service, "get_client", lambda: fake_client)
    monkeypatch.setattr(ai_service, "_resolve_model_sync", lambda: "m")

    trace: list[dict] = []
    templates.render(
        "{{ai: Outer about {{ai: Inner topic}}}}",
        {},
        default_system_prompt=None,
        trace=trace,
    )
    ai_calls = [e for e in trace if e["kind"] == "ai_call"]
    assert len(ai_calls) == 2
    # Inner runs first (the walker resolves nested blocks before
    # firing the outer call).
    assert ai_calls[0]["prompt"] == "Inner topic"
    assert ai_calls[1]["prompt"] == "Outer about [Inner topic-result]"


def test_trace_none_is_no_op() -> None:
    """When trace is not passed, render() produces no trace overhead."""
    # Nothing to assert beyond "doesn't crash"; the absence of any
    # trace.append surface is the actual contract.
    out = templates.render("hi {{x??there}}", {})
    assert out == "hi there"
