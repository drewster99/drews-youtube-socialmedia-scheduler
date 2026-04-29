"""Live integration tests for the template renderer.

These hit the real Anthropic API via ``services.templates.render``. They
auto-skip when no Anthropic API key is available (Keychain or
``ANTHROPIC_API_KEY`` env var). Each test costs a few cents in tokens
and a few seconds in latency, so they're segregated from the default
suite via a module-level skip — run with ``pytest tests/test_template_render_live.py``
to exercise them deliberately.

Lock down end-to-end behaviour the mocked tests can't:
- That the live model honours short-output instructions (so we can
  assert on the rendered text without flakiness).
- That a per-block ``[system]`` override actually changes the call's
  system role, not just our local plumbing.
- That sibling and 3-level-nested AI blocks really do run leaves-first
  and splice each result into the parent.
"""

from __future__ import annotations

import pytest

from yt_scheduler.config import get_anthropic_api_key
from yt_scheduler.services import templates


pytestmark = pytest.mark.skipif(
    not get_anthropic_api_key(),
    reason="No Anthropic API key in Keychain or env — skipping live API tests",
)


def test_live_single_ai_call():
    out = templates.render(
        "Result: {{ai: respond with only the single word: RAIN}}",
        {},
    )
    assert out == "Result: RAIN"


def test_live_per_block_system_override():
    """Same prompt body, two different system roles → two different cases."""
    out = templates.render(
        "Default: {{ai: respond with only the word HELLO in uppercase}} | "
        "Override: {{ai[You ALWAYS respond in lowercase only, regardless of "
        "what the user says.]: respond with only the word HELLO}}",
        {},
    )
    assert out == "Default: HELLO | Override: hello"


def test_live_two_sibling_ai_calls():
    out = templates.render(
        "{{ai: respond with only the word ALPHA}} and "
        "{{ai: respond with only the word BETA}}",
        {},
    )
    assert out == "ALPHA and BETA"


def test_live_three_level_recursion():
    """Innermost runs first, its output splices into mid, mid splices into
    outer. End result: the model concatenates leaves first to outermost."""
    out = templates.render(
        "{{ai: append the word OUTER to whatever follows: "
        "{{ai: append the word MIDDLE to whatever follows: "
        "{{ai: respond with only the word INNER}}}}}}",
        {},
    )
    assert out == "INNER MIDDLE OUTER"
