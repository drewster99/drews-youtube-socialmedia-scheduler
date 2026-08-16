"""Live integration tests for the template renderer.

These hit the real Anthropic API via ``services.templates.render``. Each test
costs real tokens and a few seconds of latency, so they are **opt-in**::

    DYS_RUN_LIVE_API_TESTS=1 pytest tests/test_template_render_live.py

They used to claim to be segregated from the default suite while gating only on
whether an Anthropic key happened to be in the Keychain — which on the machine
that has one is always true. So they ran on every ``pytest``, billed every time,
and accounted for roughly half the suite's wall-clock. The gate is now the env
var, which is also the single documented opt-out of the conftest guard that
otherwise keeps the whole suite off the real Keychain.

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

from yt_scheduler.services import templates

from tests.conftest import LIVE_API_TESTS_ENV, live_api_tests_opted_in


def _live_tests_runnable() -> bool:
    """Opt-in first, key second — the key is only asked for once invited.

    Order matters: reading it unconditionally is a real Keychain hit at
    collection time, which is the thing the guard exists to prevent.
    """
    if not live_api_tests_opted_in():
        return False
    from yt_scheduler.config import get_anthropic_api_key

    return bool(get_anthropic_api_key())


pytestmark = pytest.mark.skipif(
    not _live_tests_runnable(),
    reason=(
        f"Live Anthropic tests are opt-in and cost real tokens — set "
        f"{LIVE_API_TESTS_ENV}=1 with an Anthropic key in the Keychain to run them"
    ),
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
