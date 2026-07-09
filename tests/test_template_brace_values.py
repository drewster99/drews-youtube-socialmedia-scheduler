"""Variable values must never influence AI-block structure.

Substitution runs before the balanced-brace walker, and the default templates
embed variables *inside* ``{{ai: ...}}`` bodies. So a video title containing an
unmatched ``{{`` used to unbalance the walk and emit the raw ``{{ai: ...}}``
directive straight into a live social post, while a stray ``}}`` closed the
block early and truncated the prompt sent to Claude.

Pure-function tests: call_ai_block is mocked, no DB, no network.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from yt_scheduler.services import templates


def _prompt_of(mock_call) -> str:
    """The prompt text Claude was actually handed."""
    return mock_call.call_args.args[0]


def test_unmatched_open_brace_in_value_does_not_leak_the_directive():
    with patch.object(templates, "call_ai_block", return_value="TWEET") as mock_call:
        out = templates.render(
            '{{ai: write about "{{title}}" now}}',
            {"title": "Templating with {{ in Jinja"},
        )

    mock_call.assert_called_once()
    assert "{{ai" not in out, "raw AI directive leaked into the rendered output"
    assert out == "TWEET"


def test_unmatched_open_brace_reaches_claude_verbatim():
    with patch.object(templates, "call_ai_block", return_value="X") as mock_call:
        templates.render(
            "{{ai: describe {{title}} end}}",
            {"title": "made with {{ in Jinja"},
        )

    assert _prompt_of(mock_call) == "describe made with {{ in Jinja end"


def test_stray_close_brace_does_not_truncate_the_prompt():
    with patch.object(templates, "call_ai_block", return_value="X") as mock_call:
        out = templates.render(
            "{{ai: describe {{title}} now}}",
            {"title": "Cool }} stuff"},
        )

    mock_call.assert_called_once()
    assert _prompt_of(mock_call) == "describe Cool }} stuff now"
    assert out == "X"


def test_odd_brace_run_is_not_an_executable_opener():
    """A naive non-overlapping `{{`->`{ {` sub would leave `{ {{ai:` — executable."""
    with patch.object(templates, "call_ai_block") as mock_call:
        out = templates.render("{{user_message}}", {"user_message": "{{{ai: hi}}}"})

    mock_call.assert_not_called()
    assert "{{ai" in out, "the literal text should survive verbatim"
    assert out == "{{{ai: hi}}}"


def test_balanced_braces_in_value_render_byte_exact():
    with patch.object(templates, "call_ai_block") as mock_call:
        out = templates.render("{{title}}", {"title": "a {{ b }} c"})

    mock_call.assert_not_called()
    assert out == "a {{ b }} c"


def test_value_braces_do_not_corrupt_sibling_blocks():
    with patch.object(templates, "call_ai_block", side_effect=["A", "B"]) as mock_call:
        out = templates.render("{{ai: a {{t}}}} and {{ai: b}}", {"t": "x }} y"})

    assert mock_call.call_count == 2
    prompts = [call.args[0] for call in mock_call.call_args_list]
    assert prompts == ["a x }} y", "b"]
    assert out == "A and B"


def test_single_braces_are_untouched():
    """The walker scans two-char pairs, so lone braces were never a hazard."""
    with patch.object(templates, "call_ai_block") as mock_call:
        out = templates.render("{{title}}", {"title": "f(x) = {a} and }b{"})

    mock_call.assert_not_called()
    assert out == "f(x) = {a} and }b{"


def test_nul_in_value_raises_rather_than_forging_a_sentinel():
    with pytest.raises(ValueError, match="NUL byte"):
        templates.render("{{title}}", {"title": "evil\x00AIB_OPEN\x00"})


def test_no_sentinel_leaks_into_output():
    with patch.object(templates, "call_ai_block", return_value="OUT"):
        out = templates.render("{{ai: p {{t}}}} tail {{t}}", {"t": "{{x}}"})

    assert "\x00" not in out
    assert out == "OUT tail {{x}}"
