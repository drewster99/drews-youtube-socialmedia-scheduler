"""Regression tests for ai._extract_text.

The function collects all type=="text" blocks, joins them, and raises
ClaudeEmptyResponseError when the combined result is empty or whitespace-only.
A legacy fallback also handles plain mock objects without a .type attribute.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Ensure src/ is importable (mirrors conftest.py which inserts it once, but we
# import directly without the fixture machinery here so be explicit).
SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from yt_scheduler.services.ai import (  # noqa: E402 — must follow the sys.path bootstrap above
    ClaudeEmptyResponseError,
    _extract_text,
)


# ---------------------------------------------------------------------------
# Tiny in-process fakes — no mock library needed.
# ---------------------------------------------------------------------------

class _Block:
    """Minimal block that carries .type and .text, matching the SDK shape."""

    def __init__(self, type_: str, text: str) -> None:
        self.type = type_
        self.text = text


class _Message:
    """Minimal message carrying a .content list and .stop_reason."""

    def __init__(self, blocks: list, stop_reason: str = "end_turn") -> None:
        self.content = blocks
        self.stop_reason = stop_reason


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_empty_string_text_block_raises() -> None:
    """A single type=="text" block whose .text is "" must raise."""
    msg = _Message([_Block("text", "")])
    with pytest.raises(ClaudeEmptyResponseError):
        _extract_text(msg)


def test_whitespace_only_text_block_raises() -> None:
    """A single type=="text" block whose .text is whitespace-only must raise."""
    msg = _Message([_Block("text", "   ")])
    with pytest.raises(ClaudeEmptyResponseError):
        _extract_text(msg)


def test_no_text_blocks_at_all_raises() -> None:
    """A response with only non-text blocks (e.g. tool_use) must raise.

    In the real SDK, tool_use blocks do not have a .text attribute, so the
    legacy fallback path (which only handles plain mock objects that DO have
    .text) doesn't rescue them. We model that correctly here with a block
    whose .type is "tool_use" and that has no .text attribute at all.
    """

    class _ToolUseBlock:
        type = "tool_use"
        # No .text attribute — matches the real SDK shape.

    msg = _Message([_ToolUseBlock()])
    with pytest.raises(ClaudeEmptyResponseError):
        _extract_text(msg)


def test_empty_content_list_raises() -> None:
    """A response with an empty content list must raise."""
    msg = _Message([])
    with pytest.raises(ClaudeEmptyResponseError):
        _extract_text(msg)


def test_multiple_text_blocks_partial_empty_returns_combined() -> None:
    """When one block has real text and another is empty, the real text is returned.

    The function combines ALL type=="text" blocks. As long as the combined
    result is non-empty after stripping, it should return (not raise), even
    if individual blocks contribute empty strings.
    """
    msg = _Message([_Block("text", "foo"), _Block("text", "")])
    result = _extract_text(msg)
    assert result == "foo"


def test_multiple_text_blocks_all_empty_raises() -> None:
    """Two type=="text" blocks that are both empty combined → raise."""
    msg = _Message([_Block("text", ""), _Block("text", "   ")])
    with pytest.raises(ClaudeEmptyResponseError):
        _extract_text(msg)


def test_normal_single_text_block_returns_text() -> None:
    """The happy-path: a single non-empty type=="text" block returns its text."""
    msg = _Message([_Block("text", "Hello world")])
    assert _extract_text(msg) == "Hello world"


def test_non_text_block_before_text_block_is_skipped() -> None:
    """A tool_use block before a text block must not cause a raise."""
    msg = _Message([_Block("tool_use", "tool_data"), _Block("text", "actual text")])
    assert _extract_text(msg) == "actual text"


def test_combined_text_from_multiple_blocks() -> None:
    """Multiple non-empty text blocks are joined in order."""
    msg = _Message([_Block("text", "part one "), _Block("text", "part two")])
    result = _extract_text(msg)
    assert result == "part one part two"


def test_error_captures_stop_reason() -> None:
    """ClaudeEmptyResponseError.stop_reason reflects the message's stop_reason."""
    msg = _Message([], stop_reason="refusal")
    with pytest.raises(ClaudeEmptyResponseError) as exc_info:
        _extract_text(msg)
    assert exc_info.value.stop_reason == "refusal"


def test_error_message_mentions_refusal_for_refusal_stop_reason() -> None:
    """The error message uses a human-readable refusal label when appropriate."""
    msg = _Message([], stop_reason="refusal")
    with pytest.raises(ClaudeEmptyResponseError) as exc_info:
        _extract_text(msg)
    assert "refusal" in str(exc_info.value).lower()


def test_legacy_mock_without_type_attribute_still_works() -> None:
    """Blocks lacking a .type attribute (old test mocks) still return via
    the fallback path, as long as .text is a non-empty string."""

    class _LegacyBlock:
        # No .type attribute — mimics old-style MagicMock(text="something")
        def __init__(self, text: str) -> None:
            self.text = text

    msg = _Message([_LegacyBlock("legacy text")])
    assert _extract_text(msg) == "legacy text"


def test_legacy_mock_with_empty_text_raises() -> None:
    """Old-style mock blocks with empty .text still trigger the raise path."""

    class _LegacyBlock:
        def __init__(self, text: str) -> None:
            self.text = text

    msg = _Message([_LegacyBlock("")])
    with pytest.raises(ClaudeEmptyResponseError):
        _extract_text(msg)
