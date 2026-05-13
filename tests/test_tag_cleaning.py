"""Unit tests for ``services.ai._clean_tags`` — the post-processor that
enforces the 1–2 word / ≤24 char / dedupe / cap-15 invariants on every
tag list the LLM returns.

The seed prompts (``tags_from_metadata_prompt`` /
``tags_from_frames_prompt``) tell the model the same rules, but the
model occasionally slips out a sentence-shaped tag or repeats a near
duplicate. The server-side filter is the guardrail.
"""

from __future__ import annotations

from yt_scheduler.services.ai import _clean_tags


def test_drops_long_tags() -> None:
    """A tag over 24 characters is dropped, not truncated."""
    raw = "ai, this tag is far too long to fit in twenty-four characters, ml"
    assert _clean_tags(raw) == ["ai", "ml"]


def test_drops_multi_word_tags() -> None:
    """Anything beyond 2 words is dropped — sentence-shaped tags fail SEO."""
    raw = "machine learning, a hands on tutorial about ml, python"
    assert _clean_tags(raw) == ["machine learning", "python"]


def test_dedupes_case_insensitively() -> None:
    """The cleaner lowercases first, then dedupes."""
    raw = "Python, python, AI, ai, ML"
    assert _clean_tags(raw) == ["python", "ai", "ml"]


def test_strips_quotes_and_whitespace() -> None:
    raw = '  "python" , "ai"  ,  ml  '
    assert _clean_tags(raw) == ["python", "ai", "ml"]


def test_caps_at_15_tags() -> None:
    raw = ",".join(f"tag{i}" for i in range(30))
    out = _clean_tags(raw)
    assert len(out) == 15
    assert out[0] == "tag0"
    assert out[-1] == "tag14"


def test_empty_pieces_dropped() -> None:
    """Trailing comma, blank entries — all dropped silently."""
    assert _clean_tags("python,,ai,") == ["python", "ai"]


def test_empty_input_returns_empty() -> None:
    assert _clean_tags("") == []
    assert _clean_tags("   ") == []


def test_24_char_boundary_inclusive() -> None:
    """A tag exactly 24 chars long is allowed; 25 is dropped."""
    twenty_four = "a" * 24
    twenty_five = "a" * 25
    assert _clean_tags(f"{twenty_four}, {twenty_five}, ai") == [twenty_four, "ai"]
