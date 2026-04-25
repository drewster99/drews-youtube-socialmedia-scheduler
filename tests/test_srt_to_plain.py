"""SRT → plain text normalisation."""

from __future__ import annotations

from yt_scheduler.services.transcripts import srt_to_plain_text


def test_basic_srt_block_strips_number_and_timestamp() -> None:
    srt = (
        "1\n"
        "00:00:00,000 --> 00:00:03,600\n"
        "I feel like I maybe couldn't have picked\n"
        "\n"
        "2\n"
        "00:00:01,680 --> 00:00:06,080\n"
        "the worst possible time to [laughter]\n"
    )
    plain = srt_to_plain_text(srt)
    assert "00:00" not in plain
    assert "-->" not in plain
    assert "1\n" not in plain.split("I feel")[0]
    assert "I feel like I maybe couldn't have picked" in plain
    assert "the worst possible time to [laughter]" in plain


def test_handles_multi_line_cue_text() -> None:
    srt = (
        "1\n"
        "00:00:00,000 --> 00:00:05,000\n"
        "first line of cue\n"
        "second line of cue\n"
    )
    plain = srt_to_plain_text(srt)
    assert "first line of cue" in plain
    assert "second line of cue" in plain
    assert "00:00" not in plain


def test_handles_period_separator_timestamps() -> None:
    """Some sources use '.' instead of ',' as ms separator (VTT-style)."""
    srt = "1\n00:00:00.000 --> 00:00:03.600\nhello world\n"
    plain = srt_to_plain_text(srt)
    assert plain.strip() == "hello world"


def test_strips_webvtt_header() -> None:
    vtt = (
        "WEBVTT\n"
        "\n"
        "00:00:00.000 --> 00:00:03.600\n"
        "hello world\n"
    )
    plain = srt_to_plain_text(vtt)
    assert "WEBVTT" not in plain
    assert plain.strip() == "hello world"


def test_empty_input_returns_empty() -> None:
    assert srt_to_plain_text("") == ""
    assert srt_to_plain_text("   \n\n  ") == ""


def test_already_plain_text_passes_through() -> None:
    """Plain text without any SRT structure should round-trip cleanly."""
    text = "this is\nalready plain text\nno timestamps"
    assert srt_to_plain_text(text) == text


def test_user_example_from_recheck() -> None:
    """The exact YouTube SRT output the user pasted in chat."""
    srt = (
        "1\n00:00:00,000 --> 00:00:03,600\nI feel like I maybe couldn't have picked\n\n"
        "2\n00:00:01,680 --> 00:00:06,080\nthe worst possible time to [laughter]\n\n"
        "3\n00:00:03,600 --> 00:00:07,839\nbecome a freelancer because there\n\n"
        "4\n00:00:06,080 --> 00:00:09,360\nprobably is very little to any\n"
    )
    plain = srt_to_plain_text(srt)
    # No SRT artefacts
    for needle in ["00:00", "-->", "[number]"]:
        assert needle not in plain
    # All four utterances present
    for utterance in [
        "I feel like I maybe couldn't have picked",
        "the worst possible time to [laughter]",
        "become a freelancer because there",
        "probably is very little to any",
    ]:
        assert utterance in plain
