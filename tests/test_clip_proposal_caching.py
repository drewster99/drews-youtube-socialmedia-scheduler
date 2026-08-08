"""The proposal loop marks prompt-cache breakpoints on its round messages.

Caching is a prefix match (tools -> system -> messages) with a 20-block read
lookback. The dominant cost is the ~15.5k-token transcript, re-sent every round;
it is pinned with a FIXED breakpoint on messages[0] so the read is found at 0
blocks back regardless of how many check_range blocks a round appends. A MOVING
breakpoint on the newest turn additionally caches recent history when it falls
inside the lookback window. Both go on copies so the canonical list never
accumulates markers past Anthropic's limit of four.
"""
from yt_scheduler.services import clipper


def _cache_marked_blocks(messages: list[dict]) -> list:
    """Every content block across all messages that carries a cache breakpoint."""
    marked = []
    for m in messages:
        content = m["content"]
        blocks = content if isinstance(content, list) else [content]
        for b in blocks:
            if isinstance(b, dict) and b.get("cache_control"):
                marked.append(b)
    return marked


def test_round_one_marks_only_the_transcript_string_turn():
    messages = [{"role": "user", "content": "PARENT + huge transcript"}]
    out = clipper._messages_with_cache_breakpoints(messages)
    marked = _cache_marked_blocks(out)
    assert len(marked) == 1, "one turn -> one breakpoint"
    only = marked[0]
    assert only["type"] == "text"
    assert only["text"] == "PARENT + huge transcript"
    assert only["cache_control"] == {"type": "ephemeral"}


def test_later_rounds_pin_transcript_and_ride_the_newest_turn():
    messages = [
        {"role": "user", "content": "transcript"},
        {"role": "assistant", "content": [{"type": "text", "text": "checking"}]},
        {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "a", "content": "PASS"},
            {"type": "tool_result", "tool_use_id": "b", "content": "FAIL"},
        ]},
    ]
    out = clipper._messages_with_cache_breakpoints(messages)
    marked = _cache_marked_blocks(out)
    assert len(marked) == 2, "fixed transcript breakpoint + one moving breakpoint"
    # The FIXED breakpoint is on the transcript (messages[0]) — the read that
    # the 20-block lookback would otherwise miss on a large round.
    assert out[0]["content"][0]["type"] == "text"
    assert out[0]["content"][0]["text"] == "transcript"
    assert out[0]["content"][0]["cache_control"] == {"type": "ephemeral"}
    # The MOVING breakpoint is on the final block of the final turn.
    assert out[-1]["content"][-1]["tool_use_id"] == "b"
    assert out[-1]["content"][-1]["cache_control"] == {"type": "ephemeral"}
    # The assistant turn between them is never marked.
    assert not out[1]["content"][0].get("cache_control")


def test_never_exceeds_two_breakpoints_even_on_a_long_conversation():
    messages = [{"role": "user", "content": "transcript"}]
    for i in range(10):
        messages.append({"role": "assistant", "content": [
            {"type": "tool_use", "id": f"t{i}", "name": "check_range", "input": {}},
        ]})
        messages.append({"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": f"t{i}", "content": "PASS"},
        ]})
    out = clipper._messages_with_cache_breakpoints(messages)
    assert len(_cache_marked_blocks(out)) == 2


def test_does_not_mutate_the_canonical_messages():
    messages = [
        {"role": "user", "content": "transcript"},
        {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "a", "content": "PASS"},
        ]},
    ]
    clipper._messages_with_cache_breakpoints(messages)
    assert _cache_marked_blocks(messages) == [], (
        "the input list must stay marker-free so breakpoints can't accumulate "
        "across rounds"
    )
    assert messages[0]["content"] == "transcript", "transcript turn left a string"


def test_non_dict_tail_block_raises_rather_than_skipping_cache():
    messages = [
        {"role": "user", "content": "transcript"},
        {"role": "user", "content": ["a bare string is not annotatable"]},
    ]
    try:
        clipper._messages_with_cache_breakpoints(messages)
    except TypeError:
        return
    raise AssertionError("expected TypeError on a non-dict tail block")


def test_empty_messages_is_returned_unchanged():
    assert clipper._messages_with_cache_breakpoints([]) == []
