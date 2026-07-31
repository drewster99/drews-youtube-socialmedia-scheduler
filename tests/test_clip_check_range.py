"""The check_range tool: pre-flight feedback for the model, not enforcement.

The model gets a PASS/FAIL per candidate before it commits, so a clip that is
2 seconds over a band can be adjusted instead of silently discarded. Two
invariants matter:

1. check_range and the server-side validator must use the SAME code. If they
   could disagree, the model would be told a range passes and then watch it be
   refused for a reason it never saw.
2. check_range is advisory. The validator still decides — a model that ignores
   a FAIL must not be able to smuggle a bad clip through.
"""

from __future__ import annotations

import importlib

import pytest


def _clipper():
    """Resolved per call: conftest purges yt_scheduler.* between tests, so a
    module-scope import would hand back a dead module object."""
    return importlib.import_module("yt_scheduler.services.clipper")



def make_units(n: int, dur: float = 10.0, gap: float = 1.0) -> list:
    ClipUnit = importlib.import_module("yt_scheduler.services.clip_edges").ClipUnit
    units, t = [], 0.0
    for i in range(1, n + 1):
        units.append(ClipUnit(index=i, text=f"unit {i}.", start=t, end=t + dur, words=[]))
        t += dur + gap
    return units


def _check(**kw):
    base = dict(
        kind="hook", units=make_units(30), first_index=2, last_index=4,
        title="A Fine Title", parent_duration_seconds=2000.0,
        existing_ranges=[], existing_titles=[],
    )
    base.update(kw)
    return _clipper().check_clip_range(**base)


def test_passing_range_reports_every_line_and_an_overall_pass():
    result = _check()
    assert result.passed
    assert "Overall result: PASS" in result.text
    # Every dimension is reported, not just the failures — the model learns
    # more from seeing what it got right too.
    for label in ("index range", "title", "title duplicate", "clip length", "overlap check"):
        assert f"  {label}:" in result.text


def test_too_long_reports_the_limit_it_missed():
    result = _check(first_index=1, last_index=9)
    assert not result.passed
    assert "Overall result: FAIL" in result.text
    assert "over the hook window [5s, 60s]" in result.text


def test_too_short_says_under_not_over():
    result = _check(kind="short", first_index=1, last_index=2)
    assert not result.passed
    assert "under the short window [60s, 180s]" in result.text


@pytest.mark.parametrize(
    "title, expected",
    [
        ("Solo", "minimum is 2"),
        ("", "title is empty"),
        (" ".join(["word"] * 13), "maximum is 12"),
    ],
)
def test_title_word_bounds(title: str, expected: str):
    result = _check(title=title)
    assert not result.passed
    assert expected in result.text


def test_out_of_bounds_indices_fail_before_any_other_check():
    result = _check(first_index=99, last_index=120)
    assert not result.passed
    assert "out of bounds" in result.text
    assert "the transcript has 30 units" in result.text


def test_duplicate_title_and_overlap_are_reported():
    dup = _check(existing_titles=["A Fine Title"])
    assert not dup.passed and "duplicates an existing clip" in dup.text
    # units 2-4 span roughly [11, 54]
    over = _check(existing_ranges=[(10.0, 60.0)])
    assert not over.passed and "overlaps" in over.text


def test_check_and_validator_agree_on_the_same_range():
    """The invariant that makes check_range trustworthy.

    Anything check_range passes must survive the validator, and anything it
    fails must be refused — for the same stated reason.
    """
    units = make_units(30)
    cases = [
        (2, 4, "A Fine Title"),        # passes
        (1, 9, "Far Too Long Clip"),   # too long
        (2, 4, "Solo"),                # title too short
        (2, 4, " ".join(["w"] * 13)),  # title too long
    ]
    for first, last, title in cases:
        verdict = _clipper().check_clip_range(
            kind="hook", units=units, first_index=first, last_index=last,
            title=title, parent_duration_seconds=2000.0,
            existing_ranges=[], existing_titles=[],
        )
        accepted, rejected = _clipper()._validate_indexed_proposals(
            [{"first_index": first, "last_index": last, "title": title,
              "reason": "r", "rating": 3}],
            kind="hook", units=units, existing_ranges=[], max_proposals=8,
            parent_duration_seconds=2000.0, existing_titles=[],
        )
        assert verdict.passed == (len(accepted) == 1), (
            f"check_range and validator disagree on {title!r}: "
            f"check passed={verdict.passed}, accepted={len(accepted)}"
        )
        if not verdict.passed:
            # Same reason, not merely the same verdict.
            assert rejected[0].detail in verdict.text, (
                f"validator reason {rejected[0].detail!r} not in the check text"
            )


def test_check_range_is_advisory_not_enforcement():
    """A model that ignores a FAIL still cannot get the clip through."""
    units = make_units(30)
    verdict = _clipper().check_clip_range(
        kind="hook", units=units, first_index=1, last_index=9,
        title="Far Too Long Clip", parent_duration_seconds=2000.0,
        existing_ranges=[], existing_titles=[],
    )
    assert not verdict.passed
    accepted, rejected = _clipper()._validate_indexed_proposals(
        [{"first_index": 1, "last_index": 9, "title": "Far Too Long Clip",
          "reason": "r", "rating": 4}],
        kind="hook", units=units, existing_ranges=[], max_proposals=8,
        parent_duration_seconds=2000.0, existing_titles=[],
    )
    assert accepted == [] and len(rejected) == 1


# --- the multi-round loop ------------------------------------------------

class _Block:
    def __init__(self, name, payload, block_id="tu_1"):
        self.type = "tool_use"
        self.name = name
        self.input = payload
        self.id = block_id


class _Msg:
    def __init__(self, blocks, stop_reason="tool_use"):
        self.content = blocks
        self.stop_reason = stop_reason
        self.usage = None


def _install_fake_claude(monkeypatch, scripted):
    """Return the recorded request kwargs; `scripted` is one _Msg per round."""
    ai = importlib.import_module("yt_scheduler.services.ai")
    clipper = importlib.import_module("yt_scheduler.services.clipper")

    sent: list[dict] = []
    rounds = iter(scripted)

    class _Client:
        class messages:
            @staticmethod
            def create(**kw):
                sent.append(kw)
                return next(rounds)

    monkeypatch.setattr(ai, "get_client", lambda: _Client())

    async def _model():
        return "claude-x"

    monkeypatch.setattr(ai, "_resolve_model", _model)

    async def _editorial(kind, *, project_id):
        return "## editorial"

    monkeypatch.setattr(clipper, "editorial_block_for_kind", _editorial)
    return sent


@pytest.mark.asyncio
async def test_check_calls_are_answered_then_proposals_accepted(monkeypatch):
    """The whole point: the model checks, we answer, it proposes, we accept."""
    units = make_units(30)
    scripted = [
        _Msg([
            _Block("check_range", {"first_index": 2, "last_index": 4, "title": "Good One Here"}, "a"),
            _Block("check_range", {"first_index": 1, "last_index": 9, "title": "Way Too Long"}, "b"),
        ]),
        _Msg([_Block("propose_clips", {"proposals": [
            {"first_index": 2, "last_index": 4, "title": "Good One Here",
             "reason": "r", "rating": 4},
        ]})]),
    ]
    sent = _install_fake_claude(monkeypatch, scripted)

    out = await _clipper().propose_clips_for_kind_indexed(
        kind="hook", units=units, parent_title="P", parent_duration_seconds=2000.0,
        existing_ranges=[], project_id=1, max_proposals=5,
    )
    assert [p.title for p in out.accepted] == ["Good One Here"]
    assert out.error is None

    # Round 2 must carry the tool_results for BOTH checks, in one user turn.
    second = sent[1]["messages"]
    results = [b for m in second if isinstance(m.get("content"), list)
               for b in m["content"] if isinstance(b, dict) and b.get("type") == "tool_result"]
    assert len(results) == 2
    joined = "\n".join(r["content"] for r in results)
    assert "Overall result: PASS" in joined
    assert "over the hook window" in joined, "the failing candidate was told why"


@pytest.mark.asyncio
async def test_both_tools_offered_and_choice_is_any(monkeypatch):
    """Pinning tool_choice to propose_clips would make check_range unreachable
    no matter what the prompt says."""
    scripted = [_Msg([_Block("propose_clips", {"proposals": []})])]
    sent = _install_fake_claude(monkeypatch, scripted)
    await _clipper().propose_clips_for_kind_indexed(
        kind="hook", units=make_units(30), parent_title="P",
        parent_duration_seconds=2000.0, existing_ranges=[], project_id=1,
    )
    assert sent[0]["tool_choice"] == {"type": "any"}
    assert {t["name"] for t in sent[0]["tools"]} == {"check_range", "propose_clips"}


@pytest.mark.asyncio
async def test_endless_checking_stops_and_reports(monkeypatch):
    """A model that never proposes must not loop on our money forever."""
    check_forever = [
        _Msg([_Block("check_range", {"first_index": 2, "last_index": 4, "title": "Round Trip"})])
        for _ in range(_clipper().MAX_PROPOSAL_ROUNDS)
    ]
    sent = _install_fake_claude(monkeypatch, check_forever)
    out = await _clipper().propose_clips_for_kind_indexed(
        kind="hook", units=make_units(30), parent_title="P",
        parent_duration_seconds=2000.0, existing_ranges=[], project_id=1,
    )
    assert len(sent) == _clipper().MAX_PROPOSAL_ROUNDS
    assert out.accepted == [] and out.rejected == []
    assert out.error is not None and "without ever calling propose_clips" in out.error


@pytest.mark.asyncio
async def test_a_malformed_check_argument_is_answered_not_fatal(monkeypatch):
    """A bad range from the model gets a FAIL it can act on, not a crash."""
    scripted = [
        _Msg([_Block("check_range", {"first_index": "not a number", "last_index": 4, "title": "T T"})]),
        _Msg([_Block("propose_clips", {"proposals": []})]),
    ]
    sent = _install_fake_claude(monkeypatch, scripted)
    out = await _clipper().propose_clips_for_kind_indexed(
        kind="hook", units=make_units(30), parent_title="P",
        parent_duration_seconds=2000.0, existing_ranges=[], project_id=1,
    )
    assert out.error is None
    results = [b for m in sent[1]["messages"] if isinstance(m.get("content"), list)
               for b in m["content"] if isinstance(b, dict) and b.get("type") == "tool_result"]
    assert "could not read the range you sent" in results[0]["content"]


@pytest.mark.asyncio
async def test_neither_tool_is_a_failure_not_an_empty_result(monkeypatch):
    scripted = [_Msg([], stop_reason="max_tokens")]
    _install_fake_claude(monkeypatch, scripted)
    out = await _clipper().propose_clips_for_kind_indexed(
        kind="hook", units=make_units(30), parent_title="P",
        parent_duration_seconds=2000.0, existing_ranges=[], project_id=1,
    )
    assert out.error is not None
    assert "max_tokens" in out.error and "neither check_range nor propose_clips" in out.error


@pytest.mark.asyncio
async def test_a_json_stringified_proposals_array_is_coerced(monkeypatch):
    """After several check_range rounds the model sometimes submits proposals
    as a JSON STRING ("[{...}]") instead of a native array. Rejecting it threw
    away a whole kind's validated candidates; it must be parsed instead."""
    import json as _json
    units = make_units(30)
    scripted = [
        _Msg([_Block("propose_clips", {"proposals": _json.dumps([
            {"first_index": 2, "last_index": 4, "title": "A Real Clip",
             "reason": "r", "rating": 4},
        ])})]),
    ]
    _install_fake_claude(monkeypatch, scripted)
    out = await _clipper().propose_clips_for_kind_indexed(
        kind="hook", units=units, parent_title="P", parent_duration_seconds=2000.0,
        existing_ranges=[], project_id=1, max_proposals=5,
    )
    assert out.error is None, out.error
    assert [p.title for p in out.accepted] == ["A Real Clip"]


@pytest.mark.asyncio
async def test_a_non_json_string_proposals_still_fails_loudly(monkeypatch):
    """A string that isn't a JSON array is a real malformed submission — fail,
    don't silently return nothing."""
    scripted = [_Msg([_Block("propose_clips", {"proposals": "sorry, none good"})])]
    _install_fake_claude(monkeypatch, scripted)
    out = await _clipper().propose_clips_for_kind_indexed(
        kind="hook", units=make_units(30), parent_title="P",
        parent_duration_seconds=2000.0, existing_ranges=[], project_id=1,
    )
    assert out.error is not None and "no usable 'proposals'" in out.error


@pytest.mark.asyncio
async def test_endless_checking_is_forced_to_submit_on_the_final_round(monkeypatch):
    """A model that only ever calls check_range must be MADE to submit on the
    last round, not fail with 'never called propose_clips'. Regression: a real
    hook run burned all 6 rounds checking and lost the whole batch."""
    clipper = _clipper()
    units = make_units(30)
    # check_range for every round up to the last; a propose_clips on the last
    # (the model complying with the forced tool_choice).
    scripted = [
        _Msg([_Block("check_range",
                     {"first_index": 2, "last_index": 4, "title": "Round Check"},
                     f"c{r}")])
        for r in range(clipper.MAX_PROPOSAL_ROUNDS - 1)
    ]
    scripted.append(_Msg([_Block("propose_clips", {"proposals": [
        {"first_index": 2, "last_index": 4, "title": "Forced Submission",
         "reason": "r", "rating": 4},
    ]})]))
    sent = _install_fake_claude(monkeypatch, scripted)

    out = await clipper.propose_clips_for_kind_indexed(
        kind="hook", units=units, parent_title="P", parent_duration_seconds=2000.0,
        existing_ranges=[], project_id=1, max_proposals=8,
    )
    assert out.error is None, out.error
    assert [p.title for p in out.accepted] == ["Forced Submission"]

    # The LAST request forced propose_clips; earlier ones left the choice open.
    assert sent[-1]["tool_choice"] == {"type": "tool", "name": "propose_clips"}
    assert sent[0]["tool_choice"] == {"type": "any"}

    # The round before the forced one carries the "final check round" nudge.
    penultimate_user = sent[-1]["messages"][-1]
    texts = [b.get("text", "") for b in penultimate_user["content"]
             if isinstance(b, dict) and b.get("type") == "text"]
    assert any("final check round" in t for t in texts)
