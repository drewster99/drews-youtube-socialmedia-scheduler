"""Promo-chain job states must describe what is actually happening.

Jobs used to be created in state ``cutting``, so with the 4-wide chain
semaphore a 30-clip Confirm showed "Cutting clip from parent…" for ~28 queued
jobs for minutes — while nothing was cutting. Worse, Generate-confirm jobs
adopt the already-cut preview file and never cut at all, yet wore the same
label. The rules under test:

* a new job is ``pending`` ("Queued…") until the chain does real work;
* ``cutting`` is stamped by the chain itself, only when it actually runs a cut;
* an adopted-preview job never passes through ``cutting`` and never re-cuts.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest


async def _make_job(auto_actions, tmp_path: Path, *, adopt_cut: bool) -> str:
    """Create a promo job via start_promo_from_cut with the chain stubbed out,
    so the initial state can be observed before any work happens."""
    parent = tmp_path / "uploads" / "parent.mp4"
    parent.write_bytes(b"\x00" * 16)
    existing_cut = None
    if adopt_cut:
        existing_cut = tmp_path / "uploads" / "adopted_cut.mp4"
        existing_cut.write_bytes(b"\x00" * 16)
    return await auto_actions.start_promo_from_cut(
        parent_id="parentVid01",
        project_id=1,
        parent_video_path=parent,
        cut_start_seconds=1.0,
        cut_end_seconds=5.0,
        title="A Clip",
        item_type="segment",
        existing_cut_path=existing_cut,
    )


@pytest.mark.asyncio
async def test_new_job_is_pending_not_cutting(isolated_db, monkeypatch, tmp_path):
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")

    async def chain_stub(job_id: str) -> None:
        return None

    monkeypatch.setattr(auto_actions, "_run_promo_chain", chain_stub)

    for adopt_cut in (True, False):
        job_id = await _make_job(auto_actions, tmp_path, adopt_cut=adopt_cut)
        state = auto_actions._UPLOAD_JOBS[job_id]["state"]
        assert state == auto_actions.PROMO_STATE_PENDING, (
            f"adopt_cut={adopt_cut}: a queued job must read 'Queued…', not "
            f"claim active work (got {state!r})"
        )
        auto_actions._UPLOAD_JOBS.pop(job_id, None)


@pytest.mark.asyncio
async def test_chain_stamps_cutting_only_while_actually_cutting(
    isolated_db, monkeypatch, tmp_path,
):
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")
    clipper = importlib.import_module("yt_scheduler.services.clipper")

    async def chain_stub(job_id: str) -> None:
        return None

    monkeypatch.setattr(auto_actions, "_run_promo_chain", chain_stub)
    job_id = await _make_job(auto_actions, tmp_path, adopt_cut=False)

    state_at_cut: list[str] = []

    async def fake_cut(**kwargs):
        state_at_cut.append(auto_actions._UPLOAD_JOBS[job_id]["state"])
        raise RuntimeError("stop after the cut step")

    monkeypatch.setattr(clipper, "cut_clip_from_parent", fake_cut)

    await auto_actions._run_promo_chain_inner(job_id)

    assert state_at_cut == [auto_actions.PROMO_STATE_CUTTING], (
        "the chain must flip pending → cutting exactly when the cut runs"
    )
    assert auto_actions._UPLOAD_JOBS[job_id]["state"] == (
        f"failed:{auto_actions.PROMO_STATE_CUTTING}"
    )
    auto_actions._UPLOAD_JOBS.pop(job_id, None)


@pytest.mark.asyncio
async def test_adopted_preview_job_never_cuts_and_never_shows_cutting(
    isolated_db, monkeypatch, tmp_path,
):
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")
    clipper = importlib.import_module("yt_scheduler.services.clipper")
    youtube = importlib.import_module("yt_scheduler.services.youtube")

    async def chain_stub(job_id: str) -> None:
        return None

    monkeypatch.setattr(auto_actions, "_run_promo_chain", chain_stub)
    job_id = await _make_job(auto_actions, tmp_path, adopt_cut=True)

    async def must_not_cut(**kwargs):
        raise AssertionError(
            "an adopted-preview job re-cutting is the double-encode bug"
        )

    monkeypatch.setattr(clipper, "cut_clip_from_parent", must_not_cut)

    state_at_upload: list[str] = []

    def fake_upload(**kwargs):
        state_at_upload.append(auto_actions._UPLOAD_JOBS[job_id]["state"])
        raise RuntimeError("stop after reaching the upload step")

    monkeypatch.setattr(youtube, "upload_video", fake_upload)

    await auto_actions._run_promo_chain_inner(job_id)

    assert state_at_upload == [auto_actions.PROMO_STATE_UPLOADING], (
        "an adopted-cut job must go pending → uploading with no cutting phase"
    )
    assert auto_actions._UPLOAD_JOBS[job_id]["state"] == (
        f"failed:{auto_actions.PROMO_STATE_UPLOADING}"
    )
    auto_actions._UPLOAD_JOBS.pop(job_id, None)
