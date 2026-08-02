"""Failed promo jobs must survive and stay actionable — not vanish.

The bug this guards against: 11 uploads died on the YouTube daily quota with
their cut files intact on disk and every retry parameter persisted in
``pending_promo_jobs`` — and then disappeared forever, because the promos page
read only the in-memory job dict (failed entries TTL-evict after 10 minutes)
and the startup resume selects only ``status='pending'``. Data survived; every
pathway back to it was missing.

The rules under test:

* a persisted failed row surfaces in :func:`inflight_promo_jobs` (state
  ``failed``) after the in-memory entry is gone;
* Retry flips the row to ``pending``, clears the stale error, and re-spawns
  the chain from the intact cut file;
* Dismiss marks the row ``dismissed`` (it stops surfacing) and deletes the cut
  file — unless a ``videos`` row references that file;
* a job that already uploaded to YouTube (row INSERT never completed) is
  refused, not re-uploaded.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest


async def _seed_failed_row(
    db, tmp_path: Path, *, job_id: str = "job_failed01",
    with_file: bool = True, youtube_video_id: str | None = None,
) -> Path | None:
    cut_file = None
    if with_file:
        cut_file = tmp_path / "uploads" / f"clip_short_{job_id}.mp4"
        cut_file.write_bytes(b"\x00" * 64)
    parent_file = tmp_path / "uploads" / "parent.mp4"
    parent_file.write_bytes(b"\x00" * 64)
    await db.execute(
        """INSERT INTO pending_promo_jobs (
               job_id, project_id, parent_id, forced_item_type, title,
               parent_video_path, local_path, cut_start_seconds, cut_end_seconds,
               youtube_video_id, status, last_error)
           VALUES (?, 1, 'parentVid01', 'short', 'A Failed Clip',
                   ?, ?, 10.0, 40.0, ?, 'failed',
                   'ResumableUploadError: HttpError 429 quota exceeded')""",
        (job_id, str(parent_file), str(cut_file) if cut_file else None,
         youtube_video_id),
    )
    await db.commit()
    return cut_file


async def _row(db, job_id: str) -> dict:
    rows = await db.execute_fetchall(
        "SELECT * FROM pending_promo_jobs WHERE job_id = ?", (job_id,),
    )
    assert rows, f"row {job_id} missing"
    return dict(rows[0])


@pytest.mark.asyncio
async def test_persisted_failed_job_surfaces_in_pending_jobs(isolated_db, tmp_path):
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")
    await _seed_failed_row(isolated_db, tmp_path)

    jobs = await auto_actions.inflight_promo_jobs("parentVid01", 1)

    assert len(jobs) == 1
    job = jobs[0]
    assert job["job_id"] == "job_failed01"
    assert job["state"] == "failed"
    assert job["item_type"] == "short"
    assert job["title"] == "A Failed Clip"
    assert "429" in job["last_error"]


@pytest.mark.asyncio
async def test_live_in_memory_entry_wins_over_the_persisted_row(
    isolated_db, tmp_path,
):
    """While the job is still in memory, the fresher failed:<step> entry must
    be the only one listed — no duplicate card for the same job."""
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")
    await _seed_failed_row(isolated_db, tmp_path)
    auto_actions._UPLOAD_JOBS["job_failed01"] = {
        "job_id": "job_failed01", "parent_id": "parentVid01", "project_id": 1,
        "forced_item_type": "short", "video_id": None,
        "state": "failed:uploading", "last_error": "quota", "title": "A Failed Clip",
    }
    try:
        jobs = await auto_actions.inflight_promo_jobs("parentVid01", 1)
    finally:
        auto_actions._UPLOAD_JOBS.pop("job_failed01", None)

    assert [j["state"] for j in jobs] == ["failed:uploading"]


@pytest.mark.asyncio
async def test_retry_respawns_from_the_intact_cut_file(
    isolated_db, monkeypatch, tmp_path,
):
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")
    cut_file = await _seed_failed_row(isolated_db, tmp_path)

    async def chain_stub(job_id: str) -> None:
        return None

    monkeypatch.setattr(auto_actions, "_run_promo_chain", chain_stub)

    public = await auto_actions.retry_failed_promo_job(
        "job_failed01", parent_id="parentVid01", project_id=1,
    )

    try:
        assert public["state"] == auto_actions.PROMO_STATE_PENDING
        live = auto_actions._UPLOAD_JOBS["job_failed01"]
        assert live["local_path"] == str(cut_file), "must reuse the intact cut"
        row = await _row(isolated_db, "job_failed01")
        assert row["status"] == "pending"
        assert row["last_error"] is None, "stale quota error must be cleared"
    finally:
        auto_actions._UPLOAD_JOBS.pop("job_failed01", None)


@pytest.mark.asyncio
async def test_retry_refuses_a_currently_running_job(isolated_db, tmp_path):
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")
    await _seed_failed_row(isolated_db, tmp_path)
    auto_actions._UPLOAD_JOBS["job_failed01"] = {
        "job_id": "job_failed01", "state": "uploading",
    }
    try:
        with pytest.raises(RuntimeError, match="already running"):
            await auto_actions.retry_failed_promo_job(
                "job_failed01", parent_id="parentVid01", project_id=1,
            )
    finally:
        auto_actions._UPLOAD_JOBS.pop("job_failed01", None)


@pytest.mark.asyncio
async def test_retry_refuses_an_already_uploaded_job(isolated_db, tmp_path):
    """youtube_video_id set means the upload finished — re-running would
    duplicate the YouTube video. Must refuse with the import guidance."""
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")
    await _seed_failed_row(
        isolated_db, tmp_path, youtube_video_id="ytAlreadyUp1",
    )

    with pytest.raises(RuntimeError, match="import it from the dashboard"):
        await auto_actions.retry_failed_promo_job(
            "job_failed01", parent_id="parentVid01", project_id=1,
        )
    assert (await _row(isolated_db, "job_failed01"))["status"] == "failed"
    assert "job_failed01" not in auto_actions._UPLOAD_JOBS


@pytest.mark.asyncio
async def test_retry_is_scoped_to_the_parent(isolated_db, tmp_path):
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")
    await _seed_failed_row(isolated_db, tmp_path)

    with pytest.raises(LookupError):
        await auto_actions.retry_failed_promo_job(
            "job_failed01", parent_id="someOtherParent", project_id=1,
        )


@pytest.mark.asyncio
async def test_dismiss_marks_row_and_deletes_the_cut_file(isolated_db, tmp_path):
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")
    cut_file = await _seed_failed_row(isolated_db, tmp_path)
    assert cut_file.exists()

    await auto_actions.dismiss_failed_promo_job(
        "job_failed01", parent_id="parentVid01", project_id=1,
    )

    assert not cut_file.exists(), "dismiss deletes the derived cut file"
    row = await _row(isolated_db, "job_failed01")
    assert row["status"] == "dismissed"
    assert row["cut_start_seconds"] == 10.0, "params stay — clip is re-creatable"
    assert await auto_actions.inflight_promo_jobs("parentVid01", 1) == []


@pytest.mark.asyncio
async def test_dismiss_never_deletes_a_file_a_video_row_references(
    isolated_db, tmp_path,
):
    """If the job was retried and succeeded elsewhere, its file now belongs to
    a videos row — dismissing the stale card must not break that video."""
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")
    cut_file = await _seed_failed_row(isolated_db, tmp_path)
    await isolated_db.execute(
        "INSERT INTO videos (id, project_id, title, status, video_file_path) "
        "VALUES ('vidOwnsFile', 1, 'Owns The File', 'draft', ?)",
        (str(cut_file),),
    )
    await isolated_db.commit()

    await auto_actions.dismiss_failed_promo_job(
        "job_failed01", parent_id="parentVid01", project_id=1,
    )

    assert cut_file.exists(), "a referenced file must never be deleted"
    assert (await _row(isolated_db, "job_failed01"))["status"] == "dismissed"


@pytest.mark.asyncio
async def test_dismissed_rows_are_not_resumed_at_startup(isolated_db, tmp_path):
    auto_actions = importlib.import_module("yt_scheduler.services.auto_actions")
    await _seed_failed_row(isolated_db, tmp_path)
    await auto_actions.dismiss_failed_promo_job(
        "job_failed01", parent_id="parentVid01", project_id=1,
    )

    resumed = await auto_actions.resume_pending_promo_jobs(window_hours=24)

    assert resumed == 0
    assert "job_failed01" not in auto_actions._UPLOAD_JOBS
