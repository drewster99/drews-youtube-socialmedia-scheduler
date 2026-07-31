#!/usr/bin/env python3
"""Iterate on clip-proposal behaviour WITHOUT rebuilding the app.

The packaged app runs a copy of ``yt_scheduler`` bundled inside the .app, so
every tweak to ``services/clipper.py`` normally needs ``macos/build.sh --debug``
+ relaunch before it can be tested against a real Generate run. That is a
multi-minute loop for tuning the check_range / propose_clips conversation.

This harness runs ``propose_clips_for_kind_indexed`` directly from ``src/`` —
the same code pytest imports — against the REAL Anthropic model and a REAL
parent transcript, and prints the full round-by-round trace. Edit clipper.py,
re-run this, read the trace. No rebuild.

Usage (the USER runs this; it reads the Anthropic key from the Keychain, which
fires ONE password prompt — not the ~7 the full app fires, and no server):

    ! .venv/bin/python scripts/clip_dev.py hook
    ! .venv/bin/python scripts/clip_dev.py all --parent jaI3fRPiMoE

Arguments:
    kind         hook | short | segment | all         (default: all)
    --parent ID  parent video id                       (default: jaI3fRPiMoE)
    --max N      max proposals per kind (override)      (default: the per-kind default)

Notes:
  * Units are built from the parent's stored SRT transcript (one unit per cue).
    That is close enough to exercise the loop MECHANICS (does the model submit,
    how many rounds, what passes) — which is what usually needs tuning. It is
    NOT identical to the on-device word-stream segmentation the app uses, so
    treat exact clip boundaries / counts as indicative, not final.
  * The editorial prompt block is taken from the in-code seed, so no DB is
    touched. DYS_DATA_DIR is pointed at a throwaway dir for the same reason.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

# Point everything at a scratch data dir BEFORE importing yt_scheduler, so the
# frozen DB_PATH never touches the real publisher.db. The transcript is read
# from the real DB through a separate read-only connection below.
_SCRATCH = Path(tempfile.mkdtemp(prefix="clip_dev_"))
os.environ["DYS_DATA_DIR"] = str(_SCRATCH)

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "src"))

_REAL_DB = (
    Path.home()
    / "Library/Application Support"
    / "com.nuclearcyborg.drews-socialmedia-scheduler"
    / "publisher.db"
)


def _load_transcript(parent_id: str) -> tuple[str, float]:
    """(SRT text, duration_seconds) for the parent, read-only from the real DB."""
    if not _REAL_DB.exists():
        sys.exit(f"Real DB not found at {_REAL_DB}")
    con = sqlite3.connect(f"file:{_REAL_DB}?mode=ro", uri=True)
    try:
        row = con.execute(
            "SELECT transcript, duration_seconds FROM videos WHERE id = ?",
            (parent_id,),
        ).fetchone()
    finally:
        con.close()
    if not row or not row[0]:
        sys.exit(f"No transcript stored for parent {parent_id!r}")
    return row[0], float(row[1] or 0.0)


_TS = re.compile(r"(\d{2}):(\d{2}):(\d{2}),(\d{3})\s*-->\s*(\d{2}):(\d{2}):(\d{2}),(\d{3})")


def _secs(h, m, s, ms) -> float:
    return int(h) * 3600 + int(m) * 60 + int(s) + int(ms) / 1000.0


def _units_from_srt(srt: str):
    """One ClipUnit per SRT cue, sorted by start. Enough to drive the loop."""
    from yt_scheduler.services.clip_edges import ClipUnit

    cues: list[tuple[float, float, str]] = []
    blocks = re.split(r"\n\s*\n", srt.strip())
    for block in blocks:
        m = _TS.search(block)
        if not m:
            continue
        start = _secs(*m.group(1, 2, 3, 4))
        end = _secs(*m.group(5, 6, 7, 8))
        text = " ".join(
            line.strip()
            for line in block.splitlines()
            if line.strip() and not _TS.search(line) and not line.strip().isdigit()
        ).strip()
        if text and end > start:
            cues.append((start, end, text))
    cues.sort(key=lambda c: c[0])
    return [
        ClipUnit(index=i + 1, text=t, start=s, end=e, words=[])
        for i, (s, e, t) in enumerate(cues)
    ]


async def _run_kind(kind: str, units, parent_title: str, duration: float, max_n):
    from yt_scheduler.services import clipper

    t0 = time.monotonic()
    out = await clipper.propose_clips_for_kind_indexed(
        kind=kind,
        units=units,
        parent_title=parent_title,
        parent_duration_seconds=duration,
        existing_ranges=[],
        project_id=1,
        existing_titles=[],
        max_proposals=max_n,
    )
    elapsed = time.monotonic() - t0

    print(f"\n===== {kind.upper()}  ({elapsed:.1f}s) =====")
    print(f"raw_count={out.raw_count}  accepted={len(out.accepted)}  "
          f"rejected={len(out.rejected)}  error={out.error!r}")
    for p in out.accepted:
        print(f"  ACC  {p.rating}*  {p.duration_seconds:6.1f}s  {p.title}")
    for r in out.rejected:
        print(f"  REJ  [{r.reason.value:20s}] {r.title!r}: {r.detail}")
    return out


async def _main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("kind", nargs="?", default="all",
                    choices=["hook", "short", "segment", "all"])
    ap.add_argument("--parent", default="jaI3fRPiMoE")
    ap.add_argument("--max", type=int, default=None)
    args = ap.parse_args()

    # Round summaries ("round N: answered X", "for hook: raw->accepted") come
    # from clipper's INFO logs — surface them, timestamped, so the trace shows
    # exactly how the conversation unfolded.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
    )
    # ai.py's full REQUEST/RESPONSE dumps are enormous; keep them out of the
    # trace. If you need them, set this to INFO.
    logging.getLogger("yt_scheduler.services.ai").setLevel(logging.WARNING)

    from yt_scheduler.config import get_anthropic_api_key
    if not get_anthropic_api_key():
        sys.exit("No Anthropic key in the Keychain — set it in the app's Settings first.")

    srt, duration = _load_transcript(args.parent)
    units = _units_from_srt(srt)
    print(f"parent={args.parent}  duration={duration:.0f}s  units={len(units)}  "
          f"scratch={_SCRATCH}")

    # No DB: the editorial block comes from the in-code seed for the kind.
    from yt_scheduler.services import clipper, prompts as prompt_service

    async def _editorial(kind, *, project_id):
        seed = prompt_service._SEEDS_BY_KEY[clipper.CLIP_EDITORIAL_PROMPT_KEYS[kind]]
        from yt_scheduler.services import templates as template_service
        return await template_service.async_render(seed.body, {"kind": kind})

    clipper.editorial_block_for_kind = _editorial  # type: ignore[assignment]

    kinds = ["hook", "short", "segment"] if args.kind == "all" else [args.kind]
    results = {}
    for k in kinds:
        results[k] = await _run_kind(k, units, args.parent, duration, args.max)

    print("\n===== SUMMARY =====")
    for k, out in results.items():
        status = out.error or f"{len(out.accepted)} accepted / {out.raw_count} raw"
        print(f"  {k:8s} {status}")


if __name__ == "__main__":
    asyncio.run(_main())
