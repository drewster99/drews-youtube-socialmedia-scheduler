#!/usr/bin/env python3
"""Regenerate ``preview_clips.json`` for clip_previewer.py — NO LLM, NO cutting,
NO transcription, NO app.

It takes the FIXED clip selections (title + unit index range, in
``preview_selections.json``) and the stored word-stream dump, rebuilds the exact
units, and runs the CURRENT ``clip_edges.compute_edges`` — so the previewer hears
whatever the edge logic currently produces. Re-run after every edit to
``clip_edges`` and hit Refresh in the browser.

Variants (emitted side-by-side per clip so you can A/B them):
    current  cut as-is
    a        snap the END forward to the nearest real pause
    b        rescue a contiguous tail (extend past the next word, fade covers it)
    ab       both

    .venv/bin/python scripts/gen_preview_clips.py                  # hooks, current + ab
    .venv/bin/python scripts/gen_preview_clips.py --variants current,a,b,ab
    .venv/bin/python scripts/gen_preview_clips.py --kind all
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
os.environ.setdefault("DYS_DATA_DIR", tempfile.mkdtemp(prefix="clip_preview_"))
sys.path.insert(0, str(HERE.parent / "src"))

SELECTIONS = HERE / "preview_selections.json"
OUT = HERE / "preview_clips.json"
DEFAULT_SOURCE = str(
    Path.home()
    / "Library/Application Support/com.nuclearcyborg.drews-socialmedia-scheduler"
    / "uploads/source_6a85d12aa21eef48.mov"
)

# Preview-only edge tuning (sandbox; NOT yet in production clip_edges). The tail
# extends by the ACTUAL inter-word pause, floored at TAIL_MIN and capped at
# TAIL_MAX; (a) snaps the end forward to the nearest unit whose trailing gap is
# >= SNAP_PAUSE_THRESHOLD.
SNAP_PAUSE_THRESHOLD_SECONDS = 0.12   # (a) end-snap: what counts as a "pause"
TAIL_MIN_SECONDS = 0.30               # minimum tail extension (floor)
TAIL_MAX_SECONDS = 0.50               # take the full pause up to here (cap)


def _latest_dump() -> str:
    hits = sorted(glob.glob(str(Path(tempfile.gettempdir()) / "dys_wordstream_*.json")))
    if not hits:
        sys.exit("No dys_wordstream_*.json found — run a Generate once so the dump exists.")
    return hits[-1]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dump", default=None, help="word-stream dump (default: latest in tmp)")
    ap.add_argument("--source", default=DEFAULT_SOURCE)
    ap.add_argument("--kind", default="hook", choices=["hook", "short", "segment", "all"])
    ap.add_argument("--variants", default="ab")
    args = ap.parse_args()

    from yt_scheduler.services import clip_edges as ce
    from yt_scheduler.services.clip_edges import build_units, snap_clip_end_to_pause
    from yt_scheduler.services.transcription import TranscriptWord

    dump_path = args.dump or _latest_dump()
    dump = json.loads(Path(dump_path).read_text())
    words = [
        TranscriptWord(start=w["start"], end=w["end"], word=w["word"],
                       probability=w.get("probability") or 1.0)
        for w in dump["words"]
    ]
    units = build_units(words)
    quantum = ce.detect_quantum(units)
    sels = json.loads(SELECTIONS.read_text())
    if args.kind != "all":
        sels = [s for s in sels if s["kind"] == args.kind]
    variants = [v.strip() for v in args.variants.split(",") if v.strip()]

    def intermediates(v: str, first: int, last: int) -> dict:
        """Every interim + final edge value, computed here in the sandbox (not via
        production compute_edges) so the tail rule can be tuned without touching
        clip_edges. Head + fade still match production; the TAIL is the new
        min/max-clamped pause rule."""
        do_snap = v in ("a", "ab")
        end = (snap_clip_end_to_pause(units, last, threshold=SNAP_PAUSE_THRESHOLD_SECONDS)
               if do_snap else last)
        a, b = units[first - 1], units[end - 1]
        prev_end = units[first - 2].end if first > 1 else None
        next_start = units[end].start if end < len(units) else None
        at_end = next_start is None
        head_gap = (a.start - prev_end) if prev_end is not None else a.start
        tail_gap = None if at_end else (next_start - b.end)
        head_room = min(ce.HEAD_PAD_SECONDS, max(quantum, head_gap))
        # TAIL: use the real pause, floored at TAIL_MIN, capped at TAIL_MAX.
        # transcript end = unlimited pause -> the cap.
        eff_tail = TAIL_MAX_SECONDS if at_end else tail_gap
        tail_room = min(TAIL_MAX_SECONDS, max(TAIL_MIN_SECONDS, eff_tail))
        return {
            "end": end, "snapped": end != last, "at_end": at_end,
            "head_gap": head_gap, "head_room": head_room,
            "tail_gap": tail_gap, "tail_room": tail_room,
            "final_start": round(max(0.0, a.start - head_room), 3),
            "final_end": round(b.end + tail_room, 3),
        }

    def calc_str(last: int, im: dict) -> str:
        tg = "end" if im["at_end"] else f"{im['tail_gap']:.2f}"
        head = f"HEAD gap {im['head_gap']:.2f} -> room {im['head_room']:.2f} (max {ce.HEAD_PAD_SECONDS})"
        tail = (f"TAIL gap {tg} -> room {im['tail_room']:.2f} "
                f"(min {TAIL_MIN_SECONDS}, max {TAIL_MAX_SECONDS})")
        snap = f"snap {last}->{im['end']}" + ("*" if im["snapped"] else "") + f" (thr {SNAP_PAUSE_THRESHOLD_SECONDS})"
        return f"q{round(quantum, 2)}   {head}   {tail}   {snap}"

    clips = []
    for s in sels:
        for v in variants:
            im = intermediates(v, s["first_index"], s["last_index"])
            end = im["end"]
            label = f"  [{v}]" if len(variants) > 1 else ""
            clips.append({
                "title": s["title"] + label,
                "kind": s["kind"],
                "variant": v,
                "first_index": s["first_index"],
                "last_index": end,
                "start": im["final_start"],
                "end": im["final_end"],
                "fade_in": ce.EDGE_FADE_SECONDS,
                "fade_out": ce.EDGE_FADE_SECONDS,
                "first_text": units[s["first_index"] - 1].text,
                "last_text": units[end - 1].text,
                "calc": calc_str(s["last_index"], im),
            })

    OUT.write_text(json.dumps(
        {"source": args.source, "dump": dump_path, "detected_quantum": dump.get("detected_quantum_seconds"),
         "clips": clips}, indent=2))
    print(f"wrote {OUT}")
    print(f"  {len(sels)} selections x {len(variants)} variants = {len(clips)} clips  "
          f"(kind={args.kind}, quantum={dump.get('detected_quantum_seconds')})")
    print(f"  source: {args.source}")
    print(f"  dump:   {dump_path}")


if __name__ == "__main__":
    main()
