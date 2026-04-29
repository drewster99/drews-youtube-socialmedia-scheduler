"""Reassign videos to the project whose YouTube channel actually owns them.

The active-project credential leak that lived in the codebase before the
recent multi-project fixes meant uploads + imports could land in the
"wrong" project's videos table while the actual YouTube video lived on
a different channel. This script repairs that drift.

Usage:
    python scripts/reproject_videos.py            # dry run (no DB writes)
    python scripts/reproject_videos.py --apply    # apply moves
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

# Make the package importable when invoked directly.
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent / "src"))


async def _list_for_project(slug: str, max_results: int) -> set[str]:
    """Return the YouTube video IDs owned by the project's channel.

    Binds the active project so ``youtube.list_channel_videos`` uses
    that project's OAuth credentials.
    """
    from yt_scheduler.services import youtube
    from yt_scheduler.services.auth import set_active_project

    set_active_project(slug)
    items = await asyncio.to_thread(youtube.list_channel_videos, max_results)
    out: set[str] = set()
    for item in items:
        snippet = item.get("snippet", {}) or {}
        resource_id = snippet.get("resourceId", {}) or {}
        video_id = resource_id.get("videoId") or item.get("id")
        if video_id:
            out.add(str(video_id))
    return out


async def reproject(apply: bool, max_per_channel: int) -> int:
    from yt_scheduler.config import ensure_dirs
    ensure_dirs()
    from yt_scheduler.database import get_db
    from yt_scheduler.services.auth import get_credentials

    db = await get_db()
    project_rows = await db.execute_fetchall(
        "SELECT id, slug, name, youtube_channel_id FROM projects ORDER BY id"
    )
    if not project_rows:
        print("No projects in DB.")
        return 0

    print(f"Found {len(project_rows)} project(s):")
    for p in project_rows:
        print(
            f"  id={p['id']:>3}  slug={p['slug']:<30}  channel={p['youtube_channel_id'] or '(unbound)'}"
        )
    print()

    # video_id → owning project_id, derived from each project's own
    # channel uploads playlist (so we trust the channel's view, not
    # what the DB currently claims).
    video_to_project: dict[str, int] = {}
    skipped_projects: list[tuple[str, str]] = []
    for p in project_rows:
        slug = p["slug"]
        if not get_credentials(slug):
            skipped_projects.append((slug, "no credentials stored"))
            continue
        try:
            owned = await _list_for_project(slug, max_per_channel)
        except Exception as exc:
            skipped_projects.append((slug, f"list failed: {exc}"))
            continue
        print(f"  {slug}: channel owns {len(owned)} video(s)")
        for vid in owned:
            existing = video_to_project.get(vid)
            if existing is not None and existing != p["id"]:
                print(
                    f"    WARN video {vid} appears under two channels "
                    f"(project {existing} and {p['id']}) — keeping first"
                )
                continue
            video_to_project[vid] = int(p["id"])
    print()

    if skipped_projects:
        print("Skipped projects:")
        for slug, reason in skipped_projects:
            print(f"  {slug}: {reason}")
        print()

    db_rows = await db.execute_fetchall(
        "SELECT id, project_id, title FROM videos ORDER BY created_at"
    )

    moves: list[tuple[str, str, int, int]] = []
    not_found: list[tuple[str, str, int]] = []
    correct: int = 0
    for row in db_rows:
        vid = row["id"]
        title = (row["title"] or "")[:60]
        actual = video_to_project.get(vid)
        current = int(row["project_id"]) if row["project_id"] is not None else None
        if actual is None:
            not_found.append((vid, title, current or 0))
        elif current is None or actual != current:
            moves.append((vid, title, current or 0, actual))
        else:
            correct += 1

    print(f"DB videos: {len(db_rows)}")
    print(f"  already correct:   {correct}")
    print(f"  to move:           {len(moves)}")
    print(f"  ownership unknown: {len(not_found)}  (not on any authed channel)")
    print()

    if moves:
        print("Moves:")
        for vid, title, src, dst in moves:
            print(f"  {vid}  '{title}'  project {src} → {dst}")
        print()

    if not_found:
        print("Unknown ownership (left alone):")
        for vid, title, src in not_found:
            print(f"  {vid}  '{title}'  currently project {src}")
        print()

    if not apply:
        print("Dry run. Re-run with --apply to commit the moves.")
        return 0

    if not moves:
        print("No moves to apply.")
        return 0

    for vid, _title, _src, dst in moves:
        await db.execute(
            "UPDATE videos SET project_id = ?, updated_at = datetime('now') "
            "WHERE id = ?",
            (dst, vid),
        )
    await db.commit()
    print(f"Applied: {len(moves)} video(s) reassigned.")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually update the DB. Without this flag the script is a dry run.",
    )
    parser.add_argument(
        "--max-per-channel", type=int, default=500,
        help="Max videos to fetch per channel via the uploads playlist (default 500).",
    )
    args = parser.parse_args()
    asyncio.run(reproject(apply=args.apply, max_per_channel=args.max_per_channel))


if __name__ == "__main__":
    main()
