"""Video model — extended in later phases with project_id, tier, transcript pointer."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

PrivacyStatus = Literal["unlisted", "private", "public"]
VideoStatus = Literal["draft", "uploaded", "captioned", "ready", "published"]
Tier = Literal["hook", "short", "segment", "video"]

def youtube_video_id_of(video: dict) -> str | None:
    """The row's YouTube video id, or None when it has no YouTube video.

    Read from ``videos.youtube_video_id`` (migration 037). This used to be
    inferred from the primary key's length, because the id of a YouTube-backed
    row *is* its YouTube video id — a record's type derived from the shape of
    its key, which nothing enforced and which was invisible at the call site.
    """
    if "youtube_video_id" not in video:
        raise KeyError(
            "youtube_video_id is not in this row — the query that loaded it "
            "must select the column. Treating a missing column as 'no YouTube "
            "video' is the silent wrong answer this column exists to remove."
        )
    return video["youtube_video_id"] or None


def is_youtube_backed(video: dict) -> bool:
    """Whether this row has a YouTube video behind it."""
    return youtube_video_id_of(video) is not None


class Video(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: int | None = None
    title: str
    episode_number: int | None = None
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    privacy_status: PrivacyStatus = "unlisted"
    publish_at: datetime | None = None
    thumbnail_path: str | None = None
    video_file_path: str | None = None
    pinned_links: str = ""
    status: VideoStatus = "draft"
    tier: Tier | None = None
    duration_seconds: float | None = None
    transcript_id: int | None = None
    transcript_is_edited: bool = False
    transcript_source: str | None = None
    transcript_created_at: datetime | None = None
    transcript_updated_at: datetime | None = None
    description_generated_at: datetime | None = None
    tags_generated_at: datetime | None = None
    imported_from_youtube: bool = False
    # Provenance of the on-disk file referenced by ``video_file_path``
    # (migration 026 + the generated_clip extension). One of:
    # 'uploaded' | 'youtube_download' | 'user_attached' | 'generated_clip'.
    # ``None`` for pre-026 rows or rows with no local file at all.
    source_file_origin: str | None = None
    # Range cut from the parent's local file when this row was produced
    # by the Generate-from-source flow (migration 027). ``None`` for
    # everything else — manual uploads, imports, the parent itself.
    # Used by Generate's same-kind overlap filter on subsequent runs.
    cut_start_seconds: float | None = None
    cut_end_seconds: float | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
