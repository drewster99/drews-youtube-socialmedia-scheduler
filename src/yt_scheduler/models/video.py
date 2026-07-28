"""Video model — extended in later phases with project_id, tier, transcript pointer."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

PrivacyStatus = Literal["unlisted", "private", "public"]
VideoStatus = Literal["draft", "uploaded", "captioned", "ready", "published"]
Tier = Literal["hook", "short", "segment", "video"]

#: Length of a YouTube video id. A YouTube-backed row *is* keyed by that id;
#: items created outside YouTube get a generated token instead, which is how
#: the two are told apart throughout the app.
_YOUTUBE_VIDEO_ID_LENGTH = 11


def is_youtube_backed(video_id: str) -> bool:
    """Whether this row has a YouTube video behind it.

    Names the `len(id) == 11` test already used in app.py, video_routes and
    promo_routes, so callers asking "does YouTube state apply here?" say that
    rather than restating the encoding.

    It is a structural assumption, not a stored fact: it holds because a
    YouTube-backed row is keyed by its YouTube video id, and ids minted for
    non-YouTube items are a different length. A generated id that happened to
    be 11 characters would be misread.
    """
    return len(video_id or "") == _YOUTUBE_VIDEO_ID_LENGTH


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
