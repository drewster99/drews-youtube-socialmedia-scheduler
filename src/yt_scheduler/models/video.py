"""Video model — extended in later phases with project_id, tier, transcript pointer."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

PrivacyStatus = Literal["unlisted", "private", "public"]
VideoStatus = Literal["draft", "uploaded", "captioned", "ready", "published"]
Tier = Literal["hook", "short", "segment", "video"]


class Video(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: int | None = None
    title: str
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
    imported_from_youtube: bool = False
    created_at: datetime | None = None
    updated_at: datetime | None = None
