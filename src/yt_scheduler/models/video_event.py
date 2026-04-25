"""Per-video activity log entry."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

EventType = Literal[
    "created",
    "imported",
    "uploaded",
    "metadata_updated",
    "publish_scheduled",
    "published",
    "social_post_scheduled",
    "social_post_published",
]


class VideoEvent(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    video_id: str
    type: EventType
    payload: dict
    created_at: datetime | None = None
