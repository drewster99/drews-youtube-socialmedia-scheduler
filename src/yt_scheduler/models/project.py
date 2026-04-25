"""Project model — top-level container for videos, templates, social accounts."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class Project(BaseModel):
    """A project corresponds to a single YouTube channel and a set of social accounts."""

    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    name: str
    slug: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    youtube_channel_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
