"""Social media account, decoupled from any single project so it can be reused."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

Platform = Literal["twitter", "bluesky", "mastodon", "linkedin", "threads"]


class SocialAccount(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    platform: Platform
    username: str
    display_name: str | None = None
    credentials_ref: str
    created_at: datetime | None = None
