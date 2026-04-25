"""Social and prompt templates share a renderer; the models split for type clarity."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

Tier = Literal["hook", "short", "segment", "video"]


class Template(BaseModel):
    """Per-platform social template body + media settings."""

    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    project_id: int | None = None
    name: str
    description: str = ""
    platforms: dict
    applies_to: list[Tier] = Field(default_factory=lambda: ["hook", "short", "segment", "video"])
    is_builtin: bool = False
    created_at: datetime | None = None
    updated_at: datetime | None = None


class PromptTemplate(BaseModel):
    """LLM prompt template — uses the same {{variable}} engine as social templates."""

    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    project_id: int | None = None
    key: str
    name: str
    body: str
    applies_to: list[Tier] = Field(default_factory=lambda: ["hook", "short", "segment", "video"])
    updated_at: datetime | None = None
