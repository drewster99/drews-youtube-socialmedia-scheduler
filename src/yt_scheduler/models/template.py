"""Social and prompt templates share a renderer; the models split for type clarity."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

Tier = Literal["hook", "short", "segment", "video"]

#: What a template declares it can be used for. Overlaps Tier by construction,
#: but is deliberately a separate vocabulary: 'standalone' is a kind of item,
#: never a duration-derived tier, so it must not leak into videos.tier. Without
#: it a standalone item could not be covered by any template, and so could
#: never enter a smart queue.
TemplateAppliesTo = Literal["hook", "short", "segment", "video", "standalone"]


class Template(BaseModel):
    """Per-platform social template body + media settings."""

    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    project_id: int | None = None
    name: str
    description: str = ""
    platforms: dict
    applies_to: list[TemplateAppliesTo] = Field(default_factory=lambda: ["hook", "short", "segment", "video"])
    is_builtin: bool = False
    created_at: datetime | None = None
    updated_at: datetime | None = None


class PromptTemplate(BaseModel):
    """LLM prompt template — uses the same {{variable}} engine as social templates.

    ``system_body`` is the optional system prompt; ``None`` means "send no
    system prompt" (matches today's behaviour for the description seeds).
    Stored alongside ``body`` so the user can edit both halves from
    Project Settings.
    """

    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    project_id: int | None = None
    key: str
    name: str
    body: str
    system_body: str | None = None
    applies_to: list[TemplateAppliesTo] = Field(default_factory=lambda: ["hook", "short", "segment", "video"])
    updated_at: datetime | None = None
