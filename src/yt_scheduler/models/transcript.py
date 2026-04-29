"""Multiple transcripts per video, with provenance metadata."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

TranscriptSource = Literal[
    "youtube",
    "apple_speech",
    "mlx_whisper",
    "whispercpp",
    "user_edited",
]


class Transcript(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int | None = None
    video_id: str
    source: TranscriptSource
    source_detail: str | None = None  # e.g. mlx-whisper model name
    text: str
    created_at: datetime | None = None
