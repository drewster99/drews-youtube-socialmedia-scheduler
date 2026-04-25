"""Pydantic models exposed at route boundaries.

Models are added here incrementally as later phases need them. The current set
covers the foundation (project, video, template, transcript, social account,
video event, prompt template). Routes still pass dicts internally where
convenient — these models are for request/response validation and for
documenting the shape of new tables.
"""

from __future__ import annotations

from .project import Project
from .social_account import SocialAccount
from .template import PromptTemplate, Template
from .transcript import Transcript
from .video import Video
from .video_event import VideoEvent

__all__ = [
    "Project",
    "PromptTemplate",
    "SocialAccount",
    "Template",
    "Transcript",
    "Video",
    "VideoEvent",
]
