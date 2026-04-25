"""Pydantic model round-trip checks."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from yt_scheduler.models import (
    Project,
    PromptTemplate,
    SocialAccount,
    Template,
    Transcript,
    Video,
    VideoEvent,
)


def test_project_round_trip() -> None:
    p = Project(name="Default", slug="default", youtube_channel_id="UCxxx")
    payload = p.model_dump()
    assert Project.model_validate(payload) == p


def test_project_slug_validation() -> None:
    Project(name="ok", slug="ok-slug")
    with pytest.raises(ValueError):
        Project(name="bad", slug="Has Spaces")


def test_video_defaults() -> None:
    v = Video(id="abc123", title="Hello")
    assert v.tags == []
    assert v.privacy_status == "unlisted"
    assert v.status == "draft"
    assert v.imported_from_youtube is False


def test_template_default_applies_to_all_tiers() -> None:
    t = Template(name="new_video", platforms={})
    assert set(t.applies_to) == {"hook", "short", "segment", "video"}


def test_prompt_template_round_trip() -> None:
    pt = PromptTemplate(
        key="description_from_transcript",
        name="Description from transcript",
        body="Make a description from {{transcript}}",
    )
    assert PromptTemplate.model_validate(pt.model_dump()) == pt


def test_social_account_round_trip() -> None:
    s = SocialAccount(
        platform="bluesky",
        username="me.bsky.social",
        credentials_ref="bluesky:abc",
    )
    assert SocialAccount.model_validate(s.model_dump()) == s


def test_transcript_round_trip() -> None:
    t = Transcript(
        video_id="abc", source="mlx_whisper", source_detail="large-v3", text="hi"
    )
    assert Transcript.model_validate(t.model_dump()) == t


def test_video_event_round_trip() -> None:
    e = VideoEvent(
        video_id="abc",
        type="metadata_updated",
        payload={"title": {"old": "A", "new": "B"}},
        created_at=datetime.now(tz=timezone.utc),
    )
    assert VideoEvent.model_validate(e.model_dump()) == e
