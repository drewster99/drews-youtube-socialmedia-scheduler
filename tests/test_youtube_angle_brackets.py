"""Regression tests for the '<' / '>' substitution on YouTube-bound text.

Google documents the constraint on the Videos resource: snippet.title and
snippet.description "may contain all valid UTF-8 characters except < and >".
Sending either character fails the request outright (invalidTitle /
invalidDescription), and Google documents no escape hatch.

The app therefore swaps them for single guillemets. What these tests pin down:

  1. The substitution itself, and that it is idempotent.
  2. That it runs on BOTH outbound YouTube calls — insert (upload_video) and
     update (update_video_metadata) — for title AND description. The insert
     path was historically unsanitized.
  3. That AI-generated titles/descriptions are cleaned before they are ever
     returned, so the value we PERSIST equals the value YouTube receives.
     The prompts also ask Claude to avoid the characters, but a prompt is a
     request, not a guarantee — this is the part that actually holds.
  4. That social-post copy is NOT sanitized: X/Bluesky/Mastodon accept angle
     brackets, and mangling them there would corrupt valid posts.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from yt_scheduler.services import ai, youtube  # noqa: E402
from yt_scheduler.services.youtube import sanitize_youtube_text  # noqa: E402

DIRTY = "Understanding <NSObject> and <UIView>"
CLEAN = "Understanding ‹NSObject› and ‹UIView›"


# ---------------------------------------------------------------------------
# 1. The substitution
# ---------------------------------------------------------------------------

def test_substitutes_both_angle_brackets():
    assert sanitize_youtube_text(DIRTY) == CLEAN


def test_leaves_clean_text_untouched():
    text = "A normal title — no brackets, em-dashes & ampersands are fine"
    assert sanitize_youtube_text(text) == text


def test_is_idempotent():
    """It runs at intake AND again on the way out, so double-application must
    be a no-op — otherwise guillemets would compound on every push."""
    once = sanitize_youtube_text(DIRTY)
    assert sanitize_youtube_text(once) == once


def test_does_not_invent_html_entities():
    """'&lt;' is undocumented folklore; we must not emit it. Google never says
    YouTube decodes entities, so a viewer could see a literal '&lt;'."""
    assert "&lt;" not in sanitize_youtube_text(DIRTY)
    assert "&gt;" not in sanitize_youtube_text(DIRTY)


# ---------------------------------------------------------------------------
# 2. Both outbound YouTube paths
# ---------------------------------------------------------------------------

class _FakeVideosResource:
    """Captures the request body instead of calling Google."""

    def __init__(self):
        self.inserted_body = None
        self.updated_body = None

    def insert(self, *, part, body, media_body, notifySubscribers=None, **kw):
        self.inserted_body = body
        return _FakeRequest({"id": "abc12345678"})

    def update(self, *, part, body, **kw):
        self.updated_body = body
        return _FakeRequest(body)

    def list(self, *, part, id, **kw):
        return _FakeRequest(
            {"items": [{"snippet": {"title": "old"}, "status": {"privacyStatus": "unlisted"}}]}
        )


class _FakeRequest:
    def __init__(self, response):
        self._response = response
        self.resumable = None

    def execute(self):
        return self._response

    def next_chunk(self, num_retries=0):
        # The real resumable loop calls this until it yields a response.
        return None, self._response


class _FakeYouTubeService:
    def __init__(self, videos):
        self._videos = videos

    def videos(self):
        return self._videos


@pytest.fixture
def fake_videos(monkeypatch, tmp_path):
    videos = _FakeVideosResource()
    monkeypatch.setattr(
        youtube, "get_youtube_service", lambda: _FakeYouTubeService(videos)
    )
    # upload_video wraps the file in a real MediaFileUpload; hand it a real file.
    monkeypatch.setattr(youtube, "MediaFileUpload", lambda *a, **kw: _FakeRequest(None))
    return videos


def test_upload_sanitizes_title_and_description(fake_videos, tmp_path):
    """The insert path was the gap: it pushed raw text straight into the
    snippet while the update path sanitized."""
    video_file = tmp_path / "clip.mp4"
    video_file.write_bytes(b"not really a video")

    youtube.upload_video(file_path=video_file, title=DIRTY, description=DIRTY)

    snippet = fake_videos.inserted_body["snippet"]
    assert snippet["title"] == CLEAN
    assert snippet["description"] == CLEAN


def test_update_sanitizes_title_and_description(fake_videos):
    youtube.update_video_metadata("abc12345678", title=DIRTY, description=DIRTY)

    snippet = fake_videos.updated_body["snippet"]
    assert snippet["title"] == CLEAN
    assert snippet["description"] == CLEAN


# ---------------------------------------------------------------------------
# 3. Generated text is clean before it is persisted
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_generated_description_is_sanitized(monkeypatch):
    """The four DB write sites store whatever ai.py hands back, so ai.py is
    where the DB/YouTube invariant is actually enforced."""
    monkeypatch.setattr(ai, "_extract_text", lambda message: DIRTY)
    monkeypatch.setattr(ai, "_resolve_model", _async_return("claude-test"))
    monkeypatch.setattr(ai, "get_client", lambda: _FakeClaudeClient())
    monkeypatch.setattr(
        ai, "_render_template_body", _async_passthrough_render()
    )

    async def fake_prompt(key, *, project_id, prefer_promo_variant=False):
        return {"body": "irrelevant", "system": None}

    import yt_scheduler.services.prompts as prompts_service
    monkeypatch.setattr(prompts_service, "get_prompt_with_fallback", fake_prompt)

    result = await ai.generate_seo_description(
        title="t", transcript="x", project_id=1
    )
    assert result == CLEAN


def test_fallback_title_is_sanitized():
    """The deterministic filename fallback runs when the AI call fails — it
    reaches YouTube too, so it needs the same treatment."""
    assert "<" not in ai.fallback_title_from_filename("a <weird> name.mp4")
    assert ">" not in ai.fallback_title_from_filename("a <weird> name.mp4")


# ---------------------------------------------------------------------------
# 4. Social copy is deliberately left alone
# ---------------------------------------------------------------------------

def test_social_platforms_keep_their_angle_brackets():
    """Only YouTube forbids these. Sanitizing social posts would be a bug."""
    import inspect
    source = inspect.getsource(ai.call_ai_block)
    assert "_sanitized_for_youtube" not in source


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _async_return(value):
    async def _inner(*a, **kw):
        return value
    return _inner


def _async_passthrough_render():
    async def _inner(body, variables):
        return body
    return _inner


class _FakeClaudeClient:
    class messages:  # noqa: N801 — mirrors the anthropic client shape
        @staticmethod
        def create(**kwargs):
            return object()


# ---------------------------------------------------------------------------
# 5. Pre-supplied promo titles (never touched an AI generator)
# ---------------------------------------------------------------------------

def test_pre_supplied_promo_title_is_sanitized_before_it_is_stored():
    """Generate-from-source proposes a clip title and the user can edit it in the
    review screen, so it never passes through ai.py. upload_video sanitizes it
    outbound — but the promo chain also INSERTs it into SQLite. Without an
    explicit sanitize the DB would keep '<' while YouTube served '‹', which is
    exactly the split this whole change exists to prevent."""
    import inspect

    from yt_scheduler.services import auto_actions

    # _run_promo_chain is a thin semaphore wrapper; the work is in the inner fn.
    source = inspect.getsource(auto_actions._run_promo_chain_inner)
    marker = 'title = youtube.sanitize_youtube_text(title)'
    assert marker in source, (
        "the promo chain must sanitize its title before job['title'] / the INSERT"
    )
    # ...and it must happen before the title is stashed and later INSERTed.
    assert source.index(marker) < source.index('job["title"] = title')
    assert source.index(marker) < source.index("INSERT INTO videos")
