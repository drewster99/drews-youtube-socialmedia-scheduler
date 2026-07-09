"""Guards against the stored-XSS and inline-handler defects found in the audit.

There is no JS test runner in this project, and the JS lives inside Jinja
templates which emit it verbatim. Text invariants over the template sources are
therefore the feasible guard: they catch a future edit dropping an escape.

This module deliberately imports no ``yt_scheduler`` module, so it cannot touch
the production database.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader

TEMPLATES_DIR = (
    Path(__file__).resolve().parents[1] / "src" / "yt_scheduler" / "templates_html"
)


def _read(name: str) -> str:
    return (TEMPLATES_DIR / name).read_text(encoding="utf-8")


# Raw interpolations that reached innerHTML unescaped. Each value is
# attacker-influenced: YouTube commenters, YouTube-supplied titles/URLs, or a
# platform error string.
FORBIDDEN_PATTERNS: list[tuple[str, str]] = [
    ("video_detail.html", "${c.authorDisplayName}"),
    ("video_detail.html", "${c.textDisplay}"),
    ("video_detail.html", "${video.transcript}</textarea>"),
    ("moderation.html", "${i.keyword}"),
    ("moderation.html", "${i.author || '—'}"),
    ("moderation.html", "${i.matched_keyword}"),
    ("moderation.html", "${(i.comment_text || '').slice(0, 80)}"),
    ("home.html", "${i.title}</a>"),
    ("home.html", "${i.project_name}</span>"),
    ("dashboard.html", 'alt="${v.title}"'),
    ("dashboard.html", 'src="${v.thumbnail_url}"'),
    ("dashboard.html", "<h3>${v.title}</h3>"),
    ("dashboard.html", "<h3>${episodeChip}${v.title}</h3>"),
    ("socials_compose.html", "'Error: ' + (p.error || 'unknown')"),
    ("project_settings.html", "<code>${data.channel_id}</code>"),
    ("project_settings.html", ">${c.label}${reauth}${prem}<"),
]


@pytest.mark.parametrize(("template", "pattern"), FORBIDDEN_PATTERNS)
def test_no_unescaped_interpolation(template: str, pattern: str) -> None:
    assert pattern not in _read(template), (
        f"{template} interpolates {pattern} into innerHTML without escaping"
    )


REQUIRED_PATTERNS: list[tuple[str, str]] = [
    ("video_detail.html", "${escapeHtml(c.authorDisplayName)}"),
    ("video_detail.html", "${escapeHtml(c.textDisplay)}"),
    ("video_detail.html", "${escapeHtml(video.transcript)}"),
    ("moderation.html", "function escapeHtml"),
    ("moderation.html", "${escapeHtml(i.matched_keyword)}"),
    ("home.html", "${escapeHtml(i.title)}"),
    ("dashboard.html", "${escapeAttr(v.thumbnail_url)}"),
    ("socials_compose.html", "escapeHtmlBasic(p.error || 'unknown')"),
    ("project_settings.html", "${escapeHtml(c.label)}"),
]


@pytest.mark.parametrize(("template", "pattern"), REQUIRED_PATTERNS)
def test_escaping_applied(template: str, pattern: str) -> None:
    assert pattern in _read(template), f"{template} is missing {pattern}"


# An `onclick="fn('${escapeHtml(v)}')"` sits in a JS-string-inside-an-HTML-attribute
# double context: the parser entity-decodes the attribute before the JS engine
# parses it, so an HTML escaper cannot make it safe. These must use data-* +
# delegated listeners instead.
FORBIDDEN_INLINE_HANDLERS: list[tuple[str, str]] = [
    ("settings.html", 'onclick="confirmDeleteCredential('),
    ("settings.html", 'onclick="refreshCredentialUsername('),
    ("settings.html", 'onclick="reconnectCredential('),
    ("video_detail.html", "focusFailedPost(${JSON.stringify"),
]


@pytest.mark.parametrize(("template", "pattern"), FORBIDDEN_INLINE_HANDLERS)
def test_no_interpolated_inline_handlers(template: str, pattern: str) -> None:
    assert pattern not in _read(template), (
        f"{template} still builds an inline handler by interpolation: {pattern}"
    )


REQUIRED_DELEGATION: list[tuple[str, str]] = [
    ("settings.html", 'data-cred-action="reconnect"'),
    ("settings.html", 'data-cred-action="refresh-username"'),
    ("settings.html", 'data-cred-action="delete-credential"'),
    ("settings.html", "closest?.('[data-cred-action]')"),
    ("video_detail.html", 'class="event-failed-post-link"'),
    ("video_detail.html", "closest?.('.event-failed-post-link')"),
]


@pytest.mark.parametrize(("template", "pattern"), REQUIRED_DELEGATION)
def test_delegated_handlers_wired(template: str, pattern: str) -> None:
    assert pattern in _read(template), f"{template} is missing delegation wiring: {pattern}"


@pytest.mark.parametrize(
    "template",
    sorted(p.name for p in TEMPLATES_DIR.glob("*.html")),
)
def test_template_still_parses(template: str) -> None:
    """A dropped brace in the edited JS would surface here as a Jinja syntax error."""
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)), autoescape=True)
    env.get_template(template)
