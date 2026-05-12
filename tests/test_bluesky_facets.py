"""Tests for ``_build_bluesky_facets`` — URL + hashtag richtext indexing."""

from __future__ import annotations

from yt_scheduler.services.social import _build_bluesky_facets


def _slice(text: str, facet: dict) -> str:
    encoded = text.encode("utf-8")
    idx = facet["index"]
    return encoded[idx["byteStart"]:idx["byteEnd"]].decode("utf-8")


def test_no_facets_for_plain_text() -> None:
    assert _build_bluesky_facets("just some words, no link or tag") == []


def test_single_url() -> None:
    text = "watch this https://youtu.be/abc123 today"
    facets = _build_bluesky_facets(text)
    assert len(facets) == 1
    assert _slice(text, facets[0]) == "https://youtu.be/abc123"
    assert facets[0]["features"] == [
        {"$type": "app.bsky.richtext.facet#link", "uri": "https://youtu.be/abc123"}
    ]


def test_url_trailing_punctuation_excluded() -> None:
    text = "see https://example.com/foo."
    facets = _build_bluesky_facets(text)
    assert _slice(text, facets[0]) == "https://example.com/foo"
    assert facets[0]["features"][0]["uri"] == "https://example.com/foo"


def test_url_with_balanced_parens_preserved() -> None:
    # Wikipedia-style path — the trailing `)` is part of the URL because
    # there's a matching `(` earlier in the path.
    text = "read https://en.wikipedia.org/wiki/Foo_(bar) for context"
    facets = _build_bluesky_facets(text)
    assert _slice(text, facets[0]) == "https://en.wikipedia.org/wiki/Foo_(bar)"


def test_url_wrapped_in_parens_strips_closer() -> None:
    text = "(see https://example.com)"
    facets = _build_bluesky_facets(text)
    assert _slice(text, facets[0]) == "https://example.com"


def test_url_with_balanced_parens_then_period() -> None:
    text = "read https://en.wikipedia.org/wiki/Foo_(bar)."
    facets = _build_bluesky_facets(text)
    assert _slice(text, facets[0]) == "https://en.wikipedia.org/wiki/Foo_(bar)"


def test_url_with_emoji_uses_byte_offsets() -> None:
    # 🎬 is 4 UTF-8 bytes — naive str-index math would point at the wrong
    # span and Bluesky would render the link as garbled text.
    text = "🎬 https://youtu.be/abc"
    facets = _build_bluesky_facets(text)
    assert _slice(text, facets[0]) == "https://youtu.be/abc"


def test_hashtag_strips_hash_and_keeps_span() -> None:
    text = "new clip #VideoEditing"
    facets = _build_bluesky_facets(text)
    assert len(facets) == 1
    assert _slice(text, facets[0]) == "#VideoEditing"
    assert facets[0]["features"] == [
        {"$type": "app.bsky.richtext.facet#tag", "tag": "VideoEditing"}
    ]


def test_hashtag_must_start_with_letter() -> None:
    # "#1" is a numeric token, not a tag — Bluesky rejects pure-digit tags.
    assert _build_bluesky_facets("ranked #1 today") == []


def test_hashtag_only_at_word_boundary() -> None:
    # `foo#bar` is not a tag (no preceding whitespace / start-of-string).
    assert _build_bluesky_facets("foo#bar baz") == []


def test_url_and_hashtag_together() -> None:
    text = "drop https://x.com/p/1 #launch"
    facets = _build_bluesky_facets(text)
    assert len(facets) == 2
    by_type = {f["features"][0]["$type"]: f for f in facets}
    link = by_type["app.bsky.richtext.facet#link"]
    tag = by_type["app.bsky.richtext.facet#tag"]
    assert _slice(text, link) == "https://x.com/p/1"
    assert _slice(text, tag) == "#launch"
    assert tag["features"][0]["tag"] == "launch"


# --- bare domains (no scheme) -------------------------------------------------

def test_bare_domain_gets_synthesized_https_uri() -> None:
    text = "code at github.com/me/repo today"
    facets = _build_bluesky_facets(text)
    assert len(facets) == 1
    assert _slice(text, facets[0]) == "github.com/me/repo"
    assert facets[0]["features"] == [
        {"$type": "app.bsky.richtext.facet#link", "uri": "https://github.com/me/repo"}
    ]


def test_bare_domain_with_www_and_no_path() -> None:
    text = "visit www.example.com please"
    facets = _build_bluesky_facets(text)
    assert len(facets) == 1
    assert _slice(text, facets[0]) == "www.example.com"
    assert facets[0]["features"][0]["uri"] == "https://www.example.com"


def test_bare_domain_trailing_period_excluded() -> None:
    text = "see example.org."
    facets = _build_bluesky_facets(text)
    assert len(facets) == 1
    assert _slice(text, facets[0]) == "example.org"
    assert facets[0]["features"][0]["uri"] == "https://example.org"


def test_email_is_not_a_bare_domain_link() -> None:
    assert _build_bluesky_facets("ping me@example.com anytime") == []


def test_scheme_url_not_double_matched_as_bare_domain() -> None:
    text = "go to https://example.com/x now"
    facets = _build_bluesky_facets(text)
    assert len(facets) == 1
    assert facets[0]["features"][0]["uri"] == "https://example.com/x"


def test_filename_with_unlisted_extension_not_linked() -> None:
    # .py / .md / .go aren't in the curated TLD list, so prose mentioning a
    # filename doesn't accidentally become a link.
    assert _build_bluesky_facets("open main.py and notes.md") == []


def test_bare_domain_and_hashtag_together() -> None:
    text = "repo at github.io/x #opensource"
    facets = _build_bluesky_facets(text)
    assert len(facets) == 2
    by_type = {f["features"][0]["$type"]: f for f in facets}
    assert by_type["app.bsky.richtext.facet#link"]["features"][0]["uri"] == "https://github.io/x"
    assert _slice(text, by_type["app.bsky.richtext.facet#tag"]) == "#opensource"


def test_hashtag_with_dot_domain_suffix_is_only_a_tag() -> None:
    # `#foo.com` — the `#` lookbehind keeps the bare-domain matcher from
    # producing a link facet that overlaps the `#foo` hashtag facet.
    text = "check #foo.com today"
    facets = _build_bluesky_facets(text)
    assert len(facets) == 1
    assert facets[0]["features"][0]["$type"] == "app.bsky.richtext.facet#tag"
    assert facets[0]["features"][0]["tag"] == "foo"
