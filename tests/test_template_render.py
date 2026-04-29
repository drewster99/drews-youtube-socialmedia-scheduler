"""Behavioural tests for the unified template renderer.

Covers:
- {{var}} substitution (literal-on-miss)
- {{var!}} required substitution (raises on miss)
- {{var??default}} optional substitution with fallback text
- {{ai: prompt}} blocks with var interpolation
- sibling AI blocks rendered independently
- nested {{ai: ... {{ai: ...}} ...}} resolved inside-out
- per-block system override via {{ai[system text]: prompt}}
- model / max_tokens / default_system_prompt overrides
- unbalanced openers surfaced as literal text
"""

from __future__ import annotations

import pytest
from unittest.mock import patch

from yt_scheduler.services import templates


def _fake_ai(prompt, *, system=None, model=None, max_tokens=512):
    return f"[AI({prompt})]"


def test_plain_variable_substitution():
    out = templates.render("Hello {{name}}!", {"name": "Drew"})
    assert out == "Hello Drew!"


def test_unknown_variable_left_literal():
    out = templates.render("Hello {{name}}!", {})
    assert out == "Hello {{name}}!"


def test_required_variable_present():
    out = templates.render("Hello {{name!}}!", {"name": "Drew"})
    assert out == "Hello Drew!"


def test_required_variable_missing_raises():
    with pytest.raises(templates.MissingRequiredVariable) as exc_info:
        templates.render("Hello {{name!}}!", {})
    assert exc_info.value.name == "name"
    assert "name" in str(exc_info.value)


def test_required_variable_inside_ai_block():
    """A required miss inside an AI block aborts before any Claude call."""
    with patch.object(templates, "call_ai_block") as mock_call:
        with pytest.raises(templates.MissingRequiredVariable):
            templates.render("{{ai: write about {{title!}}}}", {})
    mock_call.assert_not_called()


def test_default_text_used_when_variable_missing():
    out = templates.render("Hello {{name??stranger}}!", {})
    assert out == "Hello stranger!"


def test_default_text_ignored_when_variable_present():
    out = templates.render("Hello {{name??stranger}}!", {"name": "Drew"})
    assert out == "Hello Drew!"


def test_empty_default_text():
    """{{var??}} renders empty when missing — replaces the on_missing='empty' use case."""
    out = templates.render("Hello {{name??}}!", {})
    assert out == "Hello !"


def test_default_text_with_spaces_and_punctuation():
    out = templates.render("{{greeting??Good morning, friend!}}", {})
    assert out == "Good morning, friend!"


def test_default_text_is_literal_no_recursive_substitution():
    """Default text is treated as a literal string — {{title}} inside the
    default does NOT get re-substituted from the variables dict."""
    out = templates.render(
        "{{name??Hello {{title}}}}",
        {"title": "Foo"},
    )
    # 'name' is missing, so the default text is used verbatim — including
    # the literal '{{title}}' (no recursion into the default).
    assert out == "Hello {{title}}"


def test_ai_block_with_variable_inside():
    with patch.object(templates, "call_ai_block", side_effect=_fake_ai):
        out = templates.render(
            "Pre {{ai: write about {{title}} please}} post",
            {"title": "Foo"},
        )
    assert out == "Pre [AI(write about Foo please)] post"


def test_sibling_ai_blocks_independent():
    seen: list[str] = []

    def fake(prompt, *, system=None, model=None, max_tokens=512):
        seen.append(prompt)
        return f"<{prompt}>"

    with patch.object(templates, "call_ai_block", side_effect=fake):
        out = templates.render("{{ai: a}} and {{ai: b}}", {})
    assert seen == ["a", "b"]
    assert out == "<a> and <b>"


def test_nested_ai_blocks_resolve_inside_out():
    seen: list[str] = []

    def fake(prompt, *, system=None, model=None, max_tokens=512):
        seen.append(prompt)
        return prompt.upper().replace(" ", "_")

    with patch.object(templates, "call_ai_block", side_effect=fake):
        out = templates.render("{{ai: outer {{ai: inner}} done}}", {})
    assert seen == ["inner", "outer INNER done"]
    assert out == "OUTER_INNER_DONE"


def test_nested_with_variables_at_each_level():
    seen: list[str] = []

    def fake(prompt, *, system=None, model=None, max_tokens=512):
        seen.append(prompt)
        return f"<{prompt}>"

    with patch.object(templates, "call_ai_block", side_effect=fake):
        out = templates.render(
            "{{ai: blurb about {{title}}: {{ai: summarize {{transcript}}}} ok}}",
            {"title": "Foo", "transcript": "hello world"},
        )
    assert seen == [
        "summarize hello world",
        "blurb about Foo: <summarize hello world> ok",
    ]
    assert out == "<blurb about Foo: <summarize hello world> ok>"


def test_unbalanced_opener_emitted_literal():
    with patch.object(templates, "call_ai_block") as mock_call:
        out = templates.render("before {{ai: never closes", {})
    mock_call.assert_not_called()
    assert out == "before {{ai: never closes"


def test_per_block_system_override():
    seen: list[dict] = []

    def fake(prompt, *, system=None, model=None, max_tokens=512):
        seen.append({"prompt": prompt, "system": system})
        return f"<{prompt}>"

    with patch.object(templates, "call_ai_block", side_effect=fake):
        out = templates.render(
            "{{ai[Be terse]: write a haiku about {{topic}}}} -- {{ai: standard}}",
            {"topic": "rain"},
        )
    # First block uses the override; second falls back to the default.
    assert seen[0]["system"] == "Be terse"
    assert seen[0]["prompt"] == "write a haiku about rain"
    assert seen[1]["system"] == templates.DEFAULT_AI_SYSTEM
    assert seen[1]["prompt"] == "standard"
    assert out == "<write a haiku about rain> -- <standard>"


def test_default_system_override_at_render_call():
    seen: list[dict] = []

    def fake(prompt, *, system=None, model=None, max_tokens=512):
        seen.append({"prompt": prompt, "system": system})
        return ""

    with patch.object(templates, "call_ai_block", side_effect=fake):
        templates.render(
            "{{ai: hi}}",
            {},
            default_system_prompt="Custom system",
        )
    assert seen[0]["system"] == "Custom system"


def test_nested_inner_inherits_outer_system_override():
    seen: list[dict] = []

    def fake(prompt, *, system=None, model=None, max_tokens=512):
        seen.append({"prompt": prompt, "system": system})
        return prompt

    with patch.object(templates, "call_ai_block", side_effect=fake):
        templates.render(
            "{{ai[Outer system]: outer with {{ai: inner block}}}}",
            {},
            default_system_prompt="Default system",
        )
    # The inner block has no [system] override, so it inherits the
    # render-call default (NOT the outer block's override). The outer
    # block uses its own override.
    assert seen[0] == {"prompt": "inner block", "system": "Default system"}
    assert seen[1] == {"prompt": "outer with inner block", "system": "Outer system"}


def test_ai_opener_inside_var_value_still_walked():
    """Documented current behaviour: vars are substituted first, so a value
    that contains ``{{ai:`` will be walked by the AI evaluator."""
    with patch.object(templates, "call_ai_block", side_effect=_fake_ai):
        out = templates.render(
            "Hello {{user_message}}",
            {"user_message": "{{ai: hi}}"},
        )
    assert out == "Hello [AI(hi)]"


def test_legacy_render_template_alias_still_works():
    out = templates.render_template("Hello {{name}}!", {"name": "Drew"})
    assert out == "Hello Drew!"


def test_aitch_word_not_treated_as_ai_opener():
    """``{{aitch}}`` (a regular variable named 'aitch') must not be parsed
    as an AI opener. The colon/bracket discriminator handles this."""
    with patch.object(templates, "call_ai_block") as mock_call:
        out = templates.render("Letter {{aitch}}", {"aitch": "H"})
    mock_call.assert_not_called()
    assert out == "Letter H"


# --- merge_variables ----------------------------------------------------


def test_merge_variables_inheritance_chain():
    """global -> project -> parent.custom -> self.builtins -> self.custom,
    later wins on key collision."""
    out = templates.merge_variables(
        global_vars={"signoff": "global", "color": "blue"},
        project_vars={"signoff": "project", "tag": "podcast"},
        parent_item_vars={"signoff": "parent", "footer": "p-footer"},
        self_builtins={"title": "Episode 1", "url": "https://yt/x"},
        self_item_vars={"signoff": "self"},
    )
    # signoff: appears at all four custom layers; self_item_vars wins.
    assert out["signoff"] == "self"
    # color: only in global; falls through.
    assert out["color"] == "blue"
    # tag: only in project; falls through.
    assert out["tag"] == "podcast"
    # footer: only on parent; falls through.
    assert out["footer"] == "p-footer"
    # builtins always present.
    assert out["title"] == "Episode 1"
    assert out["url"] == "https://yt/x"


def test_merge_variables_built_in_does_not_inherit_from_parent():
    """A child's title is the child's title — never the parent's, even
    when not overridden. Built-ins live only in self_builtins."""
    out = templates.merge_variables(
        parent_item_vars={"title": "PARENT title"},  # custom-level "title"
        self_builtins={"title": "child title"},
    )
    assert out["title"] == "child title"


def test_merge_variables_no_parent():
    out = templates.merge_variables(
        global_vars={"x": "g"},
        project_vars={"x": "p"},
        self_builtins={"y": "yy"},
    )
    assert out == {"x": "p", "y": "yy"}


def test_merge_variables_all_empty():
    assert templates.merge_variables() == {}


# --- extract_media_directives -------------------------------------------


def _img(name: str, path: str, alt: str = "") -> dict:
    return {"shortname": name, "path": path, "alt_text": alt}


def test_extract_media_video_directive():
    cleaned, paths, alts = templates.extract_media_directives(
        "Watch this: {{video}} cool right?",
        video_path="/u/clip.mp4",
    )
    assert cleaned == "Watch this:  cool right?"
    assert paths == ["/u/clip.mp4"]
    assert alts == [""]


def test_extract_media_thumbnail_directive():
    cleaned, paths, _ = templates.extract_media_directives(
        "{{thumbnail}}",
        thumbnail_path="/u/thumb.jpg",
    )
    assert cleaned == ""
    assert paths == ["/u/thumb.jpg"]


def test_extract_media_video_skipped_when_path_missing():
    """Bare {{video}} silently drops when no video_path is supplied."""
    cleaned, paths, _ = templates.extract_media_directives(
        "Hi {{video}} there",
    )
    assert cleaned == "Hi  there"
    assert paths == []


def test_extract_media_image_specific():
    cleaned, paths, alts = templates.extract_media_directives(
        "Look {{image:cat}} and {{image:mom}}!",
        images=[
            _img("cat", "/u/cat.png", "a cat"),
            _img("mom", "/u/mom.png", "my mom"),
        ],
    )
    assert cleaned == "Look  and !"
    assert paths == ["/u/cat.png", "/u/mom.png"]
    assert alts == ["a cat", "my mom"]


def test_extract_media_image_unknown_raises():
    """{{image:specific}} with a missing shortname is a real bug — raise."""
    with pytest.raises(templates.UnknownImageShortname) as exc:
        templates.extract_media_directives(
            "Look {{image:nope}}",
            images=[_img("cat", "/u/cat.png")],
        )
    assert exc.value.shortname == "nope"


def test_extract_media_image_wildcard():
    """{{image:*}} attaches every image, in caller's pre-sorted order."""
    cleaned, paths, alts = templates.extract_media_directives(
        "Photos: {{image:*}}",
        images=[
            _img("a", "/u/a.png", "alt-a"),
            _img("b", "/u/b.png", "alt-b"),
            _img("c", "/u/c.png", ""),
        ],
    )
    assert cleaned == "Photos: "
    assert paths == ["/u/a.png", "/u/b.png", "/u/c.png"]
    assert alts == ["alt-a", "alt-b", ""]


def test_extract_media_wildcard_with_no_images_silent():
    cleaned, paths, _ = templates.extract_media_directives(
        "x {{image:*}} y",
    )
    assert cleaned == "x  y"
    assert paths == []


def test_extract_media_combined_directives_preserve_order():
    """When multiple directives appear, the resulting media lists are in
    the order encountered in the body."""
    cleaned, paths, _ = templates.extract_media_directives(
        "{{thumbnail}} | {{image:two}} | {{video}} | {{image:one}}",
        video_path="/u/v.mp4",
        thumbnail_path="/u/t.jpg",
        images=[_img("one", "/u/1.png"), _img("two", "/u/2.png")],
    )
    assert cleaned == " |  |  | "
    assert paths == ["/u/t.jpg", "/u/2.png", "/u/v.mp4", "/u/1.png"]


def test_extract_media_does_not_touch_regular_variables():
    """{{title}}, {{ai: ...}}, etc. pass through untouched — directives
    only match the media-specific tokens."""
    cleaned, paths, _ = templates.extract_media_directives(
        "Hello {{title}}! See {{video}} also {{ai: write something}}",
        video_path="/u/clip.mp4",
    )
    assert cleaned == "Hello {{title}}! See  also {{ai: write something}}"
    assert paths == ["/u/clip.mp4"]


