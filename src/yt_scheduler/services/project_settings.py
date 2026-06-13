"""Per-project key-value settings.

Used for: auto-action toggles per upload/import column, posting delays/spacings,
default template per tier, etc. Values are stored as TEXT and parsed by the
caller.
"""

from __future__ import annotations

import json
import math
from typing import Any

from yt_scheduler.database import get_db, write_transaction

# Defaults expressed as Python objects; serialised to JSON when stored.
AUTO_ACTION_DEFAULTS_UPLOAD = {
    "auto_transcribe": True,
    "auto_transcribe_backend": None,
    "auto_transcribe_model": None,
    "auto_description": True,
    "auto_tags": False,
    "auto_tags_include_title": True,
    "auto_tags_include_description": True,
    "auto_tags_include_transcript": True,
    "auto_tags_mode": "replace",
    "auto_thumbnail": True,
    "auto_socials": {
        "twitter": False,
        "bluesky": False,
        "mastodon": False,
        "linkedin": False,
        "threads": False,
    },
}

AUTO_ACTION_DEFAULTS_IMPORT = {
    "auto_transcribe": True,
    "auto_transcribe_backend": None,
    "auto_transcribe_model": None,
    "auto_description": False,
    "auto_tags": False,
    "auto_tags_include_title": True,
    "auto_tags_include_description": True,
    "auto_tags_include_transcript": True,
    "auto_tags_mode": "add",
    "auto_thumbnail": False,
    "auto_socials": {
        "twitter": False,
        "bluesky": False,
        "mastodon": False,
        "linkedin": False,
        "threads": False,
    },
}

POSTING_DEFAULTS = {
    "post_video_delay_minutes": 15,
    "inter_post_spacing_minutes": 5,
    "default_template_video": "announce_video",
    "default_template_segment": "announce_video",
    "default_template_short": "announce_video",
    "default_template_hook": "announce_video",
}


async def get_setting(project_id: int, key: str) -> str | None:
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT value FROM project_settings WHERE project_id = ? AND key = ?",
        (project_id, key),
    )
    return rows[0]["value"] if rows else None


async def set_setting(project_id: int, key: str, value: str) -> None:
    async with write_transaction() as db:
        await db.execute(
            "INSERT INTO project_settings (project_id, key, value) VALUES (?, ?, ?) "
            "ON CONFLICT(project_id, key) DO UPDATE SET value = excluded.value",
            (project_id, key, value),
        )


async def get_json(project_id: int, key: str, default: Any = None) -> Any:
    raw = await get_setting(project_id, key)
    if raw is None:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return default


async def set_json(project_id: int, key: str, value: Any) -> None:
    await set_setting(project_id, key, json.dumps(value))


async def get_all(project_id: int) -> dict[str, str]:
    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT key, value FROM project_settings WHERE project_id = ?",
        (project_id,),
    )
    return {r["key"]: r["value"] for r in rows}


async def get_auto_actions(project_id: int) -> dict:
    """Return the full auto-actions matrix with defaults filled in."""
    upload = await get_json(project_id, "auto_actions_upload", {})
    import_ = await get_json(project_id, "auto_actions_import", {})
    return {
        "upload": {**AUTO_ACTION_DEFAULTS_UPLOAD, **(upload or {})},
        "import": {**AUTO_ACTION_DEFAULTS_IMPORT, **(import_ or {})},
    }


async def get_posting_settings(project_id: int) -> dict:
    """Return posting delay/spacing settings + per-tier default templates."""
    stored = await get_json(project_id, "posting", {})
    return {**POSTING_DEFAULTS, **(stored or {})}


# Per-tier promo schedule delays. ``initial`` is the gap between the
# parent's publish time and the first promo of that tier; ``subsequent``
# is the gap between consecutive promos in the tier. Stored as
# {value, unit} so the user's chosen unit round-trips exactly; mirrors
# scheduler.DEFAULT_PROMO_DELAYS (hook 4h/99h, short 18h/6d, segment 3d/9d).
PROMO_DELAY_DEFAULTS = {
    "hook":    {"initial": {"value": 4, "unit": "hours"},
                "subsequent": {"value": 99, "unit": "hours"}},
    "short":   {"initial": {"value": 18, "unit": "hours"},
                "subsequent": {"value": 6, "unit": "days"}},
    "segment": {"initial": {"value": 3, "unit": "days"},
                "subsequent": {"value": 9, "unit": "days"}},
}

_PROMO_DELAY_UNITS = {"minutes", "hours", "days"}
_PROMO_DELAY_TIERS = ("hook", "short", "segment")


def validate_promo_delays(payload: dict) -> dict:
    """Validate + normalize a promo_delays payload into the canonical
    {tier: {initial|subsequent: {value, unit}}} shape.

    Raises ValueError on bad input — the caller maps that to HTTP 400.
    Surfacing the error beats silently falling back to a default.
    """
    if not isinstance(payload, dict):
        raise ValueError("promo_delays must be an object")
    out: dict = {}
    for tier in _PROMO_DELAY_TIERS:
        tcfg = payload.get(tier)
        if not isinstance(tcfg, dict):
            raise ValueError(f"missing delay settings for tier '{tier}'")
        out[tier] = {}
        for key in ("initial", "subsequent"):
            spec = tcfg.get(key)
            if not isinstance(spec, dict):
                raise ValueError(f"{tier}.{key} must be an object")
            unit = spec.get("unit")
            if unit not in _PROMO_DELAY_UNITS:
                raise ValueError(
                    f"{tier}.{key}.unit must be one of "
                    f"{sorted(_PROMO_DELAY_UNITS)}"
                )
            try:
                value = float(spec.get("value"))
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"{tier}.{key}.value must be a number"
                ) from exc
            # NaN passes every comparison silently (NaN < 0 is False, NaN > cap
            # is False) and would flow into timedelta() producing nonsensical or
            # crashing schedules. Inf likewise defeats the >366-day overflow guard.
            if not math.isfinite(value):
                raise ValueError(
                    f"{tier}.{key}.value must be a finite number, got {value!r}"
                )
            if value < 0:
                raise ValueError(f"{tier}.{key}.value must be >= 0")
            # Upper bound: keeps a bogus value from overflowing the
            # timedelta() that _promo_delays_to_timedeltas builds.
            minutes = value * {"minutes": 1, "hours": 60, "days": 1440}[unit]
            if minutes > 366 * 24 * 60:
                raise ValueError(
                    f"{tier}.{key} is unreasonably large (max ~1 year)"
                )
            if value.is_integer():
                value = int(value)
            out[tier][key] = {"value": value, "unit": unit}
    return out


async def get_promo_delays(project_id: int) -> dict:
    """Per-tier promo schedule delays, merged over defaults so a partial
    or absent stored value still yields a complete set."""
    stored = await get_json(project_id, "promo_delays", {}) or {}
    out: dict = {}
    for tier, default in PROMO_DELAY_DEFAULTS.items():
        tcfg = stored.get(tier) or {}
        out[tier] = {
            "initial": {**default["initial"], **(tcfg.get("initial") or {})},
            "subsequent": {
                **default["subsequent"], **(tcfg.get("subsequent") or {})
            },
        }
    return out
