/**
 * Shared timestamp formatting. Loaded in <head> by base.html, so it exists
 * before both the banners (which format during a fetch they start at parse
 * time) and any page's own JS (which sits in the content block, above the
 * end-of-body script tags).
 *
 * `_ensureUtc` previously existed as six byte-identical copies — dashboard,
 * home, moderation, promo_videos, socials_compose and video_detail each
 * carried one, and none of them was reachable from static/js/. This is the
 * one copy; add call sites, not copies.
 */
(function () {
    'use strict';

    const DAY_NAMES = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    const MINUTE_MS = 60 * 1000;
    const HOUR_MS = 60 * MINUTE_MS;
    const DAY_MS = 24 * HOUR_MS;

    /**
     * SQLite writes `datetime('now')` as a naive UTC string with a space
     * separator ("2026-04-27 20:00:00"). That is not an ISO 8601 form, so
     * `new Date()` falls back to implementation-defined parsing and reads it
     * as *local* time — silently shifting every timestamp by the viewer's UTC
     * offset. It first showed up in Safari rendering a 20:00 UTC event as
     * "8:00pm" for a viewer on EDT. Coerce to ISO-with-Z so it parses as UTC.
     *
     * Values that already carry an offset (the ISO strings the scheduler
     * writes, e.g. "2026-09-17T15:00:00+00:00") pass through untouched.
     */
    function ensureUtc(iso) {
        if (typeof iso !== 'string') return iso;
        if (/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}/.test(iso) && !/(?:Z|[+-]\d\d:?\d\d)$/.test(iso)) {
            return iso.replace(' ', 'T') + 'Z';
        }
        return iso;
    }

    /** "Mon 4/27 2:30pm" — the format the dashboard and promo screens already use. */
    function formatWhen(iso) {
        const d = new Date(ensureUtc(iso));
        if (Number.isNaN(d.getTime())) return null;
        const sameYear = d.getFullYear() === new Date().getFullYear();
        const datePart = sameYear
            ? `${d.getMonth() + 1}/${d.getDate()}`
            : `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-`
                + `${String(d.getDate()).padStart(2, '0')}`;
        let hour = d.getHours();
        const minute = String(d.getMinutes()).padStart(2, '0');
        const meridiem = hour >= 12 ? 'pm' : 'am';
        hour = hour % 12 || 12;
        return `${DAY_NAMES[d.getDay()]} ${datePart} ${hour}:${minute}${meridiem}`;
    }

    /**
     * "5 days ago" — the half a reader acts on. Whether a failure is minutes
     * old or a week old decides whether it is the thing currently breaking or
     * a leftover, and an absolute date alone makes that a subtraction.
     */
    function formatAge(iso) {
        const d = new Date(ensureUtc(iso));
        if (Number.isNaN(d.getTime())) return null;
        const elapsed = Date.now() - d.getTime();
        if (elapsed < 0) return 'in the future';
        if (elapsed < MINUTE_MS) return 'just now';
        const units = [
            [DAY_MS, 'day'],
            [HOUR_MS, 'hour'],
            [MINUTE_MS, 'minute'],
        ];
        for (const [size, name] of units) {
            const count = Math.floor(elapsed / size);
            if (count >= 1) return `${count} ${name}${count === 1 ? '' : 's'} ago`;
        }
        return 'just now';
    }

    /** "Mon 4/27 2:30pm (5 days ago)", or null when there is no usable timestamp. */
    function formatWhenWithAge(iso) {
        const when = formatWhen(iso);
        if (!when) return null;
        const age = formatAge(iso);
        return age ? `${when} (${age})` : when;
    }

    window.dysDateTime = { ensureUtc, formatWhen, formatAge, formatWhenWithAge };
})();
