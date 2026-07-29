/**
 * App-wide banner for social posts whose most recent send attempt failed.
 *
 * A failed send used to exist only as a short-lived toast and a badge on the
 * one page that owns the post — invisible from everywhere else, and invisible
 * for scheduled sends that fail with no page open at all. This loads from
 * base.html on every page and stays up until the failed posts are retried
 * successfully or deleted. The social_posts table is the single source of
 * truth, so there is no separate dismissed state to drift out of sync.
 */
(function () {
    'use strict';

    const POLL_MS = 30000;
    const ERROR_PREVIEW_CHARS = 200;

    function escapeText(value) {
        const div = document.createElement('div');
        div.textContent = value == null ? '' : String(value);
        return div.innerHTML;
    }

    function render(posts) {
        const banner = document.getElementById('failed-sends-banner');
        if (!banner) return;

        if (!posts.length) {
            banner.style.display = 'none';
            banner.innerHTML = '';
            return;
        }

        const newest = posts[0];
        let error = newest.error || 'send failed';
        if (error.length > ERROR_PREVIEW_CHARS) {
            error = error.slice(0, ERROR_PREVIEW_CHARS) + '…';
        }
        const others = posts.length - 1;
        banner.innerHTML =
            `<span><strong>${posts.length} social post${posts.length === 1 ? '' : 's'} failed to send.</strong> `
            + `${escapeText(newest.platform)} — ${escapeText(newest.video_title)}: ${escapeText(error)} `
            + `<a href="${escapeText(newest.page_url)}">View →</a>`
            + (others ? ` <span class="failed-sends-banner__more">(and ${others} more)</span>` : '')
            + `</span>`;
        banner.style.display = '';
    }

    async function check() {
        try {
            const resp = await fetch('/api/social/failed-posts');
            if (!resp.ok) return;
            render(await resp.json());
        } catch (err) {
            // Network hiccup: keep whatever is currently shown; the next poll
            // (or the next window focus) retries.
        }
    }

    window.addEventListener('focus', check);
    check();
    setInterval(check, POLL_MS);
})();
