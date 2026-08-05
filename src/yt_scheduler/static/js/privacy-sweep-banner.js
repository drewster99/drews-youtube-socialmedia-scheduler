/**
 * App-wide banner for a video-privacy sweep that has stopped working.
 *
 * The sweep asks YouTube whether each video is public, and two guards act on
 * its answer: auto-add decides a video is live from it, and the send gate
 * refuses to announce a video it believes is not public. When the sweep stops,
 * neither guard knows — they go on trusting a value nothing has re-verified,
 * and the failure is visible only in a log file nobody has open.
 *
 * Loaded from base.html on every page, like the reconcile and failed-sends
 * banners, for the same reason: the thing it reports on runs unattended, so the
 * report has to reach wherever the user actually is.
 *
 * Deliberately quiet. A single failed sweep is routine — a sleeping laptop, a
 * token mid-refresh, one bad minute of network — and the server decides what
 * counts as persistent (MIN_FAILURES_BEFORE_SURFACING). This renders whatever
 * the endpoint returns rather than second-guessing it, so the threshold lives
 * in one place.
 */
(function () {
    'use strict';

    const POLL_MS = 120000;

    let lastRenderedKey = null;

    function escapeText(value) {
        const div = document.createElement('div');
        div.textContent = value == null ? '' : String(value);
        return div.innerHTML;
    }

    /**
     * How long this has been broken, not merely that it is.
     *
     * last_success_at survives a failed run on purpose. Without it the banner
     * could only say "the check is failing", which reads the same at four
     * minutes and four days — and the difference is exactly what decides
     * whether to care right now.
     */
    function brokenForText(row) {
        if (!row.last_success_at) {
            return 'It has never completed successfully.';
        }
        const age = window.dysDateTime.formatAge(row.last_success_at);
        return `Last successful check ${escapeText(age)}.`;
    }

    function rowHtml(row) {
        const project = escapeText(row.project_name || row.project_slug);
        const count = Number(row.consecutive_failures) || 0;
        const attempts = `${count} failed attempt${count === 1 ? '' : 's'} in a row`;
        // The error is the actionable part — a revoked token and a network
        // outage need different things done about them — so it is shown rather
        // than reduced to "sync failed".
        const why = row.error
            ? `<div class="privacy-sweep-banner__why">${escapeText(row.error)}</div>`
            : '';
        return `
            <div class="privacy-sweep-banner__row">
                <strong>${project}</strong> — ${escapeText(attempts)}.
                ${escapeText(brokenForText(row))}
                ${why}
            </div>`;
    }

    function render(failures) {
        const banner = document.getElementById('privacy-sweep-banner');
        if (!banner) return;

        if (!failures.length) {
            banner.style.display = 'none';
            banner.innerHTML = '';
            lastRenderedKey = null;
            return;
        }

        // Rebuilding identical HTML on every poll would restart any CSS
        // transition and fight a text selection the user is holding.
        const key = failures
            .map((f) => `${f.project_id}:${f.consecutive_failures}:${f.error || ''}`)
            .join('|');
        if (key === lastRenderedKey) return;
        lastRenderedKey = key;

        banner.innerHTML =
            `<div class="privacy-sweep-banner__head">`
            + `Video privacy checks are failing. Published or unlisted changes `
            + `made on YouTube are not being picked up, so scheduled posts may `
            + `be held back or sent for the wrong videos.`
            + `</div>`
            + failures.map(rowHtml).join('');
        banner.style.display = '';
    }

    async function poll() {
        try {
            const resp = await fetch('/api/projects/video-privacy-sweep-failures');
            if (!resp.ok) return;
            const data = await resp.json();
            render(data.failures || []);
        } catch (err) {
            // A failed poll says nothing about the sweep, so it must not clear
            // or invent a banner — leave whatever is on screen alone.
        }
    }

    poll();
    setInterval(poll, POLL_MS);
})();
