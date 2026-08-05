/**
 * App-wide banner for scheduled YouTube publishes that failed.
 *
 * The publish job's only caller is a timer, so its failure happens with no
 * page open — and the video afterwards reads as merely "scheduled", which no
 * page shows as trouble. Meanwhile every approved social post for it sits
 * unsent, and nothing retries until the next app restart. This is the
 * failed-sends banner's twin for the step BEFORE the social posts:
 * `videos.publish_failed_at` is the single source of truth, and rows leave the
 * list only because something real resolved them — a successful publish, a new
 * schedule, or the user cancelling the schedule.
 *
 * Retrying here is safe in the way retrying a social post is not: flipping a
 * video public is idempotent (public stays public), so there is no
 * double-post ambiguity to arbitrate.
 */
(function () {
    'use strict';

    const POLL_MS = 30000;
    const ERROR_PREVIEW_CHARS = 200;

    let latestRows = [];
    let lastRenderedKey = null;
    // Video ids with an action running, so a poll can't replace a disabled
    // "Publishing…" button with a fresh enabled one mid-request.
    const inFlight = new Set();

    function escapeText(value) {
        const div = document.createElement('div');
        div.textContent = value == null ? '' : String(value);
        return div.innerHTML;
    }

    function escapeAttribute(value) {
        return escapeText(value).replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }

    function rowHtml(row) {
        const id = escapeAttribute(String(row.video_id));
        const slug = escapeAttribute(String(row.project_slug));
        const when = row.publish_failed_at
            ? window.dysDateTime.formatWhenWithAge(row.publish_failed_at)
            : null;
        const error = String(row.publish_error || '').slice(0, ERROR_PREVIEW_CHARS);
        const disabled = inFlight.has(String(row.video_id)) ? ' disabled' : '';
        return `
            <div class="failed-publish-banner__row" data-video-id="${id}">
                ${when ? `<span class="failed-publish-banner__when">${escapeText(when)}</span>` : ''}
                <strong>${escapeText(row.project_name)}</strong> —
                <a href="/projects/${slug}/videos/${id}">${escapeText(row.title)}</a>:
                <span class="failed-publish-banner__error">${escapeText(error)}</span>
                <span class="failed-publish-banner__actions">
                    <button type="button" class="btn btn-sm failed-publish-banner__action"
                            data-action="publish" data-video-id="${id}"${disabled}>Publish now</button>
                    <button type="button" class="btn btn-sm failed-publish-banner__action"
                            data-action="cancel" data-video-id="${id}"${disabled}>Cancel schedule</button>
                </span>
            </div>`;
    }

    function render(rows) {
        const banner = document.getElementById('failed-publish-banner');
        if (!banner) return;
        latestRows = rows;

        if (!rows.length) {
            banner.style.display = 'none';
            banner.innerHTML = '';
            lastRenderedKey = null;
            return;
        }

        const key = rows
            .map((r) => `${r.video_id}:${r.publish_failed_at}:${inFlight.has(String(r.video_id))}`)
            .join('|');
        if (key === lastRenderedKey) return;
        lastRenderedKey = key;

        const head = rows.length === 1
            ? '1 scheduled YouTube publish failed.'
            : `${rows.length} scheduled YouTube publishes failed.`;
        banner.innerHTML =
            `<div class="failed-publish-banner__head">${escapeText(head)}`
            + ` Its social posts are waiting on it and will not send until it publishes.</div>`
            + rows.map(rowHtml).join('');
        banner.style.display = '';
        bindActions(banner);
    }

    function bindActions(banner) {
        banner.querySelectorAll('.failed-publish-banner__action').forEach((button) => {
            button.addEventListener('click', () => runAction(button));
        });
    }

    async function runAction(button) {
        const videoId = button.dataset.videoId;
        const action = button.dataset.action;
        if (!videoId || inFlight.has(videoId)) return;
        inFlight.add(videoId);
        lastRenderedKey = null;
        render(latestRows);
        try {
            const url = action === 'publish'
                ? `/api/videos/${encodeURIComponent(videoId)}/publish`
                : `/api/videos/${encodeURIComponent(videoId)}/schedule`;
            const resp = await fetch(url, {
                method: action === 'publish' ? 'POST' : 'DELETE',
            });
            if (action === 'publish' && resp.ok) {
                // publish_video_job reports a failed YouTube step in its body
                // with HTTP 200 — a scheduled-job summary, not a REST verdict —
                // so "ok" alone would clear the row while the publish failed.
                const body = await resp.json();
                if (body && body.publish_error) {
                    console.error('Publish retry failed:', body.publish_error);
                }
            }
        } catch (err) {
            console.error('Failed-publish action error:', err);
        } finally {
            inFlight.delete(videoId);
            lastRenderedKey = null;
            await poll();
        }
    }

    async function poll() {
        try {
            const resp = await fetch('/api/videos/failed-publishes');
            if (!resp.ok) return;
            render(await resp.json() || []);
        } catch (err) {
            // A failed poll says nothing about the publishes; leave the banner.
        }
    }

    poll();
    setInterval(poll, POLL_MS);
})();
