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
    // Video ids with an action running, so a poll can't replace the row's
    // disabled buttons with fresh enabled ones mid-request.
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

        // Keyed on the RENDERED when-text, not publish_failed_at alone: the
        // age is relative, so "5 minutes ago" goes stale with every field
        // unchanged. Same rule as the failed-sends banner's renderKey.
        const key = JSON.stringify(rows.map((r) => [
            r.video_id, r.title, r.project_name, r.project_slug,
            r.publish_error,
            r.publish_failed_at
                ? window.dysDateTime.formatWhenWithAge(r.publish_failed_at)
                : null,
            inFlight.has(String(r.video_id)),
        ]));
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
            // No _silent: app.js's global fetch wrapper already toasts non-ok
            // responses and network errors. Only the HTTP-200 body outcomes —
            // publish_video_job returns a scheduled-job summary, not a REST
            // verdict — and the success confirmations are handled here.
            const resp = await fetch(url, {
                method: action === 'publish' ? 'POST' : 'DELETE',
            });
            if (resp.ok) {
                if (action === 'cancel') {
                    showToast('Schedule cancelled.', 'success');
                } else {
                    const body = await resp.json();
                    if (body && body.published) {
                        showToast('Published.', 'success');
                    } else if (body && body.publish_error) {
                        showToast(`Publish failed: ${body.publish_error}`, 'error');
                    } else if (body && body.publish_blocked) {
                        // The fire-time gate also cancelled the schedule
                        // server-side, so this row is about to leave the banner
                        // WITHOUT the video going out — this toast is the only
                        // surface that says so.
                        showToast(
                            `Not published: ${body.publish_blocked}. The schedule `
                            + `was cancelled — fix the description, then schedule `
                            + `again.`, 'error');
                    } else {
                        // skipped_missing_video / skipped_archived / anything
                        // new: name it, rather than reading "no error" as
                        // success.
                        showToast('Not published: ' + JSON.stringify(body), 'error');
                    }
                }
            }
            // Non-ok: the global wrapper has already toasted the detail.
        } catch (err) {
            // Network throw: also already toasted by the wrapper.
        } finally {
            inFlight.delete(videoId);
            lastRenderedKey = null;
            // Re-render from cache BEFORE the re-read: poll() returns without
            // rendering on a non-ok response or a network throw, which would
            // leave this row's buttons disabled until the next successful poll.
            render(latestRows);
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
