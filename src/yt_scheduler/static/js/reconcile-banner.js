/**
 * App-wide banner for smart-queue template reconciliation.
 *
 * Reconciliation runs on a background worker and rewrites real schedules —
 * adding, deleting and re-rendering posts that will actually go out. That must
 * be visible from every screen, not only the one that triggered it, so this
 * loads from base.html on every page.
 *
 * Polling rather than a socket: the app has no push channel, and a status
 * check is one indexed query. Idle polling is slow on purpose; it speeds up
 * while work is in flight so progress looks live.
 */
(function () {
    'use strict';

    const IDLE_MS = 15000;
    const BUSY_MS = 1500;

    let timer = null;
    let lastSignature = '';

    function describe(job) {
        const scope = job.queue_name ? ` — ${job.queue_name}` : '';
        if (job.total > 0) {
            return `${job.label}${scope}: ${job.done} of ${job.total}`;
        }
        // total is 0 until the handler has counted its work; "starting" is
        // honest, where "0 of 0" reads like nothing to do.
        return `${job.label}${scope}: starting…`;
    }

    function render(status) {
        const banner = document.getElementById('reconcile-banner');
        if (!banner) return;

        const active = status.active || [];
        const failed = status.failed || [];
        if (!active.length && !failed.length) {
            banner.style.display = 'none';
            banner.innerHTML = '';
            return;
        }

        const parts = [];
        if (active.length) {
            const running = active.filter(j => j.status === 'running');
            const queued = active.length - running.length;
            const lead = running.length ? running.map(describe).join('; ')
                                        : 'Queued';
            parts.push(
                `<span class="reconcile-banner__spinner" aria-hidden="true"></span>`
                + `<span><strong>Updating schedules to match template changes.</strong> `
                + `${escapeText(lead)}`
                + (queued ? ` (${queued} more queued)` : '')
                + ` — smart schedules can't be saved until this finishes.</span>`
            );
        }
        for (const job of failed) {
            parts.push(
                `<span class="reconcile-banner__failed"><strong>Schedule update failed</strong>`
                + ` — ${escapeText(job.queue_name)}: ${escapeText(job.label)}.`
                + ` ${escapeText(job.error || 'no detail')}`
                + ` <button type="button" class="btn btn-sm" data-dismiss-job="${job.id}"`
                + ` data-queue="${job.queue_id}"`
                + ` data-project="${escapeText(job.project_slug || '')}">Dismiss</button></span>`
            );
        }
        banner.innerHTML = parts.join('<br>');
        banner.style.display = '';
    }

    function escapeText(value) {
        const div = document.createElement('div');
        div.textContent = value == null ? '' : String(value);
        return div.innerHTML;
    }

    async function poll() {
        let status = null;
        try {
            const resp = await fetch('/api/reconcile-status');
            if (resp.ok) status = await resp.json();
        } catch (err) {
            // A poll that can't reach the server says nothing about whether
            // work is running, so leave whatever is on screen rather than
            // clearing it and implying everything finished.
        }
        if (status) {
            const signature = JSON.stringify(status);
            if (signature !== lastSignature) {
                lastSignature = signature;
                render(status);
                document.dispatchEvent(
                    new CustomEvent('reconcile-status', {detail: status})
                );
            }
        }
        const busy = status && status.busy;
        timer = setTimeout(poll, busy ? BUSY_MS : IDLE_MS);
    }

    /** Poll now instead of waiting out the current interval.
     *
     * Idle polling is 15 seconds, which is the right cadence for noticing work
     * somebody else started and the wrong one for work the user just started
     * here: pressing a button that enqueues a job and then watching nothing
     * happen for fifteen seconds is the same dead air the background worker
     * exists to remove. A page that enqueues calls this; `lastSignature` is
     * cleared so the render happens even if the status is unchanged.
     */
    function refresh() {
        lastSignature = '';
        clearTimeout(timer);
        poll();
    }

    window.dysReconcileBanner = {refresh};

    document.addEventListener('click', async (event) => {
        const button = event.target.closest('[data-dismiss-job]');
        if (!button) return;
        const jobId = button.getAttribute('data-dismiss-job');
        const queueId = button.getAttribute('data-queue');
        const slug = button.getAttribute('data-project');
        if (!slug) return;
        button.disabled = true;
        try {
            await fetch(
                `/api/projects/${slug}/smart-queues/${queueId}/reconcile-jobs/${jobId}/dismiss`,
                {method: 'POST'}
            );
            refresh();
        } catch (err) {
            button.disabled = false;
        }
    });

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', poll);
    } else {
        poll();
    }
})();
