/**
 * App-wide banner for social posts whose most recent send attempt failed.
 *
 * A failed send used to exist only as a short-lived toast and a badge on the
 * one page that owns the post — invisible from everywhere else, and invisible
 * for scheduled sends that fail with no page open at all. This loads from
 * base.html on every page and stays up until the failed posts are retried
 * successfully, skipped, or deleted. The social_posts table is the single
 * source of truth: giving up on a post makes it 'skipped', so it leaves this
 * list because it is genuinely no longer failing — there is no hidden-but-still-
 * failed state to drift out of sync.
 */
(function () {
    'use strict';

    const POLL_MS = 30000;
    const ERROR_PREVIEW_CHARS = 200;

    // Collapsed shows only the newest failure; expanded lists every one. The
    // state lives here rather than in the DOM so a poll that rebuilds the
    // banner doesn't collapse the list out from under someone reading it.
    let isExpanded = false;
    let latestPosts = [];
    let lastRenderedKey = null;

    function escapeText(value) {
        const div = document.createElement('div');
        div.textContent = value == null ? '' : String(value);
        return div.innerHTML;
    }

    function escapeAttribute(value) {
        return escapeText(value).replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }

    /**
     * When the send attempt failed, or null.
     *
     * NULL failed_at means the row failed before migration 044 added the
     * column — genuinely unknown, so the line omits the time rather than
     * substituting created_at, which is when the post was *written* and can
     * predate the attempt by weeks.
     */
    function whenText(post) {
        if (!post.failed_at) return null;
        return window.dysDateTime.formatWhenWithAge(post.failed_at);
    }

    // Platform and title carry the identity, the error carries the detail,
    // the time says whether this is breaking now or is a leftover —
    // distinguishing them is what makes a list of eight scannable.
    function describe(post, errorText, { timeFirst = false } = {}) {
        const when = whenText(post);
        /* In the expanded list the time leads. Scanning eight failures, the
         * first question is "is any of this from today?" — and an error string
         * can run to a couple of hundred characters, so a trailing timestamp
         * lands in a different place on every row and cannot be scanned at all.
         * The one-line summary keeps it last, where it reads as a clause of the
         * sentence rather than a column. */
        const stamp = when
            ? `<span class="failed-sends-banner__when">${escapeText(when)}</span>`
            : '';
        const lead = (timeFirst && stamp) ? `${stamp} ` : '';
        const trail = (!timeFirst && stamp) ? `${stamp} ` : '';
        return lead
            + `<span class="failed-sends-banner__platform">${escapeText(post.platform)}</span>`
            + ` — ${escapeText(post.video_title)}: `
            + `<span class="failed-sends-banner__error">${escapeText(errorText)}</span> `
            + trail
            + `<a href="${escapeAttribute(post.page_url)}">View →</a>`;
    }

    /* Retry re-runs the ordinary send endpoint — the post row already carries
     * everything a send needs, and an absent social_account_id resolves by the
     * same precedence a first send uses.
     *
     * Skip is not "hide this": it marks the post 'skipped', the state this
     * codebase already uses for a post nobody is going to send (see
     * smart_queue_disposition's `remove`). It leaves this list because it is no
     * longer failing, not because it is filtered out. */
    function actions(post) {
        const id = escapeAttribute(String(post.id));
        const retryHint = post.retryable
            ? 'Send this again now. It is also being retried automatically.'
            : 'Send this again now.';
        return ` <button type="button" class="failed-sends-banner__action"`
            + ` title="${retryHint}"`
            + ` data-action="retry" data-post-id="${id}">Retry</button>`
            + ` <button type="button" class="failed-sends-banner__action"`
            + ` title="Give up on this post. It is marked skipped and stops being`
            + ` retried; the error and the text are kept."`
            + ` data-action="skip" data-post-id="${id}">Skip</button>`;
    }

    /* Skip is the one action here with a consequence past this posting, so it
     * asks — worded the same way the smart queue's Remove does, because they do
     * the same thing and a user should not have to learn it twice. */
    function skipWarning(post) {
        const lines = [
            'Skip this post?',
            '',
            'It is marked skipped and will not be sent or retried. The error '
                + 'text and the post content are kept.',
            '',
        ];
        if (post.smart_queue_item_id) {
            lines.push(
                'It belongs to a smart schedule. Skipping affects only this '
                + 'posting — use the schedule\'s own Remove to take the video '
                + 'out of the queue entirely.');
        } else {
            lines.push('Nothing will re-create it automatically.');
        }
        return lines.join('\n');
    }

    /** Identifies what the banner is currently showing, so an unchanged poll is a no-op.
     *
     * Keyed on the *rendered* time text, not on failed_at: the age is relative,
     * so "3 minutes ago" goes stale on its own with every field unchanged.
     * Comparing the text is what lets a poll notice a minute boundary while
     * still skipping the rebuild in the 29 seconds either side of it.
     */
    function renderKey(posts) {
        return JSON.stringify([
            isExpanded,
            posts.map((post) => [
                post.id, post.platform, post.video_title, post.error,
                post.page_url, whenText(post),
            ]),
        ]);
    }

    function render(posts) {
        const banner = document.getElementById('failed-sends-banner');
        if (!banner) return;

        latestPosts = posts;

        if (!posts.length) {
            isExpanded = false;
            lastRenderedKey = null;
            banner.style.display = 'none';
            banner.innerHTML = '';
            return;
        }

        // Rebuilding identical markup every 30s would reset the list's scroll
        // position and drop keyboard focus on the toggle mid-read.
        const key = renderKey(posts);
        if (key === lastRenderedKey) return;
        lastRenderedKey = key;

        const others = posts.length - 1;
        // With nothing beyond the newest there is no toggle, so an expanded
        // state left over from a larger batch would strand an untruncated
        // error with no way to collapse it. One post always reads as collapsed.
        const showList = isExpanded && others > 0;
        const heading =
            `<strong>${posts.length} social post${posts.length === 1 ? '' : 's'} failed to send.</strong>`;

        let summary = heading;
        if (!showList) {
            const newest = posts[0];
            let error = newest.error || 'send failed';
            if (error.length > ERROR_PREVIEW_CHARS) {
                error = error.slice(0, ERROR_PREVIEW_CHARS) + '…';
            }
            summary += ` ${describe(newest, error)}`;
        }
        if (others) {
            // aria-controls only while the list exists: pointing it at an id
            // that isn't in the DOM resolves to nothing for a screen reader.
            summary +=
                ` <button type="button" class="failed-sends-banner__toggle"`
                + ` aria-expanded="${showList}"`
                + (showList ? ` aria-controls="failed-sends-banner-list"` : '')
                + `>`
                + (showList ? 'Hide ▴' : `and ${others} more ▾`)
                + `</button>`;
        }

        // Expanded shows each error in full rather than the 200-char preview —
        // the whole point of opening it is to read what actually went wrong.
        // The list scrolls internally so a long one can't swallow the page.
        const list = showList
            ? `<ul id="failed-sends-banner-list" class="failed-sends-banner__list">`
                + posts.map((post) =>
                    `<li>${describe(post, post.error || 'send failed', {timeFirst: true})}`
                    + `${actions(post)}</li>`).join('')
                + `</ul>`
            : '';

        banner.innerHTML = `<div class="failed-sends-banner__summary">${summary}</div>${list}`;
        banner.style.display = '';

        const toggle = banner.querySelector('.failed-sends-banner__toggle');
        if (toggle) {
            toggle.addEventListener('click', () => {
                isExpanded = !isExpanded;
                render(latestPosts);
                const refocused = banner.querySelector('.failed-sends-banner__toggle');
                if (refocused) refocused.focus();
            });
        }

        banner.querySelectorAll('.failed-sends-banner__action').forEach((button) => {
            button.addEventListener('click', () => runAction(button));
        });
    }

    /** Run Retry or Skip for one post, then re-read the list.
     *
     * Both outcomes are reported. A retry that fails again must not look like
     * one that worked — it stays in the banner carrying the new error.
     */
    async function runAction(button) {
        const postId = button.dataset.postId;
        const action = button.dataset.action;
        if (action === 'skip') {
            const post = latestPosts.find((p) => String(p.id) === String(postId));
            if (post && !confirm(skipWarning(post))) return;
        }
        const url = action === 'retry'
            ? `/api/social/posts/${encodeURIComponent(postId)}/send`
            : `/api/social/posts/${encodeURIComponent(postId)}/skip`;

        // Disable both buttons on the row: a second click while the first is in
        // flight sends the post twice.
        const row = button.closest('li') || button.parentElement;
        const buttons = row ? row.querySelectorAll('.failed-sends-banner__action') : [button];
        buttons.forEach((b) => { b.disabled = true; });
        button.textContent = action === 'retry' ? 'Sending…' : 'Skipping…';

        try {
            const resp = await fetch(url, {method: 'POST'});
            if (resp.ok) {
                if (typeof showToast === 'function') {
                    showToast(action === 'retry' ? 'Sent.' : 'Skipped.', 'success');
                }
            } else {
                const body = await resp.json().catch(() => ({}));
                // 409 on retry is the duplicate guard, whose detail is a payload
                // rather than a string; say so instead of rendering [object].
                const detail = typeof body.detail === 'string'
                    ? body.detail
                    : `HTTP ${resp.status}`;
                if (typeof showToast === 'function') showToast(detail, 'error');
            }
        } catch (err) {
            if (typeof showToast === 'function') {
                showToast(`Could not ${action} the post: ${err.message}`, 'error');
            }
        } finally {
            // Re-read either way: on success the row is gone, and on failure the
            // error text has changed.
            lastRenderedKey = null;
            await check();
        }
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

    // A page that removes or retries a failed post knows the list changed
    // before the next poll does. Without this the banner keeps naming a post
    // that no longer exists — including a "View →" link to a card that is gone.
    // Exposing the existing check keeps one fetch/render implementation.
    window.refreshFailedSendsBanner = check;
})();
