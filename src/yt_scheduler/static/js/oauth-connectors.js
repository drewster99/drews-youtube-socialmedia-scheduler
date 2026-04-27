/* Per-platform OAuth connector helpers, shared between Settings and
 * project Settings.
 *
 * The OAuth client (Client ID / Secret) for Twitter, LinkedIn, and
 * Threads is configured *once* in Settings → OAuth client credentials
 * and stored in Keychain. Each connector below just opens the popup;
 * the server reads the stored credentials. If none are stored, the
 * server returns a 400 whose ``detail`` text directs the user to
 * Settings — `openOAuthPopup` already surfaces that as a toast.
 *
 * Mastodon registers itself dynamically per instance, and Bluesky uses
 * AT-proto OAuth with the install acting as its own client, so neither
 * needs a stored client_id — only the instance URL / handle the user
 * is connecting.
 *
 * When called with a non-null ``projectSlug``, the start endpoint
 * receives ``project_slug`` so the OAuth callback binds the resulting
 * credential as the project's default for that platform.
 */

async function connectLinkedIn(projectSlug = null) {
    try {
        await openOAuthPopup('/api/oauth/linkedin/start', {
            origin: window.location.origin,
            project_slug: projectSlug,
        }, 'linkedin-oauth');
    } catch (_) {}
}

async function connectTwitter(projectSlug = null) {
    try {
        await openOAuthPopup('/api/oauth/twitter/start', {
            origin: window.location.origin,
            project_slug: projectSlug,
        }, 'twitter-oauth');
    } catch (_) {}
}

async function connectMastodon(projectSlug = null) {
    const instance = prompt('Mastodon instance URL\n\n(e.g. https://mastodon.social — the app will register itself there.)', 'https://mastodon.social');
    if (!instance) return;
    try {
        await openOAuthPopup('/api/oauth/mastodon/start', {
            instance_url: instance.trim(),
            origin: window.location.origin,
            project_slug: projectSlug,
        }, 'mastodon-oauth');
    } catch (_) {}
}

async function connectThreads(projectSlug = null) {
    try {
        await openOAuthPopup('/api/oauth/threads/start', {
            origin: window.location.origin,
            project_slug: projectSlug,
        }, 'threads-oauth');
    } catch (_) {}
}

async function connectBluesky(projectSlug = null) {
    const handle = prompt('Your Bluesky handle (e.g. you.bsky.social — no @):');
    if (!handle) return;
    try {
        await openOAuthPopup('/api/oauth/bluesky/start', {
            origin: window.location.origin,
            handle: handle.trim(),
            project_slug: projectSlug,
        });
    } catch (_) {}
}
