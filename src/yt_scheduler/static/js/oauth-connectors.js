/* Per-platform OAuth connector helpers, shared between Settings and
 * project Settings.
 *
 * Each connector prompts for whatever the user must supply (LinkedIn /
 * Twitter / Threads need client_id+client_secret; Mastodon needs an
 * instance URL; Bluesky needs the user's handle), then opens an OAuth
 * popup against the platform's `/api/oauth/<platform>/start` endpoint.
 *
 * When called with a non-null ``projectSlug``, the start endpoint
 * receives ``project_slug`` so the OAuth callback binds the resulting
 * credential as the project's default for that platform. When called
 * with ``null`` (or no argument), the credential is created without a
 * project binding — the existing General Settings flow.
 */

async function connectLinkedIn(projectSlug = null) {
    const clientId = prompt('LinkedIn Client ID\n\n(developers.linkedin.com → your app → Auth tab → Application credentials)');
    if (!clientId) return;
    const clientSecret = prompt('LinkedIn Primary Client Secret');
    if (!clientSecret) return;
    try {
        await openOAuthPopup('/api/oauth/linkedin/start', {
            client_id: clientId.trim(),
            client_secret: clientSecret.trim(),
            origin: window.location.origin,
            project_slug: projectSlug,
        }, 'linkedin-oauth');
    } catch (_) {}
}

async function connectTwitter(projectSlug = null) {
    const clientId = prompt('X / Twitter OAuth 2.0 Client ID\n\n(developer.x.com → your app → Keys and tokens)');
    if (!clientId) return;
    const clientSecret = prompt('Client Secret\n\n(Optional — leave blank for a public app.)') || '';
    try {
        await openOAuthPopup('/api/oauth/twitter/start', {
            client_id: clientId.trim(),
            client_secret: clientSecret.trim(),
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
    const clientId = prompt('Threads / Meta App Client ID\n\n(developers.facebook.com → your app → App settings → Basic)');
    if (!clientId) return;
    const clientSecret = prompt('Threads / Meta App Secret');
    if (!clientSecret) return;
    try {
        await openOAuthPopup('/api/oauth/threads/start', {
            client_id: clientId.trim(),
            client_secret: clientSecret.trim(),
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
