/**
 * Threads "Uninstall" (deauthorize) callback — Cloudflare Pages Function.
 *
 * Meta POSTs `signed_request=<base64url-sig>.<base64url-payload>` to this URL
 * when a user removes the app from their Threads / Meta account. The Scheduler
 * is self-hosted and keeps the user's token only on their own machine, so there
 * is nothing to do server-side: we verify the signature (when THREADS_APP_SECRET
 * is configured) and return 200. The local app discovers the revoked token on
 * its next API call and flags the credential as "needs re-auth".
 *
 * Cloudflare setup: Pages project → Settings → Environment variables →
 *   THREADS_APP_SECRET = <your Threads app secret>
 * (Optional but recommended — without it the signature isn't checked.)
 */

export async function onRequestPost(context) {
  const { request, env } = context;
  let userId = null;
  try {
    const form = await request.formData();
    const signedRequest = form.get("signed_request");
    if (typeof signedRequest === "string") {
      const data = await parseSignedRequest(signedRequest, env.THREADS_APP_SECRET);
      if (data) userId = data.user_id || data.user || null;
    }
  } catch (_) {
    // Malformed body — still 200 so Meta doesn't retry forever.
  }
  if (userId) console.log("Threads uninstall callback for user", userId);
  return new Response(JSON.stringify({ status: "ok" }), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

export async function onRequestGet(context) {
  // Browsable so you can confirm the route deployed and the signing secret is
  // wired up. (It only checks the secret is *present*, not that it matches Meta.)
  const secret = context.env && context.env.THREADS_APP_SECRET ? "configured" : "NOT configured";
  return new Response(
    `Threads uninstall callback — accepts POSTs from Meta only.\n`
      + `Signing secret (THREADS_APP_SECRET): ${secret}.\n`,
    { status: 200, headers: { "content-type": "text/plain; charset=utf-8" } },
  );
}

/* --- Meta signed_request parsing (HMAC-SHA256) ------------------------------
 * Duplicated in callback-threads-delete.js so each Function stays self-
 * contained — a shared module under functions/ would itself become a route. */

async function parseSignedRequest(signedRequest, appSecret) {
  const dot = signedRequest.indexOf(".");
  if (dot < 1) return null;
  const sigPart = signedRequest.slice(0, dot);
  const payloadPart = signedRequest.slice(dot + 1);

  let payload;
  try {
    payload = JSON.parse(new TextDecoder().decode(b64urlDecode(payloadPart)));
  } catch (_) {
    return null;
  }

  if (appSecret) {
    if (payload.algorithm && String(payload.algorithm).toUpperCase() !== "HMAC-SHA256") {
      return null;
    }
    const key = await crypto.subtle.importKey(
      "raw",
      new TextEncoder().encode(appSecret),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["verify"],
    );
    const ok = await crypto.subtle.verify(
      "HMAC",
      key,
      b64urlDecode(sigPart),
      new TextEncoder().encode(payloadPart),
    );
    if (!ok) return null;
  }
  return payload;
}

function b64urlDecode(s) {
  s = s.replace(/-/g, "+").replace(/_/g, "/");
  while (s.length % 4) s += "=";
  const bin = atob(s);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes;
}
