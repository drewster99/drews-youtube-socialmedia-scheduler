#!/usr/bin/env bash
# Sync this repo's cloudflare/ files into the nuclearcyborg.com static-site git
# repo, then commit + push if anything changed. The site is wired to Cloudflare
# Pages via the GitHub integration, so the push itself triggers the Cloudflare
# build — make sure you're on the branch the Pages project watches (Production
# branch in Pages → Settings → Builds & deployments) before running this; any
# other branch gives you a Preview deploy, not Production.
#
#   cloudflare/apps/      -> $DEST/apps/         merged (NEVER deletes)
#   cloudflare/functions/ -> $DEST/functions/    mirrored (--delete: this dir is wholly ours)
#
# Usage:
#   scripts/deploy-cloudflare.sh             # sync, commit, push
#   scripts/deploy-cloudflare.sh --dry-run   # show what would change, do nothing
#   DEST=/path/to/site scripts/deploy-cloudflare.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SRC="$REPO_ROOT/cloudflare"
DEST="${DEST:-/Users/andrew/Documents/ncc/web/sites/nuclearcyborg.com}"

DRY_RUN=0
case "${1:-}" in
  -n|--dry-run) DRY_RUN=1 ;;
  -h|--help)    sed -n '2,15p' "$0"; exit 0 ;;
  "") ;;
  *) echo "Unknown argument: $1" >&2; echo "Usage: $0 [--dry-run]" >&2; exit 2 ;;
esac

# --- sanity checks --------------------------------------------------------
[ -d "$SRC/apps" ]      || { echo "Missing $SRC/apps" >&2; exit 1; }
[ -d "$SRC/functions" ] || { echo "Missing $SRC/functions" >&2; exit 1; }
[ -d "$DEST" ]          || { echo "Destination not found: $DEST" >&2; exit 1; }
[ -d "$DEST/.git" ]     || { echo "Destination is not a git repo: $DEST" >&2; exit 1; }

# Refuse if cloudflare/apps/ contains any index.html — that folder is meant to
# hold only our additive files; an index.html would clobber the destination's.
if [ -n "$(find "$SRC/apps" -name index.html -print 2>/dev/null)" ]; then
  echo "Refusing to deploy: $SRC/apps contains an index.html (would clobber the site's)." >&2
  exit 1
fi

# --- sync -----------------------------------------------------------------
RSYNC_FLAGS=(-a --itemize-changes)
[ "$DRY_RUN" -eq 1 ] && RSYNC_FLAGS+=(--dry-run)

echo "Syncing apps/ (merge, no deletes): $SRC/apps/ -> $DEST/apps/"
rsync "${RSYNC_FLAGS[@]}" "$SRC/apps/" "$DEST/apps/"

echo "Syncing functions/ (mirror): $SRC/functions/ -> $DEST/functions/"
rsync "${RSYNC_FLAGS[@]}" --delete "$SRC/functions/" "$DEST/functions/"

if [ "$DRY_RUN" -eq 1 ]; then
  echo "(dry run — no files written, nothing committed)"
  exit 0
fi

# --- stage exactly what we manage, commit + push if there's anything new --
cd "$DEST"

# functions/ is wholly ours, so add the whole tree (catches additions, edits,
# deletions). For apps/ we stage only the files we actually copied — leaving
# every other file under apps/ untouched in case the site puts non-managed
# content there.
APP_PATHS=()
while IFS= read -r f; do
  APP_PATHS+=("apps/${f#"$SRC/apps/"}")
done < <(find "$SRC/apps" -type f)

git add functions "${APP_PATHS[@]}"

if git diff --cached --quiet; then
  # Nothing newly staged. Still push any commit a previous run left behind.
  if git rev-parse --abbrev-ref --symbolic-full-name '@{u}' >/dev/null 2>&1 \
     && [ -n "$(git log '@{u}..' --oneline)" ]; then
    echo "Nothing changed, but there are unpushed commits — pushing."
    git push
  else
    echo "Nothing to deploy."
  fi
  exit 0
fi

echo "Changes to commit:"
git diff --cached --stat

git commit -m "scheduler: sync Threads OAuth bounce page + Meta callbacks"
git push
echo "Deployed and pushed."
