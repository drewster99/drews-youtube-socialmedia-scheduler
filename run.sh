#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"

# Data directories — StaticFiles mounts happen at import time, so the uploads
# dir must exist before the server starts. Default matches yt_scheduler/config.py
# (~/Library/Application Support/<bundle_id>/) so dev runs share the .app's data.
BUNDLE_ID="com.nuclearcyborg.drews-socialmedia-scheduler"
DATA_DIR="${DYS_DATA_DIR:-${YTP_DATA_DIR:-$HOME/Library/Application Support/$BUNDLE_ID}}"
mkdir -p "$DATA_DIR/uploads" "$DATA_DIR/templates"

# Create venv if it doesn't exist, or if it's stale (e.g. directory was renamed,
# leaving pip scripts with shebangs pointing to a path that no longer exists).
venv_needs_rebuild() {
    [ -d "$VENV_DIR" ] || return 0
    "$VENV_DIR/bin/python3" -c 'import sys' >/dev/null 2>&1 || return 0
    "$VENV_DIR/bin/pip" --version >/dev/null 2>&1 || return 0
    return 1
}

if venv_needs_rebuild; then
    echo "Recreating virtual environment (stale or missing)..."
    rm -r "$VENV_DIR" 2>/dev/null || true
    python3 -m venv "$VENV_DIR"
fi

# Activate
source "$VENV_DIR/bin/activate"

# Install/update deps (skip if already satisfied)
pip install -q -e ".[social,dev,transcription-mlx]"

# Forward all arguments to yt-scheduler
exec yt-scheduler "$@"
