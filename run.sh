#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"

# Data directories — StaticFiles mounts happen at import time, so the uploads
# dir must exist before the server starts.
DATA_DIR="${YTP_DATA_DIR:-$HOME/.yt-scheduler}"
mkdir -p "$DATA_DIR/uploads" "$DATA_DIR/templates"

# Create venv if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

# Activate
source "$VENV_DIR/bin/activate"

# Install/update deps (skip if already satisfied)
pip install -q -e ".[social,dev,transcription-mlx]"

# Forward all arguments to yt-scheduler
exec yt-scheduler "$@"
