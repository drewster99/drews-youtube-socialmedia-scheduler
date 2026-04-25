"""Shared pytest fixtures."""

from __future__ import annotations

import sys
from pathlib import Path

# Make `src/` importable so tests can `from yt_scheduler import ...`
SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
