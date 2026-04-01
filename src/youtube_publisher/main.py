"""Entry point for the YouTube Publisher application."""

from __future__ import annotations

import sys

import uvicorn

from youtube_publisher.config import HOST, PORT


def main():
    """Run the application."""
    uvicorn.run(
        "youtube_publisher.app:app",
        host=HOST,
        port=PORT,
        reload="--reload" in sys.argv,
    )


if __name__ == "__main__":
    main()
