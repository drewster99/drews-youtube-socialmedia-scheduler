"""Entry point for the YouTube Publisher application."""

from __future__ import annotations

import json
import sys

import uvicorn

from youtube_publisher.config import HOST, PORT


def main():
    """Run the application or handle CLI commands."""
    args = sys.argv[1:]

    if not args or args[0] == "serve" or args[0] == "--reload":
        # Default: run the web server
        uvicorn.run(
            "youtube_publisher.app:app",
            host=HOST,
            port=PORT,
            reload="--reload" in args,
        )

    elif args[0] == "install":
        from youtube_publisher.services.daemon import install_service
        result = install_service()
        print(json.dumps(result, indent=2))
        if result.get("commands"):
            print("\nUseful commands:")
            for name, cmd in result["commands"].items():
                print(f"  {name}: {cmd}")

    elif args[0] == "uninstall":
        from youtube_publisher.services.daemon import uninstall_service
        result = uninstall_service()
        print(json.dumps(result, indent=2))

    elif args[0] == "status":
        from youtube_publisher.services.daemon import get_service_status
        result = get_service_status()
        print(json.dumps(result, indent=2))

    elif args[0] == "auth":
        from youtube_publisher.config import ensure_dirs
        ensure_dirs()
        from youtube_publisher.services.auth import run_oauth_flow
        client_secrets = args[1] if len(args) > 1 else None
        run_oauth_flow(client_secrets)
        print("Authentication successful!")

    else:
        print("YouTube Publisher")
        print()
        print("Commands:")
        print("  youtube-publisher              Start the web server (default)")
        print("  youtube-publisher serve         Start the web server")
        print("  youtube-publisher install       Install as background service (launchd/systemd)")
        print("  youtube-publisher uninstall     Remove background service")
        print("  youtube-publisher status        Check service status")
        print("  youtube-publisher auth [path]   Run YouTube OAuth flow")
        print()
        print(f"Web UI: http://{HOST}:{PORT}")


if __name__ == "__main__":
    main()
