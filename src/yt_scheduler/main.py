"""Entry point for the Drew's YT Scheduler application."""

from __future__ import annotations

import json
import os
import sys

import uvicorn

from yt_scheduler.config import HOST, LOG_DIR, PORT, ensure_dirs


def _redirect_stdio_to_log() -> None:
    """Send stdout + stderr to ``LOG_DIR/server.log`` so launchd can use
    ``/dev/null`` for its own redirects.

    launchd plists don't expand ``~`` or ``$HOME`` in StandardOutPath, and
    relative paths there resolve to ``/`` (launchd's cwd), so we can't bake a
    correct user log path into the embedded plist. Doing the redirect here
    lets the Python side compute the right path from $HOME at runtime.

    Triggered by ``DYS_REDIRECT_LOGS=1`` (set in the embedded launch agent
    plist) so terminal/dev runs keep printing to the console.
    """
    ensure_dirs()
    log_file = LOG_DIR / "server.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(log_file), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    os.dup2(fd, 1)
    os.dup2(fd, 2)
    os.close(fd)
    sys.stdout = os.fdopen(1, "w", buffering=1)
    sys.stderr = os.fdopen(2, "w", buffering=1)


def main():
    """Run the application or handle CLI commands."""
    if os.getenv("DYS_REDIRECT_LOGS") == "1":
        _redirect_stdio_to_log()

    args = sys.argv[1:]

    if not args or args[0] == "serve" or args[0] == "--reload":
        # Default: run the web server
        uvicorn.run(
            "yt_scheduler.app:app",
            host=HOST,
            port=PORT,
            reload="--reload" in args,
        )

    elif args[0] == "install":
        from yt_scheduler.services.daemon import install_service
        result = install_service()
        print(json.dumps(result, indent=2))
        if result.get("commands"):
            print("\nUseful commands:")
            for name, cmd in result["commands"].items():
                print(f"  {name}: {cmd}")

    elif args[0] == "uninstall":
        from yt_scheduler.services.daemon import uninstall_service
        result = uninstall_service()
        print(json.dumps(result, indent=2))

    elif args[0] == "status":
        from yt_scheduler.services.daemon import get_service_status
        result = get_service_status()
        print(json.dumps(result, indent=2))

    elif args[0] == "auth":
        ensure_dirs()
        from yt_scheduler.services.auth import run_oauth_flow
        client_secrets = args[1] if len(args) > 1 else None
        run_oauth_flow(client_secret_path=client_secrets)
        print("Authentication successful!")

    else:
        print("Drew's YT Scheduler")
        print()
        print("Commands:")
        print("  yt-scheduler              Start the web server (default)")
        print("  yt-scheduler serve         Start the web server")
        print("  yt-scheduler install       Install background service (Linux only — macOS uses the .app)")
        print("  yt-scheduler uninstall     Remove background service")
        print("  yt-scheduler status        Check service status")
        print("  yt-scheduler auth [path]   Run YouTube OAuth flow")
        print()
        print(f"Web UI: http://{HOST}:{PORT}")


if __name__ == "__main__":
    main()
