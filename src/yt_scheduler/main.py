"""Entry point for the Drew's Video + Socials Scheduler application."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

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


def _bundle_passphrase(*, confirm: bool) -> str:
    """Get the backup passphrase from ``DYS_BUNDLE_PASSPHRASE`` or an interactive prompt."""
    env = os.getenv("DYS_BUNDLE_PASSPHRASE")
    if env:
        return env
    import getpass

    passphrase = getpass.getpass("Backup passphrase: ")
    if not passphrase:
        print("A passphrase is required.", file=sys.stderr)
        sys.exit(2)
    if confirm:
        again = getpass.getpass("Confirm passphrase: ")
        if again != passphrase:
            print("Passphrases did not match.", file=sys.stderr)
            sys.exit(2)
    return passphrase


def _server_is_running() -> bool:
    """True if the scheduler server appears to be running.

    Primary check: the PID file written by the server at startup. If it
    contains a PID for a live process, the server is running. A stale file
    (left behind by a crash) is detected via os.kill(pid, 0) — if the
    process doesn't exist the file is stale and we remove it.

    Fallback: if no PID file exists (e.g. an older server build that
    predates this file), we probe the port on both IPv4 and IPv6 without
    SO_REUSEADDR so a live bind reliably causes EADDRINUSE.
    """
    import os
    import socket

    from yt_scheduler.config import PID_FILE

    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
        except (ValueError, OSError):
            # Unreadable or malformed — treat as stale.
            PID_FILE.unlink(missing_ok=True)
        else:
            try:
                os.kill(pid, 0)
                # Signal delivered: process is alive.
                return True
            except ProcessLookupError:
                # Process is gone — stale lock file.
                PID_FILE.unlink(missing_ok=True)
            except PermissionError:
                # Process exists but we don't own it — very unlikely for a
                # single-user app, but treat it as live to be safe.
                return True

    # Fallback port probe: try both IPv4 and IPv6, no SO_REUSEADDR so that a
    # genuinely bound port surfaces as EADDRINUSE.
    for family, addr in (
        (socket.AF_INET, "127.0.0.1"),
        (socket.AF_INET6, "::1"),
    ):
        try:
            with socket.socket(family, socket.SOCK_STREAM) as sock:
                sock.bind((addr, PORT))
        except OSError:
            return True
        except Exception:
            pass

    return False


def main():
    """Run the application or handle CLI commands."""
    if os.getenv("DYS_REDIRECT_LOGS") == "1":
        _redirect_stdio_to_log()

    args = sys.argv[1:]

    if not args or args[0] == "serve" or args[0] == "--reload":
        # Default: run the web server.
        #
        # uvicorn's default access log line is ``INFO:    127.0.0.1:NNNN -
        # "GET /api/build HTTP/1.1" 200 OK`` with no timestamp. When the
        # log rolls into the .app's Server Monitor or the user opens
        # ~/Library/Logs/<bundle>/server.log directly, every line is
        # functionally indistinguishable in time. Inject a log_config
        # that prefixes every record with ``YYYY-MM-DDTHH:MM:SS,sss``
        # and the level — same format for app, error, and access loggers
        # — so logs are usable without a tail timestamp tool.
        log_config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "%(asctime)s.%(msecs)03d %(levelname)s %(name)s — %(message)s",
                    "datefmt": "%Y-%m-%dT%H:%M:%S",
                },
                "access": {
                    "format": "%(asctime)s.%(msecs)03d %(levelname)s %(client_addr)s — \"%(request_line)s\" %(status_code)s",
                    "datefmt": "%Y-%m-%dT%H:%M:%S",
                    "class": "uvicorn.logging.AccessFormatter",
                },
            },
            "handlers": {
                "default": {
                    "formatter": "default",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                },
                "access": {
                    "formatter": "access",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                },
            },
            "loggers": {
                # uvicorn's main logger (startup / shutdown / config)
                "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
                "uvicorn.error": {"level": "INFO"},
                "uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
                # Our own app logger so /api routes' info/warn/error
                # lines also get timestamped consistently.
                "yt_scheduler": {"handlers": ["default"], "level": "INFO", "propagate": False},
            },
        }
        uvicorn.run(
            "yt_scheduler.app:app",
            host=HOST,
            port=PORT,
            reload="--reload" in args,
            log_config=log_config,
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

    elif args[0] == "export-all":
        if len(args) < 2:
            print("Usage: yt-scheduler export-all <output-file.dysbak>", file=sys.stderr)
            sys.exit(2)
        ensure_dirs()
        from yt_scheduler.services import backup
        passphrase = _bundle_passphrase(confirm=True)
        try:
            summary = backup.export_bundle(Path(args[1]), passphrase)
        except backup.BackupError as exc:
            print(f"Export failed: {exc}", file=sys.stderr)
            sys.exit(1)
        db_note = "" if summary["includes_db"] else ", no database found"
        print(
            f"Wrote {summary['path']} ({summary['bytes']:,} bytes) — "
            f"{summary['data_files']} data file(s), {summary['secret_count']} secret(s){db_note}"
        )
        print("This file contains all your credentials — delete it after transferring it.")

    elif args[0] == "import-all":
        if len(args) < 2:
            print("Usage: yt-scheduler import-all <input-file.dysbak>", file=sys.stderr)
            sys.exit(2)
        if _server_is_running():
            from yt_scheduler.config import PID_FILE

            print(
                "The scheduler server appears to be running. Restoring now would "
                "overwrite the live database underneath it and corrupt your data.\n"
                "Stop the server first — quitting the macOS app fully stops it. "
                "Note: simply killing the process is NOT enough when it's installed "
                "as a launch agent/service, because launchd will relaunch it; use "
                "the app to stop it (or `launchctl bootout` the service).\n"
                f"If you're certain it is not running, delete the stale PID file "
                f"({PID_FILE}) and try again.",
                file=sys.stderr,
            )
            sys.exit(1)
        from yt_scheduler.services import backup
        passphrase = _bundle_passphrase(confirm=False)
        try:
            result = backup.import_bundle(Path(args[1]), passphrase)
        except backup.BackupError as exc:
            print(f"Import failed: {exc}", file=sys.stderr)
            sys.exit(1)
        print(
            f"Restored data to {result['data_dir']} and "
            f"{result['secret_count']} secret(s) to secure storage."
        )
        if result["pre_import_path"]:
            print(f"Your previous data was kept at: {result['pre_import_path']}")

    else:
        print("Drew's Video + Socials Scheduler")
        print()
        print("Commands:")
        print("  yt-scheduler              Start the web server (default)")
        print("  yt-scheduler serve         Start the web server")
        print("  yt-scheduler install       Install background service (Linux only — macOS uses the .app)")
        print("  yt-scheduler uninstall     Remove background service")
        print("  yt-scheduler status        Check service status")
        print("  yt-scheduler auth [path]   Run YouTube OAuth flow")
        print("  yt-scheduler export-all <file>   Export all data + secrets to an encrypted backup")
        print("  yt-scheduler import-all <file>   Restore from an encrypted backup (replaces current data)")
        print()
        print(f"Web UI: http://{HOST}:{PORT}")


if __name__ == "__main__":
    main()
