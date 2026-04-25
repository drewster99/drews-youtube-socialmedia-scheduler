"""Platform-specific daemon/service installation.

macOS: LaunchAgent (runs on user login, auto-restart)
Linux: systemd user service (runs on user login, auto-restart)
"""

from __future__ import annotations

import logging
import os
import platform
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from textwrap import dedent
from xml.sax.saxutils import escape as xml_escape

from yt_scheduler.config import DATA_DIR

logger = logging.getLogger(__name__)


MACOS_AGENT_LABEL = "com.nuclearcyborg.drews-socialmedia-scheduler"
LEGACY_MACOS_AGENT_LABEL = "com.youtube-publisher"
MACOS_AGENT_DIR = Path.home() / "Library" / "LaunchAgents"
MACOS_AGENT_PATH = MACOS_AGENT_DIR / f"{MACOS_AGENT_LABEL}.plist"
LEGACY_MACOS_AGENT_PATH = MACOS_AGENT_DIR / f"{LEGACY_MACOS_AGENT_LABEL}.plist"

LINUX_SERVICE_NAME = "drews-yt-scheduler"
LEGACY_LINUX_SERVICE_NAME = "youtube-publisher"
LINUX_SERVICE_DIR = Path.home() / ".config" / "systemd" / "user"
LINUX_SERVICE_PATH = LINUX_SERVICE_DIR / f"{LINUX_SERVICE_NAME}.service"
LEGACY_LINUX_SERVICE_PATH = LINUX_SERVICE_DIR / f"{LEGACY_LINUX_SERVICE_NAME}.service"

LOG_DIR = DATA_DIR / "logs"


def _find_executable() -> list[str]:
    """Find the yt-scheduler executable as a list of arguments.

    Returns a list like ["/path/to/yt-scheduler"] or
    ["/path/to/python", "-m", "yt_scheduler.main"].
    Returning a list avoids path-splitting bugs when paths contain spaces.
    """
    exe = shutil.which("yt-scheduler")
    if exe:
        return [exe]
    # Fallback: use the current Python interpreter with the module
    return [sys.executable, "-m", "yt_scheduler.main"]


def _generate_launchd_plist() -> str:
    """Generate a macOS LaunchAgent plist."""
    parts = _find_executable()
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # XML-escape all interpolated values to prevent malformed plist
    program_args = "\n".join(
        f"            <string>{xml_escape(p)}</string>" for p in parts
    )
    log_dir_escaped = xml_escape(str(LOG_DIR))

    return dedent(f"""\
        <?xml version="1.0" encoding="UTF-8"?>
        <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
        <plist version="1.0">
        <dict>
            <key>Label</key>
            <string>{xml_escape(MACOS_AGENT_LABEL)}</string>

            <key>ProgramArguments</key>
            <array>
        {program_args}
            </array>

            <key>RunAtLoad</key>
            <true/>

            <key>KeepAlive</key>
            <dict>
                <key>SuccessfulExit</key>
                <false/>
            </dict>

            <key>ThrottleInterval</key>
            <integer>10</integer>

            <key>StandardOutPath</key>
            <string>{log_dir_escaped}/stdout.log</string>

            <key>StandardErrorPath</key>
            <string>{log_dir_escaped}/stderr.log</string>

            <key>EnvironmentVariables</key>
            <dict>
                <key>PATH</key>
                <string>/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin</string>
            </dict>

            <key>ProcessType</key>
            <string>Background</string>
        </dict>
        </plist>
    """)


def _generate_systemd_unit() -> str:
    """Generate a Linux systemd user service unit."""
    parts = _find_executable()
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # shlex.join properly quotes paths with spaces for systemd
    exec_start = shlex.join(parts)

    return dedent(f"""\
        [Unit]
        Description=Drew's YT Scheduler — video publishing and social media automation
        After=network-online.target
        Wants=network-online.target

        [Service]
        Type=simple
        ExecStart={exec_start}
        Restart=on-failure
        RestartSec=10
        Environment=PATH=/usr/local/bin:/usr/bin:/bin

        [Install]
        WantedBy=default.target
    """)


def install_service() -> dict:
    """Install the background service for the current platform.

    Returns a status dict with instructions.
    """
    system = platform.system()

    if system == "Darwin":
        return _install_macos()
    elif system == "Linux":
        return _install_linux()
    else:
        return {
            "status": "unsupported",
            "message": f"Automatic service install not supported on {system}. "
                       "Run 'yt-scheduler' manually or set up your own service.",
        }


def _install_macos() -> dict:
    """Install macOS LaunchAgent."""
    plist = _generate_launchd_plist()

    MACOS_AGENT_DIR.mkdir(parents=True, exist_ok=True)

    # Tear down the legacy LaunchAgent if it's still around from before the rename.
    if LEGACY_MACOS_AGENT_PATH.exists():
        subprocess.run(
            ["launchctl", "bootout", f"gui/{os.getuid()}", str(LEGACY_MACOS_AGENT_PATH)],
            capture_output=True,
        )
        try:
            LEGACY_MACOS_AGENT_PATH.unlink()
        except OSError:
            pass

    # Unload existing if present (ignore errors — may not be loaded)
    if MACOS_AGENT_PATH.exists():
        subprocess.run(
            ["launchctl", "bootout", f"gui/{os.getuid()}", str(MACOS_AGENT_PATH)],
            capture_output=True,
        )

    MACOS_AGENT_PATH.write_text(plist)

    # Load the agent
    result = subprocess.run(
        ["launchctl", "bootstrap", f"gui/{os.getuid()}", str(MACOS_AGENT_PATH)],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        # Fallback to legacy load command
        result = subprocess.run(
            ["launchctl", "load", str(MACOS_AGENT_PATH)],
            capture_output=True,
            text=True,
        )

    if result.returncode != 0:
        return {
            "status": "error",
            "platform": "macos",
            "plist_path": str(MACOS_AGENT_PATH),
            "message": f"Failed to load LaunchAgent: {result.stderr.strip() or 'unknown error'}",
        }

    return {
        "status": "ok",
        "platform": "macos",
        "plist_path": str(MACOS_AGENT_PATH),
        "log_dir": str(LOG_DIR),
        "message": "LaunchAgent installed. Drew's YT Scheduler will start on login and auto-restart if it crashes.",
        "commands": {
            "status": f"launchctl print gui/{os.getuid()}/{MACOS_AGENT_LABEL}",
            "stop": f"launchctl kill SIGTERM gui/{os.getuid()}/{MACOS_AGENT_LABEL}",
            "uninstall": f"launchctl bootout gui/{os.getuid()} {MACOS_AGENT_PATH}",
            "logs": f"tail -f {LOG_DIR}/stderr.log",
        },
    }


def _install_linux() -> dict:
    """Install Linux systemd user service."""
    unit = _generate_systemd_unit()

    LINUX_SERVICE_DIR.mkdir(parents=True, exist_ok=True)

    # Tear down legacy unit if present.
    if LEGACY_LINUX_SERVICE_PATH.exists():
        subprocess.run(
            ["systemctl", "--user", "disable", "--now", LEGACY_LINUX_SERVICE_NAME],
            capture_output=True,
        )
        try:
            LEGACY_LINUX_SERVICE_PATH.unlink()
        except OSError:
            pass

    LINUX_SERVICE_PATH.write_text(unit)

    # Reload systemd and enable
    reload_result = subprocess.run(
        ["systemctl", "--user", "daemon-reload"],
        capture_output=True,
        text=True,
    )
    if reload_result.returncode != 0:
        return {
            "status": "error",
            "platform": "linux",
            "service_path": str(LINUX_SERVICE_PATH),
            "message": f"Failed to reload systemd: {reload_result.stderr.strip() or 'unknown error'}",
        }

    result = subprocess.run(
        ["systemctl", "--user", "enable", "--now", LINUX_SERVICE_NAME],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        return {
            "status": "error",
            "platform": "linux",
            "service_path": str(LINUX_SERVICE_PATH),
            "message": f"Failed to enable service: {result.stderr.strip() or 'unknown error'}",
        }

    return {
        "status": "ok",
        "platform": "linux",
        "service_path": str(LINUX_SERVICE_PATH),
        "message": "systemd user service installed and started. Will auto-start on login.",
        "commands": {
            "status": f"systemctl --user status {LINUX_SERVICE_NAME}",
            "stop": f"systemctl --user stop {LINUX_SERVICE_NAME}",
            "restart": f"systemctl --user restart {LINUX_SERVICE_NAME}",
            "logs": f"journalctl --user -u {LINUX_SERVICE_NAME} -f",
            "uninstall": f"systemctl --user disable --now {LINUX_SERVICE_NAME}",
        },
    }


def uninstall_service() -> dict:
    """Uninstall the background service.

    Removes both the current and any legacy plist/unit so a stale pre-rename
    install doesn't keep auto-launching the old binary.
    """
    system = platform.system()

    if system == "Darwin":
        removed = False
        for label, plist_path in (
            (LEGACY_MACOS_AGENT_LABEL, LEGACY_MACOS_AGENT_PATH),
            (MACOS_AGENT_LABEL, MACOS_AGENT_PATH),
        ):
            if plist_path.exists():
                subprocess.run(
                    ["launchctl", "bootout", f"gui/{os.getuid()}", str(plist_path)],
                    capture_output=True,
                )
                plist_path.unlink()
                removed = True
                logger.info("Removed LaunchAgent %s", label)
        return {"status": "ok", "message": "LaunchAgent removed" if removed else "No LaunchAgent found"}

    elif system == "Linux":
        removed = False
        for service_name, service_path in (
            (LEGACY_LINUX_SERVICE_NAME, LEGACY_LINUX_SERVICE_PATH),
            (LINUX_SERVICE_NAME, LINUX_SERVICE_PATH),
        ):
            if service_path.exists():
                subprocess.run(
                    ["systemctl", "--user", "disable", "--now", service_name],
                    capture_output=True,
                )
                service_path.unlink()
                removed = True
        if removed:
            subprocess.run(["systemctl", "--user", "daemon-reload"], capture_output=True)
        return {"status": "ok", "message": "systemd service removed" if removed else "No systemd service found"}

    return {"status": "unsupported"}


def get_service_status() -> dict:
    """Check if the background service is installed and running."""
    system = platform.system()

    if system == "Darwin":
        installed = MACOS_AGENT_PATH.exists()
        running = False
        if installed:
            result = subprocess.run(
                ["launchctl", "print", f"gui/{os.getuid()}/{MACOS_AGENT_LABEL}"],
                capture_output=True,
                text=True,
            )
            running = result.returncode == 0

        return {
            "platform": "macos",
            "installed": installed,
            "running": running,
            "plist_path": str(MACOS_AGENT_PATH) if installed else None,
        }

    elif system == "Linux":
        installed = LINUX_SERVICE_PATH.exists()
        running = False
        if installed:
            result = subprocess.run(
                ["systemctl", "--user", "is-active", LINUX_SERVICE_NAME],
                capture_output=True,
                text=True,
            )
            running = result.stdout.strip() == "active"

        return {
            "platform": "linux",
            "installed": installed,
            "running": running,
            "service_path": str(LINUX_SERVICE_PATH) if installed else None,
        }

    return {"platform": system.lower(), "installed": False, "running": False}
