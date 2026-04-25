"""Background-service installation for non-macOS hosts.

On macOS, the .app bundle ships an embedded launchd plist that's registered
through ``SMAppService`` from the Swift wrapper — see ``macos/build.sh`` and
``macos/DrewsYTScheduler/.../LaunchAgentController.swift``. The CLI does not
expose an install path on Darwin to avoid two competing service registrations.

On Linux we still write a systemd user unit so headless deployments can run
``yt-scheduler install`` without external orchestration.
"""

from __future__ import annotations

import logging
import platform
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from textwrap import dedent

from yt_scheduler.config import LOG_DIR

logger = logging.getLogger(__name__)


LINUX_SERVICE_NAME = "drews-yt-scheduler"
LINUX_SERVICE_DIR = Path.home() / ".config" / "systemd" / "user"
LINUX_SERVICE_PATH = LINUX_SERVICE_DIR / f"{LINUX_SERVICE_NAME}.service"


_MACOS_INSTRUCTION = (
    "On macOS the background server is managed by the Drew's YT Scheduler "
    ".app via SMAppService. Open the app and use Settings → Background "
    "service to install or restart it."
)


def _find_executable() -> list[str]:
    """Return argv to launch the server. Falls back to the current Python +
    module if no console script is on PATH."""
    exe = shutil.which("yt-scheduler")
    if exe:
        return [exe]
    return [sys.executable, "-m", "yt_scheduler.main"]


def _generate_systemd_unit() -> str:
    parts = _find_executable()
    LOG_DIR.mkdir(parents=True, exist_ok=True)
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
    """Install the background service for the current platform."""
    system = platform.system()

    if system == "Darwin":
        return {"status": "unsupported", "platform": "macos", "message": _MACOS_INSTRUCTION}
    if system == "Linux":
        return _install_linux()
    return {
        "status": "unsupported",
        "platform": system.lower(),
        "message": f"Automatic install not supported on {system}.",
    }


def _install_linux() -> dict:
    unit = _generate_systemd_unit()
    LINUX_SERVICE_DIR.mkdir(parents=True, exist_ok=True)
    LINUX_SERVICE_PATH.write_text(unit)

    reload_result = subprocess.run(
        ["systemctl", "--user", "daemon-reload"], capture_output=True, text=True
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
        capture_output=True, text=True,
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
        "message": "systemd user service installed and started.",
        "commands": {
            "status": f"systemctl --user status {LINUX_SERVICE_NAME}",
            "stop": f"systemctl --user stop {LINUX_SERVICE_NAME}",
            "restart": f"systemctl --user restart {LINUX_SERVICE_NAME}",
            "logs": f"journalctl --user -u {LINUX_SERVICE_NAME} -f",
            "uninstall": f"systemctl --user disable --now {LINUX_SERVICE_NAME}",
        },
    }


def uninstall_service() -> dict:
    system = platform.system()
    if system == "Darwin":
        return {"status": "unsupported", "platform": "macos", "message": _MACOS_INSTRUCTION}
    if system == "Linux":
        if not LINUX_SERVICE_PATH.exists():
            return {"status": "ok", "message": "No systemd service found"}
        subprocess.run(
            ["systemctl", "--user", "disable", "--now", LINUX_SERVICE_NAME],
            capture_output=True,
        )
        LINUX_SERVICE_PATH.unlink()
        subprocess.run(["systemctl", "--user", "daemon-reload"], capture_output=True)
        return {"status": "ok", "message": "systemd service removed"}
    return {"status": "unsupported"}


def get_service_status() -> dict:
    system = platform.system()
    if system == "Darwin":
        return {"platform": "macos", "installed": False, "running": False, "message": _MACOS_INSTRUCTION}
    if system == "Linux":
        installed = LINUX_SERVICE_PATH.exists()
        running = False
        if installed:
            result = subprocess.run(
                ["systemctl", "--user", "is-active", LINUX_SERVICE_NAME],
                capture_output=True, text=True,
            )
            running = result.stdout.strip() == "active"
        return {
            "platform": "linux",
            "installed": installed,
            "running": running,
            "service_path": str(LINUX_SERVICE_PATH) if installed else None,
        }
    return {"platform": system.lower(), "installed": False, "running": False}
