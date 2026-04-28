#!/bin/bash
set -euo pipefail

# ============================================================================
# Drew's YT Scheduler — macOS App Build Script
#
# Produces a self-contained .app bundle with:
#   - Swift window-based app (no menubar-only mode)
#   - Embedded relocatable Python runtime (python-build-standalone)
#   - The yt_scheduler Python package installed into that runtime
#   - Embedded LaunchAgent plist for SMAppService
#   - Stamped build identity (Info.plist + _build_info.py inside the package)
#   - Code-signed with an auto-detected Developer ID, optionally notarized
#
# Usage:
#   ./build.sh --debug    # local dev — signs if a cert is found, no DMG, no notarize
#   ./build.sh --release  # signs + notarizes .app, then builds DMG, then notarizes DMG
# ============================================================================

# This script is macOS-only — Swift, codesign, security CLI, the standalone
# Python tarball, and SMAppService-targeted launch agents are all Darwin.
HOST_OS="$(uname -s)"
if [ "$HOST_OS" != "Darwin" ]; then
    echo "ERROR: macos/build.sh only runs on macOS (current: $HOST_OS)."
    exit 1
fi

# --- argument parsing -------------------------------------------------------

BUILD_KIND=""
FORCE_NO_SIGN=false

for arg in "$@"; do
    case $arg in
        --debug)       BUILD_KIND="debug" ;;
        --release)     BUILD_KIND="release" ;;
        --no-sign)     FORCE_NO_SIGN=true ;;
        *)
            echo "ERROR: unknown argument: $arg"
            echo "Usage: $0 --debug | --release [--no-sign]"
            exit 1
            ;;
    esac
done

if [ -z "$BUILD_KIND" ]; then
    echo "ERROR: must specify --debug or --release"
    echo "Usage: $0 --debug | --release [--no-sign]"
    exit 1
fi

# Notarization is implied by --release. Debug never notarizes.
if [ "$BUILD_KIND" = "release" ]; then
    NOTARIZE=true
else
    NOTARIZE=false
fi

# --- paths ------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"
APP_NAME="Drew's YT Scheduler"
APP_BUNDLE="$BUILD_DIR/$APP_NAME.app"
SWIFT_PACKAGE_DIR="$SCRIPT_DIR/DrewsYTScheduler"
SWIFT_TARGET="DrewsYTScheduler"
PYTHON_VERSION="3.12"
PYTHON_FULL_VERSION="3.12.8"
BUNDLE_ID="com.nuclearcyborg.drews-socialmedia-scheduler"
LAUNCH_AGENT_LABEL="$BUNDLE_ID"
NOTARIZE_PROFILE="${NOTARIZE_PROFILE:-YTScheduler}"

# --- build identity ---------------------------------------------------------

# VERSION read from pyproject.toml (single source of truth).
VERSION=$(grep -E '^version' "$PROJECT_DIR/pyproject.toml" | head -1 | sed -E 's/^version[^"]*"([^"]+)".*/\1/')
if [ -z "$VERSION" ]; then
    echo "ERROR: could not parse version from pyproject.toml"
    exit 1
fi
BUILD_NUMBER=$(date +%s)
BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
BUILD_ID=$(uuidgen)

# --- signing ----------------------------------------------------------------

DEVELOPER_ID="${DEVELOPER_ID:-}"
if [ -z "$DEVELOPER_ID" ]; then
    DEVELOPER_ID=$(security find-identity -v -p codesigning 2>/dev/null \
        | grep "Developer ID Application" \
        | head -1 \
        | sed 's/.*"\(.*\)".*/\1/' \
        || true)
fi

if [ "$FORCE_NO_SIGN" = true ]; then
    SIGN=false
    DEVELOPER_ID=""
elif [ -n "$DEVELOPER_ID" ]; then
    SIGN=true
else
    SIGN=false
fi

if [ "$BUILD_KIND" = "release" ] && [ "$SIGN" != true ]; then
    echo "ERROR: --release requires a Developer ID Application cert in Keychain."
    echo "       Run: security find-identity -v -p codesigning"
    exit 1
fi

# Notarize-no-sign cannot happen: --release requires sign (above) and only
# --release sets NOTARIZE=true.

# --- summary ----------------------------------------------------------------

echo "=== Drew's YT Scheduler Build ==="
echo "Kind:       $BUILD_KIND"
echo "Version:    $VERSION (#$BUILD_NUMBER)"
echo "Build ID:   $BUILD_ID"
echo "Build dir:  $BUILD_DIR"
echo "Sign:       $([ "$SIGN" = true ] && echo "$DEVELOPER_ID" || echo "no")"
echo "Notarize:   $NOTARIZE"
echo

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# ============================================================================
# Step 1: Build the Swift app
# ============================================================================
echo "=== Step 1: Building Swift app ==="

cd "$SWIFT_PACKAGE_DIR"
swift build -c release --arch arm64 --arch x86_64 2>&1 | tail -5
SWIFT_BINARY=$(swift build -c release --arch arm64 --arch x86_64 --show-bin-path)/$SWIFT_TARGET
if [ ! -f "$SWIFT_BINARY" ]; then
    echo "ERROR: Swift build failed"
    exit 1
fi
cd "$SCRIPT_DIR"

# ============================================================================
# Step 2: Bundle layout + Info.plist + LaunchAgent plist
# ============================================================================
echo
echo "=== Step 2: Creating .app bundle ==="

mkdir -p "$APP_BUNDLE/Contents/MacOS"
mkdir -p "$APP_BUNDLE/Contents/Resources"
mkdir -p "$APP_BUNDLE/Contents/Library/LaunchAgents"

cp "$SWIFT_BINARY" "$APP_BUNDLE/Contents/MacOS/$SWIFT_TARGET"

# App icon: build a proper multi-resolution AppIcon.icns from the
# 1024x1024 source PNG. Without this the .app shows the generic macOS
# placeholder icon in Finder, the Dock, and the menu bar.
ICON_SOURCE="$PROJECT_DIR/macos/Resources/AppIcon.png"
if [ -f "$ICON_SOURCE" ]; then
    ICONSET_TMP="$(mktemp -d)/AppIcon.iconset"
    mkdir -p "$ICONSET_TMP"
    # iconutil expects the standard 10-image set: 16, 32, 64, 128, 256,
    # 512, 1024px in @1x / @2x pairs. sips downscales from the source.
    for spec in \
        "16:icon_16x16.png" \
        "32:icon_16x16@2x.png" \
        "32:icon_32x32.png" \
        "64:icon_32x32@2x.png" \
        "128:icon_128x128.png" \
        "256:icon_128x128@2x.png" \
        "256:icon_256x256.png" \
        "512:icon_256x256@2x.png" \
        "512:icon_512x512.png" \
        "1024:icon_512x512@2x.png"; do
        size="${spec%%:*}"
        name="${spec##*:}"
        sips -z "$size" "$size" "$ICON_SOURCE" --out "$ICONSET_TMP/$name" >/dev/null
    done
    iconutil -c icns "$ICONSET_TMP" -o "$APP_BUNDLE/Contents/Resources/AppIcon.icns"
    rm -rf "$(dirname "$ICONSET_TMP")"
    echo "App icon installed: Contents/Resources/AppIcon.icns"
else
    echo "WARN: $ICON_SOURCE not found — bundle will use the generic icon"
fi

# Info.plist — note LSUIElement=false so the app shows in the Dock and has
# real windows. The menu-bar item is opt-in via Settings + SMAppService login
# items, NOT a side-effect of LSUIElement.
cat > "$APP_BUNDLE/Contents/Info.plist" << PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key>
    <string>Drew's YT Scheduler</string>
    <key>CFBundleDisplayName</key>
    <string>Drew's YT Scheduler</string>
    <key>CFBundleIdentifier</key>
    <string>$BUNDLE_ID</string>
    <key>CFBundleVersion</key>
    <string>$BUILD_NUMBER</string>
    <key>CFBundleShortVersionString</key>
    <string>$VERSION</string>
    <key>CFBundleExecutable</key>
    <string>$SWIFT_TARGET</string>
    <key>CFBundleIconFile</key>
    <string>AppIcon</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>LSMinimumSystemVersion</key>
    <string>14.0</string>
    <key>LSUIElement</key>
    <false/>
    <key>NSHighResolutionCapable</key>
    <true/>
    <key>NSSupportsAutomaticTermination</key>
    <false/>
    <key>NSSupportsSuddenTermination</key>
    <false/>
    <key>NSSpeechRecognitionUsageDescription</key>
    <string>Drew's YT Scheduler uses on-device Speech Recognition to transcribe your videos. The audio stays on this Mac.</string>
    <key>NSMicrophoneUsageDescription</key>
    <string>Drew's YT Scheduler uses Speech Recognition on local video files; macOS classifies this as microphone access.</string>
    <key>DYSBuildKind</key>
    <string>$BUILD_KIND</string>
    <key>DYSBuildId</key>
    <string>$BUILD_ID</string>
    <key>DYSBuildDate</key>
    <string>$BUILD_DATE</string>
    <key>DYSBuildNumber</key>
    <string>$BUILD_NUMBER</string>
    <key>DYSLaunchAgentLabel</key>
    <string>$LAUNCH_AGENT_LABEL</string>
</dict>
</plist>
PLIST

# Embedded LaunchAgent plist that SMAppService.agent(plistName:) will register.
# Paths in BundleProgram / ProgramArguments[0] are relative to the .app bundle
# root (`<App>.app/`), so we always launch the matching embedded Python.
#
# StandardErrorPath is /tmp/<bundle>.boot.log: an ABSOLUTE path that captures
# anything launchd or Python writes BEFORE the Python entry point reaches
# ``_redirect_stdio_to_log()`` and dups stdout/stderr into the user's
# ``~/Library/Logs/<bundle>/server.log``. This catches:
#   * launchd spawn failures (exit code 78 EX_CONFIG, LWCR mismatches)
#   * Python ImportError / SyntaxError before main() runs
#   * Any traceback during config-time initialisation
# launchd doesn't expand ``~`` or ``$HOME`` in StandardOutPath, which is why
# we anchor in ``/tmp`` instead of the user's Logs directory.
LAUNCH_AGENT_PLIST="$APP_BUNDLE/Contents/Library/LaunchAgents/$LAUNCH_AGENT_LABEL.plist"
cat > "$LAUNCH_AGENT_PLIST" << AGENTPLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$LAUNCH_AGENT_LABEL</string>
    <key>BundleProgram</key>
    <string>Contents/Resources/python/bin/yt_scheduler_launcher.sh</string>
    <key>ProgramArguments</key>
    <array>
        <string>Contents/Resources/python/bin/yt_scheduler_launcher.sh</string>
        <string>-m</string>
        <string>yt_scheduler.main</string>
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
    <string>/tmp/$LAUNCH_AGENT_LABEL.boot.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/$LAUNCH_AGENT_LABEL.boot.log</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
        <key>DYS_REDIRECT_LOGS</key>
        <string>1</string>
    </dict>
    <key>ProcessType</key>
    <string>Background</string>
</dict>
</plist>
AGENTPLIST

# ============================================================================
# Step 3: Embed relocatable Python runtime
# ============================================================================
echo
echo "=== Step 3: Embedding Python runtime ==="

PYTHON_EMBED_DIR="$APP_BUNDLE/Contents/Resources/python"
mkdir -p "$PYTHON_EMBED_DIR"

# python-build-standalone (indygreg) — relocatable, signed-friendly, no system deps.
PYTHON_STANDALONE_URL="https://github.com/indygreg/python-build-standalone/releases/download/20241219/cpython-${PYTHON_FULL_VERSION}+20241219-aarch64-apple-darwin-install_only_stripped.tar.gz"
PYTHON_CACHE="$BUILD_DIR/../python-standalone.tar.gz"
PYTHON_SHA256=""

if [ ! -f "$PYTHON_CACHE" ]; then
    echo "Downloading standalone Python ${PYTHON_FULL_VERSION}..."
    curl -L -o "$PYTHON_CACHE" "$PYTHON_STANDALONE_URL"
fi

if [ -n "$PYTHON_SHA256" ]; then
    ACTUAL_SHA256=$(shasum -a 256 "$PYTHON_CACHE" | awk '{print $1}')
    if [ "$ACTUAL_SHA256" != "$PYTHON_SHA256" ]; then
        echo "ERROR: SHA256 mismatch (expected $PYTHON_SHA256, got $ACTUAL_SHA256)"
        exit 1
    fi
fi

tar xzf "$PYTHON_CACHE" -C "$PYTHON_EMBED_DIR" --strip-components=1
if [ ! -f "$PYTHON_EMBED_DIR/bin/python3" ]; then
    echo "ERROR: Python extraction failed"
    exit 1
fi

EMBED_PYTHON="$PYTHON_EMBED_DIR/bin/python3"
"$EMBED_PYTHON" --version

# Wrapper shell script — necessary because launchd invokes BundleProgram
# with cwd=/ and passes the (relative) ProgramArguments[0] verbatim as argv[0].
# Indygreg's relocatable Python needs an *absolute* argv[0] to discover its
# sibling lib/python3.12/encodings module; given a relative path it falls back
# to the build-time prefix /install which doesn't exist on the user's disk.
# This wrapper resolves to its own absolute directory and execs Python from it.
LAUNCHER="$PYTHON_EMBED_DIR/bin/yt_scheduler_launcher.sh"
cat > "$LAUNCHER" << 'LAUNCHER_EOF'
#!/bin/bash
SELF_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "$SELF_DIR/python3.12" "$@"
LAUNCHER_EOF
chmod +x "$LAUNCHER"

# ============================================================================
# Step 4: Install the yt_scheduler package + dependencies
# ============================================================================
echo
echo "=== Step 4: Installing dependencies + package ==="

"$EMBED_PYTHON" -m pip install --upgrade pip --quiet
"$EMBED_PYTHON" -m pip install --quiet --no-warn-script-location \
    -r "$PROJECT_DIR/requirements-app.txt"

# Install the package itself (no deps — we already pinned them above).
"$EMBED_PYTHON" -m pip install --quiet --no-deps --no-warn-script-location \
    "$PROJECT_DIR"

# Locate the installed package inside the embedded site-packages so we can
# drop _build_info.py + _migrations/ alongside it.
SITE_PACKAGES=$("$EMBED_PYTHON" -c "import yt_scheduler, os; print(os.path.dirname(yt_scheduler.__file__))")
if [ ! -d "$SITE_PACKAGES" ]; then
    echo "ERROR: could not locate installed yt_scheduler package"
    exit 1
fi
echo "Installed package at: $SITE_PACKAGES"

# Stamp build identity into the package — build_info.py imports this when present.
cat > "$SITE_PACKAGES/_build_info.py" << BUILDINFO
# Generated by macos/build.sh — do not edit.
BUILD_KIND = "$BUILD_KIND"
VERSION = "$VERSION"
BUILD_NUMBER = "$BUILD_NUMBER"
BUILD_DATE = "$BUILD_DATE"
BUILD_ID = "$BUILD_ID"
BUILDINFO

# Ship the SQL migrations inside the package so the runner finds them.
mkdir -p "$SITE_PACKAGES/_migrations"
cp "$PROJECT_DIR/migrations/"*.sql "$SITE_PACKAGES/_migrations/"

# ============================================================================
# Step 5: Strip cruft (release only — leave debug builds heavier so we can
# attach a debugger / inspect bytecode without re-bundling)
# ============================================================================
if [ "$BUILD_KIND" = "release" ]; then
    echo
    echo "=== Step 5: Stripping unused files ==="
    find "$PYTHON_EMBED_DIR" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find "$PYTHON_EMBED_DIR" -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true
    find "$PYTHON_EMBED_DIR" -type d -name "test" -exec rm -rf {} + 2>/dev/null || true
    find "$PYTHON_EMBED_DIR" -name "*.pyc" -delete 2>/dev/null || true
    rm -rf "$PYTHON_EMBED_DIR/bin/pip"* 2>/dev/null || true
    rm -rf "$PYTHON_EMBED_DIR/lib/python${PYTHON_VERSION}/ensurepip" 2>/dev/null || true
    rm -rf "$PYTHON_EMBED_DIR/lib/python${PYTHON_VERSION}/tkinter" 2>/dev/null || true
    rm -rf "$PYTHON_EMBED_DIR/lib/python${PYTHON_VERSION}/turtle"* 2>/dev/null || true
    rm -rf "$PYTHON_EMBED_DIR/lib/python${PYTHON_VERSION}/idlelib" 2>/dev/null || true
    rm -rf "$PYTHON_EMBED_DIR/lib/python${PYTHON_VERSION}/lib2to3" 2>/dev/null || true
    rm -rf "$PYTHON_EMBED_DIR/share" 2>/dev/null || true
    rm -rf "$PYTHON_EMBED_DIR/include" 2>/dev/null || true
fi

BUNDLE_SIZE=$(du -sh "$APP_BUNDLE" | cut -f1)
echo "Bundle size: $BUNDLE_SIZE"

# ============================================================================
# Step 6: Code signing
# ============================================================================
if [ "$SIGN" = true ]; then
    echo
    echo "=== Step 6: Code signing ==="
    echo "Identity: $DEVELOPER_ID"

    # Sign innermost Mach-O files first. ``--identifier`` gives each library
    # a sub-identifier of the bundle so SMAppService's LWCR check is satisfied.
    find "$APP_BUNDLE" \( -name "*.so" -o -name "*.dylib" \) -print0 | while IFS= read -r -d '' lib; do
        rel="${lib#$APP_BUNDLE/Contents/}"
        sub_id="$BUNDLE_ID.$(basename "$lib" | tr -d ' ')"
        codesign --force --sign "$DEVELOPER_ID" --options runtime --timestamp \
            --identifier "$sub_id" \
            "$lib" 2>/dev/null || echo "  warn: could not sign $rel"
    done

    # The embedded Python interpreter is the SMAppService helper executable.
    # It MUST have an identifier that is a child of the .app's bundle
    # identifier, otherwise launchd refuses to spawn the launch agent
    # (exit code 78 EX_CONFIG, "needs LWCR update").
    codesign --force --sign "$DEVELOPER_ID" --options runtime --timestamp \
        --identifier "$BUNDLE_ID.python3" \
        --entitlements "$SCRIPT_DIR/Entitlements.plist" \
        "$EMBED_PYTHON"

    # Sign python3.12 too — the symlink target is what actually executes.
    codesign --force --sign "$DEVELOPER_ID" --options runtime --timestamp \
        --identifier "$BUNDLE_ID.python3" \
        --entitlements "$SCRIPT_DIR/Entitlements.plist" \
        "$PYTHON_EMBED_DIR/bin/python3.12"

    # Sign the launcher shim so the .app's outer seal stays valid.
    codesign --force --sign "$DEVELOPER_ID" --options runtime --timestamp \
        --identifier "$BUNDLE_ID.launcher" \
        "$LAUNCHER"

    # Sign the Swift binary explicitly (no --deep on the .app below).
    codesign --force --sign "$DEVELOPER_ID" --options runtime --timestamp \
        "$APP_BUNDLE/Contents/MacOS/$SWIFT_TARGET"

    # Outer .app sign — NO --deep. ``--deep`` would recursively re-sign every
    # nested executable with the .app's identifier (overwriting the
    # explicit ``--identifier $BUNDLE_ID.python3`` we just set above).
    codesign --force --sign "$DEVELOPER_ID" --options runtime --timestamp \
        --entitlements "$SCRIPT_DIR/Entitlements.plist" \
        "$APP_BUNDLE"

    codesign --verify --deep --strict "$APP_BUNDLE"
    echo "Signature OK"
fi

# ============================================================================
# Step 7: Notarize the .app, then build the DMG, then notarize the DMG
# (release-only; debug stops here)
# ============================================================================
if [ "$BUILD_KIND" = "debug" ]; then
    echo
    echo "=== Build Complete (debug) ==="
    echo "App: $APP_BUNDLE"
    echo "Run: open \"$APP_BUNDLE\""
    exit 0
fi

# Always build a DMG for release.
DMG_PATH="$BUILD_DIR/$APP_NAME.dmg"

if [ "$NOTARIZE" = true ]; then
    echo
    echo "=== Step 7a: Notarizing .app ==="
    APP_ZIP="$BUILD_DIR/$APP_NAME.zip"
    /usr/bin/ditto -c -k --keepParent "$APP_BUNDLE" "$APP_ZIP"
    xcrun notarytool submit "$APP_ZIP" --keychain-profile "$NOTARIZE_PROFILE" --wait
    xcrun stapler staple "$APP_BUNDLE"
    rm -f "$APP_ZIP"
fi

echo
echo "=== Step 7b: Building DMG ==="
hdiutil create -volname "$APP_NAME" -srcfolder "$APP_BUNDLE" -ov -format UDZO "$DMG_PATH"

if [ "$NOTARIZE" = true ]; then
    echo
    echo "=== Step 7c: Notarizing DMG ==="
    xcrun notarytool submit "$DMG_PATH" --keychain-profile "$NOTARIZE_PROFILE" --wait
    xcrun stapler staple "$DMG_PATH"
fi

echo
echo "=== Build Complete (release) ==="
echo "App: $APP_BUNDLE"
echo "DMG: $DMG_PATH"
[ "$NOTARIZE" = true ] && echo "Notarized: yes"
