#!/bin/bash
set -euo pipefail

# ============================================================================
# Drew's YT Scheduler — macOS App Build Script
#
# Creates a fully self-contained .app bundle with:
#   - Swift menubar app
#   - Embedded Python runtime (python.org framework build)
#   - All Python dependencies pre-installed
#   - Your Python source code
#   - Code signed & notarized
#
# Prerequisites:
#   - Xcode (with swift CLI)
#   - An Apple Developer ID certificate in your Keychain
#   - Python 3.12+ on your build machine (only used during build)
#
# Usage:
#   ./build.sh                    # Build only
#   ./build.sh --sign             # Build + code sign
#   ./build.sh --sign --notarize  # Build + sign + notarize
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR/.."
BUILD_DIR="$SCRIPT_DIR/build"
APP_NAME="Drew's YT Scheduler"
APP_BUNDLE="$BUILD_DIR/$APP_NAME.app"
SWIFT_PACKAGE_DIR="$SCRIPT_DIR/DrewsYTScheduler"
SWIFT_TARGET="DrewsYTScheduler"
PYTHON_VERSION="3.12"
PYTHON_FULL_VERSION="3.12.8"

# Signing config — change these to your values
DEVELOPER_ID="${DEVELOPER_ID:-}"
TEAM_ID="${TEAM_ID:-}"
BUNDLE_ID="com.nuclearcyborg.drews-socialmedia-scheduler"
NOTARIZE_PROFILE="${NOTARIZE_PROFILE:-YTScheduler}"  # stored via `xcrun notarytool store-credentials`

SIGN=false
NOTARIZE=false

for arg in "$@"; do
    case $arg in
        --sign) SIGN=true ;;
        --notarize) NOTARIZE=true; SIGN=true ;;
    esac
done

echo "=== Drew's YT Scheduler Build ==="
echo "Building in: $BUILD_DIR"
echo "Sign: $SIGN | Notarize: $NOTARIZE"

# Clean
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# ============================================================================
# Step 1: Build the Swift menubar app
# ============================================================================
echo ""
echo "=== Step 1: Building Swift app ==="

cd "$SWIFT_PACKAGE_DIR"
swift build -c release --arch arm64 --arch x86_64 2>&1 | tail -5

SWIFT_BINARY=$(swift build -c release --arch arm64 --arch x86_64 --show-bin-path)/$SWIFT_TARGET

if [ ! -f "$SWIFT_BINARY" ]; then
    echo "ERROR: Swift build failed"
    exit 1
fi

echo "Swift binary: $SWIFT_BINARY"

# ============================================================================
# Step 2: Create the app bundle structure
# ============================================================================
echo ""
echo "=== Step 2: Creating app bundle ==="

mkdir -p "$APP_BUNDLE/Contents/MacOS"
mkdir -p "$APP_BUNDLE/Contents/Resources"

cp "$SWIFT_BINARY" "$APP_BUNDLE/Contents/MacOS/$SWIFT_TARGET"

# Info.plist
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
    <string>1</string>
    <key>CFBundleShortVersionString</key>
    <string>0.1.0</string>
    <key>CFBundleExecutable</key>
    <string>$SWIFT_TARGET</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>LSMinimumSystemVersion</key>
    <string>14.0</string>
    <key>LSUIElement</key>
    <true/>
    <key>NSHighResolutionCapable</key>
    <true/>
    <key>NSSupportsAutomaticTermination</key>
    <false/>
    <key>NSSupportsSuddenTermination</key>
    <false/>
</dict>
</plist>
PLIST

# ============================================================================
# Step 3: Embed Python runtime
# ============================================================================
echo ""
echo "=== Step 3: Embedding Python runtime ==="

PYTHON_EMBED_DIR="$APP_BUNDLE/Contents/Resources/python"
mkdir -p "$PYTHON_EMBED_DIR"

# Use the python.org standalone build (relocatable)
# Download if not cached
PYTHON_STANDALONE_URL="https://github.com/indygreg/python-build-standalone/releases/download/20241219/cpython-${PYTHON_FULL_VERSION}+20241219-aarch64-apple-darwin-install_only_stripped.tar.gz"
PYTHON_CACHE="$BUILD_DIR/python-standalone.tar.gz"
# SHA256 of cpython-3.12.8+20241219-aarch64-apple-darwin-install_only_stripped.tar.gz
# To update: download the file, run `shasum -a 256 <file>`, and paste the hash here.
PYTHON_SHA256=""

if [ ! -f "$PYTHON_CACHE" ]; then
    echo "Downloading standalone Python ${PYTHON_FULL_VERSION}..."
    curl -L -o "$PYTHON_CACHE" "$PYTHON_STANDALONE_URL"
fi

if [ -n "$PYTHON_SHA256" ]; then
    echo "Verifying download checksum..."
    if command -v shasum &>/dev/null; then
        ACTUAL_SHA256=$(shasum -a 256 "$PYTHON_CACHE" | awk '{print $1}')
    elif command -v sha256sum &>/dev/null; then
        ACTUAL_SHA256=$(sha256sum "$PYTHON_CACHE" | awk '{print $1}')
    else
        echo "WARNING: Neither shasum nor sha256sum found — skipping checksum verification"
        ACTUAL_SHA256="$PYTHON_SHA256"
    fi

    if [ "$ACTUAL_SHA256" != "$PYTHON_SHA256" ]; then
        echo "ERROR: SHA256 checksum mismatch!"
        echo "  Expected: $PYTHON_SHA256"
        echo "  Got:      $ACTUAL_SHA256"
        echo "The downloaded file may be corrupted or tampered with."
        echo "Delete $PYTHON_CACHE and try again, or update PYTHON_SHA256 if the release changed."
        exit 1
    fi
    echo "Checksum verified."
else
    echo "WARNING: PYTHON_SHA256 is not set — skipping checksum verification."
    echo "  To enable verification, run: shasum -a 256 $PYTHON_CACHE"
    echo "  Then set PYTHON_SHA256 in this script."
fi

echo "Extracting Python..."
tar xzf "$PYTHON_CACHE" -C "$PYTHON_EMBED_DIR" --strip-components=1

# Verify
if [ ! -f "$PYTHON_EMBED_DIR/bin/python3" ]; then
    echo "ERROR: Python extraction failed"
    exit 1
fi

echo "Python embedded at: $PYTHON_EMBED_DIR"
"$PYTHON_EMBED_DIR/bin/python3" --version

# ============================================================================
# Step 4: Install Python dependencies into the embedded Python
# ============================================================================
echo ""
echo "=== Step 4: Installing Python dependencies ==="

"$PYTHON_EMBED_DIR/bin/python3" -m pip install --upgrade pip --quiet
"$PYTHON_EMBED_DIR/bin/python3" -m pip install \
    --quiet \
    --no-warn-script-location \
    -r "$PROJECT_DIR/requirements-app.txt"

echo "Dependencies installed."

# ============================================================================
# Step 5: Copy Python source code
# ============================================================================
echo ""
echo "=== Step 5: Copying Python source ==="

PYTHON_SRC_DIR="$APP_BUNDLE/Contents/Resources/yt_scheduler_src"
mkdir -p "$PYTHON_SRC_DIR"

# Copy the source as a proper package
cp -R "$PROJECT_DIR/src/yt_scheduler" "$PYTHON_SRC_DIR/"

# Bundle the migration .sql files inside the package so the runner finds them
# at runtime regardless of where Python imports it from.
cp -R "$PROJECT_DIR/migrations" "$PYTHON_SRC_DIR/yt_scheduler/_migrations"

echo "Source copied to: $PYTHON_SRC_DIR"

# ============================================================================
# Step 6: Strip unnecessary files to reduce bundle size
# ============================================================================
echo ""
echo "=== Step 6: Stripping unnecessary files ==="

# Remove test files, __pycache__, .pyc files in source
find "$PYTHON_EMBED_DIR" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find "$PYTHON_EMBED_DIR" -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true
find "$PYTHON_EMBED_DIR" -type d -name "test" -exec rm -rf {} + 2>/dev/null || true
find "$PYTHON_EMBED_DIR" -name "*.pyc" -delete 2>/dev/null || true

# Remove pip (not needed at runtime)
rm -rf "$PYTHON_EMBED_DIR/bin/pip"* 2>/dev/null || true
rm -rf "$PYTHON_EMBED_DIR/lib/python${PYTHON_VERSION}/ensurepip" 2>/dev/null || true

# Remove things we definitely don't need
rm -rf "$PYTHON_EMBED_DIR/lib/python${PYTHON_VERSION}/tkinter" 2>/dev/null || true
rm -rf "$PYTHON_EMBED_DIR/lib/python${PYTHON_VERSION}/turtle*" 2>/dev/null || true
rm -rf "$PYTHON_EMBED_DIR/lib/python${PYTHON_VERSION}/idlelib" 2>/dev/null || true
rm -rf "$PYTHON_EMBED_DIR/lib/python${PYTHON_VERSION}/lib2to3" 2>/dev/null || true
rm -rf "$PYTHON_EMBED_DIR/share" 2>/dev/null || true
rm -rf "$PYTHON_EMBED_DIR/include" 2>/dev/null || true

BUNDLE_SIZE=$(du -sh "$APP_BUNDLE" | cut -f1)
echo "Bundle size after stripping: $BUNDLE_SIZE"

# ============================================================================
# Step 7: Code sign (if requested)
# ============================================================================
if [ "$SIGN" = true ]; then
    echo ""
    echo "=== Step 7: Code signing ==="

    if [ -z "$DEVELOPER_ID" ]; then
        # Try to find a Developer ID certificate
        DEVELOPER_ID=$(security find-identity -v -p codesigning | grep "Developer ID Application" | head -1 | sed 's/.*"\(.*\)".*/\1/')
        if [ -z "$DEVELOPER_ID" ]; then
            echo "ERROR: No Developer ID certificate found. Set DEVELOPER_ID env var."
            exit 1
        fi
    fi

    echo "Signing with: $DEVELOPER_ID"

    # Sign all .so and .dylib files first (innermost to outermost)
    echo "Signing shared libraries..."
    find "$APP_BUNDLE" \( -name "*.so" -o -name "*.dylib" \) -print0 | while IFS= read -r -d '' lib; do
        codesign --force --sign "$DEVELOPER_ID" \
            --options runtime \
            --timestamp \
            "$lib" 2>/dev/null || echo "  Warning: could not sign $lib"
    done

    # Sign the embedded Python binary
    echo "Signing Python binary..."
    codesign --force --sign "$DEVELOPER_ID" \
        --options runtime \
        --timestamp \
        --entitlements "$SCRIPT_DIR/Entitlements.plist" \
        "$PYTHON_EMBED_DIR/bin/python3"

    # Sign the main app
    echo "Signing app bundle..."
    codesign --force --deep --sign "$DEVELOPER_ID" \
        --options runtime \
        --timestamp \
        --entitlements "$SCRIPT_DIR/Entitlements.plist" \
        "$APP_BUNDLE"

    echo "Verifying signature..."
    codesign --verify --deep --strict "$APP_BUNDLE"
    echo "Signature valid."
fi

# ============================================================================
# Step 8: Notarize (if requested)
# ============================================================================
if [ "$NOTARIZE" = true ]; then
    echo ""
    echo "=== Step 8: Notarizing ==="

    DMG_PATH="$BUILD_DIR/$APP_NAME.dmg"

    # Create a DMG for notarization
    echo "Creating DMG..."
    hdiutil create -volname "$APP_NAME" \
        -srcfolder "$APP_BUNDLE" \
        -ov -format UDZO \
        "$DMG_PATH"

    echo "Submitting to Apple notary service..."
    xcrun notarytool submit "$DMG_PATH" \
        --keychain-profile "$NOTARIZE_PROFILE" \
        --wait

    echo "Stapling notarization ticket..."
    xcrun stapler staple "$DMG_PATH"

    echo "Notarization complete: $DMG_PATH"
fi

# ============================================================================
# Done
# ============================================================================
echo ""
echo "=== Build Complete ==="
echo "App: $APP_BUNDLE"
FINAL_SIZE=$(du -sh "$APP_BUNDLE" | cut -f1)
echo "Size: $FINAL_SIZE"

if [ "$SIGN" = true ]; then
    echo "Signed: Yes"
fi
if [ "$NOTARIZE" = true ]; then
    echo "Notarized: Yes"
    echo "DMG: $BUILD_DIR/$APP_NAME.dmg"
fi

echo ""
echo "To test: open \"$APP_BUNDLE\""
