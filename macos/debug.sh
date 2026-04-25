#!/bin/bash
# Dumps everything we know about the .app + agent + signature + logs.
# Run after a build to figure out why the agent isn't running.

set +e  # don't bail on the first missing thing
BUNDLE_ID="com.nuclearcyborg.drews-socialmedia-scheduler"
APP="/Users/andrew/Documents/ncc_source/cursor/drews-youtube-socialmedia-scheduler/macos/build/Drew's YT Scheduler.app"
PORT=8008

bar() { printf "\n========== %s ==========\n" "$1"; }

bar "On-disk .app"
ls -la "$APP" 2>&1 | head -3
echo
echo "Info.plist build identity:"
/usr/libexec/PlistBuddy -c "Print :DYSBuildId"     "$APP/Contents/Info.plist" 2>&1
/usr/libexec/PlistBuddy -c "Print :DYSBuildKind"   "$APP/Contents/Info.plist" 2>&1
/usr/libexec/PlistBuddy -c "Print :CFBundleVersion" "$APP/Contents/Info.plist" 2>&1

bar "Embedded Python binary"
PY="$APP/Contents/Resources/python/bin/python3"
ls -la "$PY" 2>&1
file "$PY" 2>&1
"$PY" --version 2>&1

bar "Code signature: python3"
codesign --display --verbose=2 "$PY" 2>&1 | head -15

bar "Code signature: .app"
codesign --display --verbose=2 "$APP" 2>&1 | head -10

bar "Embedded LaunchAgent plist"
cat "$APP/Contents/Library/LaunchAgents/$BUNDLE_ID.plist" 2>&1

bar "launchctl state for the agent"
launchctl print "gui/$UID/$BUNDLE_ID" 2>&1 | head -60

bar "Port $PORT"
lsof -ti :$PORT 2>&1 | head -5
[ -z "$(lsof -ti :$PORT 2>/dev/null)" ] && echo "(port is free)"

bar "/tmp boot log (launchd-captured stderr)"
BOOT_LOG="/tmp/$BUNDLE_ID.boot.log"
if [ -f "$BOOT_LOG" ]; then
    ls -la "$BOOT_LOG"
    echo
    tail -50 "$BOOT_LOG"
else
    echo "($BOOT_LOG does not exist)"
fi

bar "User Logs server.log (post-redirect)"
SERVER_LOG="$HOME/Library/Logs/$BUNDLE_ID/server.log"
if [ -f "$SERVER_LOG" ]; then
    ls -la "$SERVER_LOG"
    echo
    tail -50 "$SERVER_LOG"
else
    echo "($SERVER_LOG does not exist)"
fi

bar "Application data dir"
DATA="$HOME/Library/Application Support/$BUNDLE_ID"
if [ -d "$DATA" ]; then
    ls -la "$DATA" 2>&1 | head
else
    echo "($DATA does not exist)"
fi

bar "Console.app logs from .app + agent (last 2 minutes)"
log show --predicate "process == 'DrewsYTScheduler' OR process == 'python3' OR process == 'python3.12'" --last 2m --style compact 2>&1 | tail -40

bar "Done. Save this output and paste it back."
