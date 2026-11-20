#!/bin/bash
# Build "RAGNART Ingest.app" -- run ON THE MAC (needs swiftc from the Xcode command line tools).
#
#   cd scripts/macos/ragnart-ingest-app && ./build.sh            # first install
#   ./build.sh --replace                                          # rebuild over an existing app
#
# Why an app: macOS charges Local Network access (reaching the hub on the LAN) to the app that
# makes the connection. A bare python process launched by launchd has no identity to approve, so
# the endpoint runner is started through this small launcher app instead (main.swift runs the
# runner as a child process).
#
# First use: the first network access shows a "RAGNART Ingest" Local Network prompt. Approve it
# while you are at the Mac; a LaunchAgent that runs unattended cannot answer it.
# Rebuilding gives the app a new ad-hoc signature, so macOS may ask for approval again.
set -euo pipefail

APP="$HOME/Applications/RAGNART Ingest.app"
HERE="$(cd "$(dirname "$0")" && pwd)"

# Never replace an installed app (and lose its approval) by accident.
if [[ -e "$APP" && "${1:-}" != "--replace" ]]; then
    echo "$APP exists. Run with --replace to rebuild it." >&2
    exit 1
fi

# Compile into a scratch directory, then assemble the bundle.
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
swiftc -O -o "$WORK/ragnart-ingest" "$HERE/main.swift"

rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS"
cp "$HERE/Info.plist" "$APP/Contents/Info.plist"
cp "$WORK/ragnart-ingest" "$APP/Contents/MacOS/ragnart-ingest"

# Ad-hoc signature with a fixed identifier, so the Local Network approval attaches to a stable name.
codesign --force --sign - --identifier com.ragnart.ingest "$APP"
echo "Built $APP"
