#!/usr/bin/env bash
# Build facecrop-cli2 — the vision-lab head-crop / stacked-9:16 CLI.
#
# Compiles the cli/ sources together with the CLI-safe FaceCropLab logic files.
# VideoProcessor/Models/CropExporter are pure logic and define the shared types
# (VideoProcessor, ClassificationMode, FramingParams, ...) the CLI depends on;
# the SwiftUI/@main files (ContentView/Overlay/FaceCropLabApp) are app-only and
# must NOT be included (and VideoProcessor must shadow Vision.VideoProcessor,
# which only happens when the whole module is compiled together).
set -euo pipefail
cd "$(dirname "$0")"
OUT="${1:-/tmp/facecrop-cli2}"

# Pin the deployment target to match the production app build (macos/build.sh).
# At macOS 15.0 the AVFoundation composition/export APIs we use are current; their
# replacements (AVVideoComposition.Configuration et al., deprecated-in-26.0) don't
# exist before 26.0, so a 15.0-targeting binary is warning-clean and still runs on
# newer hosts. Without an explicit target swiftc builds for the host OS and emits
# 26.0 deprecation noise the production build never sees.
swiftc -O -target arm64-apple-macos15.0 \
  cli/*.swift \
  FaceCropLab/FaceCropLab/VideoProcessor.swift \
  FaceCropLab/FaceCropLab/Models.swift \
  FaceCropLab/FaceCropLab/CropExporter.swift \
  -o "$OUT"

echo "built $OUT"
