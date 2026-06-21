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

swiftc -O \
  cli/*.swift \
  FaceCropLab/FaceCropLab/VideoProcessor.swift \
  FaceCropLab/FaceCropLab/Models.swift \
  FaceCropLab/FaceCropLab/CropExporter.swift \
  -o "$OUT"

echo "built $OUT"
