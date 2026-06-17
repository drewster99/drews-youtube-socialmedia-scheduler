import SwiftUI
import AVKit
import AppKit
import UniformTypeIdentifiers

struct ContentView: View {
    @State private var processor = VideoProcessor()
    @State private var player = AVPlayer()
    @State private var videoURL: URL?
    @State private var sampleInterval: Double = 0.05
    @State private var classificationMode: ClassificationMode = .center
    @State private var activenessMetric: ActivenessMetric = .movement
    @State private var cropAnxiety: Double = (15.0 - 3.0) / 14.5   // → 3.0s threshold
    @State private var currentTime: Double = 0

    /// Seconds before a left↔right crop switch is allowed: anxiety 0 → 15s, 1 → 0.5s.
    private var cropThreshold: Double { 15.0 - cropAnxiety * 14.5 }
    @State private var isPlaying = false
    @State private var timeObserver: Any?
    @State private var processingTask: Task<Void, Never>?
    /// Monotonic time of the last frame-step; the observer defers to `step`
    /// for a short window so late seek callbacks can't bounce the playhead.
    @State private var lastStepNanos: UInt64 = 0
    /// While a frame-step seek is in flight, the time it's seeking toward. The
    /// overlay's `currentTime` only advances when the seek lands, so the overlay
    /// can't get ahead of the (slow, 4K) video; rapid steps chain off this.
    @State private var seekTargetTime: Double?
    @State private var isExporting = false
    @State private var exportProgress: Double = 0
    @State private var exportError: String?

    var body: some View {
        VStack(spacing: 8) {
            controlBar
            playerArea
        }
        .padding(8)
        .frame(minWidth: 940, minHeight: 680)
        .onAppear(perform: installTimeObserver)
        .onDisappear(perform: removeTimeObserver)
        .alert("Export failed", isPresented: Binding(get: { exportError != nil }, set: { if !$0 { exportError = nil } })) {
            Button("OK", role: .cancel) { exportError = nil }
        } message: {
            Text(exportError ?? "")
        }
    }

    private var controlBar: some View {
        HStack(spacing: 10) {
            Button("Open Video…", action: openVideo)

            Divider().frame(height: 18)

            Text("Interval")
            TextField("", value: $sampleInterval, format: .number.precision(.fractionLength(2)))
                .frame(width: 52)
                .textFieldStyle(.roundedBorder)
            Text("s")
            Button("Reprocess", action: reprocess)
                .disabled(videoURL == nil || processor.isProcessing)

            Picker("", selection: $classificationMode) {
                ForEach(ClassificationMode.allCases) { Text($0.rawValue).tag($0) }
            }
            .pickerStyle(.segmented)
            .frame(width: 190)
            .onChange(of: classificationMode) {
                // Re-derive instantly from the cached detections — no Vision
                // re-run. Safe during processing: the processor re-buckets the
                // frames so far and the live run continues under the new mode.
                processor.rederive(mode: classificationMode, cropThreshold: cropThreshold, metric: activenessMetric)
            }

            Text("Active by")
            Picker("", selection: $activenessMetric) {
                ForEach(ActivenessMetric.allCases) { Text($0.rawValue).tag($0) }
            }
            .pickerStyle(.segmented)
            .frame(width: 150)
            .onChange(of: activenessMetric) {
                processor.rederive(mode: classificationMode, cropThreshold: cropThreshold, metric: activenessMetric)
            }

            Text("Anxiety")
            Slider(value: $cropAnxiety, in: 0...1)
                .frame(width: 90)
                .onChange(of: cropAnxiety) {
                    processor.rederive(mode: classificationMode, cropThreshold: cropThreshold, metric: activenessMetric)
                }
            Text(String(format: "%.1fs", cropThreshold)).font(.caption.monospaced())

            Divider().frame(height: 18)

            Button(isPlaying ? "Pause" : "Play", action: togglePlay)
                .keyboardShortcut(.space, modifiers: [])
                .disabled(videoURL == nil)
            Button("◀ Frame", action: { step(-1) })
                .keyboardShortcut(.leftArrow, modifiers: [])
                .disabled(processor.frames.isEmpty)
            Button("Frame ▶", action: { step(1) })
                .keyboardShortcut(.rightArrow, modifiers: [])
                .disabled(processor.frames.isEmpty)

            if processor.isProcessing {
                ProgressView(value: processor.progress).frame(width: 120)
            }

            Divider().frame(height: 18)

            Button("Crop 9:16 & Export…", action: exportCrop)
                .disabled(videoURL == nil || processor.frames.isEmpty || processor.isProcessing || isExporting)
            if isExporting {
                ProgressView(value: exportProgress).frame(width: 100)
                Text("\(Int(exportProgress * 100))%").font(.caption.monospaced())
            }

            Spacer()
            Text(statusLine).font(.caption.monospaced()).foregroundStyle(.secondary)
        }
    }

    @ViewBuilder
    private var playerArea: some View {
        if videoURL != nil {
            VideoPlayer(player: player)
                .overlay {
                    GeometryReader { _ in
                        Canvas { ctx, size in
                            guard processor.imageSize.width > 0 else { return }
                            OverlayRenderer.draw(ctx, size: size,
                                                 frame: currentAnalysis(),
                                                 imageSize: processor.imageSize,
                                                 summary: processor.timingSummary)
                        }
                        .allowsHitTesting(false)
                    }
                }
        } else {
            ContentUnavailableView("Open a video to begin",
                                   systemImage: "video.badge.waveform",
                                   description: Text("Pick a file; it's sampled at the chosen interval and analyzed with Vision."))
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        }
    }

    private static func mmss(_ seconds: Double) -> String {
        let s = Int(seconds.rounded())
        return String(format: "%d:%02d", s / 60, s % 60)
    }

    private var statusLine: String {
        let count = processor.frames.count
        let src = processor.sourceSize
        let proc = processor.imageSize
        var parts: [String] = []
        if src.width > 0 {
            let srcStr = "\(Int(src.width))×\(Int(src.height))"
            parts.append(proc.width > 0 && (Int(proc.width) != Int(src.width) || Int(proc.height) != Int(src.height))
                ? "\(srcStr)→\(Int(proc.width))×\(Int(proc.height))"
                : srcStr)
        }
        if processor.videoDuration > 0 { parts.append("len \(Self.mmss(processor.videoDuration))") }
        parts.append(String(format: "t=%.2fs", currentTime))
        parts.append(count > 0 ? "frame \(currentFrameIndex() + 1)/\(count)" : "—")
        parts.append(processor.statusMessage)
        return parts.joined(separator: " · ")
    }

    /// Index of the last frame whose time ≤ `t` (binary search; frames are
    /// in ascending time order).
    private static func frameIndex(for t: Double, in frames: [FrameAnalysis]) -> Int {
        guard !frames.isEmpty else { return 0 }
        var lo = 0, hi = frames.count - 1, ans = 0
        while lo <= hi {
            let mid = (lo + hi) / 2
            if frames[mid].time <= t + 1e-6 { ans = mid; lo = mid + 1 } else { hi = mid - 1 }
        }
        return ans
    }

    private func currentAnalysis() -> FrameAnalysis? {
        let frames = processor.frames
        guard !frames.isEmpty else { return nil }
        return frames[Self.frameIndex(for: currentTime, in: frames)]
    }

    private func currentFrameIndex() -> Int {
        Self.frameIndex(for: currentTime, in: processor.frames)
    }

    private func installTimeObserver() {
        guard timeObserver == nil else { return }
        let interval = CMTime(seconds: 0.05, preferredTimescale: 600)
        timeObserver = player.addPeriodicTimeObserver(forInterval: interval, queue: .main) { time in
            MainActor.assumeIsolated {
                let playing = player.rate != 0
                if isPlaying != playing { isPlaying = playing }
                // While frame-stepping (paused), `step` owns the playhead;
                // ignore seek-driven callbacks for a short window so late
                // callbacks from earlier seeks can't bounce the playhead back
                // to a higher frame (the "holding ← wraps around" bug).
                if DispatchTime.now().uptimeNanoseconds &- lastStepNanos < 250_000_000 { return }
                // Snap to the analyzed-frame boundary so the overlay only
                // redraws when the displayed frame changes — otherwise fast
                // scrubbing floods the main queue and the overlay lags behind.
                // The observer only runs when we're tracking the player live
                // (playback / native scrub), not mid-step; clear any stale step
                // target so the next step chains off the real position.
                seekTargetTime = nil
                let frames = processor.frames
                let target = frames.isEmpty
                    ? time.seconds
                    : frames[Self.frameIndex(for: time.seconds, in: frames)].time
                if abs(currentTime - target) > 1e-6 { currentTime = target }
            }
        }
    }

    private func removeTimeObserver() {
        if let token = timeObserver {
            player.removeTimeObserver(token)
            timeObserver = nil
        }
    }

    private func togglePlay() {
        if player.rate == 0 { player.play() } else { player.pause() }
        isPlaying = player.rate != 0
    }

    private func step(_ delta: Int) {
        let frames = processor.frames
        guard !frames.isEmpty else { return }
        player.pause()
        isPlaying = false
        lastStepNanos = DispatchTime.now().uptimeNanoseconds
        // Chain off the in-flight target (if any) so rapid steps advance even
        // before a slow seek lands.
        let baseTime = seekTargetTime ?? currentTime
        let idx = max(0, min(frames.count - 1, Self.frameIndex(for: baseTime, in: frames) + delta))
        let t = frames[idx].time
        seekTargetTime = t
        // Advance the overlay only when the (possibly slow, 4K) exact seek has
        // actually landed, so the analysis overlay always matches the displayed
        // video frame. Setting currentTime up-front let the overlay jump ahead of
        // the still-seeking video — drawing a face where the shown frame had none.
        player.seek(to: CMTime(seconds: t, preferredTimescale: 600),
                    toleranceBefore: .zero, toleranceAfter: .zero) { finished in
            guard finished else { return }
            DispatchQueue.main.async {
                MainActor.assumeIsolated {
                    currentTime = t
                    if seekTargetTime == t { seekTargetTime = nil }
                    lastStepNanos = DispatchTime.now().uptimeNanoseconds
                }
            }
        }
    }

    private func openVideo() {
        let panel = NSOpenPanel()
        panel.allowsMultipleSelection = false
        panel.canChooseDirectories = false
        panel.allowedContentTypes = [.movie, .video, .mpeg4Movie, .quickTimeMovie]
        panel.directoryURL = URL(
            fileURLWithPath: "/Users/andrew/Library/Application Support/com.nuclearcyborg.drews-socialmedia-scheduler/uploads",
            isDirectory: true)
        guard panel.runModal() == .OK, let url = panel.url else { return }
        videoURL = url
        currentTime = 0
        player.replaceCurrentItem(with: AVPlayerItem(url: url))
        reprocess()
    }

    private func reprocess() {
        guard let url = videoURL else { return }
        processingTask?.cancel()
        processingTask = Task { await processor.process(url: url, interval: sampleInterval, mode: classificationMode, cropThreshold: cropThreshold, metric: activenessMetric) }
    }

    /// Render the moving 9:16 crop (the current analysis trajectory) of the
    /// loaded video to a new file the user picks.
    private func exportCrop() {
        guard let src = videoURL, !processor.frames.isEmpty else { return }
        let panel = NSSavePanel()
        panel.allowedContentTypes = [.quickTimeMovie]
        panel.nameFieldStringValue = src.deletingPathExtension().lastPathComponent + "_9x16.mov"
        guard panel.runModal() == .OK, let outURL = panel.url else { return }

        let centers = CropExporter.centers(from: processor.frames)
        isExporting = true
        exportProgress = 0
        Task {
            do {
                try await CropExporter.export(source: src, to: outURL, centers: centers, progress: { p in
                    DispatchQueue.main.async { MainActor.assumeIsolated { exportProgress = p } }
                })
                await MainActor.run {
                    isExporting = false
                    NSWorkspace.shared.activateFileViewerSelecting([outURL])
                }
            } catch {
                await MainActor.run {
                    isExporting = false
                    exportError = error.localizedDescription
                }
            }
        }
    }
}
