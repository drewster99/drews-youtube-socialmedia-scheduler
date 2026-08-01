// Native clip previewer — play the SELECTED [start,end] ranges of the source
// video with sample-exact start/stop and the SAME AVMutableAudioMix cosine fade
// clipcrop applies. No cutting, no transcription, no LLM, no app rebuild.
//
// It reads scripts/preview_clips.json (regenerate with gen_preview_clips.py) and
// lists each clip with its first/last transcript text + three buttons:
//   ▶ first   the first N seconds (real fade-in, clean stop)
//   ▶ last    the last  N seconds, ENDING EXACTLY on the cut with the real fade
//   ▶ all     the whole clip
// "Reload" re-reads the JSON, so the loop is: edit clip_edges → gen script → Reload.
//
// Run (the USER runs this — opens a window, no Keychain, no DB):
//   swift scripts/clip_previewer.swift
//   swift scripts/clip_previewer.swift /path/to/other_clips.json
//
// Exact stop is AVPlayerItem.forwardPlaybackEndTime; the fade is the seam-safe
// raised-cosine ramp from vision-lab/cli/stack.swift, so this previews the real
// clipcrop audio path, not a browser approximation.

import AVFoundation
import AVKit
import Cocoa

let TS: CMTimeScale = 48000

func plog(_ s: String) {
    // Lifecycle/diagnostic log, silent unless CLIPPREVIEW_DEBUG is set.
    guard ProcessInfo.processInfo.environment["CLIPPREVIEW_DEBUG"] != nil else { return }
    let line = "[\(Date())] \(s)\n"
    let path = "/tmp/clippreview.log"
    if let h = FileHandle(forWritingAtPath: path) { h.seekToEndOfFile(); h.write(Data(line.utf8)); try? h.close() }
    else { try? line.write(toFile: path, atomically: true, encoding: .utf8) }
    FileHandle.standardError.write(Data(line.utf8))
}

struct Clip: Codable {
    let title: String
    let start: Double
    let end: Double
    let fade_in: Double
    let fade_out: Double
    let first_text: String?
    let last_text: String?
    let first_index: Int?
    let last_index: Int?
    let variant: String?
    let calc: String?
}
struct ClipsFile: Codable { let source: String; let clips: [Clip] }

final class Btn: NSButton {
    var onClick: (() -> Void)?
    convenience init(_ title: String, width: CGFloat = 0, _ onClick: @escaping () -> Void) {
        self.init(title: title, target: nil, action: nil)
        self.onClick = onClick
        self.target = self
        self.action = #selector(fire)
        self.bezelStyle = .rounded
        if width > 0 { self.widthAnchor.constraint(equalToConstant: width).isActive = true }
    }
    @objc func fire() { plog("click: \(self.title)"); onClick?() }
}

/// Raised-cosine (cosine-S) fade over [start, start+duration], approximated with
/// contiguous linear sub-ramps — identical to clipcrop's stack.swift, boundary
/// CMTimes precomputed so there is no 1-sample seam (click).
func addCosineRamp(_ params: AVMutableAudioMixInputParameters,
                   fadingIn: Bool, start: Double, duration: Double) {
    guard duration > 0 else { return }
    let steps = 24
    func gain(_ x: Double) -> Float { let r = (1.0 - cos(Double.pi * x)) / 2.0; return Float(fadingIn ? r : 1.0 - r) }
    let bounds: [CMTime] = (0...steps).map {
        CMTime(seconds: start + Double($0) / Double(steps) * duration, preferredTimescale: TS)
    }
    for k in 0..<steps {
        params.setVolumeRamp(
            fromStartVolume: gain(Double(k) / Double(steps)),
            toEndVolume: gain(Double(k + 1) / Double(steps)),
            timeRange: CMTimeRange(start: bounds[k], duration: CMTimeSubtract(bounds[k + 1], bounds[k])))
    }
}

final class PreviewApp: NSObject, NSApplicationDelegate {
    let clipsPath: String
    var window: NSWindow!
    let player = AVPlayer()
    var playerView: AVPlayerView!
    var asset: AVURLAsset!
    var audioTrack: AVAssetTrack?
    var listStack: NSStackView!
    var edgeField: NSTextField!
    var fadeToggle: NSButton!
    var status: NSTextField!
    var timeObserver: Any?
    var playerItem: AVPlayerItem?
    var sourceURL: URL?
    var assetReady = false
    var clipCount = 0
    var lastMtime: Date?
    var lastPlay: (Clip, Mode)?
    var reloadTimer: Timer?

    init(clipsPath: String) { self.clipsPath = clipsPath }

    func applicationDidFinishLaunching(_ note: Notification) {
        plog("didFinishLaunching; clipsPath=\(clipsPath)")
        buildUI()
        plog("buildUI done")
        reload()
        // Auto-reload: poll the JSON's mtime so a regenerate on disk is picked up
        // within ~1s without any clicking. This is the real fix for "reload isn't
        // reloading" — the tuning loop no longer depends on a button firing.
        reloadTimer = Timer.scheduledTimer(withTimeInterval: 1.0, repeats: true) { [weak self] _ in
            self?.reloadIfChanged()
        }
    }

    func fileMtime() -> Date? {
        (try? FileManager.default.attributesOfItem(atPath: clipsPath))?[.modificationDate] as? Date
    }
    func reloadIfChanged() {
        guard let m = fileMtime() else { return }
        if let last = lastMtime, m <= last { return }
        plog("auto-reload: json changed")
        reload()
    }

    func loadClips() -> ClipsFile? {
        guard let data = FileManager.default.contents(atPath: clipsPath) else { return nil }
        return try? JSONDecoder().decode(ClipsFile.self, from: data)
    }

    func buildUI() {
        window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 1180, height: 820),
                          styleMask: [.titled, .closable, .resizable, .miniaturizable],
                          backing: .buffered, defer: false)
        window.title = "Clip previewer"
        window.center()

        playerView = AVPlayerView()
        playerView.player = player
        playerView.controlsStyle = .floating
        playerView.translatesAutoresizingMaskIntoConstraints = false
        playerView.heightAnchor.constraint(equalToConstant: 300).isActive = true

        edgeField = NSTextField(string: "5")
        edgeField.widthAnchor.constraint(equalToConstant: 48).isActive = true
        fadeToggle = NSButton(checkboxWithTitle: "cosine fade", target: nil, action: nil)
        fadeToggle.state = .on
        status = NSTextField(labelWithString: "loading...")
        status.textColor = .secondaryLabelColor

        let header = NSStackView(views: [
            Btn("Reload", width: 64) { [weak self] in self?.reload() },
            Btn("Replay", width: 64) { [weak self] in self?.replay() },
            Btn("Stop", width: 56) { [weak self] in self?.stop() },
            NSTextField(labelWithString: "edge window"), edgeField, NSTextField(labelWithString: "s"),
            fadeToggle,
            status,
        ])
        header.orientation = .horizontal
        header.spacing = 10
        header.edgeInsets = NSEdgeInsets(top: 8, left: 10, bottom: 8, right: 10)

        listStack = NSStackView()
        listStack.orientation = .vertical
        listStack.alignment = .leading
        listStack.spacing = 6
        listStack.edgeInsets = NSEdgeInsets(top: 8, left: 10, bottom: 8, right: 10)
        listStack.translatesAutoresizingMaskIntoConstraints = false

        let scroll = NSScrollView()
        scroll.hasVerticalScroller = true
        scroll.documentView = listStack
        scroll.translatesAutoresizingMaskIntoConstraints = false
        listStack.widthAnchor.constraint(equalTo: scroll.widthAnchor).isActive = true

        let root = NSStackView(views: [header, playerView, scroll])
        root.orientation = .vertical
        root.spacing = 0
        root.translatesAutoresizingMaskIntoConstraints = false
        window.contentView = root
        NSLayoutConstraint.activate([
            root.leadingAnchor.constraint(equalTo: window.contentView!.leadingAnchor),
            root.trailingAnchor.constraint(equalTo: window.contentView!.trailingAnchor),
            root.topAnchor.constraint(equalTo: window.contentView!.topAnchor),
            root.bottomAnchor.constraint(equalTo: window.contentView!.bottomAnchor),
        ])
        window.makeKeyAndOrderFront(nil)
    }

    func reload() {
        plog("reload: reading \(clipsPath)")
        stop()
        guard let file = loadClips() else {
            plog("reload: loadClips returned nil")
            status.stringValue = "cannot read \(clipsPath)"; return
        }
        plog("reload: decoded \(file.clips.count) clips, source=\(file.source)")
        lastMtime = fileMtime()
        clipCount = file.clips.count
        // Build the list FIRST — it needs only the JSON, never the video. (The
        // old code created the AVPlayerItem here, which cold-loaded the 11GB moov
        // synchronously on the main thread and deadlocked the whole UI.)
        for v in listStack.arrangedSubviews { v.removeFromSuperview() }
        for (i, c) in file.clips.enumerated() { listStack.addArrangedSubview(row(i, c)) }
        plog("reload: built rows")
        let url = URL(fileURLWithPath: file.source)
        status.stringValue = "\(clipCount) clips · \(url.lastPathComponent)"
        plog("reload: built \(clipCount) rows")
        // Load the asset ASYNCHRONOUSLY (properties off the main thread) so the UI
        // stays live; play() waits for assetReady. Only reload on source change.
        if sourceURL != url {
            sourceURL = url
            assetReady = false
            status.stringValue += " · loading video…"
            Task { @MainActor in
                plog("asset: loading \(url.lastPathComponent)")
                let a = AVURLAsset(url: url)
                _ = try? await a.load(.duration)
                self.audioTrack = try? await a.loadTracks(withMediaType: .audio).first
                self.asset = a
                // Asset properties are now loaded, so making/attaching the item no
                // longer cold-loads the moov on the main thread (the old deadlock).
                let it = AVPlayerItem(asset: a)
                self.playerItem = it
                self.player.replaceCurrentItem(with: it)
                self.assetReady = true
                plog("asset: ready (audio=\(self.audioTrack != nil))")
                self.status.stringValue = "\(self.clipCount) clips · \(url.lastPathComponent) · ready"
            }
        }
    }

    func row(_ i: Int, _ c: Clip) -> NSView {
        let first = Btn("> first", width: 74) { [weak self] in self?.play(c, mode: .first) }
        let last = Btn("> last", width: 70) { [weak self] in self?.play(c, mode: .last) }
        let all = Btn("> all", width: 62) { [weak self] in self?.play(c, mode: .all) }
        let title = NSTextField(labelWithString: c.title)
        title.font = .boldSystemFont(ofSize: 13)
        let meta = NSTextField(labelWithString:
            "units \(c.first_index ?? 0)-\(c.last_index ?? 0)   \(String(format: "%.3f-%.3f", c.start, c.end))s   "
            + "(\(String(format: "%.2f", c.end - c.start))s)   fade \(Int(c.fade_in * 1000))/\(Int(c.fade_out * 1000))ms")
        let calcT = NSTextField(labelWithString: c.calc ?? "")
        let firstT = NSTextField(labelWithString: "first: " + (c.first_text ?? ""))
        let lastT = NSTextField(labelWithString: "last:  " + (c.last_text ?? ""))
        for t in [meta, calcT, firstT, lastT] {
            t.lineBreakMode = .byTruncatingTail
            t.maximumNumberOfLines = 1
            t.widthAnchor.constraint(equalToConstant: 900).isActive = true   // self-constraint, no hierarchy
        }
        meta.font = .monospacedDigitSystemFont(ofSize: 11, weight: .regular)
        calcT.font = .monospacedDigitSystemFont(ofSize: 11, weight: .regular)
        calcT.textColor = .systemTeal
        firstT.font = .systemFont(ofSize: 11)
        lastT.font = .systemFont(ofSize: 11)
        let textCol = NSStackView(views: [title, meta, calcT, firstT, lastT])
        textCol.orientation = .vertical
        textCol.alignment = .leading
        textCol.spacing = 1
        let rowStack = NSStackView(views: [first, last, all, textCol])
        rowStack.orientation = .horizontal
        rowStack.alignment = .top
        rowStack.spacing = 8
        rowStack.edgeInsets = NSEdgeInsets(top: 6, left: 8, bottom: 6, right: 8)
        return rowStack
    }

    enum Mode { case first, last, all }

    func play(_ c: Clip, mode: Mode) {
        guard assetReady, let item = playerItem, let track = audioTrack else {
            status.stringValue = "video still loading - try again in a moment"
            plog("play blocked: not ready"); return
        }
        lastPlay = (c, mode)
        let win = max(0.5, Double(edgeField.stringValue) ?? 5)
        let from: Double, to: Double
        var fadeInAtStart = false, fadeOutAtEnd = false
        switch mode {
        case .first: from = c.start; to = min(c.end, c.start + win); fadeInAtStart = true
        case .last:  from = max(c.start, c.end - win); to = c.end; fadeOutAtEnd = true
        case .all:   from = c.start; to = c.end; fadeInAtStart = true; fadeOutAtEnd = true
        }
        let fadeOn = fadeToggle.state == .on
        let fi = fadeInAtStart ? (fadeOn ? c.fade_in : 0) : 0.02   // real fade at true edges, else anti-click
        let fo = fadeOutAtEnd ? (fadeOn ? c.fade_out : 0) : 0.02
        let params = AVMutableAudioMixInputParameters(track: track)
        addCosineRamp(params, fadingIn: true, start: from, duration: fi)
        addCosineRamp(params, fadingIn: false, start: to - fo, duration: fo)
        let mix = AVMutableAudioMix(); mix.inputParameters = [params]

        player.pause()
        removeTimeObserver()
        item.audioMix = mix                       // reuse the SAME item; just re-mix + re-seek
        plog("play: \(c.title) [\(mode)] \(String(format: "%.3f->%.3f", from, to))")
        status.stringValue = "\(c.title) [\(mode)] \(String(format: "%.3f-%.3f", from, to))s"
        player.seek(to: CMTime(seconds: from, preferredTimescale: TS),
                    toleranceBefore: .zero, toleranceAfter: .zero) { [weak self] _ in
            guard let self = self else { return }
            // Precise stop: a 10ms periodic observer pauses exactly when playback
            // reaches `to` (forwardPlaybackEndTime was overshooting into dead air).
            self.timeObserver = self.player.addPeriodicTimeObserver(
                forInterval: CMTime(seconds: 0.01, preferredTimescale: TS), queue: .main) { [weak self] t in
                guard let self = self else { return }
                if CMTimeGetSeconds(t) >= to {
                    self.player.pause(); self.removeTimeObserver()
                    self.status.stringValue = "stopped at \(String(format: "%.3f", CMTimeGetSeconds(t)))s"
                }
            }
            self.player.play()
        }
    }

    func removeTimeObserver() {
        if let t = timeObserver { player.removeTimeObserver(t); timeObserver = nil }
    }

    func replay() { if let (c, m) = lastPlay { play(c, mode: m) } else { status.stringValue = "nothing to replay yet" } }

    func stop() {
        player.pause()
        removeTimeObserver()
    }
}

let jsonArg = CommandLine.arguments.dropFirst().first { $0.hasSuffix(".json") }
let clipsPath = jsonArg ?? "scripts/preview_clips.json"
let app = NSApplication.shared
let delegate = PreviewApp(clipsPath: clipsPath)
app.delegate = delegate
app.setActivationPolicy(.regular)
app.activate(ignoringOtherApps: true)
app.run()
