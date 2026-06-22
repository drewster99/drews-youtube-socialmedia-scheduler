import Foundation
import AVFoundation
import Vision
import CoreImage
import CoreGraphics
import CoreMedia

// Offline two-pass stacked-crop compositor.
//  Pass 1 (analyze): sample faces, map count→layout (1 or 3+ → single; 2 → stacked),
//    debounce layout changes by 0.3s in BOTH directions and backdate the change to
//    the edge, then fix each segment's crop center as the trimmed-80% mean of the
//    relevant face centers (no per-frame tracking, no easing).
//  Pass 2 (render): composite each segment's fixed crop(s) into a 9:16 canvas —
//    single fills the frame; two stack 50/50 (left face → top) — with audio.

/// One stacked band's fixed framing — all fractions of source dimensions.
struct BandSpec: Sendable {
    var centerX: CGFloat
    var centerY: CGFloat
    var faceWidth: CGFloat    // size proxy for normalization (cheek-to-cheek; robust to caps/hair)
    var faceHeight: CGFloat   // only for the vertical head bias
}

enum StackLayout: Sendable {
    case single(centerX: CGFloat)                  // crop horizontal center, fraction
    case stacked(top: BandSpec, bot: BandSpec)
}

struct StackSegment: Sendable {
    var start: Double
    var layout: StackLayout
}

/// Mean of the middle 80% (drop the outer 10% each side).
private func trimmedMean(_ xs: [CGFloat], fallback: CGFloat) -> CGFloat {
    guard !xs.isEmpty else { return fallback }
    let s = xs.sorted()
    let drop = Int((Double(s.count) * 0.1).rounded(.down))
    let lo = drop, hi = s.count - drop
    let slice = lo < hi ? Array(s[lo..<hi]) : s
    return slice.reduce(0, +) / CGFloat(slice.count)
}

private struct HeadBoxJSON: Codable { var cx: Double; var cy: Double; var w: Double; var h: Double }
private struct HeadSampleJSON: Codable { var t: Double; var boxes: [HeadBoxJSON] }
private struct HeadsDocJSON: Codable { var interval: Double; var samples: [HeadSampleJSON] }

/// Segmentation + per-segment fixed specs from sampled boxes (normalized,
/// top-left, sorted left→right). Shared by the Vision-face path and the YOLO
/// head-box JSON path.
func segmentsFromSamples(_ samples: [(t: Double, faces: [CGRect])], sampleInterval: Double, debounce: Double) -> [StackSegment] {
    guard !samples.isEmpty else { return [] }
    func isStacked(_ faces: [CGRect]) -> Bool {
        faces.count == 2 && faces[0].midX < 0.5 && faces[1].midX >= 0.5
    }
    struct Run { var stacked: Bool; var lo: Int; var hi: Int }
    var runs: [Run] = []
    for (i, s) in samples.enumerated() {
        let k = isStacked(s.faces)
        if !runs.isEmpty, runs[runs.count - 1].stacked == k { runs[runs.count - 1].hi = i }
        else { runs.append(Run(stacked: k, lo: i, hi: i)) }
    }
    var bounds: [(startIdx: Int, stacked: Bool)] = [(0, runs[0].stacked)]
    var committed = runs[0].stacked
    for r in runs.dropFirst() {
        let runDur = samples[r.hi].t - samples[r.lo].t + sampleInterval
        if r.stacked != committed, runDur >= debounce {
            committed = r.stacked
            bounds.append((r.lo, committed))
        }
    }
    var segments: [StackSegment] = []
    for (bi, b) in bounds.enumerated() {
        let lo = b.startIdx
        let hi = (bi + 1 < bounds.count) ? bounds[bi + 1].startIdx - 1 : samples.count - 1
        let slice = samples[lo...hi]
        let startT = samples[lo].t
        if b.stacked {
            let twos = slice.filter { isStacked($0.faces) }
            let top = BandSpec(centerX: trimmedMean(twos.map { $0.faces[0].midX }, fallback: 0.25),
                               centerY: trimmedMean(twos.map { $0.faces[0].midY }, fallback: 0.5),
                               faceWidth: trimmedMean(twos.map { $0.faces[0].width }, fallback: 0.15),
                               faceHeight: trimmedMean(twos.map { $0.faces[0].height }, fallback: 0.2))
            let bot = BandSpec(centerX: trimmedMean(twos.map { $0.faces[1].midX }, fallback: 0.75),
                               centerY: trimmedMean(twos.map { $0.faces[1].midY }, fallback: 0.5),
                               faceWidth: trimmedMean(twos.map { $0.faces[1].width }, fallback: 0.15),
                               faceHeight: trimmedMean(twos.map { $0.faces[1].height }, fallback: 0.2))
            segments.append(StackSegment(start: startT, layout: .stacked(top: top, bot: bot)))
        } else {
            let cxs: [CGFloat] = slice.compactMap { s in s.faces.max(by: { $0.width < $1.width })?.midX }
            segments.append(StackSegment(start: startT, layout: .single(centerX: trimmedMean(cxs, fallback: 0.5))))
        }
    }
    return segments
}

/// Build segments from a YOLO head-box JSON instead of live Vision detection.
func analyzeStackSegmentsFromJSON(_ jsonPath: String, debounce: Double) -> [StackSegment] {
    guard let data = FileManager.default.contents(atPath: jsonPath),
          let doc = try? JSONDecoder().decode(HeadsDocJSON.self, from: data) else {
        print("ERROR: could not read head-box JSON: \(jsonPath)"); return []
    }
    let samples: [(t: Double, faces: [CGRect])] = doc.samples.map { s in
        let faces = s.boxes
            .map { CGRect(x: $0.cx - $0.w / 2, y: $0.cy - $0.h / 2, width: $0.w, height: $0.h) }
            .sorted { $0.midX < $1.midX }
        return (s.t, faces)
    }
    return segmentsFromSamples(samples, sampleInterval: doc.interval, debounce: debounce)
}

/// Pass 1: derive layout segments with debounced/backdated transitions and
/// fixed per-segment crop centers (Vision face path).
func analyzeStackSegments(path: String, sampleInterval: Double, maxSeconds: Double, debounce: Double) async -> [StackSegment] {
    let asset = AVURLAsset(url: URL(fileURLWithPath: path))
    guard let durSec = (try? await asset.load(.duration))?.seconds else { return [] }
    let duration = min(maxSeconds, durSec)
    let gen = AVAssetImageGenerator(asset: asset)
    gen.appliesPreferredTrackTransform = true
    gen.requestedTimeToleranceBefore = .zero
    gen.requestedTimeToleranceAfter = .zero
    gen.maximumSize = CGSize(width: VideoProcessor.maxAnalysisDimension, height: VideoProcessor.maxAnalysisDimension)
    let request = DetectFaceLandmarksRequest()
    let unit = CGSize(width: 1, height: 1)

    var times: [CMTime] = []
    var t = 0.0
    while t <= duration { times.append(CMTime(seconds: t, preferredTimescale: 600)); t += sampleInterval }
    var idxByTime: [Int64: Int] = [:]
    for (i, ct) in times.enumerated() { idxByTime[ct.value] = i }

    var byIdx: [Int: (t: Double, faces: [CGRect])] = [:]
    for await result in gen.images(for: times) {
        let ft = result.requestedTime.seconds
        let idx = idxByTime[result.requestedTime.value] ?? Int((ft / sampleInterval).rounded())
        guard let img = try? result.image else { byIdx[idx] = (ft, []); continue }
        let obs = (try? await request.perform(on: img)) ?? []
        var boxes: [CGRect] = []
        for o in obs where o.landmarks != nil {
            boxes.append(o.boundingBox.toImageCoordinates(unit, origin: .upperLeft))
        }
        boxes.sort { $0.midX < $1.midX }   // left → right
        byIdx[idx] = (ft, boxes)
    }
    var samples: [(t: Double, faces: [CGRect])] = []
    samples.reserveCapacity(times.count)
    for i in 0..<times.count { if let s = byIdx[i] { samples.append(s) } }
    return segmentsFromSamples(samples, sampleInterval: sampleInterval, debounce: debounce)
}

struct StackExportError: LocalizedError { let message: String; var errorDescription: String? { message } }

/// Build the 9:16 single/stacked CoreImage video composition. Shared by the
/// full-video render (renderStacked) and the per-clip one-pass cut (renderClipCrop)
/// so the framing geometry has a single source of truth. The handler keys off
/// `request.compositionTime.seconds` (absolute asset time) to pick each segment's
/// layout; segment `start`s are likewise absolute, so a non-zero export timeRange
/// composes correctly.
func makeStackComposition(asset: AVAsset, segments: [StackSegment], canvas: CGRect,
                          srcW: CGFloat, srcH: CGFloat, fps: Float,
                          bandZoom: CGFloat, sizeByHeight: Bool, headBias: CGFloat) async throws -> AVMutableVideoComposition {
    let starts = segments.map { $0.start }
    let segs = segments
    // The CIFilter applier closure is @Sendable, so segLayout lives inside it
    // (capturing only the Sendable `segs`/`starts` arrays).
    let base = try await AVVideoComposition.videoComposition(with: asset) { request in
        func segLayout(_ t: Double) -> StackLayout {
            guard !segs.isEmpty else { return .single(centerX: 0.5) }
            var lo = 0, hi = starts.count - 1, ans = 0
            while lo <= hi { let m = (lo + hi) / 2; if starts[m] <= t { ans = m; lo = m + 1 } else { hi = m - 1 } }
            return segs[ans].layout
        }
        let src = request.sourceImage   // display-oriented, bottom-left origin
        var output = CIImage(color: .black).cropped(to: canvas)
        func place(cropTL: CGRect, destBL: CGRect) {
            let cropBL = CGRect(x: cropTL.minX, y: srcH - cropTL.maxY, width: cropTL.width, height: cropTL.height)
            var piece = src.cropped(to: cropBL).transformed(by: CGAffineTransform(translationX: -cropBL.minX, y: -cropBL.minY))
            piece = piece.transformed(by: CGAffineTransform(scaleX: destBL.width / cropBL.width, y: destBL.height / cropBL.height))
            piece = piece.transformed(by: CGAffineTransform(translationX: destBL.minX, y: destBL.minY))
            output = piece.composited(over: output)
        }
        switch segLayout(request.compositionTime.seconds) {
        case .single(let cx):
            let cropW = min(srcW, srcH * 9.0 / 16.0)
            let x = max(0, min(srcW - cropW, cx * srcW - cropW / 2))
            place(cropTL: CGRect(x: x, y: 0, width: cropW, height: srcH), destBL: canvas)
        case .stacked(let top, let bot):
            let halfW = srcW / 2
            let maxCropH = halfW * 8.0 / 9.0
            let effZoom: CGFloat
            if sizeByHeight {
                let maxFaceH = max(top.faceHeight, bot.faceHeight) * srcH
                effZoom = maxFaceH > 0 ? min(bandZoom, maxCropH / maxFaceH) : bandZoom
            } else {
                let maxFaceW = max(top.faceWidth, bot.faceWidth) * srcW
                effZoom = maxFaceW > 0 ? min(bandZoom, halfW / maxFaceW) : bandZoom
            }
            func bandCrop(_ spec: BandSpec, leftHalf: Bool) -> CGRect {
                var cropW: CGFloat
                var cropH: CGFloat
                if sizeByHeight {
                    cropH = min(maxCropH, spec.faceHeight * srcH * effZoom)
                    cropW = cropH * 9.0 / 8.0
                    if cropW > halfW { cropW = halfW; cropH = cropW * 8.0 / 9.0 }
                } else {
                    cropW = min(halfW, spec.faceWidth * srcW * effZoom)
                    cropH = cropW * 8.0 / 9.0
                }
                let xmin = leftHalf ? 0 : halfW
                let xmax = max(xmin, (leftHalf ? halfW : srcW) - cropW)
                let x = max(xmin, min(xmax, spec.centerX * srcW - cropW / 2))
                let headCenterY = (spec.centerY - headBias * spec.faceHeight) * srcH
                let y = max(0, min(srcH - cropH, headCenterY - cropH / 2))
                return CGRect(x: x, y: y, width: cropW, height: cropH)
            }
            // top band = upper half (Core Image bottom-left → higher y)
            place(cropTL: bandCrop(top, leftHalf: true),
                  destBL: CGRect(x: 0, y: canvas.height / 2, width: canvas.width, height: canvas.height / 2))
            place(cropTL: bandCrop(bot, leftHalf: false),
                  destBL: CGRect(x: 0, y: 0, width: canvas.width, height: canvas.height / 2))
        }
        request.finish(with: output, context: nil)
    }
    guard let comp = base.mutableCopy() as? AVMutableVideoComposition else {
        throw StackExportError(message: "Could not build video composition.")
    }
    comp.renderSize = canvas.size
    comp.frameDuration = CMTimeMakeWithSeconds(1.0 / Double(fps), preferredTimescale: 600)
    return comp
}

/// Pass 2: render the segments to a 9:16 file (with audio).
func renderStacked(path: String, segments: [StackSegment], outURL: URL, renderHeight: Int,
                   bandZoom: CGFloat, sizeByHeight: Bool = false, headBias: CGFloat = 0.15,
                   maxSeconds: Double, progress: @escaping @Sendable (Double) -> Void) async throws {
    let asset = AVURLAsset(url: URL(fileURLWithPath: path))
    guard let track = try await asset.loadTracks(withMediaType: .video).first else {
        throw StackExportError(message: "No video track.")
    }
    let natural = try await track.load(.naturalSize)
    let transform = try await track.load(.preferredTransform)
    let displayed = natural.applying(transform)
    let srcW = abs(displayed.width), srcH = abs(displayed.height)
    guard srcW > 0, srcH > 0 else { throw StackExportError(message: "Bad source size.") }
    let fps = (try? await track.load(.nominalFrameRate)) ?? 30
    let duration = min(maxSeconds, (try await asset.load(.duration)).seconds)

    func even(_ v: CGFloat) -> CGFloat { (v / 2).rounded() * 2 }
    let outH = even(renderHeight > 0 ? CGFloat(renderHeight) : srcH)
    let outW = even(outH * 9.0 / 16.0)
    let canvas = CGRect(x: 0, y: 0, width: outW, height: outH)

    let comp = try await makeStackComposition(asset: asset, segments: segments, canvas: canvas,
                                              srcW: srcW, srcH: srcH, fps: fps, bandZoom: bandZoom,
                                              sizeByHeight: sizeByHeight, headBias: headBias)

    guard let session = AVAssetExportSession(asset: asset, presetName: AVAssetExportPresetHighestQuality) else {
        throw StackExportError(message: "Could not create export session.")
    }
    session.videoComposition = comp
    session.timeRange = CMTimeRange(start: .zero, duration: CMTime(seconds: duration, preferredTimescale: 600))
    if FileManager.default.fileExists(atPath: outURL.path) { try FileManager.default.removeItem(at: outURL) }

    // Drive the export via async-let; monitor progress via states() (it ends when
    // the export finishes). `try await exported` surfaces any export error.
    async let exported: Void = session.export(to: outURL, as: .mov)
    for await state in session.states(updateInterval: 0.5) {
        if case .exporting(let p) = state { progress(p.fractionCompleted) }
    }
    try await exported
    progress(1.0)
}

/// Driver: the full native Swift pipeline — batched CoreML head detection →
/// segmentation → 9:16 stacked render. No Python, no JSON handoff (though the
/// intermediate head boxes can be dumped via `detectJSON`). Render defaults to
/// full source resolution (renderHeight 0 = zero downscaling).
func runStackAuto(path: String, config: HeadDetectionConfig, detectJSON: String?, renderOut: String?,
                  debounce: Double, renderHeight: Int, bandZoom: CGFloat, maxSeconds: Double) async {
    print("\n=== native head detection (CoreML, batched) ===")
    print(String(format: "model: %@  interval %.2fs  imgsz %d  batch %d", config.modelPath, config.interval, config.imgsz, config.batch))
    let started = Date()
    let samples: [HeadSample]
    do {
        samples = try await HeadDetector.detect(path: path, config: config, maxSeconds: maxSeconds) { frac, n in
            FileHandle.standardError.write(Data(String(format: "  detect %.0f%%  (%d sampled)\n", frac * 100, n).utf8))
        }
    } catch {
        print("detection FAILED: \(error.localizedDescription)")
        return
    }
    let two = samples.filter { $0.faces.count == 2 }.count
    print(String(format: "detected %d samples (%d with 2 heads) in %.1fs", samples.count, two, Date().timeIntervalSince(started)))

    if let detectJSON {
        do {
            try writeHeadSamplesJSON(samples, interval: config.interval, to: detectJSON)
            print("wrote head-box JSON: \(detectJSON)")
        } catch { print("JSON write FAILED: \(error.localizedDescription)") }
    }
    guard let renderOut else { return }

    let tuples = samples.map { (t: $0.t, faces: $0.faces) }
    let segs = segmentsFromSamples(tuples, sampleInterval: config.interval, debounce: debounce)
    let stackedCount = segs.filter { if case .stacked = $0.layout { return true } else { return false } }.count
    print("derived \(segs.count) segments (\(stackedCount) stacked, \(segs.count - stackedCount) single)")
    let lastPct = LockedDouble()
    do {
        try await renderStacked(path: path, segments: segs, outURL: URL(fileURLWithPath: renderOut),
                                renderHeight: renderHeight, bandZoom: bandZoom,
                                sizeByHeight: true, headBias: 0.0, maxSeconds: maxSeconds) { p in
            if p - lastPct.get() >= 0.1 || p >= 1.0 {
                lastPct.set(p)
                FileHandle.standardError.write(Data(String(format: "  render %.0f%%\n", p * 100).utf8))
            }
        }
        print("export complete: \(renderOut)")
    } catch {
        print("export FAILED: \(error.localizedDescription)")
    }
}

/// Driver: render the stack from a YOLO head-box JSON (analysis done in Python).
func runStackExportFromBoxes(path: String, jsonPath: String, outPath: String, debounce: Double,
                             renderHeight: Int, bandZoom: CGFloat, maxSeconds: Double) async {
    print("\n=== stacked export (YOLO head boxes) ===")
    let segs = analyzeStackSegmentsFromJSON(jsonPath, debounce: debounce)
    let stackedCount = segs.filter { if case .stacked = $0.layout { return true } else { return false } }.count
    print("derived \(segs.count) segments (\(stackedCount) stacked, \(segs.count - stackedCount) single) from \(jsonPath)")
    let lastPct = LockedDouble()
    do {
        try await renderStacked(path: path, segments: segs, outURL: URL(fileURLWithPath: outPath),
                                renderHeight: renderHeight, bandZoom: bandZoom,
                                sizeByHeight: true, headBias: 0.0, maxSeconds: maxSeconds) { p in
            if p - lastPct.get() >= 0.1 || p >= 1.0 {
                lastPct.set(p)
                FileHandle.standardError.write(Data(String(format: "  render %.0f%%\n", p * 100).utf8))
            }
        }
        print("export complete: \(outPath)")
    } catch {
        print("export FAILED: \(error.localizedDescription)")
    }
}

/// Debug: dump per-sample face detections + derived segments over a window.
func dumpStackAnalysis(path: String, sampleInterval: Double, maxSeconds: Double, debounce: Double) async {
    let asset = AVURLAsset(url: URL(fileURLWithPath: path))
    guard let durSec = (try? await asset.load(.duration))?.seconds else { return }
    let duration = min(maxSeconds, durSec)
    let gen = AVAssetImageGenerator(asset: asset)
    gen.appliesPreferredTrackTransform = true
    gen.requestedTimeToleranceBefore = .zero
    gen.requestedTimeToleranceAfter = .zero
    gen.maximumSize = CGSize(width: VideoProcessor.maxAnalysisDimension, height: VideoProcessor.maxAnalysisDimension)
    let request = DetectFaceLandmarksRequest()
    let unit = CGSize(width: 1, height: 1)
    var times: [CMTime] = []
    var tt = 0.0
    while tt <= duration { times.append(CMTime(seconds: tt, preferredTimescale: 600)); tt += sampleInterval }
    var idxByTime: [Int64: Int] = [:]
    for (i, ct) in times.enumerated() { idxByTime[ct.value] = i }
    var byIdx: [Int: (Double, [CGRect])] = [:]
    for await r in gen.images(for: times) {
        let ft = r.requestedTime.seconds
        let idx = idxByTime[r.requestedTime.value] ?? Int((ft / sampleInterval).rounded())
        guard let img = try? r.image else { byIdx[idx] = (ft, []); continue }
        let obs = (try? await request.perform(on: img)) ?? []
        var boxes: [CGRect] = []
        for o in obs where o.landmarks != nil { boxes.append(o.boundingBox.toImageCoordinates(unit, origin: .upperLeft)) }
        boxes.sort { $0.midX < $1.midX }
        byIdx[idx] = (ft, boxes)
    }
    print("\n=== per-sample faces (x=midX frac, w=width frac) ===")
    for i in 0..<times.count {
        guard let s = byIdx[i] else { continue }
        let desc = s.1.map { String(format: "(x%.2f w%.2f)", $0.midX, $0.width) }.joined(separator: " ")
        let straddle = (s.1.count == 2 && s.1[0].midX < 0.5 && s.1[1].midX >= 0.5) ? " STRADDLE" : (s.1.count == 2 ? " SAME-SIDE" : "")
        print(String(format: "  t=%6.2f  n=%d  %@%@", s.0, s.1.count, desc, straddle))
    }
    let segs = await analyzeStackSegments(path: path, sampleInterval: sampleInterval, maxSeconds: maxSeconds, debounce: debounce)
    print("\n=== derived segments ===")
    for (i, seg) in segs.enumerated() {
        switch seg.layout {
        case .single(let cx):
            print(String(format: "  [%2d] %7.2fs  single  cx=%.3f", i, seg.start, Double(cx)))
        case .stacked(let top, let bot):
            print(String(format: "  [%2d] %7.2fs  stacked  topX=%.3f topW=%.3f  botX=%.3f botW=%.3f",
                         i, seg.start, Double(top.centerX), Double(top.faceWidth), Double(bot.centerX), Double(bot.faceWidth)))
        }
    }
}

/// Driver: analyze then render, with a short segment summary.
func runStackExport(path: String, outPath: String, sampleInterval: Double, debounce: Double,
                    renderHeight: Int, bandZoom: CGFloat, maxSeconds: Double) async {
    print("\n=== stacked export ===")
    print(String(format: "analyzing (interval %.2fs, debounce %.2fs)…", sampleInterval, debounce))
    let segs = await analyzeStackSegments(path: path, sampleInterval: sampleInterval, maxSeconds: maxSeconds, debounce: debounce)
    let stackedCount = segs.filter { if case .stacked = $0.layout { return true } else { return false } }.count
    print("derived \(segs.count) segments (\(stackedCount) stacked, \(segs.count - stackedCount) single)")
    for (i, s) in segs.prefix(20).enumerated() {
        switch s.layout {
        case .single(let cx): print(String(format: "  [%2d] %7.2fs  single  cx=%.3f", i, s.start, Double(cx)))
        case .stacked(let top, let bot): print(String(format: "  [%2d] %7.2fs  stacked  topFaceW=%.3f botFaceW=%.3f", i, s.start, Double(top.faceWidth), Double(bot.faceWidth)))
        }
    }
    if segs.count > 20 { print("  … \(segs.count - 20) more") }
    let lastPct = LockedDouble()
    do {
        try await renderStacked(path: path, segments: segs, outURL: URL(fileURLWithPath: outPath),
                                renderHeight: renderHeight, bandZoom: bandZoom, maxSeconds: maxSeconds) { p in
            if p - lastPct.get() >= 0.1 || p >= 1.0 {
                lastPct.set(p)
                FileHandle.standardError.write(Data(String(format: "  render %.0f%%\n", p * 100).utf8))
            }
        }
        print("export complete: \(outPath)")
    } catch {
        print("export FAILED: \(error.localizedDescription)")
    }
}

// MARK: - clipcrop: one-pass per-clip cut + 9:16 recrop (production engine)

/// One-pass per-clip cut + 9:16 stacked/single recrop, driven by externally
/// computed edges + fades (Python `clip_edges`). Output is native-resolution
/// (zero downscale) 9:16 floored at `minHeight`, h264 `.mp4`. The cut + fades use
/// the supplied times exactly (high `CMTime` precision) so the upstream word-snap
/// isn't shaved. Single `AVAssetExportSession` encode — no landscape intermediate.
func renderClipCrop(parent: String, segments: [StackSegment], start: Double, end: Double,
                    fadeIn: Double, fadeOut: Double, outURL: URL, minHeight: Int,
                    bandZoom: CGFloat) async throws {
    let asset = AVURLAsset(url: URL(fileURLWithPath: parent))
    guard let track = try await asset.loadTracks(withMediaType: .video).first else {
        throw StackExportError(message: "No video track.")
    }
    let natural = try await track.load(.naturalSize)
    let transform = try await track.load(.preferredTransform)
    let displayed = natural.applying(transform)
    let srcW = abs(displayed.width), srcH = abs(displayed.height)
    guard srcW > 0, srcH > 0 else { throw StackExportError(message: "Bad source size.") }
    let fps = (try? await track.load(.nominalFrameRate)) ?? 30
    let durationSec = (try await asset.load(.duration)).seconds
    guard start >= 0, end > start, start < durationSec else {
        throw StackExportError(message: "Bad range [\(start), \(end)] for duration \(durationSec).")
    }
    let clampedEnd = min(end, durationSec)

    func even(_ v: CGFloat) -> CGFloat { (v / 2).rounded() * 2 }
    let outH = even(max(srcH, CGFloat(minHeight)))        // native height, never downscaled, floored at minHeight
    let outW = even(outH * 9.0 / 16.0)
    let canvas = CGRect(x: 0, y: 0, width: outW, height: outH)

    let comp = try await makeStackComposition(asset: asset, segments: segments, canvas: canvas,
                                              srcW: srcW, srcH: srcH, fps: fps, bandZoom: bandZoom,
                                              sizeByHeight: true, headBias: 0.0)

    // Audio fades in the SAME export, in parent time — identical to the Python
    // afade over [0, fadeIn] / [dur-fadeOut, dur] of the cut.
    let ts: CMTimeScale = 48000
    var audioMix: AVMutableAudioMix?
    let audioTracks = try await asset.loadTracks(withMediaType: .audio)
    if let aTrack = audioTracks.first, fadeIn > 0 || fadeOut > 0 {
        let params = AVMutableAudioMixInputParameters(track: aTrack)
        if fadeIn > 0 {
            params.setVolumeRamp(fromStartVolume: 0, toEndVolume: 1,
                timeRange: CMTimeRange(start: CMTime(seconds: start, preferredTimescale: ts),
                                       duration: CMTime(seconds: fadeIn, preferredTimescale: ts)))
        }
        if fadeOut > 0 {
            params.setVolumeRamp(fromStartVolume: 1, toEndVolume: 0,
                timeRange: CMTimeRange(start: CMTime(seconds: clampedEnd - fadeOut, preferredTimescale: ts),
                                       duration: CMTime(seconds: fadeOut, preferredTimescale: ts)))
        }
        let mix = AVMutableAudioMix()
        mix.inputParameters = [params]
        audioMix = mix
    }

    guard let session = AVAssetExportSession(asset: asset, presetName: AVAssetExportPresetHighestQuality) else {
        throw StackExportError(message: "Could not create export session.")
    }
    guard session.supportedFileTypes.contains(.mp4) else {
        throw StackExportError(message: "Export session does not support .mp4 output.")
    }
    session.videoComposition = comp
    session.audioMix = audioMix
    session.timeRange = CMTimeRange(start: CMTime(seconds: start, preferredTimescale: ts),
                                    duration: CMTime(seconds: clampedEnd - start, preferredTimescale: ts))

    // Write to a temp sibling and atomically move on success — never leave a
    // partial at the final path. export(to:as:) throws on any failure.
    let tmpURL = URL(fileURLWithPath: outURL.path + ".tmp.mp4")
    for u in [tmpURL, outURL] where FileManager.default.fileExists(atPath: u.path) {
        try FileManager.default.removeItem(at: u)
    }
    do {
        try await session.export(to: tmpURL, as: .mp4)
    } catch {
        try? FileManager.default.removeItem(at: tmpURL)
        throw error
    }
    try FileManager.default.moveItem(at: tmpURL, to: outURL)
}

/// Driver: detect heads over [start,end], apply the YOLO-derived croppability
/// guard, segment, then one-pass cut+recrop to a native-res 9:16 .mp4. Throws on
/// any failure (the CLI translates that to a nonzero exit for the Python caller).
func runClipCrop(parent: String, start: Double, end: Double, fadeIn: Double, fadeOut: Double,
                 outURL: URL, minHeight: Int, config: HeadDetectionConfig,
                 debounce: Double, bandZoom: CGFloat) async throws {
    let samples = try await HeadDetector.detect(path: parent, config: config,
                                                maxSeconds: end - start, startSeconds: start)
    // Croppability guard: if heads are present in too few sampled frames the clip
    // is b-roll / screen content where head-cropping is meaningless — render a
    // neutral center 9:16 and flag it. Conservative threshold: only genuinely
    // head-less clips trip it (tune later).
    let total = max(samples.count, 1)
    let withHead = samples.filter { !$0.faces.isEmpty }.count
    let presentFrac = Double(withHead) / Double(total)
    var segments: [StackSegment]
    if presentFrac < 0.3 {
        FileHandle.standardError.write(Data(String(format: "CROPPABILITY=low present=%.2f\n", presentFrac).utf8))
        segments = [StackSegment(start: start, layout: .single(centerX: 0.5))]
    } else {
        let tuples = samples.map { (t: $0.t, faces: $0.faces) }
        segments = segmentsFromSamples(tuples, sampleInterval: config.interval, debounce: debounce)
        if segments.isEmpty { segments = [StackSegment(start: start, layout: .single(centerX: 0.5))] }
    }
    try await renderClipCrop(parent: parent, segments: segments, start: start, end: end,
                             fadeIn: fadeIn, fadeOut: fadeOut, outURL: outURL,
                             minHeight: minHeight, bandZoom: bandZoom)
}
