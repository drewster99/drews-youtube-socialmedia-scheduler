import Foundation
import AVFoundation
import Vision
import CoreGraphics
import CoreMedia

// Crop-DECISION study: probe the cross-resolution disagreement and ways to
// reduce it. Runs a DENSE, contiguous pass at each width through the REAL
// VideoProcessor.classifyFrame, diffing each width's committed L/C/R trajectory
// against full-res. It evaluates several selection tunings at once (absolute
// margin, relative margin, longer activity window) on the same deterministic
// Vision results, characterizes the disagreement by run length (jitter vs
// sustained), and measures the ACTIVENESS distribution so the margins can be
// grounded in real numbers instead of guesses.

/// Selection tunings evaluated side by side. Relative configs carry a small
/// absolute floor so a near-zero incumbent can't be displaced by noise.
let cropConfigs: [(String, VideoProcessor.ClassifyTuning)] = [
    ("baseline",      VideoProcessor.ClassifyTuning()),
    ("abs0.02",       VideoProcessor.ClassifyTuning(absoluteMargin: 0.02)),
    ("rel0.5",        VideoProcessor.ClassifyTuning(absoluteMargin: 0.002, relativeMargin: 0.5)),
    ("rel1.0",        VideoProcessor.ClassifyTuning(absoluteMargin: 0.002, relativeMargin: 1.0)),
    ("rel2.0",        VideoProcessor.ClassifyTuning(absoluteMargin: 0.002, relativeMargin: 2.0)),
    ("win1.0",        VideoProcessor.ClassifyTuning(activityWindowSeconds: 1.0)),
    ("win1.0+rel1.0", VideoProcessor.ClassifyTuning(absoluteMargin: 0.002, relativeMargin: 1.0, activityWindowSeconds: 1.0)),
    ("win1.25",       VideoProcessor.ClassifyTuning(activityWindowSeconds: 1.25)),
    ("win1.25+rel1.0", VideoProcessor.ClassifyTuning(absoluteMargin: 0.002, relativeMargin: 1.0, activityWindowSeconds: 1.25)),
]

struct CropSample {
    var pos: [FacePosition]   // committed side per config
    var frac: [Double]        // eased center per config
}

/// One width's full trajectory plus the baseline activeness stats sampled from it.
struct WidthResult {
    var samples: [CropSample]
    var actMax: [Double]    // top activeness per frame (baseline config)
    var actGap: [Double]    // top1 - top2 activeness on multi-face frames (baseline)
}

private func pctl(_ xs: [Double], _ p: Double) -> Double {
    guard !xs.isEmpty else { return .nan }
    let s = xs.sorted()
    if s.count == 1 { return s[0] }
    let idx = p / 100 * Double(s.count - 1)
    let lo = Int(idx.rounded(.down)), hi = Int(idx.rounded(.up))
    if lo == hi { return s[lo] }
    return s[lo] * (1 - (idx - Double(lo))) + s[hi] * (idx - Double(lo))
}

private func rawFaces(_ buffer: CVPixelBuffer, _ request: DetectFaceLandmarksRequest, imageSize: CGSize) async -> [RawFace] {
    guard let observations = try? await request.perform(on: buffer) else { return [] }
    var faces: [RawFace] = []
    for obs in observations {
        guard let lm = obs.landmarks else { continue }
        let outer = lm.outerLips
        let inner = lm.innerLips
        faces.append(RawFace(
            boundingBox: obs.boundingBox.toImageCoordinates(imageSize, origin: .upperLeft),
            confidence: obs.confidence,
            outerLips: outer.pointsInImageCoordinates(imageSize, origin: .upperLeft),
            innerLips: inner.pointsInImageCoordinates(imageSize, origin: .upperLeft),
            outerClassification: String(describing: outer.pointsClassification),
            innerClassification: String(describing: inner.pointsClassification),
            outerIsClosed: outer.pointsClassification == .closedPath,
            innerIsClosed: inner.pointsClassification == .closedPath,
            outerPrecision: outer.precisionEstimatesPerPoint ?? [],
            innerPrecision: inner.precisionEstimatesPerPoint ?? []))
    }
    return faces
}

private func cropTrajectory(path: String, width: Int, transform: CGAffineTransform, displayed: CGSize,
                            fps: Float, assetDuration: CMTime, step: Double, maxSeconds: Double,
                            anxiety: Double) async -> WidthResult {
    var result = WidthResult(samples: [], actMax: [], actGap: [])
    let asset = AVURLAsset(url: URL(fileURLWithPath: path))
    guard let track = try? await asset.loadTracks(withMediaType: .video).first,
          let reader = try? AVAssetReader(asset: asset) else {
        print("  ERROR width \(width): could not open asset/track"); return result
    }
    let output = AVAssetReaderVideoCompositionOutput(videoTracks: [track], videoSettings: [
        kCVPixelBufferPixelFormatTypeKey as String: Int(kCVPixelFormatType_32BGRA)
    ])
    output.videoComposition = scaledComposition(track: track, transform: transform, displayed: displayed,
                                                width: width, fps: fps, assetDuration: assetDuration)
    output.alwaysCopiesSampleData = false
    guard reader.canAdd(output) else { print("  ERROR width \(width): cannot add output"); return result }
    reader.add(output)
    guard reader.startReading() else {
        print("  ERROR width \(width): startReading failed (\(reader.error?.localizedDescription ?? "?"))")
        return result
    }

    let request = DetectFaceLandmarksRequest()
    var states = cropConfigs.map { _ in VideoProcessor.ClassifyState() }
    var nextSample = step / 2
    while reader.status == .reading {
        guard let sample = output.copyNextSampleBuffer() else { break }
        let pts = CMSampleBufferGetPresentationTimeStamp(sample).seconds
        if pts > maxSeconds { break }
        guard pts >= nextSample else { continue }
        guard let buf = CMSampleBufferGetImageBuffer(sample) else { continue }
        let imgSize = CGSize(width: CVPixelBufferGetWidth(buf), height: CVPixelBufferGetHeight(buf))
        let faces = await rawFaces(buf, request, imageSize: imgSize)
        let rf = RawFrame(time: pts, imageSize: imgSize, analysisMs: 0, faces: faces)
        var poss: [FacePosition] = []
        var fracs: [Double] = []
        for ci in cropConfigs.indices {
            let fa = VideoProcessor.classifyFrame(rf, mode: .center, cropThreshold: anxiety, step: step,
                                                  metric: .movement, tuning: cropConfigs[ci].1,
                                                  state: &states[ci])
            poss.append(fa.cropPosition)
            fracs.append(imgSize.width > 0 ? Double(fa.actualCenterX) / Double(imgSize.width) : 0.5)
            if ci == 0 {
                let acts = fa.faces.map { Double($0.activeness) }.sorted(by: >)
                if let top = acts.first { result.actMax.append(top) }
                if acts.count >= 2 { result.actGap.append(acts[0] - acts[1]) }
            }
        }
        result.samples.append(CropSample(pos: poss, frac: fracs))
        nextSample += step
    }
    reader.cancelReading()
    return result
}

private func posLabel(_ p: FacePosition) -> String {
    switch p {
    case .left: return "L"
    case .center: return "C"
    case .right: return "R"
    default: return "?"
    }
}

func runCropSizeStudy(path: String, widths: [Int], step: Double, anxiety: Double, maxSeconds: Double) async {
    let asset = AVURLAsset(url: URL(fileURLWithPath: path))
    guard let track = try? await asset.loadTracks(withMediaType: .video).first,
          let natural = try? await track.load(.naturalSize),
          let transform = try? await track.load(.preferredTransform),
          let assetDuration = try? await asset.load(.duration) else {
        print("ERROR: could not load source track properties."); return
    }
    let fps = (try? await track.load(.nominalFrameRate)) ?? 0
    let displayed = natural.applying(transform)
    let dw = abs(displayed.width), dh = abs(displayed.height)
    let window = min(maxSeconds, assetDuration.seconds)
    guard fps > 0, dw > 0, dh > 0, window > 0 else { print("ERROR: invalid source."); return }
    let disp = CGSize(width: dw, height: dh)

    print("\n=== crop-decision study: selection tunings + activeness distribution ===")
    print(String(format: "window %.0fs  step %.3fs  anxiety %.1fs  (~%d samples)", window, step, anxiety, Int(window / step)))
    print("widths: \(widths.map(String.init).joined(separator: " "))  reference = largest")
    print("configs: \(cropConfigs.map { $0.0 }.joined(separator: " "))\n")

    var results: [Int: WidthResult] = [:]
    for w in widths {
        let t0 = DispatchTime.now()
        results[w] = await cropTrajectory(path: path, width: w, transform: transform, displayed: disp,
                                          fps: fps, assetDuration: assetDuration, step: step,
                                          maxSeconds: window, anxiety: anxiety)
        let secs = Double(DispatchTime.now().uptimeNanoseconds &- t0.uptimeNanoseconds) / 1_000_000_000
        print(String(format: "  width %4d  %d samples  done in %.1fs", w, results[w]?.samples.count ?? 0, secs))
        fflush(stdout)
    }

    let refWidth = widths.max() ?? widths[0]
    guard let refR = results[refWidth], !refR.samples.isEmpty else { print("ERROR: no reference."); return }
    let ref = refR.samples
    let cols = widths.filter { $0 != refWidth }

    // Activeness distribution (full-res, baseline config) — grounds the margins.
    print("\n--- activeness distribution (full-res, baseline) — grounds the margin choice ---")
    print("quantity                       p5      p25      p50      p75      p90      p95")
    func distLine(_ label: String, _ xs: [Double]) {
        var line = label.padding(toLength: 28, withPad: " ", startingAt: 0)
        for p in [5.0, 25, 50, 75, 90, 95] { line += String(format: " %8.4f", pctl(xs, p)) }
        print(line)
    }
    distLine("max activeness / frame", refR.actMax)
    distLine("top1-top2 gap (2+ faces)", refR.actGap)
    print("(a margin near the top1-top2 p50 flips ~half the competitive frames)")

    // L/C/R disagreement vs full-res, per config, per width.
    print("\n--- L/C/R disagreement vs full-res, by config (disagree %) ---")
    var head = "config".padding(toLength: 16, withPad: " ", startingAt: 0)
    for w in cols { head += String(format: " %7d", w) }
    head += "   | refL/C/R%"
    print(head)
    for ci in cropConfigs.indices {
        var line = cropConfigs[ci].0.padding(toLength: 16, withPad: " ", startingAt: 0)
        for w in cols {
            guard let t = results[w]?.samples else { line += "      —"; continue }
            let n = min(t.count, ref.count)
            var dis = 0
            for i in 0..<n where t[i].pos[ci] != ref[i].pos[ci] { dis += 1 }
            line += String(format: " %6.2f ", n > 0 ? 100.0 * Double(dis) / Double(n) : 0)
        }
        let rn = Double(ref.count)
        let l = Double(ref.filter { posLabel($0.pos[ci]) == "L" }.count) / rn * 100
        let c = Double(ref.filter { posLabel($0.pos[ci]) == "C" }.count) / rn * 100
        let r = Double(ref.filter { posLabel($0.pos[ci]) == "R" }.count) / rn * 100
        line += String(format: "   | %.0f/%.0f/%.0f", l, c, r)
        print(line)
    }

    // Disagreement run-length at baseline (config 0): jitter vs sustained.
    print("\n--- disagreement run-length at baseline (jitter vs sustained) ---")
    print("width   runs  median(s)  p95(s)   max(s)   %samples runs<=0.3s   %samples runs>1.0s")
    for w in cols {
        guard let t = results[w]?.samples else { continue }
        let n = min(t.count, ref.count)
        var runs: [Int] = []
        var cur = 0
        for i in 0..<n {
            if t[i].pos[0] != ref[i].pos[0] { cur += 1 }
            else if cur > 0 { runs.append(cur); cur = 0 }
        }
        if cur > 0 { runs.append(cur) }
        let total = runs.reduce(0, +)
        let lens = runs.map { Double($0) * step }
        let shortS = runs.filter { Double($0) * step <= 0.3 }.reduce(0, +)
        let longS = runs.filter { Double($0) * step > 1.0 }.reduce(0, +)
        print(String(format: "%5d  %5d   %7.3f  %7.3f  %7.3f         %6.1f               %6.1f",
                     w, runs.count, pctl(lens, 50), pctl(lens, 95), lens.max() ?? 0,
                     total > 0 ? 100.0 * Double(shortS) / Double(total) : 0,
                     total > 0 ? 100.0 * Double(longS) / Double(total) : 0))
    }
}
