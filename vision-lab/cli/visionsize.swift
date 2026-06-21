import Foundation
import AVFoundation
import Vision
import CoreGraphics
import CoreMedia

// Vision image-size sensitivity study. Decodes the SAME sampled frames at a
// range of widths through the production downscaler (the AVFoundation video
// compositor at renderSize=W), runs face-landmark Vision at each size, and
// measures how the algorithm's actual inputs (face count/center/size and the
// lip geometry the classifier uses) deviate from a full-resolution reference.
//
// Full-res is run multiple times; the per-metric median of those runs is the
// reference, and the spread of those runs vs the median is the noise floor that
// every smaller width is judged against. All geometry is in normalized [0,1]
// image coordinates, so no rescaling is needed and the lip-height ratio is
// scale-invariant. Lip math reuses VideoProcessor.innerLipHeight verbatim.

/// One face's normalized, algorithm-relevant geometry at a given image size.
struct FaceMetrics {
    var center: CGPoint
    var size: CGSize
    var outerLipCenter: CGPoint
    var innerLipCenter: CGPoint
    var outerLipHeight: CGFloat
    var innerLipHeight: CGFloat
    var ratio: CGFloat
}

private func meanPoint(_ pts: [CGPoint]) -> CGPoint {
    guard !pts.isEmpty else { return .zero }
    var sx: CGFloat = 0, sy: CGFloat = 0
    for p in pts { sx += p.x; sy += p.y }
    return CGPoint(x: sx / CGFloat(pts.count), y: sy / CGFloat(pts.count))
}

/// Run Vision once on a frame and map each landmarked face to normalized metrics.
private func visionFacesNormalized(_ buffer: CVPixelBuffer, _ request: DetectFaceLandmarksRequest) async -> [FaceMetrics] {
    let unit = CGSize(width: 1, height: 1)   // -> image-normalized [0,1] coordinates
    guard let observations = try? await request.perform(on: buffer) else { return [] }
    var out: [FaceMetrics] = []
    for obs in observations {
        guard let lm = obs.landmarks else { continue }
        let bbox = obs.boundingBox.toImageCoordinates(unit, origin: .upperLeft)
        let outer = lm.outerLips.pointsInImageCoordinates(unit, origin: .upperLeft)
        let inner = lm.innerLips.pointsInImageCoordinates(unit, origin: .upperLeft)
        let olh = VideoProcessor.innerLipHeight(outer)
        let ilh = VideoProcessor.innerLipHeight(inner)
        out.append(FaceMetrics(
            center: CGPoint(x: bbox.midX, y: bbox.midY),
            size: CGSize(width: bbox.width, height: bbox.height),
            outerLipCenter: meanPoint(outer),
            innerLipCenter: meanPoint(inner),
            outerLipHeight: olh,
            innerLipHeight: ilh,
            ratio: olh > 0 ? ilh / olh : 0))
    }
    return out
}

/// Greedy nearest-center one-to-one matching. Called only when counts are equal,
/// so every reference face gets a partner. Adequate for the ≤3 faces we see.
private func matchByCenter(_ ref: [FaceMetrics], _ cand: [FaceMetrics]) -> [(Int, Int)] {
    var used = Set<Int>()
    var pairs: [(Int, Int)] = []
    for (ri, r) in ref.enumerated() {
        var best = -1
        var bestD = Double.greatestFiniteMagnitude
        for (ci, c) in cand.enumerated() where !used.contains(ci) {
            let d = hypot(Double(r.center.x - c.center.x), Double(r.center.y - c.center.y))
            if d < bestD { bestD = d; best = ci }
        }
        if best >= 0 { used.insert(best); pairs.append((ri, best)) }
    }
    return pairs
}

private func median(_ xs: [Double]) -> Double {
    guard !xs.isEmpty else { return .nan }
    let s = xs.sorted()
    let n = s.count
    return n % 2 == 1 ? s[n / 2] : (s[n / 2 - 1] + s[n / 2]) / 2
}

private func medianFaceMetrics(_ faces: [FaceMetrics]) -> FaceMetrics {
    func m(_ key: (FaceMetrics) -> CGFloat) -> CGFloat { CGFloat(median(faces.map { Double(key($0)) })) }
    return FaceMetrics(
        center: CGPoint(x: m { $0.center.x }, y: m { $0.center.y }),
        size: CGSize(width: m { $0.size.width }, height: m { $0.size.height }),
        outerLipCenter: CGPoint(x: m { $0.outerLipCenter.x }, y: m { $0.outerLipCenter.y }),
        innerLipCenter: CGPoint(x: m { $0.innerLipCenter.x }, y: m { $0.innerLipCenter.y }),
        outerLipHeight: m { $0.outerLipHeight },
        innerLipHeight: m { $0.innerLipHeight },
        ratio: m { $0.ratio })
}

/// Per-width accumulators for the deviation distributions.
private struct WidthAccum {
    var faceCenter: [Double] = []
    var faceWidth: [Double] = []
    var faceHeight: [Double] = []
    var outerLipCenter: [Double] = []
    var innerLipCenter: [Double] = []
    var outerLipHeight: [Double] = []
    var innerLipHeight: [Double] = []
    var ratio: [Double] = []
    var outerLipHeightSigned: [Double] = []
    var innerLipHeightSigned: [Double] = []
    var ratioSigned: [Double] = []
    var faceCountErrors = 0
    var comparisons = 0

    mutating func add(ref r: FaceMetrics, cand c: FaceMetrics) {
        faceCenter.append(hypot(Double(r.center.x - c.center.x), Double(r.center.y - c.center.y)))
        faceWidth.append(abs(Double(r.size.width - c.size.width)))
        faceHeight.append(abs(Double(r.size.height - c.size.height)))
        outerLipCenter.append(hypot(Double(r.outerLipCenter.x - c.outerLipCenter.x), Double(r.outerLipCenter.y - c.outerLipCenter.y)))
        innerLipCenter.append(hypot(Double(r.innerLipCenter.x - c.innerLipCenter.x), Double(r.innerLipCenter.y - c.innerLipCenter.y)))
        outerLipHeight.append(abs(Double(r.outerLipHeight - c.outerLipHeight)))
        innerLipHeight.append(abs(Double(r.innerLipHeight - c.innerLipHeight)))
        ratio.append(abs(Double(r.ratio - c.ratio)))
        outerLipHeightSigned.append(Double(c.outerLipHeight - r.outerLipHeight))
        innerLipHeightSigned.append(Double(c.innerLipHeight - r.innerLipHeight))
        ratioSigned.append(Double(c.ratio - r.ratio))
    }
}

private func percentile(_ sorted: [Double], _ p: Double) -> Double {
    guard !sorted.isEmpty else { return .nan }
    if sorted.count == 1 { return sorted[0] }
    let idx = p / 100 * Double(sorted.count - 1)
    let lo = Int(idx.rounded(.down)), hi = Int(idx.rounded(.up))
    if lo == hi { return sorted[lo] }
    return sorted[lo] * (1 - (idx - Double(lo))) + sorted[hi] * (idx - Double(lo))
}

private func statRow(_ label: String, _ values: [[Double]], scale: Double, signed: Bool = false) -> String {
    var line = label.padding(toLength: 14, withPad: " ", startingAt: 0)
    for v in values {
        if v.isEmpty { line += "      —"; continue }
        let sorted = v.sorted()
        let stat = signed ? (sorted.reduce(0, +) / Double(sorted.count)) : median(sorted)
        line += String(format: " %8.3f", stat * scale)
    }
    return line
}

/// Build a video composition that orients (preferred transform) and renders at
/// the requested width, preserving aspect — the same downscaler production uses.
func scaledComposition(track: AVAssetTrack, transform: CGAffineTransform, displayed: CGSize, width: Int, fps: Float, assetDuration: CMTime) -> AVMutableVideoComposition {
    let s = CGFloat(width) / displayed.width
    func even(_ v: CGFloat) -> Int { let r = Int(v.rounded()); return r - (r % 2) }
    let layer = AVMutableVideoCompositionLayerInstruction(assetTrack: track)
    layer.setTransform(transform.concatenating(CGAffineTransform(scaleX: s, y: s)), at: .zero)
    let instruction = AVMutableVideoCompositionInstruction()
    instruction.timeRange = CMTimeRange(start: .zero, duration: assetDuration)
    instruction.layerInstructions = [layer]
    let comp = AVMutableVideoComposition()
    comp.renderSize = CGSize(width: width, height: even(displayed.height * s))
    comp.frameDuration = CMTimeMakeWithSeconds(1.0 / Double(fps), preferredTimescale: 600)
    comp.instructions = [instruction]
    return comp
}

/// Decode the sampled frames at one width and run Vision `runs` times per frame.
/// Returns [frameIndex][runIndex] = [FaceMetrics]. The track and reader MUST
/// come from the same asset instance, so the track is loaded here.
private func decodeWidthPass(path: String, sampleTimes: [Double], width: Int, runs: Int,
                             transform: CGAffineTransform, displayed: CGSize,
                             fps: Float, assetDuration: CMTime) async -> [[[FaceMetrics]]] {
    let n = sampleTimes.count
    var result: [[[FaceMetrics]]] = Array(repeating: [], count: n)
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

    var ptr = 0
    outer: while reader.status == .reading, ptr < n {
        guard let sample = output.copyNextSampleBuffer() else { break }
        let pts = CMSampleBufferGetPresentationTimeStamp(sample).seconds
        while ptr < n, sampleTimes[ptr] <= pts {
            guard let buf = CMSampleBufferGetImageBuffer(sample) else { break }
            var perRun: [[FaceMetrics]] = []
            for _ in 0..<runs { perRun.append(await visionFacesNormalized(buf, request)) }
            result[ptr] = perRun
            ptr += 1
            if ptr >= n { break outer }
        }
    }
    reader.cancelReading()
    return result
}

/// Run the full study and print the comparison tables. `maxSeconds` caps the
/// window the samples are spread across (use a small value for a quick smoke
/// test; the decode for each width stops once the last sample is reached).
func runVisionSizeStudy(path: String, widths: [Int], sampleCount: Int, refRuns: Int, maxSeconds: Double) async {
    let asset = AVURLAsset(url: URL(fileURLWithPath: path))
    guard let track = try? await asset.loadTracks(withMediaType: .video).first,
          let natural = try? await track.load(.naturalSize),
          let transform = try? await track.load(.preferredTransform),
          let assetDuration = try? await asset.load(.duration) else {
        print("ERROR: could not load source track properties.")
        return
    }
    let fps = (try? await track.load(.nominalFrameRate)) ?? 0
    let displayed = natural.applying(transform)
    let dw = abs(displayed.width), dh = abs(displayed.height)
    let duration = min(maxSeconds, assetDuration.seconds)
    guard fps > 0, dw > 0, dh > 0, duration > 0 else {
        print("ERROR: invalid source duration / size / fps.")
        return
    }
    let disp = CGSize(width: dw, height: dh)

    let n = sampleCount
    let sampleTimes = (0..<n).map { (Double($0) + 0.5) * duration / Double(n) }

    print("\n=== Vision image-size sensitivity study ===")
    print(String(format: "source %.0fx%.0f  %.2ffps  %.0fs", dw, dh, fps, duration))
    print("samples: \(n) frames, uniform across the video")
    print("widths: \(widths.map(String.init).joined(separator: " "))  (full-res run \(refRuns)x for reference + noise floor)")
    print("all values normalized [0,1] image coords, shown x1000 (ratio shown raw)\n")

    // Reference width is the largest (assumed first / full-res), run refRuns times.
    let refWidth = widths.max() ?? widths[0]

    // Decode every width. The reference width gets refRuns SEPARATE decodes (not
    // repeated Vision on one buffer) so the noise floor reflects real decode+Vision
    // non-determinism and can't be hidden by any per-buffer caching.
    var perWidth: [Int: [[[FaceMetrics]]]] = [:]
    for w in widths {
        let t0 = DispatchTime.now()
        if w == refWidth {
            var runsData: [[[FaceMetrics]]] = Array(repeating: [], count: n)
            for _ in 0..<refRuns {
                let single = await decodeWidthPass(path: path, sampleTimes: sampleTimes, width: w, runs: 1,
                                                   transform: transform, displayed: disp,
                                                   fps: fps, assetDuration: assetDuration)
                for f in 0..<n { runsData[f].append(single[f].first ?? []) }
            }
            perWidth[w] = runsData
            let secs = Double(DispatchTime.now().uptimeNanoseconds &- t0.uptimeNanoseconds) / 1_000_000_000
            print(String(format: "  width %4d  (%d separate decodes for reference)  done in %.1fs", w, refRuns, secs))
        } else {
            perWidth[w] = await decodeWidthPass(path: path, sampleTimes: sampleTimes, width: w, runs: 1,
                                                transform: transform, displayed: disp,
                                                fps: fps, assetDuration: assetDuration)
            let secs = Double(DispatchTime.now().uptimeNanoseconds &- t0.uptimeNanoseconds) / 1_000_000_000
            print(String(format: "  width %4d  (1 decode)  done in %.1fs", w, secs))
        }
        fflush(stdout)
    }

    guard let refRunsData = perWidth[refWidth] else { print("ERROR: no reference data."); return }

    // Build the per-frame reference: most-common face count across the refRuns,
    // then component-wise median of the agreeing runs (faces matched by center).
    var reference: [[FaceMetrics]] = Array(repeating: [], count: n)
    var refFrameValid = [Bool](repeating: false, count: n)
    for f in 0..<n {
        let runsF = refRunsData[f]
        guard !runsF.isEmpty else { continue }
        let counts = runsF.map { $0.count }
        let refCount = Int(median(counts.map(Double.init)))
        let valid = runsF.filter { $0.count == refCount }
        guard refCount > 0, let anchor = valid.first else {
            refFrameValid[f] = true   // a valid zero-face frame is still a valid comparison
            continue
        }
        var perPos: [[FaceMetrics]] = Array(repeating: [], count: refCount)
        for run in valid {
            for (a, c) in matchByCenter(anchor, run) { perPos[a].append(run[c]) }
        }
        reference[f] = perPos.map { medianFaceMetrics($0) }
        refFrameValid[f] = true
    }

    // Accumulate deviations per width.
    var accum: [Int: WidthAccum] = [:]
    for w in widths { accum[w] = WidthAccum() }

    for w in widths {
        guard let data = perWidth[w] else { continue }
        var a = WidthAccum()
        for f in 0..<n where refFrameValid[f] {
            let ref = reference[f]
            // For full-res, every run is a sample; for others, the single run.
            let candidates = (w == refWidth) ? data[f] : [data[f].first ?? []]
            for cand in candidates {
                a.comparisons += 1
                if cand.count != ref.count { a.faceCountErrors += 1; continue }
                if ref.isEmpty { continue }
                for (ri, ci) in matchByCenter(ref, cand) { a.add(ref: ref[ri], cand: cand[ci]) }
            }
        }
        accum[w] = a
    }

    let cols = widths
    func header() -> String {
        var h = "".padding(toLength: 14, withPad: " ", startingAt: 0)
        for w in cols { h += String(format: " %8d", w) }
        return h
    }

    // Face-count error rate.
    print("\n--- face-count error (% of comparisons where count != reference) ---")
    print(header())
    var fcLine = "#face error %".padding(toLength: 14, withPad: " ", startingAt: 0)
    for w in cols {
        let a = accum[w] ?? WidthAccum()
        let pct = a.comparisons > 0 ? 100.0 * Double(a.faceCountErrors) / Double(a.comparisons) : 0
        fcLine += String(format: " %8.2f", pct)
    }
    print(fcLine)

    // Headline: median deviation per metric.
    print("\n--- median deviation from full-res reference (normalized x1000; ratio raw) ---")
    print(header())
    print(statRow("face center", cols.map { accum[$0]?.faceCenter ?? [] }, scale: 1000))
    print(statRow("face width", cols.map { accum[$0]?.faceWidth ?? [] }, scale: 1000))
    print(statRow("face height", cols.map { accum[$0]?.faceHeight ?? [] }, scale: 1000))
    print(statRow("outer lip ctr", cols.map { accum[$0]?.outerLipCenter ?? [] }, scale: 1000))
    print(statRow("inner lip ctr", cols.map { accum[$0]?.innerLipCenter ?? [] }, scale: 1000))
    print(statRow("outer lip ht", cols.map { accum[$0]?.outerLipHeight ?? [] }, scale: 1000))
    print(statRow("inner lip ht", cols.map { accum[$0]?.innerLipHeight ?? [] }, scale: 1000))
    print(statRow("ratio err", cols.map { accum[$0]?.ratio ?? [] }, scale: 1))

    // Signed bias for the lip metrics (does smaller size push the mouth closed?).
    print("\n--- signed bias: mean(width - reference) (normalized x1000; ratio raw) ---")
    print(header())
    print(statRow("outer lip ht", cols.map { accum[$0]?.outerLipHeightSigned ?? [] }, scale: 1000, signed: true))
    print(statRow("inner lip ht", cols.map { accum[$0]?.innerLipHeightSigned ?? [] }, scale: 1000, signed: true))
    print(statRow("ratio", cols.map { accum[$0]?.ratioSigned ?? [] }, scale: 1, signed: true))

    // Detail: full distribution for the headline metric (ratio) and inner lip height.
    func detail(_ name: String, _ pick: (WidthAccum) -> [Double], scale: Double) {
        print("\n--- \(name): distribution of |deviation| (x\(Int(scale))) ---")
        print(header())
        let labels: [(String, (([Double]) -> Double))] = [
            ("min", { $0.min() ?? .nan }),
            ("p5", { percentile($0.sorted(), 5) }),
            ("median", { median($0) }),
            ("mean", { $0.isEmpty ? .nan : $0.reduce(0, +) / Double($0.count) }),
            ("p95", { percentile($0.sorted(), 95) }),
            ("max", { $0.max() ?? .nan }),
        ]
        for (lbl, fn) in labels {
            var line = lbl.padding(toLength: 14, withPad: " ", startingAt: 0)
            for w in cols {
                let v = pick(accum[w] ?? WidthAccum())
                line += v.isEmpty ? "      —" : String(format: " %8.3f", fn(v) * scale)
            }
            print(line)
        }
    }
    detail("inner lip height", { $0.innerLipHeight }, scale: 1000)
    detail("inner/outer ratio", { $0.ratio }, scale: 1000)

    let faceFrames = (0..<n).filter { !reference[$0].isEmpty }.count
    print(String(format: "\nframes with >=1 reference face: %d / %d", faceFrames, n))
}
