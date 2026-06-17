import Foundation
import AVFoundation
import Vision
import CoreGraphics
import Darwin

// Headless driver for the FaceCropLab analysis pipeline. Shares the real
// VideoProcessor.classifyFrame so behavior matches the app; only the extraction
// loop is re-implemented here, with per-batch timing + memory instrumentation
// to diagnose the "slower and slower as it goes" slowdown.
//
// Build (from vision-lab/):
//   swiftc -O -target arm64-apple-macos26.0 \
//     FaceCropLab/FaceCropLab/Models.swift \
//     FaceCropLab/FaceCropLab/VideoProcessor.swift \
//     cli/main.swift -o /tmp/facecrop-cli
// Run:
//   /tmp/facecrop-cli [videoPath] [--interval 0.05] [--mode center|edges]
//                     [--metric movement|openness] [--anxiety 3.0]
//                     [--max-seconds N] [--batch 1000]

func rssMB() -> Double {
    var info = mach_task_basic_info()
    var count = mach_msg_type_number_t(MemoryLayout<mach_task_basic_info>.size / MemoryLayout<natural_t>.size)
    let kr = withUnsafeMutablePointer(to: &info) { ptr -> kern_return_t in
        ptr.withMemoryRebound(to: integer_t.self, capacity: Int(count)) {
            task_info(mach_task_self_, task_flavor_t(MACH_TASK_BASIC_INFO), $0, &count)
        }
    }
    return kr == KERN_SUCCESS ? Double(info.resident_size) / 1_048_576 : -1
}

func ms(_ a: DispatchTime, _ b: DispatchTime) -> Double {
    Double(b.uptimeNanoseconds - a.uptimeNanoseconds) / 1_000_000
}

let defaultPath = "/Users/andrew/Library/Application Support/com.nuclearcyborg.drews-socialmedia-scheduler/uploads/source_aa7012df9512c544.mp4"

var args = Array(CommandLine.arguments.dropFirst())
var path = defaultPath
var interval = 0.05
var mode: ClassificationMode = .center
var metric: ActivenessMetric = .movement
var anxiety = 3.0
var maxSeconds = Double.greatestFiniteMagnitude
var batch = 1000
var simUI = false   // recompute timingSummary per frame like the overlay does
var exportPath: String?

var i = 0
while i < args.count {
    let a = args[i]
    func next() -> String? { i + 1 < args.count ? args[i + 1] : nil }
    switch a {
    case "--interval": if let v = next().flatMap(Double.init) { interval = v }; i += 1
    case "--mode": if let v = next() { mode = (v == "edges") ? .edges : .center }; i += 1
    case "--metric": if let v = next() { metric = (v == "openness") ? .openness : .movement }; i += 1
    case "--anxiety": if let v = next().flatMap(Double.init) { anxiety = v }; i += 1
    case "--max-seconds": if let v = next().flatMap(Double.init) { maxSeconds = v }; i += 1
    case "--batch": if let v = next().flatMap(Int.init) { batch = v }; i += 1
    case "--sim-ui": simUI = true
    case "--export": exportPath = next(); i += 1
    default: if !a.hasPrefix("--") { path = a }
    }
    i += 1
}

let step = max(0.01, interval)
let asset = AVURLAsset(url: URL(fileURLWithPath: path))
let duration = min(maxSeconds, (try await asset.load(.duration)).seconds)
print("video: \(path)")
print(String(format: "duration: %.1fs  interval: %.3fs  mode: %@  metric: %@  anxiety: %.1fs",
             duration, step, mode.rawValue, metric.rawValue, anxiety))

let generator = AVAssetImageGenerator(asset: asset)
generator.appliesPreferredTrackTransform = true
generator.requestedTimeToleranceBefore = .zero
generator.requestedTimeToleranceAfter = .zero
generator.maximumSize = CGSize(width: VideoProcessor.maxAnalysisDimension, height: VideoProcessor.maxAnalysisDimension)
let request = DetectFaceLandmarksRequest()

var times: [CMTime] = []
var t = 0.0
while t <= duration { times.append(CMTime(seconds: t, preferredTimescale: 600)); t += step }

var state = VideoProcessor.ClassifyState()
var frames: [FrameAnalysis] = []
frames.reserveCapacity(times.count)

enum Slot { case frame(RawFrame); case failed }
var pending: [Int: Slot] = [:]
var nextIndex = 0

let runStart = DispatchTime.now()
var batchStart = runStart
var batchVisionMs = 0.0
var batchClassifyMs = 0.0
var batchUIMs = 0.0
var done = 0

// Mirrors VideoProcessor.timingSummary: O(n) over all frames so far.
func simTimingSummary() {
    let times = frames.map(\.analysisMs)
    _ = (times.min(), times.max(), times.reduce(0, +) / Double(max(1, times.count)))
}

print("\nframes        wall    vision/f  classify/f  ui/f      rss")
for await result in generator.images(for: times) {
    let frameTime = result.requestedTime.seconds
    let idx = Int((frameTime / step).rounded())
    if let image = try? result.image {
        let imgSize = CGSize(width: image.width, height: image.height)
        let vStart = DispatchTime.now()
        let observations = (try? await request.perform(on: image)) ?? []
        var rawFaces: [RawFace] = []
        for obs in observations {
            guard let lm = obs.landmarks else { continue }
            let outer = lm.outerLips, inner = lm.innerLips
            rawFaces.append(RawFace(
                boundingBox: obs.boundingBox.toImageCoordinates(imgSize, origin: .upperLeft),
                confidence: obs.confidence,
                outerLips: outer.pointsInImageCoordinates(imgSize, origin: .upperLeft),
                innerLips: inner.pointsInImageCoordinates(imgSize, origin: .upperLeft),
                outerClassification: String(describing: outer.pointsClassification),
                innerClassification: String(describing: inner.pointsClassification),
                outerIsClosed: outer.pointsClassification == .closedPath,
                innerIsClosed: inner.pointsClassification == .closedPath,
                outerPrecision: outer.precisionEstimatesPerPoint ?? [],
                innerPrecision: inner.precisionEstimatesPerPoint ?? []))
        }
        let vEnd = DispatchTime.now()
        batchVisionMs += ms(vStart, vEnd)
        pending[idx] = .frame(RawFrame(time: frameTime, imageSize: imgSize, analysisMs: ms(vStart, vEnd), faces: rawFaces))
    } else {
        pending[idx] = .failed
    }

    while let slot = pending.removeValue(forKey: nextIndex) {
        if case .frame(let raw) = slot {
            let cStart = DispatchTime.now()
            let fa = VideoProcessor.classifyFrame(raw, mode: mode, cropThreshold: anxiety, step: step, metric: metric, state: &state)
            batchClassifyMs += ms(cStart, DispatchTime.now())
            frames.append(fa)
            done += 1
            if simUI {
                let uStart = DispatchTime.now()
                simTimingSummary()
                batchUIMs += ms(uStart, DispatchTime.now())
            }
            if done % batch == 0 {
                let now = DispatchTime.now()
                print(String(format: "%6d-%-6d %6.2fs   %6.3fms   %6.3fms  %7.3fms %6.0fMB",
                             done - batch, done, ms(batchStart, now) / 1000,
                             batchVisionMs / Double(batch), batchClassifyMs / Double(batch),
                             batchUIMs / Double(batch), rssMB()))
                fflush(stdout)
                batchStart = now; batchVisionMs = 0; batchClassifyMs = 0; batchUIMs = 0
            }
        }
        nextIndex += 1
    }
}

let total = ms(runStart, DispatchTime.now()) / 1000
print(String(format: "\nDONE  %d frames in %.1fs  (%.1f fps)  finalRSS %.0fMB",
             frames.count, total, Double(frames.count) / total, rssMB()))

if let outPath = exportPath {
    let centers = CropExporter.centers(from: frames)
    print("\nexporting 9:16 crop -> \(outPath)  (\(centers.count) trajectory points)")
    let lastPct = LockedDouble()
    try await CropExporter.export(source: URL(fileURLWithPath: path),
                                  to: URL(fileURLWithPath: outPath),
                                  centers: centers) { p in
        if p - lastPct.get() >= 0.1 || p >= 1.0 {
            lastPct.set(p)
            FileHandle.standardError.write(String(format: "  export %.0f%%\n", p * 100).data(using: .utf8)!)
        }
    }
    print("export complete: \(outPath)")
}

// Tiny thread-safe holder so the @Sendable progress closure can throttle prints.
final class LockedDouble: @unchecked Sendable {
    private let lock = NSLock(); private var value = 0.0
    func get() -> Double { lock.lock(); defer { lock.unlock() }; return value }
    func set(_ v: Double) { lock.lock(); value = v; lock.unlock() }
}
