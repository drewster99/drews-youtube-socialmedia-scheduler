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
//     FaceCropLab/FaceCropLab/CropExporter.swift \
//     cli/main.swift -o /tmp/facecrop-cli
// Run:
//   /tmp/facecrop-cli [videoPath] [--interval 0.05] [--mode center|edges]
//                     [--metric movement|openness] [--anxiety 3.0]
//                     [--max-seconds N] [--batch 1000] [--sim-ui] [--export OUT.mov]
//                     [--probe-extraction]
//
// --probe-extraction: measure ONLY the cost of reading every frame out of
// generator.images(for: times) (decode + downscale, no Vision, no classify),
// run once at zero tolerance and once at 0.01s before/after for comparison,
// then exit. This isolates extraction time from analysis time.

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
var probeExtraction = false
var probeParallel = false
var probeSweep = false
var probeSize = false
var probeReader = false
var probeReaderParallel = false
var probeReaderScaled = false
var probeVision = false
var visionSeconds = 20.0
var probeVisionSize = false
var visionSizeSamples = 2000
var probeCropSize = false
var probeFraming = false
var visionOverlay = false
var frameTimes: [Double] = []
var framing = FramingParams()
var stackExportPath: String?
var stackBoxesJSON: String?
var stackDump = false
var stackInterval = 0.1
var stackDebounce = 0.3
var stackBandZoom: CGFloat = 2.5   // band crop height = faceHeight × this (normalizes head size)
var margin: CGFloat = 0          // absolute selection margin for --export
var relMargin: CGFloat = 0       // relative selection margin for --export
var activityWindow = 0.5         // movement-EMA window seconds for --export
var renderHeight = 0             // export render height (0 = native, input quality)
var groups = 10
var detectHeadsJSON: String?     // --detect-heads: write native head-box JSON (drop-in for headboxctl)
var stackAutoPath: String?       // --stack-auto: native detect → segment → render, one Swift pipeline
var yoloModelPath = "/Users/andrew/Documents/ncc_source/cursor/drews-video-social-scheduler/vision-lab/models/yolov8n-pose-384.mlpackage"
var yoloConf = 0.25
var yoloIou = 0.7
var yoloImgsz = 384
var yoloBatch = 16
var clipcropOut: String?         // --clipcrop: production one-pass cut+recrop to native 9:16 .mp4
var clipStart: Double?           // --start (seconds, Python-computed edge)
var clipEnd: Double?             // --end (seconds, Python-computed edge)
var clipFadeIn = 0.0             // --fade-in (seconds, Python audio ramp)
var clipFadeOut = 0.0            // --fade-out (seconds, Python audio ramp)
var clipMinHeight = 1920         // --min-height: floor for native-res 9:16 output

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
    case "--probe-extraction": probeExtraction = true
    case "--probe-parallel": probeParallel = true
    case "--probe-sweep": probeSweep = true
    case "--probe-size": probeSize = true
    case "--probe-reader": probeReader = true
    case "--probe-reader-parallel": probeReaderParallel = true
    case "--probe-reader-scaled": probeReaderScaled = true
    case "--probe-vision": probeVision = true
    case "--vision-seconds": if let v = next().flatMap(Double.init) { visionSeconds = v }; i += 1
    case "--probe-vision-size": probeVisionSize = true
    case "--vision-size-samples": if let v = next().flatMap(Int.init) { visionSizeSamples = max(1, v) }; i += 1
    case "--probe-crop-size": probeCropSize = true
    case "--vision-overlay": visionOverlay = true
    case "--probe-framing": probeFraming = true
    case "--frame-times": if let v = next() { frameTimes = v.split(separator: ",").compactMap { Double($0) } }; i += 1
    case "--stack-export": stackExportPath = next(); i += 1
    case "--stack-from-boxes": stackBoxesJSON = next(); i += 1
    case "--detect-heads": detectHeadsJSON = next(); i += 1
    case "--stack-auto": stackAutoPath = next(); i += 1
    case "--yolo-model", "--model": if let v = next() { yoloModelPath = v }; i += 1
    case "--yolo-conf": if let v = next().flatMap(Double.init) { yoloConf = v }; i += 1
    case "--yolo-iou": if let v = next().flatMap(Double.init) { yoloIou = v }; i += 1
    case "--yolo-imgsz": if let v = next().flatMap(Int.init) { yoloImgsz = v }; i += 1
    case "--yolo-batch": if let v = next().flatMap(Int.init) { yoloBatch = v }; i += 1
    case "--clipcrop": clipcropOut = next(); i += 1
    case "--start": if let v = next().flatMap(Double.init) { clipStart = v }; i += 1
    case "--end": if let v = next().flatMap(Double.init) { clipEnd = v }; i += 1
    case "--fade-in": if let v = next().flatMap(Double.init) { clipFadeIn = v }; i += 1
    case "--fade-out": if let v = next().flatMap(Double.init) { clipFadeOut = v }; i += 1
    case "--min-height": if let v = next().flatMap(Int.init) { clipMinHeight = max(2, v) }; i += 1
    case "--stack-dump": stackDump = true
    case "--stack-interval": if let v = next().flatMap(Double.init) { stackInterval = v }; i += 1
    case "--debounce": if let v = next().flatMap(Double.init) { stackDebounce = v }; i += 1
    case "--band-zoom": if let v = next().flatMap(Double.init) { stackBandZoom = CGFloat(v) }; i += 1
    case "--kw": if let v = next().flatMap(Double.init) { framing.kWidth = CGFloat(v) }; i += 1
    case "--kup": if let v = next().flatMap(Double.init) { framing.kUp = CGFloat(v) }; i += 1
    case "--kdown": if let v = next().flatMap(Double.init) { framing.kDown = CGFloat(v) }; i += 1
    case "--head-frac": if let v = next().flatMap(Double.init) { framing.headFraction = CGFloat(v) }; i += 1
    case "--head-vfrac": if let v = next().flatMap(Double.init) { framing.headHeightFraction = CGFloat(v) }; i += 1
    case "--margin": if let v = next().flatMap(Double.init) { margin = CGFloat(v) }; i += 1
    case "--rel-margin": if let v = next().flatMap(Double.init) { relMargin = CGFloat(v) }; i += 1
    case "--activity-window": if let v = next().flatMap(Double.init) { activityWindow = v }; i += 1
    case "--render-height": if let v = next().flatMap(Int.init) { renderHeight = max(0, v) }; i += 1
    case "--groups": if let v = next().flatMap(Int.init) { groups = max(1, v) }; i += 1
    default:
        if a.hasPrefix("--") {
            FileHandle.standardError.write(Data("ERROR: unknown flag '\(a)'\n".utf8))
            exit(2)
        }
        path = a
    }
    i += 1
}

let step = max(0.01, interval)
guard FileManager.default.fileExists(atPath: path) else {
    FileHandle.standardError.write(Data("ERROR: video file not found: \(path)\n".utf8))
    exit(1)
}
let asset = AVURLAsset(url: URL(fileURLWithPath: path))
let assetDuration: Double
do {
    assetDuration = try await asset.load(.duration).seconds
} catch {
    FileHandle.standardError.write(Data("ERROR: could not read media '\(path)': \(error.localizedDescription)\n".utf8))
    exit(1)
}
guard assetDuration.isFinite, assetDuration > 0 else {
    FileHandle.standardError.write(Data("ERROR: invalid or zero duration: \(path)\n".utf8))
    exit(1)
}
let duration = min(maxSeconds, assetDuration)
print("video: \(path)")
print(String(format: "duration: %.1fs  interval: %.3fs  mode: %@  metric: %@  anxiety: %.1fs",
             duration, step, mode.rawValue, metric.rawValue, anxiety))

if probeExtraction {
    let expected = Int((duration / step).rounded()) + 1
    print("\n=== extraction probe (decode + downscale only — no Vision, no classify) ===")
    print(String(format: "requesting ~%d frames at %.0fpx max dimension\n", expected, Double(VideoProcessor.maxAnalysisDimension)))
    print("tolerance   frames  failed     wall      fps")
    for toleranceSeconds in [0.0, 0.01] {
        let r = await extractionPass(path: path, duration: duration, step: step, toleranceSeconds: toleranceSeconds)
        print(String(format: "%6.3fs    %6d  %6d  %7.2fs  %7.1f",
                     toleranceSeconds, r.frames, r.failed, r.wallSeconds, Double(r.frames) / r.wallSeconds))
        fflush(stdout)
    }
    print("\nnote: the second pass reads the same file with the OS cache warm; decode is")
    print("CPU-bound so that effect is small relative to the tolerance difference.")
    exit(0)
}

if probeParallel {
    var times: [CMTime] = []
    var pt = 0.0
    while pt <= duration { times.append(CMTime(seconds: pt, preferredTimescale: 600)); pt += step }
    print("\n=== parallel extraction probe (decode + downscale only — no Vision, no classify) ===")
    print(String(format: "splitting %d frames into %d contiguous groups, %d concurrent generators, zero tolerance\n",
                 times.count, groups, groups))
    let r = await parallelExtractionPass(path: path, times: times, groups: groups, toleranceSeconds: 0.0)
    print("groups  frames  failed     wall      fps")
    print(String(format: "%6d  %6d  %6d  %7.2fs  %7.1f",
                 groups, r.frames, r.failed, r.wallSeconds, Double(r.frames) / r.wallSeconds))
    print("\nnote: each group is a contiguous 1/N slice of the timeline with its own")
    print("AVAssetImageGenerator, so the N forward-decodes run over disjoint file regions.")
    exit(0)
}

if probeVisionSize {
    await runVisionSizeStudy(path: path, widths: [3840, 1920, 1280, 800, 640, 400, 200],
                             sampleCount: visionSizeSamples, refRuns: 5, maxSeconds: maxSeconds)
    exit(0)
}

if probeCropSize {
    await runCropSizeStudy(path: path, widths: [3840, 1920, 1280, 800, 640, 400, 200],
                           step: step, anxiety: anxiety, maxSeconds: maxSeconds)
    exit(0)
}

if visionOverlay {
    await runVisionOverlay(path: path, times: frameTimes)
    exit(0)
}

if probeFraming {
    await runFramingProbe(path: path, explicitTimes: frameTimes, params: framing)
    exit(0)
}

if stackDump {
    await dumpStackAnalysis(path: path, sampleInterval: stackInterval, maxSeconds: maxSeconds, debounce: stackDebounce)
    exit(0)
}

// Production one-pass cut + 9:16 recrop. Driven by Python-computed edges + fades;
// nonzero exit on any failure so the caller never silently ships a wrong crop.
if let out = clipcropOut {
    guard let start = clipStart, let end = clipEnd else {
        FileHandle.standardError.write(Data("ERROR: --clipcrop requires --start and --end (seconds)\n".utf8))
        exit(2)
    }
    guard FileManager.default.fileExists(atPath: path) else {
        FileHandle.standardError.write(Data("ERROR: parent not found: \(path)\n".utf8))
        exit(1)
    }
    let cfg = HeadDetectionConfig(modelPath: yoloModelPath, interval: stackInterval,
                                  imgsz: yoloImgsz, batch: yoloBatch, conf: yoloConf, iou: yoloIou)
    do {
        try await runClipCrop(parent: path, start: start, end: end, fadeIn: clipFadeIn, fadeOut: clipFadeOut,
                              outURL: URL(fileURLWithPath: out), minHeight: clipMinHeight,
                              config: cfg, debounce: stackDebounce, bandZoom: stackBandZoom)
    } catch {
        FileHandle.standardError.write(Data("ERROR: \(error.localizedDescription)\n".utf8))
        exit(1)
    }
    exit(0)
}

if detectHeadsJSON != nil || stackAutoPath != nil {
    let cfg = HeadDetectionConfig(modelPath: yoloModelPath, interval: stackInterval,
                                  imgsz: yoloImgsz, batch: yoloBatch, conf: yoloConf, iou: yoloIou)
    await runStackAuto(path: path, config: cfg, detectJSON: detectHeadsJSON, renderOut: stackAutoPath,
                       debounce: stackDebounce, renderHeight: renderHeight, bandZoom: stackBandZoom, maxSeconds: maxSeconds)
    exit(0)
}

if let json = stackBoxesJSON, let out = stackExportPath {
    await runStackExportFromBoxes(path: path, jsonPath: json, outPath: out, debounce: stackDebounce,
                                  renderHeight: renderHeight, bandZoom: stackBandZoom, maxSeconds: maxSeconds)
    exit(0)
}

if let stackOut = stackExportPath {
    await runStackExport(path: path, outPath: stackOut, sampleInterval: stackInterval,
                         debounce: stackDebounce, renderHeight: renderHeight,
                         bandZoom: stackBandZoom, maxSeconds: maxSeconds)
    exit(0)
}

if probeVision {
    guard let oneFrame = await firstScaledFrame(asset: asset, maxDim: VideoProcessor.maxAnalysisDimension) else {
        print("ERROR: could not obtain a scaled frame to test Vision on.")
        exit(1)
    }
    let box = PixelBox(oneFrame)
    print(String(format: "\n=== Vision throughput probe (DetectFaceLandmarksRequest on one %dx%d frame) ===",
                 CVPixelBufferGetWidth(oneFrame), CVPixelBufferGetHeight(oneFrame)))
    print(String(format: "each worker count runs ~%.0fs; fps = completed results / actual elapsed\n", visionSeconds))
    print("workers   results   elapsed      fps    latency/call")
    for n in [1, 2, 4, 8, 12, 16] {
        let (results, elapsed) = await visionThroughput(box: box, workers: n, seconds: visionSeconds)
        let fps = elapsed > 0 ? Double(results) / elapsed : 0
        print(String(format: "%6d  %8d  %8.2fs  %8.1f   %8.2fms",
                     n, results, elapsed, fps, fps > 0 ? Double(n) * 1000.0 / fps : 0))
        fflush(stdout)
    }
    print("\nnote: one fixed frame, no decode in the loop — pure Vision throughput. latency/call")
    print("is mean wall per perform() at that concurrency; fps that stops rising marks saturation.")
    exit(0)
}

if probeReaderScaled {
    print("\n=== linear reader + composition downscale/rotate probe (single-thread, BGRA) ===")
    if maxSeconds != .greatestFiniteMagnitude { print(String(format: "limited to first %.0fs", maxSeconds)) }
    let r = await readerCompositionScaledPass(asset: asset, maxDuration: maxSeconds, maxDim: VideoProcessor.maxAnalysisDimension)
    print(String(format: "renderSize: %d x %d   delivered: %d x %d", r.renderW, r.renderH, r.width, r.height))
    print("frames    wall      fps     status    finalRSS")
    print(String(format: "%6d  %7.2fs  %7.1f   %@   %6.0fMB",
                 r.frames, r.wallSeconds, r.wallSeconds > 0 ? Double(r.frames) / r.wallSeconds : 0,
                 r.failed ? "FAILED" : "ok", r.finalRSS))
    print("\nnote: AVAssetReaderVideoCompositionOutput applies the track's preferred transform")
    print("(orientation) and renders at renderSize (downscale) in one pipeline — the 'reader")
    print("does rotation + downscaling' path. Compare fps to the 593 fps native-YUV decode.")
    exit(0)
}

if probeReaderParallel {
    print(String(format: "\n=== parallel linear reader probe (%d AVAssetReader instances, native YUV 420v, no scale) ===", groups))
    print(String(format: "splitting %.0fs into %d contiguous segments\n", duration, groups))
    let r = await parallelReaderPass(path: path, totalDuration: duration, groups: groups)
    print("readers   frames    wall      fps      finalRSS")
    print(String(format: "%6d  %7d  %7.2fs  %7.1f   %6.0fMB",
                 groups, r.frames, r.wallSeconds, r.wallSeconds > 0 ? Double(r.frames) / r.wallSeconds : 0, r.finalRSS))
    print("\nnote: each reader pays one GOP pre-roll at its segment start (negligible). Frame")
    print("count may differ by a few frames from the single-reader 64,979 due to segment-")
    print("boundary handling; timing/fps is the figure of interest.")
    exit(0)
}

if probeReader {
    print("\n=== linear reader probe (AVAssetReader, native YUV 420v, no scale, single-threaded) ===")
    if maxSeconds != .greatestFiniteMagnitude { print(String(format: "limited to first %.0fs", maxSeconds)) }
    let r = await readerLinearPass(asset: asset, maxDuration: maxSeconds)
    print("frames    wall      fps     status    finalRSS")
    print(String(format: "%6d  %7.2fs  %7.1f   %@   %6.0fMB",
                 r.frames, r.wallSeconds, r.wallSeconds > 0 ? Double(r.frames) / r.wallSeconds : 0,
                 r.failed ? "FAILED" : "ok", r.finalRSS))
    print("\nnote: native YUV output forces decode with no RGB convert and no downscale —")
    print("pure sequential decode throughput of the 4K H.264 stream.")
    exit(0)
}

if probeSize {
    let sizeGen = AVAssetImageGenerator(asset: asset)
    sizeGen.appliesPreferredTrackTransform = true
    sizeGen.requestedTimeToleranceBefore = .zero
    sizeGen.requestedTimeToleranceAfter = .zero
    sizeGen.maximumSize = CGSize(width: VideoProcessor.maxAnalysisDimension, height: VideoProcessor.maxAnalysisDimension)
    var natural = CGSize.zero
    if let track = try? await asset.loadTracks(withMediaType: .video).first,
       let n = try? await track.load(.naturalSize),
       let xf = try? await track.load(.preferredTransform) {
        let d = n.applying(xf)
        natural = CGSize(width: abs(d.width), height: abs(d.height))
    }
    print(String(format: "\n=== image-size probe ===\nsource displayed size: %.0f x %.0f", natural.width, natural.height))
    print(String(format: "maximumSize cap: %.0f x %.0f (square box; aspect preserved)", Double(VideoProcessor.maxAnalysisDimension), Double(VideoProcessor.maxAnalysisDimension)))
    let sizeTimes = [0.0, duration / 2, max(0, duration - 1)].map { CMTime(seconds: $0, preferredTimescale: 600) }
    print("delivered CGImage sizes:")
    for await result in sizeGen.images(for: sizeTimes) {
        if let img = try? result.image {
            print(String(format: "  t=%8.2fs  ->  %d x %d px", result.requestedTime.seconds, img.width, img.height))
        } else {
            print(String(format: "  t=%8.2fs  ->  (decode failed)", result.requestedTime.seconds))
        }
    }
    exit(0)
}

if probeSweep {
    var times: [CMTime] = []
    var pt = 0.0
    while pt <= duration { times.append(CMTime(seconds: pt, preferredTimescale: 600)); pt += step }
    let counts = [1, 2, 3, 4, 6, 8, 10]
    print("\n=== parallel-scaling sweep (decode + downscale only, zero tolerance) ===")
    print(String(format: "%d frames over %.0fs of video; each group count run sequentially\n", times.count, duration))
    print("groups  frames  failed     wall      fps    speedup")
    var baseline = 0.0
    for n in counts {
        let r = await parallelExtractionPass(path: path, times: times, groups: n, toleranceSeconds: 0.0)
        if n == 1 { baseline = r.wallSeconds }
        print(String(format: "%6d  %6d  %6d  %7.2fs  %7.1f   %5.2fx",
                     n, r.frames, r.failed, r.wallSeconds, Double(r.frames) / r.wallSeconds,
                     baseline > 0 ? baseline / r.wallSeconds : 1.0))
        fflush(stdout)
    }
    exit(0)
}

let generator = AVAssetImageGenerator(asset: asset)
generator.appliesPreferredTrackTransform = true
generator.requestedTimeToleranceBefore = .zero
generator.requestedTimeToleranceAfter = .zero
generator.maximumSize = CGSize(width: VideoProcessor.maxAnalysisDimension, height: VideoProcessor.maxAnalysisDimension)
let request = DetectFaceLandmarksRequest()

var times: [CMTime] = []
var t = 0.0
while t <= duration { times.append(CMTime(seconds: t, preferredTimescale: 600)); t += step }

let cliTuning = VideoProcessor.ClassifyTuning(minConfidence: VideoProcessor.minFaceConfidence,
                                              absoluteMargin: margin, relativeMargin: relMargin,
                                              activityWindowSeconds: activityWindow)
var state = VideoProcessor.ClassifyState()
var frames: [FrameAnalysis] = []
frames.reserveCapacity(times.count)

enum Slot { case frame(RawFrame); case failed }
var pending: [Int: Slot] = [:]
var nextIndex = 0
// Exact result→sample-index lookup keyed on the echoed-back CMTime value.
var indexByTimeValue: [Int64: Int] = [:]
for (i, ct) in times.enumerated() { indexByTimeValue[ct.value] = i }

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
    let idx = indexByTimeValue[result.requestedTime.value] ?? Int((frameTime / step).rounded())
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
            let fa = VideoProcessor.classifyFrame(raw, mode: mode, cropThreshold: anxiety, step: step, metric: metric, tuning: cliTuning, state: &state)
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

// Defensive: flush any results stranded after a gap, in ascending time order
// (mirrors VideoProcessor.process so a dropped result can't lose trailing frames).
for k in pending.keys.sorted() {
    if case .frame(let raw)? = pending[k] {
        frames.append(VideoProcessor.classifyFrame(raw, mode: mode, cropThreshold: anxiety, step: step, metric: metric, tuning: cliTuning, state: &state))
    }
}

let total = ms(runStart, DispatchTime.now()) / 1000
print(String(format: "\nDONE  %d frames in %.1fs  (%.1f fps)  finalRSS %.0fMB",
             frames.count, total, Double(frames.count) / total, rssMB()))

if let outPath = exportPath {
    let centers = CropExporter.centers(from: frames)
    let renderSize: CGSize? = renderHeight > 0
        ? CGSize(width: (Double(renderHeight) * 9.0 / 16.0 / 2.0).rounded() * 2, height: Double(renderHeight))
        : nil
    print("\nexporting 9:16 crop (margin \(margin)) -> \(outPath)  (\(centers.count) trajectory points)")
    let lastPct = LockedDouble()
    try await CropExporter.export(source: URL(fileURLWithPath: path),
                                  to: URL(fileURLWithPath: outPath),
                                  centers: centers, renderSize: renderSize) { p in
        if p - lastPct.get() >= 0.1 || p >= 1.0 {
            lastPct.set(p)
            FileHandle.standardError.write(Data(String(format: "  export %.0f%%\n", p * 100).utf8))
        }
    }
    print("export complete: \(outPath)")
}

/// One extraction-only pass: build the same sample times, read every frame out
/// of `images(for:)` (forcing decode + downscale by touching `.image`), do
/// nothing with them, and return the wall-clock total. The only variable is the
/// before/after tolerance, so two calls isolate the cost of frame-exact seeking.
/// A fresh asset + generator per call avoids any cross-pass generator state.
func extractionPass(path: String, duration: Double, step: Double, toleranceSeconds: Double) async -> (frames: Int, failed: Int, wallSeconds: Double) {
    let asset = AVURLAsset(url: URL(fileURLWithPath: path))
    let gen = AVAssetImageGenerator(asset: asset)
    gen.appliesPreferredTrackTransform = true
    let tol = CMTime(seconds: toleranceSeconds, preferredTimescale: 600)
    gen.requestedTimeToleranceBefore = tol
    gen.requestedTimeToleranceAfter = tol
    gen.maximumSize = CGSize(width: VideoProcessor.maxAnalysisDimension, height: VideoProcessor.maxAnalysisDimension)

    var times: [CMTime] = []
    var t = 0.0
    while t <= duration { times.append(CMTime(seconds: t, preferredTimescale: 600)); t += step }

    var frameCount = 0
    var failCount = 0
    let start = DispatchTime.now()
    for await result in gen.images(for: times) {
        if (try? result.image) != nil { frameCount += 1 } else { failCount += 1 }
    }
    return (frameCount, failCount, ms(start, DispatchTime.now()) / 1000)
}

/// Split `times` into `groups` CONTIGUOUS slices (group 0 = earliest 1/N of the
/// timeline, etc.) and read each slice with its OWN AVAssetImageGenerator on its
/// own child task, all concurrently. Contiguous (not strided) so each generator
/// does one forward decode over a disjoint file region — no cross-generator GOP
/// re-decode. Returns the wall-clock total for the whole fan-out.
func parallelExtractionPass(path: String, times: [CMTime], groups: Int, toleranceSeconds: Double) async -> (frames: Int, failed: Int, wallSeconds: Double) {
    let total = times.count
    let chunkSize = max(1, (total + groups - 1) / groups)
    var chunks: [[CMTime]] = []
    var start = 0
    while start < total {
        let end = min(start + chunkSize, total)
        chunks.append(Array(times[start..<end]))
        start = end
    }

    let t0 = DispatchTime.now()
    var totalFrames = 0
    var totalFailed = 0
    await withTaskGroup(of: (Int, Int).self) { group in
        for chunk in chunks {
            group.addTask {
                let asset = AVURLAsset(url: URL(fileURLWithPath: path))
                let gen = AVAssetImageGenerator(asset: asset)
                gen.appliesPreferredTrackTransform = true
                let tol = CMTime(seconds: toleranceSeconds, preferredTimescale: 600)
                gen.requestedTimeToleranceBefore = tol
                gen.requestedTimeToleranceAfter = tol
                gen.maximumSize = CGSize(width: VideoProcessor.maxAnalysisDimension, height: VideoProcessor.maxAnalysisDimension)
                var f = 0, x = 0
                for await result in gen.images(for: chunk) {
                    if (try? result.image) != nil { f += 1 } else { x += 1 }
                }
                return (f, x)
            }
        }
        for await (f, x) in group { totalFrames += f; totalFailed += x }
    }
    return (totalFrames, totalFailed, ms(t0, DispatchTime.now()) / 1000)
}

/// Decode every frame of the video front-to-back through a single AVAssetReader
/// — the linear, forward-only decode path (no per-time seeking like
/// AVAssetImageGenerator). Output is requested as native YUV 420v so the
/// decoder decompresses but does NOT color-convert or scale: this isolates raw
/// sequential decode throughput. nil outputSettings would have vended the
/// stored (still-compressed) samples — no decode — so a pixel format is
/// required to actually force decompression. Single-threaded by design.
func readerLinearPass(asset: AVURLAsset, maxDuration: Double) async -> (frames: Int, failed: Bool, wallSeconds: Double, finalRSS: Double) {
    guard let track = try? await asset.loadTracks(withMediaType: .video).first,
          let reader = try? AVAssetReader(asset: asset) else {
        return (0, true, 0, rssMB())
    }
    let output = AVAssetReaderTrackOutput(track: track, outputSettings: [
        kCVPixelBufferPixelFormatTypeKey as String: Int(kCVPixelFormatType_420YpCbCr8BiPlanarVideoRange)
    ])
    output.alwaysCopiesSampleData = false
    guard reader.canAdd(output) else { return (0, true, 0, rssMB()) }
    reader.add(output)
    if maxDuration != .greatestFiniteMagnitude {
        reader.timeRange = CMTimeRange(start: .zero, duration: CMTime(seconds: maxDuration, preferredTimescale: 600))
    }

    let t0 = DispatchTime.now()
    guard reader.startReading() else { return (0, true, 0, rssMB()) }
    var count = 0
    var keepGoing = true
    while keepGoing {
        autoreleasepool {
            guard reader.status == .reading, let sample = output.copyNextSampleBuffer() else {
                keepGoing = false
                return
            }
            if CMSampleBufferGetImageBuffer(sample) != nil { count += 1 }
        }
    }
    let wall = ms(t0, DispatchTime.now()) / 1000
    return (count, reader.status == .failed, wall, rssMB())
}

/// Linearly decode ONE contiguous time range through its own AVAssetReader and
/// return the decoded-frame count. Sets timeRange before startReading so the
/// reader seeks once to the segment (one GOP pre-roll) then streams forward.
func readerSegmentCount(path: String, start: Double, duration: Double) async -> Int {
    let asset = AVURLAsset(url: URL(fileURLWithPath: path))
    guard let track = try? await asset.loadTracks(withMediaType: .video).first,
          let reader = try? AVAssetReader(asset: asset) else { return 0 }
    let output = AVAssetReaderTrackOutput(track: track, outputSettings: [
        kCVPixelBufferPixelFormatTypeKey as String: Int(kCVPixelFormatType_420YpCbCr8BiPlanarVideoRange)
    ])
    output.alwaysCopiesSampleData = false
    guard reader.canAdd(output) else { return 0 }
    reader.add(output)
    reader.timeRange = CMTimeRange(start: CMTime(seconds: start, preferredTimescale: 600),
                                   duration: CMTime(seconds: duration, preferredTimescale: 600))
    guard reader.startReading() else { return 0 }

    var count = 0
    var keepGoing = true
    while keepGoing {
        autoreleasepool {
            guard reader.status == .reading, let sample = output.copyNextSampleBuffer() else {
                keepGoing = false
                return
            }
            if CMSampleBufferGetImageBuffer(sample) != nil { count += 1 }
        }
    }
    return count
}

/// Split [0, totalDuration] into `groups` contiguous segments and decode each
/// with its own AVAssetReader concurrently — linear decode fanned out across
/// segments, to find the true hardware decode ceiling (vs the single-reader
/// 593 fps, which was single-thread-bound, not decode-bound).
func parallelReaderPass(path: String, totalDuration: Double, groups: Int) async -> (frames: Int, wallSeconds: Double, finalRSS: Double) {
    let seg = totalDuration / Double(groups)
    let t0 = DispatchTime.now()
    var total = 0
    await withTaskGroup(of: Int.self) { group in
        for g in 0..<groups {
            let start = Double(g) * seg
            let dur = (g == groups - 1) ? (totalDuration - start) : seg
            group.addTask {
                await readerSegmentCount(path: path, start: start, duration: dur)
            }
        }
        for await c in group { total += c }
    }
    return (total, ms(t0, DispatchTime.now()) / 1000, rssMB())
}

/// Linear single-reader decode where the READ PIPELINE itself does rotation +
/// downscale: AVAssetReaderVideoCompositionOutput renders each frame through a
/// video composition that (a) applies the track's preferred transform
/// (orientation) and (b) renders at renderSize (the 800-box downscale). Output
/// is BGRA — what Vision/Core Image would consume. This is the "reader does the
/// scaling" path; its per-frame composition cost is what we want to time.
func readerCompositionScaledPass(asset: AVURLAsset, maxDuration: Double, maxDim: CGFloat) async -> (frames: Int, width: Int, height: Int, renderW: Int, renderH: Int, failed: Bool, wallSeconds: Double, finalRSS: Double) {
    guard let track = try? await asset.loadTracks(withMediaType: .video).first,
          let natural = try? await track.load(.naturalSize),
          let transform = try? await track.load(.preferredTransform),
          let assetDuration = try? await asset.load(.duration) else {
        return (0, 0, 0, 0, 0, true, 0, rssMB())
    }
    let fps = (try? await track.load(.nominalFrameRate)) ?? 0
    let displayed = natural.applying(transform)
    let dw = abs(displayed.width), dh = abs(displayed.height)
    guard fps > 0, dw > 0, dh > 0 else { return (0, 0, 0, 0, 0, true, 0, rssMB()) }

    // Fit the display-oriented frame into the maxDim box; even dimensions.
    let s = maxDim / max(dw, dh)
    func even(_ v: CGFloat) -> Int { let r = Int(v.rounded()); return r - (r % 2) }
    let rw = even(dw * s), rh = even(dh * s)

    // Orient then scale: source -> preferredTransform -> display coords -> scale -> render coords.
    let layer = AVMutableVideoCompositionLayerInstruction(assetTrack: track)
    layer.setTransform(transform.concatenating(CGAffineTransform(scaleX: s, y: s)), at: .zero)
    let instruction = AVMutableVideoCompositionInstruction()
    instruction.timeRange = CMTimeRange(start: .zero, duration: assetDuration)
    instruction.layerInstructions = [layer]

    let videoComp = AVMutableVideoComposition()
    videoComp.renderSize = CGSize(width: rw, height: rh)
    // Emit one composed frame per source frame (don't resample the timeline).
    videoComp.frameDuration = CMTimeMakeWithSeconds(1.0 / Double(fps), preferredTimescale: 600)
    videoComp.instructions = [instruction]

    guard let reader = try? AVAssetReader(asset: asset) else { return (0, 0, 0, rw, rh, true, 0, rssMB()) }
    let output = AVAssetReaderVideoCompositionOutput(videoTracks: [track], videoSettings: [
        kCVPixelBufferPixelFormatTypeKey as String: Int(kCVPixelFormatType_32BGRA)
    ])
    output.videoComposition = videoComp
    output.alwaysCopiesSampleData = false
    guard reader.canAdd(output) else { return (0, 0, 0, rw, rh, true, 0, rssMB()) }
    reader.add(output)
    if maxDuration != .greatestFiniteMagnitude {
        reader.timeRange = CMTimeRange(start: .zero, duration: CMTime(seconds: maxDuration, preferredTimescale: 600))
    }

    let t0 = DispatchTime.now()
    guard reader.startReading() else { return (0, 0, 0, rw, rh, true, 0, rssMB()) }
    var count = 0, fw = 0, fh = 0
    var keepGoing = true
    while keepGoing {
        autoreleasepool {
            guard reader.status == .reading, let sample = output.copyNextSampleBuffer() else {
                keepGoing = false
                return
            }
            if let buf = CMSampleBufferGetImageBuffer(sample) {
                if fw == 0 { fw = CVPixelBufferGetWidth(buf); fh = CVPixelBufferGetHeight(buf) }
                count += 1
            }
        }
    }
    return (count, fw, fh, rw, rh, reader.status == .failed, ms(t0, DispatchTime.now()) / 1000, rssMB())
}

/// Independent (pool-detached) copy of a CVPixelBuffer so it survives after the
/// reader that produced it stops. Handles planar and non-planar layouts.
func deepCopyPixelBuffer(_ src: CVPixelBuffer) -> CVPixelBuffer? {
    let w = CVPixelBufferGetWidth(src), h = CVPixelBufferGetHeight(src)
    let fmt = CVPixelBufferGetPixelFormatType(src)
    let attrs: [String: Any] = [kCVPixelBufferIOSurfacePropertiesKey as String: [:]]
    var dst: CVPixelBuffer?
    guard CVPixelBufferCreate(nil, w, h, fmt, attrs as CFDictionary, &dst) == kCVReturnSuccess,
          let dst else { return nil }
    CVPixelBufferLockBaseAddress(src, .readOnly)
    CVPixelBufferLockBaseAddress(dst, [])
    defer {
        CVPixelBufferUnlockBaseAddress(dst, [])
        CVPixelBufferUnlockBaseAddress(src, .readOnly)
    }
    if CVPixelBufferIsPlanar(src) {
        for p in 0..<CVPixelBufferGetPlaneCount(src) {
            guard let s = CVPixelBufferGetBaseAddressOfPlane(src, p),
                  let d = CVPixelBufferGetBaseAddressOfPlane(dst, p) else { continue }
            let sbpr = CVPixelBufferGetBytesPerRowOfPlane(src, p)
            let dbpr = CVPixelBufferGetBytesPerRowOfPlane(dst, p)
            let ph = CVPixelBufferGetHeightOfPlane(src, p)
            let n = min(sbpr, dbpr)
            for row in 0..<ph { memcpy(d + row * dbpr, s + row * sbpr, n) }
        }
    } else {
        guard let s = CVPixelBufferGetBaseAddress(src),
              let d = CVPixelBufferGetBaseAddress(dst) else { return nil }
        let sbpr = CVPixelBufferGetBytesPerRow(src)
        let dbpr = CVPixelBufferGetBytesPerRow(dst)
        let n = min(sbpr, dbpr)
        for row in 0..<h { memcpy(d + row * dbpr, s + row * sbpr, n) }
    }
    return dst
}

/// Decode + orient + downscale a SINGLE frame through the composition pipeline
/// and return a pool-detached copy, for use as a fixed Vision input.
func firstScaledFrame(asset: AVURLAsset, maxDim: CGFloat) async -> CVPixelBuffer? {
    guard let track = try? await asset.loadTracks(withMediaType: .video).first,
          let natural = try? await track.load(.naturalSize),
          let transform = try? await track.load(.preferredTransform),
          let assetDuration = try? await asset.load(.duration) else { return nil }
    let fps = (try? await track.load(.nominalFrameRate)) ?? 0
    let displayed = natural.applying(transform)
    let dw = abs(displayed.width), dh = abs(displayed.height)
    guard fps > 0, dw > 0, dh > 0 else { return nil }
    let s = maxDim / max(dw, dh)
    func even(_ v: CGFloat) -> Int { let r = Int(v.rounded()); return r - (r % 2) }

    let layer = AVMutableVideoCompositionLayerInstruction(assetTrack: track)
    layer.setTransform(transform.concatenating(CGAffineTransform(scaleX: s, y: s)), at: .zero)
    let instruction = AVMutableVideoCompositionInstruction()
    instruction.timeRange = CMTimeRange(start: .zero, duration: assetDuration)
    instruction.layerInstructions = [layer]
    let videoComp = AVMutableVideoComposition()
    videoComp.renderSize = CGSize(width: even(dw * s), height: even(dh * s))
    videoComp.frameDuration = CMTimeMakeWithSeconds(1.0 / Double(fps), preferredTimescale: 600)
    videoComp.instructions = [instruction]

    guard let reader = try? AVAssetReader(asset: asset) else { return nil }
    let output = AVAssetReaderVideoCompositionOutput(videoTracks: [track], videoSettings: [
        kCVPixelBufferPixelFormatTypeKey as String: Int(kCVPixelFormatType_32BGRA)
    ])
    output.videoComposition = videoComp
    guard reader.canAdd(output) else { return nil }
    reader.add(output)
    guard reader.startReading() else { return nil }
    defer { reader.cancelReading() }
    while reader.status == .reading {
        guard let sample = output.copyNextSampleBuffer() else { break }
        if let buf = CMSampleBufferGetImageBuffer(sample) { return deepCopyPixelBuffer(buf) }
    }
    return nil
}

/// Run `workers` concurrent Vision performs on a fixed frame for `seconds`,
/// counting COMPLETED results. perform(on:) is async, so N awaits keep N
/// performs genuinely in flight. Returns (completed, actualElapsedSeconds).
func visionThroughput(box: PixelBox, workers: Int, seconds: Double) async -> (Int, Double) {
    let counter = LockedInt()
    let start = DispatchTime.now()
    let deadline = DispatchTime(uptimeNanoseconds: start.uptimeNanoseconds + UInt64(seconds * 1_000_000_000))
    await withTaskGroup(of: Void.self) { group in
        for _ in 0..<workers {
            group.addTask {
                let request = DetectFaceLandmarksRequest()
                while DispatchTime.now() < deadline {
                    _ = try? await request.perform(on: box.buffer)
                    counter.increment()
                }
            }
        }
        await group.waitForAll()
    }
    return (counter.value, ms(start, DispatchTime.now()) / 1000)
}

/// Read-only CVPixelBuffer shared across concurrent Vision workers.
final class PixelBox: @unchecked Sendable {
    let buffer: CVPixelBuffer
    init(_ b: CVPixelBuffer) { self.buffer = b }
}

/// Lock-guarded completion counter for the Vision workers.
final class LockedInt: @unchecked Sendable {
    private let lock = NSLock()
    private var v = 0
    func increment() { lock.lock(); v += 1; lock.unlock() }
    var value: Int { lock.lock(); defer { lock.unlock() }; return v }
}

// Tiny thread-safe holder so the @Sendable progress closure can throttle prints.
final class LockedDouble: @unchecked Sendable {
    private let lock = NSLock(); private var value = 0.0
    func get() -> Double { lock.lock(); defer { lock.unlock() }; return value }
    func set(_ v: Double) { lock.lock(); value = v; lock.unlock() }
}
