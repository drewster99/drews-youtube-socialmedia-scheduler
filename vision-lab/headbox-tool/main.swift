import Foundation
import AVFoundation
import CoreML
import CoreImage
import CoreVideo

// Standalone, batched, 100%-Swift replacement for /tmp/yolo_boxes.py.
//
// Reads a video, samples one frame every `interval` seconds in DISPLAY
// orientation (so coordinates match the renderStacked compositor), letterboxes
// each frame to a square `imgsz` for a YOLOv8-pose CoreML model, runs inference
// in BATCHES, decodes person keypoints, derives a head box per person, and
// writes the exact JSON the Swift stack compositor already consumes:
//   { "interval": <s>, "samples": [ { "t": <s>, "boxes": [ {cx,cy,w,h} ] } ] }
// All box fields are fractions of the display-oriented frame, top-left origin.

// MARK: - JSON shapes (must match HeadsDocJSON in cli/stack.swift)

struct BoxJSON: Codable { let cx: Double; let cy: Double; let w: Double; let h: Double }
struct SampleJSON: Codable { let t: Double; let boxes: [BoxJSON] }
struct DocJSON: Codable { let interval: Double; let samples: [SampleJSON] }

// MARK: - Args

func fail(_ message: String) -> Never {
    FileHandle.standardError.write(Data((message + "\n").utf8))
    exit(2)
}

let argv = CommandLine.arguments
var positional: [String] = []
var modelPath = "/tmp/yolov8n-pose-384.mlpackage"
var maxSeconds = Double.greatestFiniteMagnitude
var interval = 0.1
var imgsz = 384
var batchSize = 16
var confThresh = 0.25
var iouThresh = 0.7

var i = 1
while i < argv.count {
    let a = argv[i]
    func next() -> String {
        guard i + 1 < argv.count else { fail("Missing value for \(a)") }
        i += 1
        return argv[i]
    }
    switch a {
    case "--model": modelPath = next()
    case "--max-seconds": guard let v = Double(next()) else { fail("Bad --max-seconds") }; maxSeconds = v
    case "--interval": guard let v = Double(next()) else { fail("Bad --interval") }; interval = v
    case "--imgsz": guard let v = Int(next()) else { fail("Bad --imgsz") }; imgsz = v
    case "--batch": guard let v = Int(next()) else { fail("Bad --batch") }; batchSize = v
    case "--conf": guard let v = Double(next()) else { fail("Bad --conf") }; confThresh = v
    case "--iou": guard let v = Double(next()) else { fail("Bad --iou") }; iouThresh = v
    default:
        if a.hasPrefix("--") { fail("Unknown flag: \(a)") }
        positional.append(a)
    }
    i += 1
}
guard positional.count == 2 else {
    fail("usage: headboxctl <video> <out.json> [--model P] [--max-seconds N] [--interval 0.1] [--imgsz 384] [--batch 16] [--conf 0.25] [--iou 0.7]")
}
let videoPath = positional[0]
let outPath = positional[1]
guard FileManager.default.fileExists(atPath: videoPath) else { fail("No such video: \(videoPath)") }
guard FileManager.default.fileExists(atPath: modelPath) else { fail("No such model: \(modelPath)") }

let S = imgsz
let confThr = confThresh
let iouThr = iouThresh

func log(_ s: String) { FileHandle.standardError.write(Data((s + "\n").utf8)) }

// MARK: - Load model

let mlConfig = MLModelConfiguration()
mlConfig.computeUnits = .all
let model: MLModel
let inputName: String
let outputName: String
do {
    let compiledURL = try MLModel.compileModel(at: URL(fileURLWithPath: modelPath))
    model = try MLModel(contentsOf: compiledURL, configuration: mlConfig)
    guard let inName = model.modelDescription.inputDescriptionsByName.keys.first else { fail("Model has no input") }
    guard let outName = model.modelDescription.outputDescriptionsByName.keys.first else { fail("Model has no output") }
    inputName = inName
    outputName = outName
    guard let outDesc = model.modelDescription.outputDescriptionsByName[outName],
          outDesc.type == .multiArray,
          let c = outDesc.multiArrayConstraint, c.dataType == .float32 else {
        fail("Model output is not a float32 multiArray")
    }
    log("model: \(modelPath)  input=\(inputName) output=\(outputName) outShape=\(c.shape)")
}

// MARK: - Asset + display-oriented composition reader

let asset = AVURLAsset(url: URL(fileURLWithPath: videoPath))
let track: AVAssetTrack
let vcomp: AVVideoComposition
let durationSec: Double
do {
    let tracks = try await asset.loadTracks(withMediaType: .video)
    guard let t = tracks.first else { fail("No video track") }
    track = t
    vcomp = try await AVVideoComposition.videoComposition(withPropertiesOf: asset)
    durationSec = try await asset.load(.duration).seconds
}

// renderSize is the display-oriented size (preferred transform applied); the
// composition reader emits frames at exactly this size, so normalize by it.
let Wd = Double(vcomp.renderSize.width)
let Hd = Double(vcomp.renderSize.height)
guard Wd > 0, Hd > 0 else { fail("Bad render size \(vcomp.renderSize)") }
let scale = min(Double(S) / Wd, Double(S) / Hd)
let padX = (Double(S) - Wd * scale) / 2.0
let padY = (Double(S) - Hd * scale) / 2.0
log(String(format: "render %.0fx%.0f  letterbox->%d  scale=%.4f padX=%.1f padY=%.1f  dur=%.1fs",
           Wd, Hd, S, scale, padX, padY, durationSec))

// MARK: - Letterbox machinery (CoreImage -> pooled BGRA buffer)

let ciContext = CIContext(options: [.cacheIntermediates: false])
let gray = CIImage(color: CIColor(red: 114.0 / 255, green: 114.0 / 255, blue: 114.0 / 255))
    .cropped(to: CGRect(x: 0, y: 0, width: S, height: S))

var poolOpt: CVPixelBufferPool?
let bufferAttrs: [String: Any] = [
    kCVPixelBufferPixelFormatTypeKey as String: kCVPixelFormatType_32BGRA,
    kCVPixelBufferWidthKey as String: S,
    kCVPixelBufferHeightKey as String: S,
    kCVPixelBufferIOSurfacePropertiesKey as String: [String: Any]()
]
CVPixelBufferPoolCreate(nil, [:] as CFDictionary, bufferAttrs as CFDictionary, &poolOpt)
guard let pool = poolOpt else { fail("Could not create pixel-buffer pool") }

func letterbox(_ src: CVPixelBuffer) -> CVPixelBuffer? {
    var outOpt: CVPixelBuffer?
    guard CVPixelBufferPoolCreatePixelBuffer(nil, pool, &outOpt) == kCVReturnSuccess,
          let out = outOpt else { return nil }
    let img = CIImage(cvPixelBuffer: src)
        .transformed(by: CGAffineTransform(scaleX: scale, y: scale))
        .transformed(by: CGAffineTransform(translationX: padX, y: padY))
        .composited(over: gray)
    ciContext.render(img, to: out)
    return out
}

// MARK: - YOLOv8-pose decode + NMS + head box

struct Candidate { var x1: Double; var y1: Double; var x2: Double; var y2: Double; var conf: Double; var kp: [(Double, Double, Double)] }

func decode(_ out: MLMultiArray) -> [Candidate] {
    let ptr = out.dataPointer.assumingMemoryBound(to: Float32.self)
    let s1 = out.strides[1].intValue       // channel stride
    let s2 = out.strides[2].intValue       // anchor stride
    let nA = out.shape[2].intValue
    var cands: [Candidate] = []
    for a in 0..<nA {
        let conf = Double(ptr[4 * s1 + a * s2])
        if conf < confThr { continue }
        let bx = Double(ptr[0 * s1 + a * s2]), by = Double(ptr[1 * s1 + a * s2])
        let bw = Double(ptr[2 * s1 + a * s2]), bh = Double(ptr[3 * s1 + a * s2])
        var kp: [(Double, Double, Double)] = []
        kp.reserveCapacity(17)
        for j in 0..<17 {
            let kx = Double(ptr[(5 + 3 * j) * s1 + a * s2])
            let ky = Double(ptr[(6 + 3 * j) * s1 + a * s2])
            let kc = Double(ptr[(7 + 3 * j) * s1 + a * s2])
            kp.append((kx, ky, kc))
        }
        cands.append(Candidate(x1: bx - bw / 2, y1: by - bh / 2, x2: bx + bw / 2, y2: by + bh / 2, conf: conf, kp: kp))
    }
    return cands
}

func iou(_ a: Candidate, _ b: Candidate) -> Double {
    let ix1 = max(a.x1, b.x1), iy1 = max(a.y1, b.y1)
    let ix2 = min(a.x2, b.x2), iy2 = min(a.y2, b.y2)
    let inter = max(0, ix2 - ix1) * max(0, iy2 - iy1)
    let areaA = (a.x2 - a.x1) * (a.y2 - a.y1)
    let areaB = (b.x2 - b.x1) * (b.y2 - b.y1)
    let uni = areaA + areaB - inter
    return uni > 0 ? inter / uni : 0
}

func nms(_ cands: [Candidate]) -> [Candidate] {
    let sorted = cands.sorted { $0.conf > $1.conf }
    var kept: [Candidate] = []
    for c in sorted {
        var overlaps = false
        for k in kept where iou(c, k) > iouThr { overlaps = true; break }
        if !overlaps { kept.append(c) }
    }
    return kept
}

// Indices: nose=0, leftEye=1, rightEye=2, leftEar=3, rightEar=4 (COCO-17).
func headBox(_ kp: [(Double, Double, Double)], thr: Double = 0.3) -> BoxJSON? {
    let face = [0, 1, 2, 3, 4]
    let present = face.filter { kp[$0].2 >= thr }
    guard present.count >= 2 else { return nil }
    let cx = present.map { kp[$0].0 }.reduce(0, +) / Double(present.count)
    var width: Double
    if kp[3].2 >= thr, kp[4].2 >= thr {
        width = abs(kp[3].0 - kp[4].0) * 1.3                 // ear-span, cap-robust
    } else if kp[1].2 >= thr, kp[2].2 >= thr {
        width = abs(kp[1].0 - kp[2].0) * 2.4                 // eye-span fallback
    } else {
        let xs = present.map { kp[$0].0 }
        let lo = xs.reduce(xs[0]) { min($0, $1) }
        let hi = xs.reduce(xs[0]) { max($0, $1) }
        width = (hi - lo) * 2.0
    }
    width = max(width, 12.0)
    var eyeY: Double
    if kp[1].2 >= thr, kp[2].2 >= thr {
        eyeY = (kp[1].1 + kp[2].1) / 2
    } else if kp[0].2 >= thr {
        eyeY = kp[0].1
    } else {
        eyeY = present.map { kp[$0].1 }.reduce(0, +) / Double(present.count)
    }
    let height = width * 1.4
    let top = eyeY - height * 0.52
    return BoxJSON(cx: cx / Wd, cy: (top + height / 2) / Hd, w: width / Wd, h: height / Hd)
}

// MARK: - Reader loop with batched inference

let reader: AVAssetReader
do { reader = try AVAssetReader(asset: asset) }
catch { fail("Could not create reader: \(error.localizedDescription)") }
let readerOutput = AVAssetReaderVideoCompositionOutput(
    videoTracks: [track],
    videoSettings: [kCVPixelBufferPixelFormatTypeKey as String: kCVPixelFormatType_32BGRA])
readerOutput.videoComposition = vcomp
readerOutput.alwaysCopiesSampleData = false
guard reader.canAdd(readerOutput) else { fail("Cannot add composition output") }
reader.add(readerOutput)
guard reader.startReading() else { fail("startReading failed: \(reader.error?.localizedDescription ?? "?")") }

var samples: [SampleJSON] = []
var batchBufs: [CVPixelBuffer] = []
var batchT: [Double] = []

func runBatch() throws {
    guard !batchBufs.isEmpty else { return }
    var providers: [MLFeatureProvider] = []
    providers.reserveCapacity(batchBufs.count)
    for b in batchBufs {
        let fv = MLFeatureValue(pixelBuffer: b)
        providers.append(try MLDictionaryFeatureProvider(dictionary: [inputName: fv]))
    }
    let results = try model.predictions(from: MLArrayBatchProvider(array: providers), options: MLPredictionOptions())
    for idx in 0..<results.count {
        let fp = results.features(at: idx)
        guard let mv = fp.featureValue(for: outputName)?.multiArrayValue else { continue }
        let kept = nms(decode(mv))
        var boxes: [BoxJSON] = []
        for c in kept {
            let kpDisp = c.kp.map { (($0.0 - padX) / scale, ($0.1 - padY) / scale, $0.2) }
            if let hb = headBox(kpDisp) { boxes.append(hb) }
        }
        boxes.sort { $0.cx < $1.cx }
        samples.append(SampleJSON(t: (batchT[idx] * 1000).rounded() / 1000, boxes: boxes))
    }
    batchBufs.removeAll(keepingCapacity: true)
    batchT.removeAll(keepingCapacity: true)
}

let startWall = Date()
let cap = min(maxSeconds, durationSec)
var nextSampleT = 0.0
var decodedFrames = 0
var sampledFrames = 0

// Each iteration is wrapped in an autorelease pool: decoding 4K frames produces
// large autoreleased CVImageBuffers (~33MB each) that would otherwise accumulate
// for the whole run and exhaust memory. Draining per-iteration keeps the resident
// set bounded to the in-flight batch.
readLoop: while reader.status == .reading {
    let keepGoing: Bool = autoreleasepool {
        guard let sbuf = readerOutput.copyNextSampleBuffer() else { return false }
        defer { CMSampleBufferInvalidate(sbuf) }
        let pts = CMSampleBufferGetPresentationTimeStamp(sbuf).seconds
        if pts > cap { return false }
        decodedFrames += 1
        if pts + 1e-6 >= nextSampleT {
            while nextSampleT <= pts { nextSampleT += interval }
            guard let pixels = CMSampleBufferGetImageBuffer(sbuf), let lb = letterbox(pixels) else { return true }
            batchBufs.append(lb)
            batchT.append(pts)
            sampledFrames += 1
            if batchBufs.count >= batchSize {
                do { try runBatch() }
                catch { fail("inference failed: \(error.localizedDescription)") }
            }
            if sampledFrames % 500 == 0 {
                let el = Date().timeIntervalSince(startWall)
                log(String(format: "  t=%.1fs  sampled=%d  (%.1f frames/s wall)", pts, sampledFrames, Double(sampledFrames) / max(el, 0.001)))
            }
        }
        return true
    }
    if !keepGoing { break }
}
autoreleasepool {
    do { try runBatch() }
    catch { fail("final inference failed: \(error.localizedDescription)") }
}

if reader.status == .failed { fail("reader failed: \(reader.error?.localizedDescription ?? "?")") }
reader.cancelReading()

samples.sort { $0.t < $1.t }
let two = samples.filter { $0.boxes.count == 2 }.count
let doc = DocJSON(interval: interval, samples: samples)
do {
    let enc = JSONEncoder()
    let data = try enc.encode(doc)
    try data.write(to: URL(fileURLWithPath: outPath))
} catch { fail("could not write JSON: \(error.localizedDescription)") }

let elapsed = Date().timeIntervalSince(startWall)
log(String(format: "wrote %d samples (%d with 2 heads) -> %@  [%.1fs, %.1f sampled-frames/s]",
           samples.count, two, outPath, elapsed, Double(sampledFrames) / max(elapsed, 0.001)))
