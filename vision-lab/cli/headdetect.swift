import Foundation
import AVFoundation
import CoreML
import CoreImage
import CoreVideo
import CoreGraphics

// Native, batched YOLOv8-pose head detection. Reads a video in DISPLAY
// orientation, letterboxes each sampled frame to a square for the CoreML model,
// runs inference in batches, decodes person keypoints, and derives one head box
// per person — returning samples directly consumable by `segmentsFromSamples`.
//
// This is the in-process equivalent of the standalone `headboxctl` tool: it lets
// `facecrop-cli2` run detect → segment → render as one Swift pipeline with no
// Python and no JSON handoff.

struct HeadDetectionConfig {
    var modelPath: String
    var interval: Double = 0.1
    var imgsz: Int = 384
    var batch: Int = 16
    var conf: Double = 0.25
    var iou: Double = 0.7
}

struct HeadDetectError: LocalizedError { let message: String; var errorDescription: String? { message } }

/// One sampled frame's normalized head boxes (top-left origin, sorted left→right).
struct HeadSample { let t: Double; let faces: [CGRect] }

enum HeadDetector {

    /// COCO-17 face keypoint indices.
    private static let nose = 0, leftEye = 1, rightEye = 2, leftEar = 3, rightEar = 4

    private struct Candidate { var x1, y1, x2, y2, conf: Double; var kp: [(x: Double, y: Double, c: Double)] }

    /// Detect head-box samples across the video. Boxes are fractions of the
    /// display-oriented frame, top-left origin, sorted by horizontal center.
    static func detect(path: String, config: HeadDetectionConfig, maxSeconds: Double,
                       startSeconds: Double = 0,
                       progress: (@Sendable (Double, Int) -> Void)? = nil) async throws -> [HeadSample] {
        guard FileManager.default.fileExists(atPath: config.modelPath) else {
            throw HeadDetectError(message: "YOLO model not found: \(config.modelPath)")
        }
        let mlConfig = MLModelConfiguration()
        mlConfig.computeUnits = .all
        // A pre-compiled .mlmodelc loads directly — no per-invocation compile (the
        // production path: one subprocess per clip would otherwise recompile every
        // clip). A .mlpackage/.mlmodel is compiled on the fly (dev convenience).
        let modelURL = URL(fileURLWithPath: config.modelPath)
        let loadURL = config.modelPath.hasSuffix(".mlmodelc")
            ? modelURL
            : try await MLModel.compileModel(at: modelURL)
        let model = try MLModel(contentsOf: loadURL, configuration: mlConfig)
        guard let inputName = model.modelDescription.inputDescriptionsByName.keys.first else {
            throw HeadDetectError(message: "Model has no input")
        }
        guard let outputName = model.modelDescription.outputDescriptionsByName.keys.first,
              let outDesc = model.modelDescription.outputDescriptionsByName[outputName],
              outDesc.type == .multiArray,
              let arrConstraint = outDesc.multiArrayConstraint, arrConstraint.dataType == .float32 else {
            throw HeadDetectError(message: "Model output is not a float32 multiArray")
        }

        let asset = AVURLAsset(url: URL(fileURLWithPath: path))
        let tracks = try await asset.loadTracks(withMediaType: .video)
        guard let track = tracks.first else { throw HeadDetectError(message: "No video track") }
        let vcomp = try await AVVideoComposition.videoComposition(withPropertiesOf: asset)
        let durationSec = try await asset.load(.duration).seconds

        // renderSize is the display-oriented frame size (preferred transform
        // applied); the composition reader emits frames at exactly this size.
        let displayW = Double(vcomp.renderSize.width)
        let displayH = Double(vcomp.renderSize.height)
        guard displayW > 0, displayH > 0 else { throw HeadDetectError(message: "Bad render size \(vcomp.renderSize)") }
        let S = config.imgsz
        let scale = min(Double(S) / displayW, Double(S) / displayH)
        let padX = (Double(S) - displayW * scale) / 2.0
        let padY = (Double(S) - displayH * scale) / 2.0

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
        guard let pool = poolOpt else { throw HeadDetectError(message: "Could not create pixel-buffer pool") }

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

        let reader = try AVAssetReader(asset: asset)
        let readerOutput = AVAssetReaderVideoCompositionOutput(
            videoTracks: [track],
            videoSettings: [kCVPixelBufferPixelFormatTypeKey as String: kCVPixelFormatType_32BGRA])
        readerOutput.videoComposition = vcomp
        readerOutput.alwaysCopiesSampleData = false
        guard reader.canAdd(readerOutput) else { throw HeadDetectError(message: "Cannot add composition output") }
        reader.add(readerOutput)
        // Window decoding to [startSeconds, startSeconds+maxSeconds] so a clip deep
        // into a long parent doesn't force a linear decode from t=0. AVAssetReader
        // seeks to the keyframe at/before the range start; emitted pts stay absolute.
        if startSeconds > 0 || maxSeconds < durationSec {
            let winDur = min(maxSeconds, max(0, durationSec - startSeconds))
            reader.timeRange = CMTimeRange(start: CMTime(seconds: startSeconds, preferredTimescale: 600),
                                           duration: CMTime(seconds: winDur, preferredTimescale: 600))
        }
        guard reader.startReading() else {
            throw HeadDetectError(message: "startReading failed: \(reader.error?.localizedDescription ?? "?")")
        }

        var samples: [HeadSample] = []
        var batchBufs: [CVPixelBuffer] = []
        var batchT: [Double] = []
        let confThr = config.conf, iouThr = config.iou

        func runBatch() throws {
            guard !batchBufs.isEmpty else { return }
            var providers: [MLFeatureProvider] = []
            providers.reserveCapacity(batchBufs.count)
            for b in batchBufs {
                providers.append(try MLDictionaryFeatureProvider(dictionary: [inputName: MLFeatureValue(pixelBuffer: b)]))
            }
            let results = try model.predictions(from: MLArrayBatchProvider(array: providers), options: MLPredictionOptions())
            for idx in 0..<results.count {
                guard let mv = results.features(at: idx).featureValue(for: outputName)?.multiArrayValue else { continue }
                let kept = nms(decode(mv, confThr: confThr), iouThr: iouThr)
                var faces: [CGRect] = []
                for c in kept {
                    let kpDisp = c.kp.map { (x: ($0.x - padX) / scale, y: ($0.y - padY) / scale, c: $0.c) }
                    if let box = headBox(kpDisp, displayW: displayW, displayH: displayH) { faces.append(box) }
                }
                faces.sort { $0.midX < $1.midX }
                samples.append(HeadSample(t: batchT[idx], faces: faces))
            }
            batchBufs.removeAll(keepingCapacity: true)
            batchT.removeAll(keepingCapacity: true)
        }

        let cap = min(startSeconds + maxSeconds, durationSec)
        var nextSampleT = startSeconds
        var sampledFrames = 0
        var loopError: Error?

        readLoop: while reader.status == .reading {
            // Per-iteration autorelease pool: 4K decode produces large
            // autoreleased CVImageBuffers that would otherwise accumulate for the
            // whole run and exhaust memory.
            let keepGoing: Bool = autoreleasepool {
                guard let sbuf = readerOutput.copyNextSampleBuffer() else { return false }
                defer { CMSampleBufferInvalidate(sbuf) }
                let pts = CMSampleBufferGetPresentationTimeStamp(sbuf).seconds
                if pts > cap { return false }
                if pts + 1e-6 >= nextSampleT {
                    while nextSampleT <= pts { nextSampleT += config.interval }
                    guard let pixels = CMSampleBufferGetImageBuffer(sbuf), let lb = letterbox(pixels) else { return true }
                    batchBufs.append(lb)
                    batchT.append(pts)
                    sampledFrames += 1
                    if batchBufs.count >= config.batch {
                        do { try runBatch() } catch { loopError = error; return false }
                    }
                    if let progress, sampledFrames % 500 == 0 { progress(min(pts / max(cap, 0.001), 1.0), sampledFrames) }
                }
                return true
            }
            if !keepGoing { break }
        }
        if let loopError { throw loopError }
        try autoreleasepool { try runBatch() }

        if reader.status == .failed { throw HeadDetectError(message: "reader failed: \(reader.error?.localizedDescription ?? "?")") }
        reader.cancelReading()
        samples.sort { $0.t < $1.t }
        return samples
    }

    /// Decode a YOLOv8-pose output tensor [1, 56, A]: 4 bbox + 1 person-conf +
    /// 17×3 keypoints per anchor, in letterboxed pixel space (conf already sigmoid).
    private static func decode(_ out: MLMultiArray, confThr: Double) -> [Candidate] {
        let ptr = out.dataPointer.assumingMemoryBound(to: Float32.self)
        let s1 = out.strides[1].intValue
        let s2 = out.strides[2].intValue
        let nA = out.shape[2].intValue
        var cands: [Candidate] = []
        for a in 0..<nA {
            let conf = Double(ptr[4 * s1 + a * s2])
            if conf < confThr { continue }
            let bx = Double(ptr[a * s2]), by = Double(ptr[s1 + a * s2])
            let bw = Double(ptr[2 * s1 + a * s2]), bh = Double(ptr[3 * s1 + a * s2])
            var kp: [(x: Double, y: Double, c: Double)] = []
            kp.reserveCapacity(17)
            for j in 0..<17 {
                kp.append((x: Double(ptr[(5 + 3 * j) * s1 + a * s2]),
                           y: Double(ptr[(6 + 3 * j) * s1 + a * s2]),
                           c: Double(ptr[(7 + 3 * j) * s1 + a * s2])))
            }
            cands.append(Candidate(x1: bx - bw / 2, y1: by - bh / 2, x2: bx + bw / 2, y2: by + bh / 2, conf: conf, kp: kp))
        }
        return cands
    }

    private static func iou(_ a: Candidate, _ b: Candidate) -> Double {
        let ix1 = max(a.x1, b.x1), iy1 = max(a.y1, b.y1)
        let ix2 = min(a.x2, b.x2), iy2 = min(a.y2, b.y2)
        let inter = max(0, ix2 - ix1) * max(0, iy2 - iy1)
        let uni = (a.x2 - a.x1) * (a.y2 - a.y1) + (b.x2 - b.x1) * (b.y2 - b.y1) - inter
        return uni > 0 ? inter / uni : 0
    }

    private static func nms(_ cands: [Candidate], iouThr: Double) -> [Candidate] {
        let sorted = cands.sorted { $0.conf > $1.conf }
        var kept: [Candidate] = []
        for c in sorted {
            var overlaps = false
            for k in kept where iou(c, k) > iouThr { overlaps = true; break }
            if !overlaps { kept.append(c) }
        }
        return kept
    }

    /// Derive a normalized head box (top-left origin) from one person's face
    /// keypoints. Width from ear-span (cap-robust) or eye-span; height = width×1.4.
    private static func headBox(_ kp: [(x: Double, y: Double, c: Double)], displayW: Double, displayH: Double, thr: Double = 0.3) -> CGRect? {
        let face = [nose, leftEye, rightEye, leftEar, rightEar]
        let present = face.filter { kp[$0].c >= thr }
        guard present.count >= 2 else { return nil }
        let cx = present.map { kp[$0].x }.reduce(0, +) / Double(present.count)
        var width: Double
        if kp[leftEar].c >= thr, kp[rightEar].c >= thr {
            width = abs(kp[leftEar].x - kp[rightEar].x) * 1.3
        } else if kp[leftEye].c >= thr, kp[rightEye].c >= thr {
            width = abs(kp[leftEye].x - kp[rightEye].x) * 2.4
        } else {
            let xs = present.map { kp[$0].x }
            let lo = xs.reduce(xs[0]) { min($0, $1) }
            let hi = xs.reduce(xs[0]) { max($0, $1) }
            width = (hi - lo) * 2.0
        }
        width = max(width, 12.0)
        var eyeY: Double
        if kp[leftEye].c >= thr, kp[rightEye].c >= thr {
            eyeY = (kp[leftEye].y + kp[rightEye].y) / 2
        } else if kp[nose].c >= thr {
            eyeY = kp[nose].y
        } else {
            eyeY = present.map { kp[$0].y }.reduce(0, +) / Double(present.count)
        }
        let height = width * 1.4
        let top = eyeY - height * 0.52
        return CGRect(x: (cx - width / 2) / displayW, y: top / displayH,
                      width: width / displayW, height: height / displayH)
    }
}

// MARK: - JSON output (same schema the Python tool and --stack-from-boxes use)

private struct HeadBoxOut: Codable { let cx, cy, w, h: Double }
private struct HeadSampleOut: Codable { let t: Double; let boxes: [HeadBoxOut] }
private struct HeadsDocOut: Codable { let interval: Double; let samples: [HeadSampleOut] }

func writeHeadSamplesJSON(_ samples: [HeadSample], interval: Double, to path: String) throws {
    let out = HeadsDocOut(interval: interval, samples: samples.map { s in
        HeadSampleOut(t: (s.t * 1000).rounded() / 1000,
                      boxes: s.faces.map { HeadBoxOut(cx: Double($0.midX), cy: Double($0.midY), w: Double($0.width), h: Double($0.height)) })
    })
    let data = try JSONEncoder().encode(out)
    try data.write(to: URL(fileURLWithPath: path))
}
