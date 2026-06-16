import Foundation
import AVFoundation
import Vision
import Observation
import CoreGraphics

/// Samples a video, runs Vision face-landmark detection once per sampled frame
/// (cached in `rawFrames`), and derives the position-bucketed, motion-tracked
/// `frames` from that cache. Changing the classification mode re-derives from
/// the cache without re-running Vision.
@MainActor
@Observable
final class VideoProcessor {
    private(set) var frames: [FrameAnalysis] = []
    /// Pixel size of the (downscaled) frames Vision actually analyzed.
    private(set) var imageSize: CGSize = .zero
    /// Original display resolution of the source video, before downscaling.
    private(set) var sourceSize: CGSize = .zero
    /// Source video duration, in seconds.
    private(set) var videoDuration: Double = 0
    private(set) var isProcessing = false
    private(set) var progress: Double = 0
    private(set) var statusMessage = "No video loaded."
    private(set) var unclassifiedCount = 0

    /// The active classification mode. `process` reads it live, so toggling
    /// mid-run re-derives going forward.
    private(set) var classificationMode: ClassificationMode = .center
    /// Seconds that must elapse before a left↔right crop switch is allowed
    /// (from the crop-anxiety slider). Read live, like the mode.
    private(set) var cropThreshold: Double = 3.0
    /// Sampling step in seconds (for the 0.5s activity window); read live.
    private(set) var sampleStep: Double = 0.05
    /// Which mouth signal picks the active face among several. Read live.
    private(set) var activenessMetric: ActivenessMetric = .movement
    /// Running derivation state, shared so a mode toggle during processing can
    /// re-derive the frames-so-far and let `process` continue consistently.
    private var classifyState = ClassifyState()
    /// Mode-independent Vision results — the expensive part, computed once.
    private var rawFrames: [RawFrame] = []

    /// Rolling history length per position, in samples.
    nonisolated static let historyLength = 200
    /// Frames are downscaled so their largest dimension is at most this many
    /// pixels before Vision runs — 4K is needless work for face landmarks.
    static let maxAnalysisDimension: CGFloat = 800
    /// Horizontal classification boundaries as fractions of image width.
    nonisolated static let leftLine: CGFloat = 0.45
    nonisolated static let rightLine: CGFloat = 0.55

    /// Min / max / average per-frame Vision time across all processed frames.
    var timingSummary: (min: Double, max: Double, avg: Double)? {
        let times = rawFrames.map(\.analysisMs)
        guard let mn = times.min(), let mx = times.max() else { return nil }
        return (mn, mx, times.reduce(0, +) / Double(times.count))
    }

    /// Sample the video every `interval` seconds (e.g. 0.25 → 4 fps), running
    /// Vision on each frame and bucketing faces with `mode`.
    func process(url: URL, interval: Double, mode: ClassificationMode, cropThreshold: Double, metric: ActivenessMetric) async {
        isProcessing = true
        frames = []
        rawFrames = []
        sourceSize = .zero
        videoDuration = 0
        unclassifiedCount = 0
        classificationMode = mode
        self.cropThreshold = cropThreshold
        self.activenessMetric = metric
        classifyState = ClassifyState()
        progress = 0
        statusMessage = "Loading…"

        let startNanos = DispatchTime.now()
        let step = max(0.01, interval)
        self.sampleStep = step
        let asset = AVURLAsset(url: url)
        do {
            let duration = try await asset.load(.duration).seconds
            guard duration.isFinite, duration > 0 else {
                statusMessage = "Could not read a valid duration."
                isProcessing = false
                return
            }
            videoDuration = duration

            if let track = try? await asset.loadTracks(withMediaType: .video).first,
               let natural = try? await track.load(.naturalSize),
               let transform = try? await track.load(.preferredTransform) {
                let displayed = natural.applying(transform)
                sourceSize = CGSize(width: abs(displayed.width), height: abs(displayed.height))
            }

            let generator = AVAssetImageGenerator(asset: asset)
            generator.appliesPreferredTrackTransform = true
            generator.requestedTimeToleranceBefore = .zero
            generator.requestedTimeToleranceAfter = .zero
            // Largest dimension capped so Vision isn't fed needless 4K pixels.
            generator.maximumSize = CGSize(width: Self.maxAnalysisDimension, height: Self.maxAnalysisDimension)
            let request = DetectFaceLandmarksRequest()

            // Build all sample times up front, then extract them in ONE batched,
            // sequentially-decoded pass (images(for:)) instead of thousands of
            // independent frame-exact image(at:) seeks that re-decode each GOP
            // repeatedly. Extraction time was never counted in analysisMs, so
            // this is the hidden cost behind a slow overall run.
            var times: [CMTime] = []
            var t = 0.0
            while t <= duration {
                times.append(CMTime(seconds: t, preferredTimescale: 600))
                t += step
            }
            let totalSamples = times.count
            var sampleIndex = 0
            var cancelled = false

            // classifyFrame is stateful (motion/activity/crop carry across frames
            // in time order), so we never assume images(for:) delivers in order:
            // buffer each Vision result by its sample index and drain the
            // contiguous run. A failed extraction still yields a result, so the
            // run can't stall on a gap.
            enum Slot { case frame(RawFrame); case failed }
            var pending: [Int: Slot] = [:]
            var nextIndex = 0

            for await result in generator.images(for: times) {
                if Task.isCancelled { cancelled = true; break }
                let frameTime = result.requestedTime.seconds
                let idx = Int((frameTime / step).rounded())

                if let image = try? result.image {
                    let imgSize = CGSize(width: image.width, height: image.height)
                    imageSize = imgSize
                    let analysisStart = DispatchTime.now()
                    let observations = (try? await request.perform(on: image)) ?? []

                    var rawFaces: [RawFace] = []
                    for obs in observations {
                        guard let lm = obs.landmarks else { continue }
                        let outer = lm.outerLips
                        let inner = lm.innerLips
                        rawFaces.append(RawFace(
                            boundingBox: obs.boundingBox.toImageCoordinates(imgSize, origin: .upperLeft),
                            outerLips: outer.pointsInImageCoordinates(imgSize, origin: .upperLeft),
                            innerLips: inner.pointsInImageCoordinates(imgSize, origin: .upperLeft),
                            outerClassification: String(describing: outer.pointsClassification),
                            innerClassification: String(describing: inner.pointsClassification),
                            outerIsClosed: outer.pointsClassification == .closedPath,
                            innerIsClosed: inner.pointsClassification == .closedPath,
                            outerPrecision: outer.precisionEstimatesPerPoint ?? [],
                            innerPrecision: inner.precisionEstimatesPerPoint ?? []))
                    }
                    let analysisMs = Double(DispatchTime.now().uptimeNanoseconds - analysisStart.uptimeNanoseconds) / 1_000_000
                    pending[idx] = .frame(RawFrame(time: frameTime, imageSize: imgSize, analysisMs: analysisMs, faces: rawFaces))
                } else {
                    pending[idx] = .failed
                }

                // Classify every contiguous frame that's now ready, in time order.
                while let slot = pending.removeValue(forKey: nextIndex) {
                    if case .frame(let raw) = slot {
                        rawFrames.append(raw)
                        frames.append(Self.classifyFrame(raw, mode: classificationMode, cropThreshold: self.cropThreshold, step: self.sampleStep, metric: self.activenessMetric, state: &classifyState))
                        unclassifiedCount = classifyState.unclassified
                    }
                    nextIndex += 1
                }

                sampleIndex += 1
                progress = min(1.0, Double(sampleIndex) / Double(max(totalSamples, 1)))
                statusMessage = "Processing \(sampleIndex)/\(totalSamples)…"
            }
            if cancelled {
                generator.cancelAllCGImageGeneration()
            } else {
                // Defensive: if a requested time yielded no result the in-loop
                // drain would stall at that index; flush whatever remains in
                // ascending time order so no trailing frames are stranded.
                for idx in pending.keys.sorted() {
                    if case .frame(let raw)? = pending[idx] {
                        rawFrames.append(raw)
                        frames.append(Self.classifyFrame(raw, mode: classificationMode, cropThreshold: self.cropThreshold, step: self.sampleStep, metric: self.activenessMetric, state: &classifyState))
                    }
                }
                unclassifiedCount = classifyState.unclassified
            }
            let elapsed = Double(DispatchTime.now().uptimeNanoseconds &- startNanos.uptimeNanoseconds) / 1_000_000_000
            statusMessage = cancelled
                ? "Cancelled after \(frames.count) frames (\(String(format: "%.1f", elapsed))s)."
                : "Done — \(frames.count) frames at \(String(format: "%.2f", step))s · \(unclassifiedCount) unclassified · \(String(format: "%.1f", elapsed))s total."
        } catch {
            statusMessage = "Failed: \(error.localizedDescription)"
        }
        progress = 1
        isProcessing = false
    }

    /// Re-bucket the cached detections under a new mode — instant, no Vision.
    /// Safe to call mid-processing: it re-derives the frames so far and hands
    /// the running state back so `process` continues under the new mode.
    func rederive(mode: ClassificationMode, cropThreshold: Double, metric: ActivenessMetric) {
        classificationMode = mode
        self.cropThreshold = cropThreshold
        self.activenessMetric = metric
        guard !rawFrames.isEmpty else { classifyState = ClassifyState(); return }
        var state = ClassifyState()
        frames = rawFrames.map { Self.classifyFrame($0, mode: mode, cropThreshold: cropThreshold, step: sampleStep, metric: metric, state: &state) }
        unclassifiedCount = state.unclassified
        classifyState = state
    }

    // MARK: - Derivation

    /// Running per-position state carried across frames while deriving motion.
    private struct ClassifyState {
        var priorOuter: [FacePosition: [CGPoint]] = [:]
        var priorInner: [FacePosition: [CGPoint]] = [:]
        var outerHist: [FacePosition: [CGFloat]] = [:]
        var innerHist: [FacePosition: [CGFloat]] = [:]
        var percentHist: [FacePosition: [CGFloat]] = [:]
        var activityHist: [FacePosition: [CGFloat]] = [:]
        /// Running EMA of |Δopen%| per position (the movement signal).
        var activityEMA: [FacePosition: CGFloat] = [:]
        var unclassified = 0
        // Crop-trajectory state, carried across frames.
        var cropPosition: FacePosition = .center
        var lastCropChange: Double = 0
        var cropStarted = false
        var cropCenterX: CGFloat = 0
        var cropCenterStarted = false
        var lastFrameTime: Double = 0
        /// Which sides held a face in the previous frame. A left↔right switch
        /// toward a side that was empty last frame is a just-appeared subject
        /// (a cut / new arrangement), not a hand-off between two present
        /// speakers, so it bypasses the anxiety threshold.
        var sidesLastFrame: Set<FacePosition> = []
    }

    private static func classifyFrame(_ rf: RawFrame, mode: ClassificationMode, cropThreshold: Double, step: Double, metric: ActivenessMetric, state: inout ClassifyState) -> FrameAnalysis {
        var faces: [DetectedFace] = []
        var curOuter: [FacePosition: [CGPoint]] = [:]
        var curInner: [FacePosition: [CGPoint]] = [:]

        // This frame's per-position values; positions with no face read as 0 below.
        var oMot: [FacePosition: CGFloat] = [:]
        var iMot: [FacePosition: CGFloat] = [:]
        var pct: [FacePosition: CGFloat] = [:]

        for face in rf.faces {
            let pos = Self.classify(boundingBox: face.boundingBox, imageWidth: rf.imageSize.width, mode: mode)
            if pos == .unclassified { state.unclassified += 1 }

            let oMotion = Self.motion(prior: state.priorOuter[pos] ?? [], current: face.outerLips)
            let iMotion = Self.motion(prior: state.priorInner[pos] ?? [], current: face.innerLips)
            let height = Self.innerLipHeight(face.innerLips)
            let outerHeight = Self.innerLipHeight(face.outerLips)
            let lipPercent = outerHeight > 0 ? height / outerHeight : 0

            oMot[pos] = oMotion
            iMot[pos] = iMotion
            pct[pos] = lipPercent
            curOuter[pos] = face.outerLips
            curInner[pos] = face.innerLips

            faces.append(DetectedFace(
                boundingBox: face.boundingBox,
                position: pos,
                outerLips: face.outerLips,
                innerLips: face.innerLips,
                outerClassification: face.outerClassification,
                innerClassification: face.innerClassification,
                outerIsClosed: face.outerIsClosed,
                innerIsClosed: face.innerIsClosed,
                outerPrecision: face.outerPrecision,
                innerPrecision: face.innerPrecision,
                outerMotion: oMotion,
                innerMotion: iMotion,
                innerHeight: height,
                outerHeight: outerHeight,
                lipPercent: lipPercent))
        }

        // Append one value per position EVERY frame (0 when absent) so all
        // charts stay synchronized; also derive a movement "activity" = an EMA
        // of |Δopen%|. The smoothing factor targets the same ~0.5s memory the
        // old fixed window used (alpha = 2/(N+1) for an N-sample span), so it
        // scales with the sampling interval.
        let activityWindow = max(2, Int((0.5 / step).rounded()))
        let emaAlpha = CGFloat(2.0 / (Double(activityWindow) + 1.0))
        for pos in [FacePosition.left, .center, .right] {
            func push(_ dict: inout [FacePosition: [CGFloat]], _ value: CGFloat) {
                var arr = dict[pos] ?? []
                arr.append(value)
                if arr.count > Self.historyLength { arr.removeFirst(arr.count - Self.historyLength) }
                dict[pos] = arr
            }
            let current = pct[pos] ?? 0
            let prevPercent = state.percentHist[pos]?.last ?? current   // previous frame's open% (before this push)
            push(&state.outerHist, oMot[pos] ?? 0)
            push(&state.innerHist, iMot[pos] ?? 0)
            push(&state.percentHist, current)

            let delta = abs(current - prevPercent)
            let priorEMA = state.activityEMA[pos] ?? delta
            let ema = emaAlpha * delta + (1 - emaAlpha) * priorEMA
            state.activityEMA[pos] = ema
            push(&state.activityHist, ema)
        }

        // Next frame's "prior" is exactly this frame — positions with no face
        // this frame fall out, so they read as empty next time.
        state.priorOuter = curOuter
        state.priorInner = curInner

        // Active face: 1 face → it; 2+ → the face scoring highest on the chosen
        // metric. Movement = this position's latest open-% activity (the EMA of
        // |Δopen%| just pushed above); Openness = instantaneous mouth-open %.
        let activityByPosition = state.activityHist
        func activeness(_ face: DetectedFace) -> CGFloat {
            switch metric {
            case .movement: return activityByPosition[face.position]?.last ?? 0
            case .openness: return face.lipPercent
            }
        }
        let activeFaceIndex: Int?
        if faces.isEmpty { activeFaceIndex = nil }
        else if faces.count == 1 { activeFaceIndex = 0 }
        else { activeFaceIndex = faces.indices.max(by: { activeness(faces[$0]) < activeness(faces[$1]) }) }

        // Record each face's selection score for on-screen telemetry.
        for i in faces.indices { faces[i].activeness = activeness(faces[i]) }

        let frameCenter = rf.imageSize.width / 2

        // The active face's horizontal center and bucket (frame center if none).
        let activeCenterX: CGFloat
        let candidate: FacePosition
        if let ai = activeFaceIndex {
            activeCenterX = faces[ai].boundingBox.midX
            let p = faces[ai].position
            candidate = (p == .unclassified) ? .center : p
        } else {
            activeCenterX = frameCenter
            candidate = .center
        }

        let sidesThisFrame = Set(faces.map { $0.position })

        // Commit the crop SIDE with anxiety hysteresis: center↔side switches
        // immediately; left↔right only after cropThreshold seconds — UNLESS the
        // destination side had no face last frame, meaning its subject just
        // appeared (a cut / new arrangement). The threshold debounces hand-offs
        // between two continuously-present speakers; it must not hold the crop
        // on a stale side after a cut, which made it crawl toward the wrong
        // (now-different) person on the committed side.
        var switched = false
        if !state.cropStarted {
            state.cropPosition = candidate
            state.lastCropChange = rf.time
            state.cropStarted = true
            switched = true
        } else if candidate != state.cropPosition {
            let viaCenter = candidate == .center || state.cropPosition == .center
            let destinationJustAppeared = !state.sidesLastFrame.contains(candidate)
            if viaCenter || destinationJustAppeared || rf.time - state.lastCropChange >= cropThreshold {
                state.cropPosition = candidate
                state.lastCropChange = rf.time
                switched = true
            }
        }
        state.sidesLastFrame = sidesThisFrame

        // Crop target = horizontal center of the face on the COMMITTED side.
        // We deliberately never chase a face on a DIFFERENT side here: moving to
        // another subject happens ONLY through the committed switch above, which
        // snaps. With no faces at all the crop centers (the "0 faces → center"
        // rule). When the committed side has no face but others are present (mid
        // hysteresis), we HOLD the current crop rather than easing toward the
        // off-side subject — that off-side chase is what made the crop slowly
        // crawl to the other person instead of snapping.
        let targetCenterX: CGFloat
        if let f = faces.first(where: { $0.position == state.cropPosition }) {
            targetCenterX = f.boundingBox.midX
        } else if faces.isEmpty {
            targetCenterX = frameCenter
        } else {
            targetCenterX = state.cropCenterStarted ? state.cropCenterX : frameCenter
        }

        // Snap on a committed switch / first frame; otherwise ease toward the
        // committed-side face over ~8 real seconds to gently follow small moves.
        let snapped = switched || !state.cropCenterStarted
        if snapped {
            state.cropCenterX = targetCenterX
            state.cropCenterStarted = true
        } else {
            let dt = max(0, rf.time - state.lastFrameTime)
            let alpha = min(1.0, dt / 8.0)
            state.cropCenterX += (targetCenterX - state.cropCenterX) * alpha
        }
        state.lastFrameTime = rf.time

        return FrameAnalysis(
            time: rf.time, imageSize: rf.imageSize, faces: faces, analysisMs: rf.analysisMs,
            activeFaceIndex: activeFaceIndex, candidateCenterX: activeCenterX, actualCenterX: state.cropCenterX,
            targetCenterX: targetCenterX, cropPosition: state.cropPosition, candidate: candidate,
            cropSnapped: snapped, secondsSinceCropChange: rf.time - state.lastCropChange,
            motionOuter: state.outerHist, motionInner: state.innerHist, percent: state.percentHist,
            activity: state.activityHist)
    }

    static func classify(boundingBox: CGRect, imageWidth: CGFloat, mode: ClassificationMode) -> FacePosition {
        guard imageWidth > 0 else { return .unclassified }
        let left = boundingBox.minX / imageWidth
        let right = boundingBox.maxX / imageWidth
        switch mode {
        case .edges:
            if right < leftLine { return .left }
            if left > rightLine { return .right }
            if left < leftLine && right > rightLine { return .center }
            return .unclassified
        case .center:
            let cx = (left + right) / 2
            if cx < leftLine { return .left }
            if cx > rightLine { return .right }
            return .center
        }
    }

    /// Inner-lip opening height: split the inner-lip Y values at the median,
    /// average the lower half and the upper half, return |upper − lower|.
    static func innerLipHeight(_ points: [CGPoint]) -> CGFloat {
        guard points.count >= 2 else { return 0 }
        let ys = points.map(\.y).sorted()
        let half = ys.count / 2
        guard half > 0 else { return 0 }
        let lower = ys[..<half]                 // smaller Y (upper in the image)
        let upper = ys[(ys.count - half)...]    // larger Y (lower in the image)
        let lowerAvg = lower.reduce(0, +) / CGFloat(lower.count)
        let upperAvg = upper.reduce(0, +) / CGFloat(upper.count)
        return abs(upperAvg - lowerAvg)
    }

    /// Σ of per-point pixel distance. Current points beyond the prior array's
    /// length are measured from (0,0), per spec.
    static func motion(prior: [CGPoint], current: [CGPoint]) -> CGFloat {
        guard !current.isEmpty else { return 0 }
        var sum: CGFloat = 0
        for (i, p) in current.enumerated() {
            let q = i < prior.count ? prior[i] : .zero
            sum += hypot(p.x - q.x, p.y - q.y)
        }
        return sum
    }
}
