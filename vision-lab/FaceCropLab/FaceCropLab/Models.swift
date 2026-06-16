import Foundation
import CoreGraphics

/// Where a face sits horizontally, per the project's edge rules:
/// left = whole face left of 45%; right = whole face right of 55%;
/// center = left edge < 45% AND right edge > 55%. A face that fits none
/// (e.g. its right edge lands between 45–55%) is `unclassified`.
enum FacePosition: String {
    case left, center, right, unclassified
}

/// How a face's bounding box is mapped to a position bucket.
enum ClassificationMode: String, CaseIterable, Identifiable {
    /// Strict edges: left = whole face < 45%, right = whole face > 55%,
    /// center = spans both lines; anything straddling a line = unclassified.
    case edges = "Edge rules"
    /// Bucket by face midpoint: center-x < 45% → left, > 55% → right, else
    /// center. Never produces `unclassified` — splits side-by-side cleanly.
    case center = "Face center"
    var id: String { rawValue }
}

/// Which mouth signal decides the active (speaking) face when several are
/// visible.
enum ActivenessMetric: String, CaseIterable, Identifiable {
    /// Total variation of mouth-open % over the last ~0.5s — a talking mouth
    /// moves, a merely-open mouth does not. The default.
    case movement = "Movement"
    /// Instantaneous mouth-open % (inner-lip height / outer-lip height).
    case openness = "Open %"
    var id: String { rawValue }
}

/// One face detected in one processed frame. Coordinates are in image
/// pixels with an upper-left origin (so they map straight onto the
/// displayed video without a Y-flip).
struct DetectedFace: Identifiable {
    let id = UUID()
    var boundingBox: CGRect
    var position: FacePosition
    /// Vision's detection confidence (0–1). Low values are usually false
    /// positives (a face found in background texture); used to keep phantoms
    /// out of the active-face selection.
    var confidence: Float
    var outerLips: [CGPoint]
    var innerLips: [CGPoint]
    /// Vision's `pointsClassification` for the region ("closedPath" /
    /// "openPath" / "disconnected"), kept as text for the on-screen label.
    var outerClassification: String
    var innerClassification: String
    /// Whether Vision marked the region as a closed loop. When false we still
    /// close the contour for drawing, but the closing segment is drawn yellow.
    var outerIsClosed: Bool
    var innerIsClosed: Bool
    var outerPrecision: [Float]
    var innerPrecision: [Float]
    /// Σ of per-point pixel distance from the prior processed frame's lip
    /// points at this same position (missing prior points treated as (0,0)).
    var outerMotion: CGFloat
    var innerMotion: CGFloat
    /// Inner-lip opening height this frame: |avg(upper-half Y) − avg(lower-half Y)|
    /// of the inner-lip points (split at the median Y).
    var innerHeight: CGFloat
    /// Outer-lip height (same median-split measure on the outer-lip points).
    var outerHeight: CGFloat
    /// innerHeight / outerHeight — mouth-openness ratio used to pick the active
    /// face. 0 when outerHeight is degenerate.
    var lipPercent: CGFloat
    /// The score this face was given when picking the active face, under the
    /// current metric (movement EMA or open %). Telemetry only.
    var activeness: CGFloat = 0

    /// Per-point precision estimates distilled to one number (mean of the
    /// 0–1 confidences). `nil` when Vision supplied no estimates.
    var outerPrecisionMean: Float? { Self.mean(outerPrecision) }
    var innerPrecisionMean: Float? { Self.mean(innerPrecision) }

    static func mean(_ values: [Float]) -> Float? {
        values.isEmpty ? nil : values.reduce(0, +) / Float(values.count)
    }
}

/// Raw, mode-independent Vision detection for one face in one frame. Cached so
/// switching classification mode only re-buckets — it never re-runs Vision.
struct RawFace {
    var boundingBox: CGRect
    var confidence: Float
    var outerLips: [CGPoint]
    var innerLips: [CGPoint]
    var outerClassification: String
    var innerClassification: String
    var outerIsClosed: Bool
    var innerIsClosed: Bool
    var outerPrecision: [Float]
    var innerPrecision: [Float]
}

/// Raw detection for one sampled frame (Vision output + timing, no bucketing).
struct RawFrame {
    var time: Double
    var imageSize: CGSize
    var analysisMs: Double
    var faces: [RawFace]
}

/// The analysis of a single sampled frame.
struct FrameAnalysis: Identifiable {
    let id = UUID()
    var time: Double
    var imageSize: CGSize
    var faces: [DetectedFace]
    /// Wall-clock milliseconds spent on Vision detection + per-face work for
    /// this frame (not counting frame extraction).
    var analysisMs: Double
    /// Index into `faces` of the active (speaking) face, or nil if no faces.
    var activeFaceIndex: Int?
    /// Image-px x-center of a 9:16 crop on the active face right now
    /// (instantaneous target — drawn as the white outline).
    var candidateCenterX: CGFloat
    /// Image-px x-center of the committed, eased crop (drawn as the purple fill).
    var actualCenterX: CGFloat
    /// Image-px x-center the eased crop is moving toward this frame.
    var targetCenterX: CGFloat
    /// The committed crop side (left/center/right) after this frame's hysteresis.
    var cropPosition: FacePosition
    /// The side this frame proposed (active face's bucket) before hysteresis.
    var candidate: FacePosition
    /// True when the crop snapped to the target this frame (committed switch /
    /// first frame); false when it eased toward it.
    var cropSnapped: Bool
    /// Seconds since the committed crop side last changed.
    var secondsSinceCropChange: Double
    /// Per-position rolling histories (last ≤20), for the fixed on-screen charts.
    var motionOuter: [FacePosition: [CGFloat]]
    var motionInner: [FacePosition: [CGFloat]]
    var percent: [FacePosition: [CGFloat]]
    /// Per-position open-% "activity": EMA of |Δopen%| (recency-weighted, ~0.5s memory).
    var activity: [FacePosition: [CGFloat]]
}
