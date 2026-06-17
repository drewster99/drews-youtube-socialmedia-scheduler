import Foundation
import AVFoundation
import CoreImage
import CoreGraphics

/// Renders a moving 9:16 crop to a new video file. The crop column for each
/// output frame is taken from the analysis trajectory (the eased `actualCenterX`,
/// expressed as a fraction of frame width) and interpolated to the output frame
/// times. Orientation, audio, and encoding are handled by AVFoundation.
enum CropExporter {

    struct ExportError: LocalizedError {
        let message: String
        var errorDescription: String? { message }
    }

    /// One trajectory sample: `fraction` is the crop center as a fraction of the
    /// display width (0…1) at `time` seconds.
    struct Center: Sendable {
        var time: Double
        var fraction: CGFloat
    }

    /// Build the crop trajectory from analyzed frames: each frame's committed,
    /// eased crop center as a fraction of frame width.
    static func centers(from frames: [FrameAnalysis]) -> [Center] {
        frames.compactMap { f in
            guard f.imageSize.width > 0 else { return nil }
            return Center(time: f.time, fraction: f.actualCenterX / f.imageSize.width)
        }
    }

    /// Linearly interpolate the crop-center fraction at `t` from the sorted
    /// `centers`. Clamps to the ends.
    static func fraction(at t: Double, in centers: [Center]) -> CGFloat {
        guard let first = centers.first else { return 0.5 }
        if t <= first.time { return first.fraction }
        guard let last = centers.last else { return 0.5 }
        if t >= last.time { return last.fraction }
        var lo = 0, hi = centers.count - 1
        while lo + 1 < hi {
            let mid = (lo + hi) / 2
            if centers[mid].time <= t { lo = mid } else { hi = mid }
        }
        let a = centers[lo], b = centers[hi]
        let span = b.time - a.time
        guard span > 1e-9 else { return a.fraction }
        let f = CGFloat((t - a.time) / span)
        return a.fraction + (b.fraction - a.fraction) * f
    }

    /// Render `source` to `output` as a 9:16 video, moving the crop column per
    /// `centers`. `renderSize` nil = native crop resolution (full source height,
    /// no downscale — input-quality output). Reports 0…1 progress. Overwrites
    /// `output`.
    static func export(source: URL, to output: URL, centers: [Center],
                       renderSize: CGSize? = nil,
                       progress: @escaping @Sendable (Double) -> Void) async throws {
        let asset = AVURLAsset(url: source)
        guard let track = try await asset.loadTracks(withMediaType: .video).first else {
            throw ExportError(message: "No video track in source.")
        }
        let natural = try await track.load(.naturalSize)
        let transform = try await track.load(.preferredTransform)
        let displayed = natural.applying(transform)
        let srcW = abs(displayed.width)
        let srcH = abs(displayed.height)
        guard srcW > 0, srcH > 0 else { throw ExportError(message: "Could not read source dimensions.") }

        // 9:16 crop column, full source height. Both output dimensions rounded to
        // even — H.264 requires it (an odd-height source would otherwise fail).
        let cropW = (srcH * 9.0 / 16.0 / 2.0).rounded() * 2
        let evenH = (srcH / 2).rounded(.down) * 2
        // Native crop resolution by default → no downscale, input-quality output.
        let outSize = renderSize ?? CGSize(width: cropW, height: evenH)
        let renderW = outSize.width

        let comp = AVMutableVideoComposition(asset: asset) { request in
            let srcImage = request.sourceImage           // already display-oriented; origin bottom-left
            let frac = fraction(at: request.compositionTime.seconds, in: centers)
            let cropX = max(0, min(srcW - cropW, frac * srcW - cropW / 2)).rounded()
            let cropped = srcImage
                .cropped(to: CGRect(x: cropX, y: 0, width: cropW, height: srcH))
                .transformed(by: CGAffineTransform(translationX: -cropX, y: 0))
            // Uniform scale: cropW:srcH == renderW:renderH == 9:16.
            let scale = renderW / cropW
            let out = cropped.transformed(by: CGAffineTransform(scaleX: scale, y: scale))
            request.finish(with: out, context: nil)
        }
        comp.renderSize = outSize

        guard let session = AVAssetExportSession(asset: asset, presetName: AVAssetExportPresetHighestQuality) else {
            throw ExportError(message: "Could not create export session.")
        }
        session.videoComposition = comp
        session.outputURL = output
        session.outputFileType = .mov
        // Render only the span we have a trajectory for (covers the whole video
        // for a full analysis; bounds a partial one).
        if let last = centers.last {
            session.timeRange = CMTimeRange(start: .zero,
                                            duration: CMTime(seconds: last.time + 0.1, preferredTimescale: 600))
        }
        if FileManager.default.fileExists(atPath: output.path) {
            try FileManager.default.removeItem(at: output)
        }

        // AVAssetExportSession isn't Sendable; box it so the progress poller can
        // read `.progress` from a child task without a data-race diagnostic
        // (the read is safe).
        struct SessionBox: @unchecked Sendable { let session: AVAssetExportSession }
        let box = SessionBox(session: session)
        let poller = Task {
            while !Task.isCancelled {
                let p = Double(box.session.progress)
                progress(p)
                if p >= 1.0 { break }
                try? await Task.sleep(nanoseconds: 250_000_000)
            }
        }
        defer { poller.cancel() }

        await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
            session.exportAsynchronously { cont.resume() }
        }

        switch session.status {
        case .completed: progress(1.0)
        case .cancelled: throw ExportError(message: "Export cancelled.")
        default: throw session.error ?? ExportError(message: "Export failed (\(session.status.rawValue)).")
        }
    }
}
