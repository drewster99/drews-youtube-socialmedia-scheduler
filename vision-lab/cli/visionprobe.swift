import Foundation
import AVFoundation
import Vision
import CoreGraphics
import ImageIO
import UniformTypeIdentifiers

// Native-Vision comparison probe: on a sample frame, draw
//   blue  = DetectHumanRectangles (person box),
//   green = head box derived from DetectHumanBodyPose head keypoints,
//   red   = the pose head keypoints (nose/eyes/ears).
// Shows what each Vision primitive gives on the real footage.

private func headBoxFromPose(_ pts: [VNHumanBodyPoseObservation.JointName: VNRecognizedPoint],
                             imgW: CGFloat, imgH: CGFloat, thr: Float) -> (box: CGRect, dots: [CGPoint])? {
    // VN points are normalized, origin bottom-left → convert to top-left pixels.
    func pt(_ j: VNHumanBodyPoseObservation.JointName) -> CGPoint? {
        guard let p = pts[j], p.confidence >= thr else { return nil }
        return CGPoint(x: p.location.x * imgW, y: (1 - p.location.y) * imgH)
    }
    let nose = pt(.nose), le = pt(.leftEye), re = pt(.rightEye), lear = pt(.leftEar), rear = pt(.rightEar)
    let present = [nose, le, re, lear, rear].compactMap { $0 }
    guard present.count >= 2 else { return nil }

    let cx = present.map(\.x).reduce(0, +) / CGFloat(present.count)
    var width: CGFloat
    if let l = lear, let r = rear { width = abs(l.x - r.x) * 1.3 }
    else if let l = le, let r = re { width = abs(l.x - r.x) * 2.4 }
    else {
        let xs = present.map(\.x)
        let lo = xs.min() ?? cx, hi = xs.max() ?? cx
        width = (hi - lo) * 2.0
    }
    width = max(width, 24)

    let eyeY: CGFloat
    if let l = le, let r = re { eyeY = (l.y + r.y) / 2 }
    else if let n = nose { eyeY = n.y }
    else { eyeY = present.map(\.y).reduce(0, +) / CGFloat(present.count) }

    let height = width * 1.4
    let top = eyeY - height * 0.52
    return (CGRect(x: cx - width / 2, y: top, width: width, height: height), present)
}

private func savePNG(_ image: CGImage, _ path: String) {
    guard let dest = CGImageDestinationCreateWithURL(URL(fileURLWithPath: path) as CFURL,
                                                     UTType.png.identifier as CFString, 1, nil) else { return }
    CGImageDestinationAddImage(dest, image, nil)
    if CGImageDestinationFinalize(dest) { print("  wrote \(path)") }
}

func runVisionOverlay(path: String, times: [Double]) async {
    let asset = AVURLAsset(url: URL(fileURLWithPath: path))
    let gen = AVAssetImageGenerator(asset: asset)
    gen.appliesPreferredTrackTransform = true
    gen.requestedTimeToleranceBefore = .zero
    gen.requestedTimeToleranceAfter = .zero
    gen.maximumSize = CGSize(width: 1280, height: 1280)
    let stamps = times.isEmpty ? [8.0] : times

    print("\n=== native Vision overlay (blue=human rect, green=pose head box, red=head kpts) ===")
    for t in stamps {
        guard let cg = try? await gen.image(at: CMTime(seconds: t, preferredTimescale: 600)).image else {
            print("  could not read frame at \(t)s"); continue
        }
        let w = CGFloat(cg.width), h = CGFloat(cg.height)
        let handler = VNImageRequestHandler(cgImage: cg, options: [:])
        let rectReq = VNDetectHumanRectanglesRequest()
        rectReq.upperBodyOnly = false
        let poseReq = VNDetectHumanBodyPoseRequest()
        do { try handler.perform([rectReq, poseReq]) }
        catch { print("  Vision failed at \(t)s: \(error.localizedDescription)"); continue }

        guard let ctx = CGContext(data: nil, width: cg.width, height: cg.height, bitsPerComponent: 8,
                                  bytesPerRow: 0, space: CGColorSpaceCreateDeviceRGB(),
                                  bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue) else { continue }
        ctx.draw(cg, in: CGRect(x: 0, y: 0, width: w, height: h))
        func bl(_ r: CGRect) -> CGRect { CGRect(x: r.minX, y: h - r.maxY, width: r.width, height: r.height) }

        let humans = (rectReq.results) ?? []
        ctx.setLineWidth(4)
        ctx.setStrokeColor(CGColor(red: 0.2, green: 0.5, blue: 1, alpha: 1))   // blue person boxes
        for ob in humans {
            // VN boundingBox is normalized, bottom-left origin → top-left pixels.
            let b = ob.boundingBox
            let r = CGRect(x: b.minX * w, y: (1 - b.maxY) * h, width: b.width * w, height: b.height * h)
            ctx.stroke(bl(r))
        }

        let poses = (poseReq.results) ?? []
        var heads = 0
        for ob in poses {
            guard let pts = try? ob.recognizedPoints(.all) else { continue }
            guard let (box, dots) = headBoxFromPose(pts, imgW: w, imgH: h, thr: 0.2) else { continue }
            heads += 1
            ctx.setLineWidth(4)
            ctx.setStrokeColor(CGColor(red: 0, green: 1, blue: 0, alpha: 1))    // green head box
            ctx.stroke(bl(box))
            ctx.setFillColor(CGColor(red: 1, green: 0, blue: 0, alpha: 1))      // red keypoints
            for d in dots { ctx.fillEllipse(in: bl(CGRect(x: d.x - 4, y: d.y - 4, width: 8, height: 8))) }
        }
        print(String(format: "  t=%.1fs: %d human rect(s), %d pose head box(es)", t, humans.count, heads))
        if let out = ctx.makeImage() { savePNG(out, "/tmp/visionprobe_\(Int(t)).png") }
    }
}
