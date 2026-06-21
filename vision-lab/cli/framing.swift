import Foundation
import AVFoundation
import Vision
import CoreGraphics
import CoreImage
import ImageIO
import UniformTypeIdentifiers

// Framing calibration for the stacked-crop redesign. No head estimate needed:
//  - single (1 face, or 3+ → widest): full source HEIGHT 9:16, centered
//    horizontally on the face.
//  - double (2 faces): each person gets the full HALF-screen width as a 9:8
//    band (so a crop can never cross the center divider), centered vertically
//    on the face and slid toward center to fit. Left face → top band.
// Renders both an overlay (green=face, red=crop) and the composed 9:16 output.

struct FramingParams {
    // Currently unused — the full-region approach is parameter-free. Kept so the
    // CLI flags still bind; reserved for reintroducing head-aware framing later.
    var kWidth: CGFloat = 1.5
    var kUp: CGFloat = 0.6
    var kDown: CGFloat = 0.1
    var headFraction: CGFloat = 0.7
    var headHeightFraction: CGFloat = 0.45
}

/// Single 9:16 crop: full source height, centered horizontally on the face.
private func singleCrop(face: CGRect, imageSize: CGSize) -> CGRect {
    let aspect: CGFloat = 9.0 / 16.0
    var cropH = imageSize.height
    var cropW = cropH * aspect
    if cropW > imageSize.width { cropW = imageSize.width; cropH = cropW / aspect }
    let x = max(0, min(imageSize.width - cropW, face.midX - cropW / 2))
    let y = max(0, min(imageSize.height - cropH, (imageSize.height - cropH) / 2))
    return CGRect(x: x, y: y, width: cropW, height: cropH)
}

/// One stacked band: the full half-screen width (9:8), centered vertically on
/// the face and slid to fit within the source.
private func bandCrop(face: CGRect, leftHalf: Bool, imageSize: CGSize) -> CGRect {
    let cropW = imageSize.width / 2
    let cropH = cropW * 8.0 / 9.0          // band is 9:8 (W:H)
    let x = leftHalf ? 0 : imageSize.width / 2
    let y = max(0, min(imageSize.height - cropH, face.midY - cropH / 2))
    return CGRect(x: x, y: y, width: cropW, height: cropH)
}

private func writePNG(_ image: CGImage, to path: String) -> Bool {
    let url = URL(fileURLWithPath: path) as CFURL
    guard let dest = CGImageDestinationCreateWithURL(url, UTType.png.identifier as CFString, 1, nil) else { return false }
    CGImageDestinationAddImage(dest, image, nil)
    return CGImageDestinationFinalize(dest)
}

private func renderOverlay(_ cg: CGImage, faces: [CGRect], crops: [CGRect], to path: String) {
    let w = cg.width, h = cg.height
    guard let ctx = CGContext(data: nil, width: w, height: h, bitsPerComponent: 8, bytesPerRow: 0,
                              space: CGColorSpaceCreateDeviceRGB(),
                              bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue) else { return }
    ctx.draw(cg, in: CGRect(x: 0, y: 0, width: w, height: h))
    func bl(_ r: CGRect) -> CGRect { CGRect(x: r.minX, y: CGFloat(h) - r.maxY, width: r.width, height: r.height) }
    // Center divider (the line a band crop must not cross).
    ctx.setLineWidth(1)
    ctx.setStrokeColor(CGColor(red: 0.3, green: 0.6, blue: 1, alpha: 0.8))
    ctx.stroke(CGRect(x: CGFloat(w) / 2 - 0.5, y: 0, width: 1, height: CGFloat(h)))
    ctx.setLineWidth(3)
    ctx.setStrokeColor(CGColor(red: 0, green: 1, blue: 0, alpha: 1))
    for f in faces { ctx.stroke(bl(f)) }
    ctx.setLineWidth(5)
    ctx.setStrokeColor(CGColor(red: 1, green: 0, blue: 0, alpha: 1))
    for c in crops { ctx.stroke(bl(c)) }
    if let out = ctx.makeImage(), writePNG(out, to: path) { print("  wrote \(path)") }
}

/// Compose the final 9:16. `bands` is each (sourceCrop, destination-in-canvas)
/// in top-left coords; we convert to Core Image's bottom-left for rendering.
private func renderComposed(_ cg: CGImage, bands: [(crop: CGRect, dest: CGRect)], to path: String) {
    let canvas = CGRect(x: 0, y: 0, width: 720, height: 1280)
    let srcH = CGFloat(cg.height)
    let ci = CIImage(cgImage: cg)
    var output = CIImage(color: .black).cropped(to: canvas)
    for band in bands {
        let crop = band.crop, dest = band.dest
        let cropBL = CGRect(x: crop.minX, y: srcH - crop.maxY, width: crop.width, height: crop.height)
        let destBL = CGRect(x: dest.minX, y: canvas.height - dest.maxY, width: dest.width, height: dest.height)
        var piece = ci.cropped(to: cropBL).transformed(by: CGAffineTransform(translationX: -cropBL.minX, y: -cropBL.minY))
        piece = piece.transformed(by: CGAffineTransform(scaleX: destBL.width / crop.width, y: destBL.height / crop.height))
        piece = piece.transformed(by: CGAffineTransform(translationX: destBL.minX, y: destBL.minY))
        output = piece.composited(over: output)
    }
    let ctx = CIContext()
    if let out = ctx.createCGImage(output, from: canvas), writePNG(out, to: path) {
        print("  wrote \(path)")
    } else {
        print("  ERROR composing \(path)")
    }
}

private func faceBoxes(_ cg: CGImage, _ request: DetectFaceLandmarksRequest) async -> [CGRect] {
    let imgSize = CGSize(width: cg.width, height: cg.height)
    let obs = (try? await request.perform(on: cg)) ?? []
    var boxes: [CGRect] = []
    for o in obs where o.landmarks != nil {
        boxes.append(o.boundingBox.toImageCoordinates(imgSize, origin: .upperLeft))
    }
    return boxes.sorted { $0.midX < $1.midX }
}

private func renderFrame(_ cg: CGImage, faces: [CGRect], label: String) {
    let imgSize = CGSize(width: cg.width, height: cg.height)
    let full = CGRect(x: 0, y: 0, width: 720, height: 1280)
    let topBand = CGRect(x: 0, y: 0, width: 720, height: 640)
    let bottomBand = CGRect(x: 0, y: 640, width: 720, height: 640)
    var crops: [CGRect] = []
    var bands: [(crop: CGRect, dest: CGRect)] = []
    if faces.count == 2 {
        let top = bandCrop(face: faces[0], leftHalf: true, imageSize: imgSize)
        let bot = bandCrop(face: faces[1], leftHalf: false, imageSize: imgSize)
        crops = [top, bot]
        bands = [(top, topBand), (bot, bottomBand)]
    } else {
        let face = faces.count == 1 ? faces[0] : (faces.max(by: { $0.width < $1.width }) ?? .zero)
        let c = singleCrop(face: face, imageSize: imgSize)
        crops = [c]
        bands = [(c, full)]
    }
    print("  \(label): \(faces.count) face(s) -> \(faces.count == 2 ? "stacked" : "single")")
    renderOverlay(cg, faces: faces, crops: crops, to: "/tmp/framing_\(label)_overlay.png")
    renderComposed(cg, bands: bands, to: "/tmp/framing_\(label)_out.png")
}

func runFramingProbe(path: String, explicitTimes: [Double], params: FramingParams) async {
    let asset = AVURLAsset(url: URL(fileURLWithPath: path))
    let gen = AVAssetImageGenerator(asset: asset)
    gen.appliesPreferredTrackTransform = true
    gen.requestedTimeToleranceBefore = .zero
    gen.requestedTimeToleranceAfter = .zero
    gen.maximumSize = CGSize(width: 1280, height: 1280)
    let request = DetectFaceLandmarksRequest()

    print("\n=== framing calibration (green=face, red=crop; *_out.png = composed 9:16) ===")

    if !explicitTimes.isEmpty {
        for (i, t) in explicitTimes.enumerated() {
            guard let cg = try? await gen.image(at: CMTime(seconds: t, preferredTimescale: 600)).image else {
                print("  ERROR: could not read frame at \(t)s"); continue
            }
            let faces = await faceBoxes(cg, request)
            renderFrame(cg, faces: faces, label: String(format: "t%05.0f_%d", t, i))
        }
        return
    }

    let duration = (try? await asset.load(.duration))?.seconds ?? 0
    var foundOne = false, foundTwo = false
    var t = 2.0
    while t < min(duration, 240), !(foundOne && foundTwo) {
        if let cg = try? await gen.image(at: CMTime(seconds: t, preferredTimescale: 600)).image {
            let faces = await faceBoxes(cg, request)
            if faces.count == 1, !foundOne {
                foundOne = true
                renderFrame(cg, faces: faces, label: String(format: "single_%05.0f", t))
            } else if faces.count == 2, !foundTwo {
                foundTwo = true
                renderFrame(cg, faces: faces, label: String(format: "stacked_%05.0f", t))
            }
        }
        t += 2.0
    }
    if !foundOne { print("  (no single-face frame found in scan window)") }
    if !foundTwo { print("  (no two-face frame found in scan window)") }
}
