import SwiftUI
import CoreGraphics

/// Draws the analysis overlay for the frame currently under the playhead.
/// All face geometry arrives in image pixels (upper-left origin); this maps
/// it onto the letterboxed video rect inside the player.
enum OverlayRenderer {

    static func aspectFit(content: CGSize, into bounds: CGSize) -> CGRect {
        guard content.width > 0, content.height > 0, bounds.width > 0, bounds.height > 0 else { return .zero }
        let scale = min(bounds.width / content.width, bounds.height / content.height)
        let w = content.width * scale
        let h = content.height * scale
        return CGRect(x: (bounds.width - w) / 2, y: (bounds.height - h) / 2, width: w, height: h)
    }

    static func draw(_ ctx: GraphicsContext, size: CGSize, frame: FrameAnalysis?, imageSize: CGSize,
                     summary: (min: Double, max: Double, avg: Double)?) {
        let videoRect = aspectFit(content: imageSize, into: size)
        guard videoRect.width > 0 else { return }
        drawTimingPanel(ctx, videoRect: videoRect, frameMs: frame?.analysisMs, summary: summary)
        let sx = videoRect.width / imageSize.width
        let sy = videoRect.height / imageSize.height
        func map(_ p: CGPoint) -> CGPoint {
            CGPoint(x: videoRect.minX + p.x * sx, y: videoRect.minY + p.y * sy)
        }

        // 45% / 55% classification guides.
        for frac in [Double(VideoProcessor.leftLine), Double(VideoProcessor.rightLine)] {
            let x = videoRect.minX + CGFloat(frac) * videoRect.width
            var line = Path()
            line.move(to: CGPoint(x: x, y: videoRect.minY))
            line.addLine(to: CGPoint(x: x, y: videoRect.maxY))
            ctx.stroke(line, with: .color(.yellow.opacity(0.45)), style: StrokeStyle(lineWidth: 1, dash: [5, 5]))
        }

        guard let frame else { return }

        func toView(_ r: CGRect) -> CGRect {
            CGRect(x: videoRect.minX + r.minX * sx, y: videoRect.minY + r.minY * sy,
                   width: r.width * sx, height: r.height * sy)
        }

        // Darken everything outside the actual (committed) crop, so the kept
        // region reads as the bright window.
        let actualCropView = toView(cropRect(centerX: frame.actualCenterX, imageSize: imageSize))
        var mask = Path()
        mask.addRect(videoRect)
        mask.addRect(actualCropView)
        ctx.fill(mask, with: .color(.black.opacity(0.6)), style: FillStyle(eoFill: true))

        for face in frame.faces {
            let box = CGRect(
                x: videoRect.minX + face.boundingBox.minX * sx,
                y: videoRect.minY + face.boundingBox.minY * sy,
                width: face.boundingBox.width * sx,
                height: face.boundingBox.height * sy)

            if face.position == .unclassified {
                ctx.fill(Path(box), with: .color(.red.opacity(0.25)))
            }
            ctx.stroke(Path(box),
                       with: .color(face.position == .unclassified ? .red : .green),
                       lineWidth: 2)

            drawLipContour(ctx, points: face.outerLips.map(map), color: .red, isClosed: face.outerIsClosed)
            drawLipContour(ctx, points: face.innerLips.map(map), color: .blue, isClosed: face.innerIsClosed)

            // Classification + distilled precision below the box.
            drawInfo(ctx, face: face, below: box)
        }

        drawBottomCharts(ctx, frame: frame, videoRect: videoRect)

        // Top-center: open-% activity (Δ of open% over ~0.5s) for L/C/R.
        let actW = min(440, videoRect.width * 0.42)
        let actRect = CGRect(x: videoRect.midX - actW / 2, y: videoRect.minY + 22, width: actW, height: 64)
        drawTriChart(ctx, rect: actRect, keyLabel: "Δopen%",
                     left: frame.activity[.left] ?? [], center: frame.activity[.center] ?? [], right: frame.activity[.right] ?? [])
    }

    /// Fixed bottom-of-screen charts: a wide open-% chart (left/center/right on
    /// one graph, with a color key) centered at the very bottom, and three
    /// motion charts (red=outer / blue=inner) centered on the left/center/right
    /// thirds just above it.
    static func drawBottomCharts(_ ctx: GraphicsContext, frame: FrameAnalysis, videoRect: CGRect) {
        let w = videoRect.width

        // Open-% chart: centered, at the very bottom.
        let pctW = min(440, w * 0.42)
        let pctH: CGFloat = 64
        let pctRect = CGRect(x: videoRect.midX - pctW / 2, y: videoRect.maxY - pctH - 8,
                             width: pctW, height: pctH)
        drawTriChart(ctx, rect: pctRect, keyLabel: "open%",
                     left: frame.percent[.left] ?? [], center: frame.percent[.center] ?? [], right: frame.percent[.right] ?? [])

        // Three motion charts above, centered on the left / center / right thirds.
        let motW = min(190, w * 0.26)
        let motH: CGFloat = 48
        let motY = pctRect.minY - 15 - motH - 6
        let centers = [videoRect.minX + w * 0.25, videoRect.midX, videoRect.minX + w * 0.75]
        for (cx, pos) in zip(centers, [FacePosition.left, .center, .right]) {
            let r = CGRect(x: cx - motW / 2, y: motY, width: motW, height: motH)
            let motSeries: [(values: [CGFloat], color: Color)] = [
                (frame.motionOuter[pos] ?? [], .red),
                (frame.motionInner[pos] ?? [], .blue),
            ]
            drawSeriesChart(ctx, rect: r, series: motSeries)
            ctx.draw(Text(pos.rawValue.prefix(1).uppercased()).font(.system(size: 10, weight: .semibold)).foregroundColor(.white.opacity(0.85)),
                     at: CGPoint(x: r.minX + 3, y: r.minY + 2), anchor: .topLeading)
        }
    }

    /// One graph with Left/Center/Right series (cyan / yellow / magenta) + key.
    static func drawTriChart(_ ctx: GraphicsContext, rect: CGRect, keyLabel: String,
                             left: [CGFloat], center: [CGFloat], right: [CGFloat]) {
        let leftColor = Color.cyan
        let centerColor = Color.yellow
        let rightColor = Color(red: 1.0, green: 0.1, blue: 0.9)
        let series: [(values: [CGFloat], color: Color)] = [
            (left, leftColor), (center, centerColor), (right, rightColor),
        ]
        drawSeriesChart(ctx, rect: rect, series: series)

        let ky = rect.minY - 15
        var kx = rect.minX + 4
        for (label, color) in [("L", leftColor), ("C", centerColor), ("R", rightColor)] {
            ctx.fill(Path(CGRect(x: kx, y: ky + 2, width: 10, height: 10)), with: .color(color))
            ctx.draw(Text("\(label) \(keyLabel)").font(.system(size: 10, weight: .semibold)).foregroundColor(.white),
                     at: CGPoint(x: kx + 13, y: ky), anchor: .topLeading)
            kx += 90
        }
    }

    /// A 9:16 full-height crop strip centered at `centerX`, clamped into frame.
    static func cropRect(centerX: CGFloat, imageSize: CGSize) -> CGRect {
        let cw = min(imageSize.width, imageSize.height * 9.0 / 16.0)
        let x = max(0, min(imageSize.width - cw, centerX - cw / 2))
        return CGRect(x: x, y: 0, width: cw, height: imageSize.height)
    }

    /// Strokes the contour through the points; the closing segment is the
    /// region color when Vision marked it closed, otherwise yellow.
    static func drawLipContour(_ ctx: GraphicsContext, points: [CGPoint], color: Color, isClosed: Bool) {
        guard points.count >= 2 else { return }
        var open = Path()
        open.move(to: points[0])
        for p in points.dropFirst() { open.addLine(to: p) }
        ctx.stroke(open, with: .color(color), lineWidth: 2)

        var closing = Path()
        closing.move(to: points[points.count - 1])
        closing.addLine(to: points[0])
        ctx.stroke(closing, with: .color(isClosed ? color : .yellow), lineWidth: 2)
    }

    static func drawSeriesChart(_ ctx: GraphicsContext, rect: CGRect, series: [(values: [CGFloat], color: Color)]) {
        ctx.fill(Path(roundedRect: rect, cornerRadius: 3), with: .color(.black.opacity(0.55)))
        ctx.stroke(Path(roundedRect: rect, cornerRadius: 3), with: .color(.white.opacity(0.25)), lineWidth: 0.5)

        let window = VideoProcessor.historyLength
        guard window > 1 else { return }
        let maxValue = max(1, series.flatMap(\.values).max() ?? 1)

        for s in series {
            guard s.values.count >= 2 else { continue }
            var path = Path()
            for (i, v) in s.values.enumerated() {
                let slot = window - s.values.count + i          // newest aligns to the right
                let x = rect.minX + (CGFloat(slot) / CGFloat(window - 1)) * rect.width
                let y = rect.maxY - (v / maxValue) * rect.height
                if i == 0 { path.move(to: CGPoint(x: x, y: y)) } else { path.addLine(to: CGPoint(x: x, y: y)) }
            }
            ctx.stroke(path, with: .color(s.color), lineWidth: 1.5)
        }
    }

    /// Upper-right panel: this frame's analysis time plus min/max/avg.
    static func drawTimingPanel(_ ctx: GraphicsContext, videoRect: CGRect,
                                frameMs: Double?, summary: (min: Double, max: Double, avg: Double)?) {
        guard frameMs != nil || summary != nil else { return }
        var lines = [frameMs.map { String(format: "this: %.1f ms", $0) } ?? "this: —"]
        if let s = summary {
            lines.append(String(format: "min %.1f · max %.1f", s.min, s.max))
            lines.append(String(format: "avg %.1f ms", s.avg))
        }
        let text = Text(lines.joined(separator: "\n"))
            .font(.system(size: 11, weight: .medium).monospaced())
            .foregroundColor(.white)
        let resolved = ctx.resolve(text)
        let s = resolved.measure(in: CGSize(width: 1000, height: 1000))
        let origin = CGPoint(x: videoRect.maxX - s.width - 8, y: videoRect.minY + 6)
        let panel = CGRect(origin: origin, size: s).insetBy(dx: -5, dy: -3)
        ctx.fill(Path(roundedRect: panel, cornerRadius: 4), with: .color(.black.opacity(0.62)))
        ctx.draw(resolved, at: origin, anchor: .topLeading)
    }

    static func drawInfo(_ ctx: GraphicsContext, face: DetectedFace, below box: CGRect) {
        func prec(_ v: Float?) -> String { v.map { String(format: "%.2f", $0) } ?? "n/a" }
        func dist(_ v: CGFloat) -> String { String(format: "%.1f", v) }
        func pct(_ v: CGFloat) -> String { String(format: "%.0f%%", v * 100) }
        let lines = [
            face.position.rawValue.uppercased(),
            "outer: \(face.outerClassification)  p=\(prec(face.outerPrecisionMean))  Δ=\(dist(face.outerMotion))",
            "inner: \(face.innerClassification)  p=\(prec(face.innerPrecisionMean))  Δ=\(dist(face.innerMotion))",
            "open: \(pct(face.lipPercent))  (\(dist(face.innerHeight))/\(dist(face.outerHeight)))",
        ].joined(separator: "\n")

        let text = Text(lines)
            .font(.system(size: 11, weight: .medium).monospaced())
            .foregroundColor(.white)
        // Back the text with a dark panel so it stays legible over any frame.
        let resolved = ctx.resolve(text)
        let textSize = resolved.measure(in: CGSize(width: 1000, height: 1000))
        let origin = CGPoint(x: box.minX, y: box.maxY + 6)
        let panel = CGRect(origin: origin, size: textSize).insetBy(dx: -4, dy: -3)
        ctx.fill(Path(roundedRect: panel, cornerRadius: 4), with: .color(.black.opacity(0.62)))
        ctx.draw(resolved, at: origin, anchor: .topLeading)
    }
}
