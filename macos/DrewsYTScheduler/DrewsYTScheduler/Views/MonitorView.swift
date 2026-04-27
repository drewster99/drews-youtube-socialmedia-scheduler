import SwiftUI
import AppKit

/// Tools → Monitor server. Live status + tail of the server log in a
/// fixed-width font, plus a "Reveal in Finder" button. Available in both
/// debug and release builds.
struct MonitorView: View {
    @ObservedObject var state: ServerStateModel
    @StateObject private var tail = LogTailReader(url: AppPaths.serverLogFile)
    @StateObject private var bootTail = LogTailReader(url: AppPaths.bootLogFile)
    @State private var autoScroll = true
    @State private var showingBootLog = false

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            if let mismatch = state.buildMismatch {
                BuildMismatchBanner(bundle: mismatch.bundle, server: mismatch.server)
                    .padding(.horizontal, 12)
                    .padding(.top, 12)
            }
            statusBlock
                .padding(.horizontal, 12)
                .padding(.top, state.buildMismatch == nil ? 12 : 0)

            Picker("", selection: $showingBootLog) {
                Text("Server log").tag(false)
                Text("Boot log (launchd)").tag(true)
            }
            .pickerStyle(.segmented)
            .padding(.horizontal, 12)

            Divider()

            // SwiftUI's ``Text`` rendering N kilobytes of monospaced log
            // text inside a ``ScrollView`` is genuinely slow on macOS —
            // every layout pass measures the whole string, beachballing
            // on scroll. ``NSTextView`` virtualizes its glyph layout, so
            // even multi-MiB logs scroll smoothly.
            LogTextView(
                text: currentContent.isEmpty ? "(empty)" : currentContent,
                autoScroll: autoScroll,
            )
            .background(Color(NSColor.textBackgroundColor))

            HStack {
                Toggle("Follow tail", isOn: $autoScroll)
                Spacer()
                Button("Reveal log in Finder") {
                    let url = showingBootLog ? AppPaths.bootLogFile : AppPaths.serverLogFile
                    NSWorkspace.shared.activateFileViewerSelecting([url])
                }
                // Restart is also allowed when status is .notFound — that's
                // the 'orphaned launchd job, no BTM record for this bundle'
                // case (typically a rebuilt .app whose old registration is
                // dangling). restartAgent() handles it: skip the no-op
                // SMAppService.unregister, force-unload via launchctl
                // bootout, kill the orphaned port-holder, register fresh.
                Button("Restart server") {
                    state.restartAgent()
                }
                .disabled(!(state.agentStatus == .enabled || state.agentStatus == .notFound))
            }
            .padding(12)
        }
        .frame(minWidth: 720, minHeight: 480)
        .onAppear {
            state.refresh()
            tail.start()
            bootTail.start()
        }
        .onDisappear {
            tail.stop()
            bootTail.stop()
        }
    }

    private var currentContent: String {
        showingBootLog ? bootTail.content : tail.content
    }

    /// Read-only ``NSTextView`` wrapped for SwiftUI. Auto-scrolls to the
    /// bottom on each text update when ``autoScroll`` is true. Glyph
    /// layout is virtualized by AppKit so multi-MiB logs scroll without
    /// the SwiftUI ``Text`` beachball.
    private struct LogTextView: NSViewRepresentable {
        let text: String
        let autoScroll: Bool

        func makeNSView(context: Context) -> NSScrollView {
            let scroll = NSTextView.scrollableTextView()
            scroll.hasVerticalScroller = true
            scroll.hasHorizontalScroller = false
            scroll.autohidesScrollers = false
            guard let textView = scroll.documentView as? NSTextView else { return scroll }
            textView.isEditable = false
            textView.isSelectable = true
            textView.isRichText = false
            textView.font = NSFont.monospacedSystemFont(ofSize: NSFont.systemFontSize, weight: .regular)
            textView.textContainerInset = NSSize(width: 8, height: 6)
            textView.drawsBackground = false
            textView.isHorizontallyResizable = false
            textView.isVerticallyResizable = true
            textView.textContainer?.widthTracksTextView = true
            return scroll
        }

        func updateNSView(_ scroll: NSScrollView, context: Context) {
            guard let textView = scroll.documentView as? NSTextView else { return }
            if textView.string != text {
                let attributed = NSAttributedString(
                    string: text,
                    attributes: [
                        .font: NSFont.monospacedSystemFont(ofSize: NSFont.systemFontSize, weight: .regular),
                        .foregroundColor: NSColor.textColor,
                    ],
                )
                textView.textStorage?.setAttributedString(attributed)
                if autoScroll {
                    textView.scrollToEndOfDocument(nil)
                }
            }
        }
    }

    private var statusBlock: some View {
        let bundle = BuildInfoReader.bundle
        return VStack(alignment: .leading, spacing: 8) {
            StatusLightsView(state: state)
            Text("App bundle: \(bundle.kind) \(bundle.version) (#\(bundle.buildNumber))  build_id=\(bundle.buildId.prefix(8))")
                .font(.footnote.monospaced())
                .foregroundStyle(.secondary)
            if let err = state.lastError {
                Text(err)
                    .font(.footnote)
                    .foregroundStyle(.red)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}
