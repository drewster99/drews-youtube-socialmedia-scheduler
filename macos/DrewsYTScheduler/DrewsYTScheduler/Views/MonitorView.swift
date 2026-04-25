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

            ScrollViewReader { proxy in
                ScrollView {
                    Text(currentContent.isEmpty ? "(empty)" : currentContent)
                        .font(.system(.body, design: .monospaced))
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .padding(10)
                        .id("logBottom")
                }
                .background(Color(NSColor.textBackgroundColor))
                .onChange(of: currentContent) { _, _ in
                    if autoScroll {
                        proxy.scrollTo("logBottom", anchor: .bottom)
                    }
                }
                .onChange(of: showingBootLog) { _, _ in
                    proxy.scrollTo("logBottom", anchor: .bottom)
                }
            }

            HStack {
                Toggle("Follow tail", isOn: $autoScroll)
                Spacer()
                Button("Reveal log in Finder") {
                    let url = showingBootLog ? AppPaths.bootLogFile : AppPaths.serverLogFile
                    NSWorkspace.shared.activateFileViewerSelecting([url])
                }
                Button("Restart server") {
                    state.restartAgent()
                }
                .disabled(state.agentStatus != .enabled)
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
