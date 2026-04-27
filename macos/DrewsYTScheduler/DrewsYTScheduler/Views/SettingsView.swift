import SwiftUI
import AppKit

/// Settings window. The background agent is always-on (installed once during
/// Welcome, never offered as a choice afterwards). The only login-related
/// toggle here controls whether a menu-bar item appears at login.
struct SettingsView: View {
    @ObservedObject var state: ServerStateModel
    @State private var menubarAtLogin = false
    @State private var probeTask: Task<Void, Never>?

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            if let mismatch = state.buildMismatch {
                BuildMismatchBanner(bundle: mismatch.bundle, server: mismatch.server)
            }

            GroupBox(label: Text("Background service").font(.headline)) {
                VStack(alignment: .leading, spacing: 10) {
                    StatusLightsView(state: state)
                    HStack {
                        // Restart is also allowed when status is .notFound —
                        // see MonitorView for why. restartAgent() handles
                        // the orphan case end-to-end.
                        Button("Restart server") { state.restartAgent() }
                            .disabled(state.busy ||
                                      !(state.agentStatus == .enabled ||
                                        state.agentStatus == .notFound))
                        Button("Reinstall…") { state.registerAgent() }
                            .disabled(state.busy)
                        Button("Activity log…") {
                            (NSApp.delegate as? AppDelegate)?.showActivityLogWindow()
                        }
                        if state.busy {
                            ProgressView().controlSize(.small)
                        }
                    }
                    if let err = state.lastError {
                        Text(err).font(.footnote).foregroundStyle(.red)
                    }
                }
                .padding(8)
            }

            GroupBox(label: Text("Menu bar").font(.headline)) {
                VStack(alignment: .leading, spacing: 4) {
                    Toggle("Show menu-bar item at login", isOn: $menubarAtLogin)
                        .onChange(of: menubarAtLogin) { _, newValue in
                            state.setLoginItemEnabled(newValue)
                            (NSApp.delegate as? AppDelegate)?.setMenuBarVisible(newValue)
                        }
                    Text("Adds a status item to the menu bar with quick controls. Also registers Drew's YT Scheduler as a login item so the menu-bar item is available after restart. Background service is unaffected — it always starts on login.")
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
                .padding(8)
            }

            GroupBox(label: Text("Data files").font(.headline)) {
                PathRow(label: "Application data", url: AppPaths.dataDirectory)
                    .padding(8)
            }

            GroupBox(label: Text("Logs").font(.headline)) {
                VStack(alignment: .leading, spacing: 6) {
                    PathRow(label: "Log directory", url: AppPaths.logDirectory)
                    PathRow(label: "Server log file", url: AppPaths.serverLogFile)
                }
                .padding(8)
            }

            Divider()
                .padding(.vertical, 8)

            HStack {
                Spacer()
                Button(action: {
                    NSWorkspace.shared.open(AppPaths.serverWebURL)
                }) {
                    Text("Open UI")
                        .font(.headline)
                        .foregroundStyle(.white)
                        .frame(minWidth: 160)
                        .padding(.vertical, 4)
                }
                .controlSize(.large)
                .buttonStyle(.borderedProminent)
                .tint(.blue)
            }
            .padding(.top, 4)

            Spacer()
        }
        .padding(20)
        .frame(minWidth: 1120, minHeight: 756)
        .onAppear {
            menubarAtLogin = state.loginItemStatus == .enabled
            startProbing()
        }
        .onDisappear {
            probeTask?.cancel()
            probeTask = nil
        }
        .onChange(of: state.loginItemStatus) { _, newValue in
            // Reflect the actual SMAppService state — so a failed register
            // (e.g. "Operation not permitted") flips the toggle back off.
            menubarAtLogin = newValue == .enabled
        }
    }

    /// Poll while Settings is open so the 3 lights stay live as the agent
    /// registers/restarts/spawns Python in the background.
    ///
    /// Cadence: 2s for the first ~10s after the window opens (so a fresh
    /// install / restart sees status flip in near-real-time), then back
    /// off to every 30s indefinitely. The aggressive cadence at start
    /// catches state changes the user just triggered; the long cadence
    /// keeps the build-mismatch detector live without burning a /api/build
    /// every 2 seconds for the lifetime of the app.
    private func startProbing() {
        probeTask?.cancel()
        probeTask = Task { @MainActor in
            var fastTicks = 5  // 5 × 2s = 10s of fast polling on entry
            while !Task.isCancelled {
                state.refresh()
                let interval: UInt64 = fastTicks > 0
                    ? 2_000_000_000
                    : 30_000_000_000
                if fastTicks > 0 { fastTicks -= 1 }
                try? await Task.sleep(nanoseconds: interval)
            }
        }
    }
}

private struct PathRow: View {
    let label: String
    let url: URL

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(label).font(.subheadline.weight(.medium))
            HStack {
                Text(url.path)
                    .font(.system(.body, design: .monospaced))
                    .lineLimit(1)
                    .truncationMode(.middle)
                    .frame(maxWidth: .infinity, alignment: .leading)
                Button("Copy") {
                    NSPasteboard.general.clearContents()
                    NSPasteboard.general.setString(url.path, forType: .string)
                }
                Button("Reveal in Finder") {
                    NSWorkspace.shared.activateFileViewerSelecting([url])
                }
            }
        }
    }
}
