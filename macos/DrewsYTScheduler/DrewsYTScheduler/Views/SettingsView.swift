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
                        Button("Restart server") { state.restartAgent() }
                            .disabled(state.agentStatus != .enabled || state.busy)
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

            HStack {
                Spacer()
                Button("Open in browser") {
                    NSWorkspace.shared.open(AppPaths.serverWebURL)
                }
            }

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

    /// Poll every 2s while Settings is open so the 3 lights stay live as the
    /// agent registers/restarts/spawns Python in the background.
    private func startProbing() {
        probeTask?.cancel()
        probeTask = Task { @MainActor in
            while !Task.isCancelled {
                state.refresh()
                try? await Task.sleep(nanoseconds: 2_000_000_000)
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
