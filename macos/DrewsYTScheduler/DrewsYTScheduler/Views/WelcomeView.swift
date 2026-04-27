import SwiftUI
import AppKit

/// First-run window. The background server agent is mandatory — installing
/// it is the first action and there's no opt-out. The menu-bar / launch-at-
/// login choice is presented once; "Open in browser" is gated on the server
/// actually being reachable AND matching this .app's build_id.
struct WelcomeView: View {
    @ObservedObject var state: ServerStateModel
    var onFinish: () -> Void

    @State private var menubarAtLogin = false
    @State private var probeTask: Task<Void, Never>?

    var body: some View {
        VStack(alignment: .leading, spacing: 18) {
            if let mismatch = state.buildMismatch {
                BuildMismatchBanner(bundle: mismatch.bundle, server: mismatch.server)
            }

            Text("Welcome to Drew's YT Scheduler")
                .font(.title2.weight(.semibold))

            Text("Drew's YT Scheduler runs a small local server in the background that handles uploads, scheduled publishing, transcript polling, and comment moderation. It needs to keep running even when this window is closed.")
                .fixedSize(horizontal: false, vertical: true)

            GroupBox(label: Text("Step 1 — Install the background service").font(.headline)) {
                VStack(alignment: .leading, spacing: 10) {
                    StatusLightsView(state: state)
                    HStack(spacing: 8) {
                        Button(action: { state.registerAgent() }) {
                            Text(state.agentStatus == .enabled ? "Reinstall background service" : "Install background service")
                        }
                        .disabled(state.busy)
                        if state.busy {
                            ProgressView().controlSize(.small)
                        }
                    }
                    if state.agentStatus == .requiresApproval {
                        Text("macOS will open System Settings → Login Items. Enable “Drew's YT Scheduler”, then return here.")
                            .font(.footnote)
                            .foregroundStyle(.secondary)
                    }
                    if let err = state.lastError {
                        Text(err).font(.footnote).foregroundStyle(.red)
                    }
                }
                .padding(8)
            }

            GroupBox(label: Text("Step 2 — Menu bar at login (optional)").font(.headline)) {
                VStack(alignment: .leading, spacing: 4) {
                    Toggle("Show menu-bar item at login", isOn: $menubarAtLogin)
                        .onChange(of: menubarAtLogin) { _, newValue in
                            state.setLoginItemEnabled(newValue)
                            (NSApp.delegate as? AppDelegate)?.setMenuBarVisible(newValue)
                        }
                    Text("The background service is installed regardless. This only controls whether a status item appears in the menu bar after you log in.")
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
                .padding(8)
            }

            // Visual separation — Open UI is the welcome flow's only exit;
            // it deserves its own row, well clear of the install controls
            // above so the user doesn't accidentally hit it before they're
            // ready.
            Divider()
                .padding(.vertical, 4)

            HStack {
                Spacer()
                Button(action: {
                    NSWorkspace.shared.open(AppPaths.serverWebURL)
                    onFinish()
                }) {
                    Text("Open UI")
                        .font(.headline)
                        .frame(minWidth: 160)
                        .padding(.vertical, 4)
                }
                .controlSize(.large)
                .buttonStyle(.borderedProminent)
                .keyboardShortcut(.defaultAction)
                .disabled(!canOpenBrowser)
            }
            .padding(.top, 8)
        }
        .padding(24)
        .frame(width: 650)
        .onAppear {
            menubarAtLogin = state.loginItemStatus == .enabled
            startProbing()
        }
        .onDisappear {
            probeTask?.cancel()
            probeTask = nil
        }
    }

    /// Open-in-browser is only enabled when the server actually answers and
    /// reports the same build_id as this .app shell. Prevents the user
    /// landing on Safari's "can't connect" page (or worse, a stale build).
    private var canOpenBrowser: Bool {
        state.agentStatus == .enabled && state.serverMatchesBundle
    }

    /// Poll the server every second until it answers — gives the user
    /// immediate feedback that the install actually produced a working
    /// server, and keeps Open-in-browser disabled until that happens.
    private func startProbing() {
        probeTask?.cancel()
        probeTask = Task { @MainActor in
            while !Task.isCancelled {
                state.refresh()
                try? await Task.sleep(nanoseconds: 1_000_000_000)
            }
        }
    }
}
