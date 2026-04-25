import Foundation
import ServiceManagement
import SwiftUI

/// Observable wrapper around ``LaunchAgentController`` + the running server's
/// HTTP probe. The .app's UI binds to this rather than to SMAppService
/// directly so we can refresh on demand and surface mismatched build IDs.
@MainActor
final class ServerStateModel: ObservableObject {
    static let shared = ServerStateModel()

    enum Reachability: Equatable {
        case unknown
        case connectionRefused(String)
        case responseError(String)
        case ok(BuildInfo)
    }

    @Published private(set) var agentStatus: SMAppService.Status = .notRegistered
    @Published private(set) var loginItemStatus: SMAppService.Status = .notRegistered
    @Published private(set) var reachability: Reachability = .unknown
    @Published private(set) var lastError: String?
    @Published private(set) var busy: Bool = false

    /// (1) Registered with SMAppService — the launchd plist is loaded.
    var isRegistered: Bool { agentStatus == .enabled }

    /// (2) Process is alive on the port. We get this when any HTTP response
    /// comes back (even an error), since the OS only refuses TCP when no
    /// process is listening.
    var isRunning: Bool {
        switch reachability {
        case .ok, .responseError: return true
        case .connectionRefused, .unknown: return false
        }
    }

    /// (3) Server answered ``/api/build`` with the same build_id this .app
    /// shell was built with. Implies registered + running + healthy + matched.
    var isReachable: Bool { serverMatchesBundle }

    private var refreshTask: Task<Void, Never>?

    private init() {}

    func refresh() {
        agentStatus = LaunchAgentController.shared.agentStatus
        loginItemStatus = LaunchAgentController.shared.loginItemStatus
        refreshTask?.cancel()
        refreshTask = Task { [weak self] in
            await self?.probeServer()
        }
    }

    private func probeServer() async {
        let result = await BuildInfoReader.probeServer(port: AppPaths.serverPort)
        let prev = reachability
        switch result {
        case .ok(let info):
            reachability = .ok(info)
            if case .ok = prev { /* steady state, don't spam */ } else {
                ActivityLog.shared.log(.info, .probe,
                    "/api/build OK — server is \(info.kind) \(info.version) (#\(info.buildNumber)) build_id=\(info.buildId.prefix(8))")
            }
        case .responseError(let msg):
            reachability = .responseError(msg)
            ActivityLog.shared.log(.warn, .probe,
                "TCP connected but HTTP /api/build failed: \(msg)")
        case .connectionRefused(let msg):
            reachability = .connectionRefused(msg)
            if case .connectionRefused = prev { /* steady state */ } else {
                ActivityLog.shared.log(.warn, .probe,
                    "no listener on port \(AppPaths.serverPort): \(msg)")
            }
        }
    }

    /// True when the running server's build_id matches this .app shell.
    var serverMatchesBundle: Bool {
        guard case let .ok(server) = reachability else { return false }
        return server.buildId == BuildInfoReader.bundle.buildId
    }

    /// Mismatched build identity between the .app shell and the running
    /// server — surfaced as a banner in Welcome / Settings / Monitor.
    var buildMismatch: (bundle: BuildInfo, server: BuildInfo)? {
        guard case let .ok(server) = reachability else { return nil }
        let bundle = BuildInfoReader.bundle
        if bundle.buildId != server.buildId {
            return (bundle, server)
        }
        return nil
    }

    // --- agent actions ------------------------------------------------------

    func registerAgent() {
        Task {
            busy = true
            defer { busy = false }
            do {
                try await LaunchAgentController.shared.registerAgent()
                lastError = nil
            } catch {
                lastError = "Register failed: \(error.localizedDescription)"
            }
            // Wait briefly for launchd to bring the new process up before
            // probing — otherwise reachability lags one refresh cycle.
            try? await Task.sleep(nanoseconds: 1_500_000_000)
            refresh()
        }
    }

    func unregisterAgent() {
        Task {
            busy = true
            defer { busy = false }
            do {
                try await LaunchAgentController.shared.unregisterAgent()
                lastError = nil
            } catch {
                lastError = "Unregister failed: \(error.localizedDescription)"
            }
            refresh()
        }
    }

    func restartAgent() {
        Task {
            busy = true
            defer { busy = false }
            do {
                try await LaunchAgentController.shared.restartAgent()
                lastError = nil
            } catch {
                lastError = "Restart failed: \(error.localizedDescription)"
            }
            try? await Task.sleep(nanoseconds: 1_500_000_000)
            refresh()
        }
    }

    // --- login item actions -------------------------------------------------

    func setLoginItemEnabled(_ enabled: Bool) {
        Task {
            do {
                try await LaunchAgentController.shared.setLoginItemEnabled(enabled)
                lastError = nil
            } catch {
                lastError = "Login item update failed: \(error.localizedDescription)"
            }
            refresh()
        }
    }
}
