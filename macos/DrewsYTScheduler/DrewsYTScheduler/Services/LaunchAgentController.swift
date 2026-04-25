import Foundation
import ServiceManagement

/// Wraps SMAppService for both the background server agent and the optional
/// "launch app at login" mainApp item.
///
/// Every action through this controller is logged to ``ActivityLog.shared``
/// with the SMAppService status BEFORE and AFTER the call so the user can
/// trace what the OS reported back versus what we asked for.
@MainActor
final class LaunchAgentController {
    static let shared = LaunchAgentController()

    private let agentPlistName: String

    private init() {
        let bundleId = Bundle.main.bundleIdentifier ?? "com.nuclearcyborg.drews-socialmedia-scheduler"
        self.agentPlistName = "\(bundleId).plist"
    }

    private var agentService: SMAppService {
        SMAppService.agent(plistName: agentPlistName)
    }

    private var loginItemService: SMAppService {
        SMAppService.mainApp
    }

    // --- background server agent --------------------------------------------

    var agentStatus: SMAppService.Status {
        agentService.status
    }

    /// Register and load the launch agent.
    ///
    /// **Always** does ``unregister()`` first when the service is currently
    /// registered. This is necessary — empirically ``SMAppService.register()``
    /// is a no-op when the same Label already has a BTM record, even when
    /// the on-disk plist's ``BundleProgram`` has changed. Without this
    /// step, rebuilds of the .app keep launchd spawning the OLD binary
    /// path forever.
    func registerAgent() async throws {
        let service = agentService
        let before = service.status
        ActivityLog.shared.log(.info, .agent,
            "register() called — pre-status=\(before.displayName), plist=\(agentPlistName)")

        if before != .notRegistered {
            ActivityLog.shared.log(.info, .agent,
                "unregistering first to force launchd to re-read the embedded plist")
            do {
                try await service.unregister()
                ActivityLog.shared.log(.info, .agent,
                    "unregister() returned — status=\(service.status.displayName)")
            } catch {
                // Continue anyway; some failure modes still let register()
                // succeed afterward. We log it so it's not silent.
                ActivityLog.shared.log(.warn, .agent,
                    "unregister() threw (continuing): \(error.localizedDescription)")
            }
            // Give launchd a beat to fully tear down the old job.
            try? await Task.sleep(nanoseconds: 800_000_000)
        }

        do {
            try service.register()
        } catch {
            ActivityLog.shared.log(.error, .agent, "register() threw: \(error.localizedDescription)")
            throw error
        }
        let after = service.status
        ActivityLog.shared.log(.info, .agent, "register() returned — post-status=\(after.displayName)")
        switch after {
        case .enabled, .requiresApproval:
            return
        case .notRegistered, .notFound:
            let msg = "register() succeeded but post-status is \(after.displayName) — the OS silently no-op'd"
            ActivityLog.shared.log(.error, .agent, msg)
            throw NSError(domain: "LaunchAgentController", code: -1, userInfo: [NSLocalizedDescriptionKey: msg])
        @unknown default:
            let msg = "register() returned with unknown post-status"
            ActivityLog.shared.log(.error, .agent, msg)
            throw NSError(domain: "LaunchAgentController", code: -2, userInfo: [NSLocalizedDescriptionKey: msg])
        }
    }

    func unregisterAgent() async throws {
        let service = agentService
        ActivityLog.shared.log(.info, .agent, "unregister() called — pre-status=\(service.status.displayName)")
        do {
            try await service.unregister()
        } catch {
            ActivityLog.shared.log(.error, .agent, "unregister() threw: \(error.localizedDescription)")
            throw error
        }
        ActivityLog.shared.log(.info, .agent, "unregister() returned — post-status=\(service.status.displayName)")
    }

    /// Restart by unregister+register so launchd reads the (possibly newer)
    /// plist from disk. After-the-first-time-approved, this round-trip does
    /// not re-prompt.
    func restartAgent() async throws {
        let service = agentService
        ActivityLog.shared.log(.info, .agent, "restart: pre-status=\(service.status.displayName)")
        if service.status != .notRegistered {
            try await service.unregister()
            try? await Task.sleep(nanoseconds: 1_000_000_000)
            ActivityLog.shared.log(.info, .agent, "restart: after unregister, status=\(service.status.displayName)")
        }
        try service.register()
        ActivityLog.shared.log(.info, .agent, "restart: after register, status=\(service.status.displayName)")
    }

    // --- "launch at login" main app -----------------------------------------

    var loginItemStatus: SMAppService.Status {
        loginItemService.status
    }

    func setLoginItemEnabled(_ enabled: Bool) async throws {
        let service = loginItemService
        ActivityLog.shared.log(.info, .loginItem,
            "setLoginItemEnabled(\(enabled)) — pre-status=\(service.status.displayName)")
        do {
            if enabled {
                // Mirror the agent: when BTM has a stale "disabled" record
                // for our app, register() throws "Operation not permitted".
                // Force-replace by unregistering first.
                if service.status != .notRegistered {
                    try? await service.unregister()
                    try? await Task.sleep(nanoseconds: 500_000_000)
                }
                try service.register()
            } else {
                if service.status == .enabled || service.status == .requiresApproval {
                    try await service.unregister()
                }
            }
        } catch {
            ActivityLog.shared.log(.error, .loginItem, "login-item update threw: \(error.localizedDescription)")
            throw error
        }
        ActivityLog.shared.log(.info, .loginItem,
            "setLoginItemEnabled(\(enabled)) returned — post-status=\(service.status.displayName)")
    }

    var loginItemIsEnabled: Bool {
        loginItemService.status == .enabled
    }
}

extension SMAppService.Status {
    /// Human-readable label. Note: ``.enabled`` means "registered with
    /// launchd" — NOT "process is currently running". The TCP probe is the
    /// signal for "process running".
    var displayName: String {
        switch self {
        case .notRegistered:    return "not registered"
        case .enabled:          return "registered"
        case .requiresApproval: return "needs approval (System Settings)"
        case .notFound:         return "not found in bundle"
        @unknown default:       return "unknown"
        }
    }
}
