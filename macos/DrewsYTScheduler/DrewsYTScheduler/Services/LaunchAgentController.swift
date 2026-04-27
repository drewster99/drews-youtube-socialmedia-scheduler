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

    /// Restart by unregister + kill running process + register, so launchd
    /// reads the (possibly newer) plist from disk and the new spawn isn't
    /// blocked by the old one still holding the port.
    ///
    /// Empirically ``SMAppService.unregister()`` only tells launchd to stop
    /// scheduling new launches — a process that's already alive (via
    /// ``RunAtLoad`` / ``KeepAlive`` semantics or simply because launchd
    /// hasn't gotten around to reaping it) will keep its TCP listener open.
    /// If we then ``register()`` and launchd tries to spawn a fresh
    /// instance, the new process can't bind and either crashes or never
    /// comes up. So we explicitly TERM (then KILL) anything still on the
    /// port before re-registering.
    func restartAgent() async throws {
        let service = agentService
        ActivityLog.shared.log(.info, .agent, "restart: pre-status=\(service.status.displayName)")
        if service.status != .notRegistered {
            try await service.unregister()
            try? await Task.sleep(nanoseconds: 1_000_000_000)
            ActivityLog.shared.log(.info, .agent, "restart: after unregister, status=\(service.status.displayName)")
        }

        await Self.killProcessOnPort(AppPaths.serverPort)

        try service.register()
        ActivityLog.shared.log(.info, .agent, "restart: after register, status=\(service.status.displayName)")
    }

    /// SIGTERM (and if it doesn't die, SIGKILL) any process holding a
    /// TCP listener on ``port``. Logs every PID it finds and the result
    /// of each kill. No-op when nothing is listening.
    static func killProcessOnPort(_ port: Int) async {
        let pids = listeningPIDs(onPort: port)
        guard !pids.isEmpty else {
            ActivityLog.shared.log(.info, .agent,
                "restart: nothing listening on port \(port) — nothing to kill")
            return
        }

        ActivityLog.shared.log(.info, .agent,
            "restart: killing \(pids.count) PID(s) on port \(port): \(pids.map(String.init).joined(separator: ", "))")

        // First pass — polite SIGTERM. Most well-behaved processes (the
        // Python server included) flush state and exit on this.
        for pid in pids {
            sendSignal(pid: pid, signal: "TERM")
        }

        // Wait up to ~5s for the port to free.
        for _ in 0..<10 {
            try? await Task.sleep(nanoseconds: 500_000_000)
            if listeningPIDs(onPort: port).isEmpty {
                ActivityLog.shared.log(.info, .agent,
                    "restart: port \(port) freed after SIGTERM")
                return
            }
        }

        // Still holding the port — escalate to SIGKILL.
        let stragglers = listeningPIDs(onPort: port)
        ActivityLog.shared.log(.warn, .agent,
            "restart: \(stragglers.count) PID(s) still holding port \(port) after SIGTERM — sending SIGKILL")
        for pid in stragglers {
            sendSignal(pid: pid, signal: "KILL")
        }
        try? await Task.sleep(nanoseconds: 500_000_000)
        if !listeningPIDs(onPort: port).isEmpty {
            ActivityLog.shared.log(.error, .agent,
                "restart: port \(port) STILL not free after SIGKILL — register() will likely fail to bind")
        }
    }

    /// Return PIDs listening on ``port`` (parsed from ``lsof -ti``).
    /// Returns ``[]`` on tool error or no listener; never throws.
    private static func listeningPIDs(onPort port: Int) -> [pid_t] {
        let proc = Process()
        proc.executableURL = URL(fileURLWithPath: "/usr/sbin/lsof")
        proc.arguments = ["-ti", "tcp:\(port)", "-sTCP:LISTEN"]
        let pipe = Pipe()
        proc.standardOutput = pipe
        proc.standardError = Pipe()
        do {
            try proc.run()
            proc.waitUntilExit()
        } catch {
            return []
        }
        let data = pipe.fileHandleForReading.readDataToEndOfFile()
        guard let text = String(data: data, encoding: .utf8) else { return [] }
        return text
            .split(whereSeparator: \.isNewline)
            .compactMap { Int32($0.trimmingCharacters(in: .whitespaces)) }
    }

    /// Run ``/bin/kill -<signal> <pid>``. Logs failure but doesn't throw.
    private static func sendSignal(pid: pid_t, signal: String) {
        let proc = Process()
        proc.executableURL = URL(fileURLWithPath: "/bin/kill")
        proc.arguments = ["-\(signal)", String(pid)]
        proc.standardOutput = Pipe()
        proc.standardError = Pipe()
        do {
            try proc.run()
            proc.waitUntilExit()
            if proc.terminationStatus != 0 {
                ActivityLog.shared.log(.warn, .agent,
                    "kill -\(signal) \(pid) exited \(proc.terminationStatus)")
            }
        } catch {
            ActivityLog.shared.log(.warn, .agent,
                "kill -\(signal) \(pid) threw: \(error.localizedDescription)")
        }
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
