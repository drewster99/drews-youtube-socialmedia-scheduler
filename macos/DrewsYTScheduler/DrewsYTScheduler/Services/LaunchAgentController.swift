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

    /// Restart by unregister + force-unload from launchd + kill listener +
    /// register, so launchd reads the new plist from disk and the new spawn
    /// isn't blocked by the old one still holding the port.
    ///
    /// Why not just SMAppService.unregister + register?
    ///
    /// The agent plist sets ``KeepAlive = { SuccessfulExit: false }``,
    /// which means launchd respawns the process every time it exits non-
    /// zero. When we SIGTERM or SIGKILL the running server, that's a
    /// non-zero exit, and launchd dutifully spawns it RIGHT BACK from the
    /// OLD on-disk binary path that BTM still has cached. SMAppService's
    /// ``unregister()`` is supposed to detach the job before that happens,
    /// but in practice BTM sometimes leaves the job in a "pending
    /// unregister" state — launchd keeps managing it, our kill triggers a
    /// respawn, and we lose the race.
    ///
    /// The reliable sequence is:
    ///
    ///  1. ``SMAppService.unregister()`` — best-effort, tells BTM to drop
    ///     the record.
    ///  2. ``launchctl bootout gui/<uid>/<label>`` — forcibly unload the
    ///     job from launchd. After this, SIGTERM/SIGKILL is a final death,
    ///     not a "please respawn me" signal.
    ///  3. SIGTERM / SIGKILL anything still listening on the port (in case
    ///     the launchd-issued TERM didn't catch it).
    ///  4. ``SMAppService.register()`` — re-bootstrap from the new plist.
    func restartAgent() async throws {
        let service = agentService
        ActivityLog.shared.log(.info, .agent, "restart: pre-status=\(service.status.displayName)")

        // Step 1: SMAppService.unregister(). Best-effort — keep going even
        // if it throws. The bootout below is what actually has to succeed
        // for the kill step to take.
        if service.status != .notRegistered {
            do {
                try await service.unregister()
                ActivityLog.shared.log(.info, .agent,
                    "restart: SMAppService.unregister() returned, status=\(service.status.displayName)")
            } catch {
                ActivityLog.shared.log(.warn, .agent,
                    "restart: SMAppService.unregister() threw (continuing): \(error.localizedDescription)")
            }
            try? await Task.sleep(nanoseconds: 500_000_000)
        }

        // Step 2: force-unload from launchd. This is the step that breaks
        // the KeepAlive respawn loop.
        let label = Self.agentLabel
        await Self.launchctlBootout(label: label)

        // Step 3: belt-and-suspenders — kill anything still on the port.
        // After bootout, launchd has already TERMed the process; this only
        // catches the rare straggler.
        await Self.killProcessOnPort(AppPaths.serverPort)

        // Step 4: bring the new version up.
        try service.register()
        ActivityLog.shared.log(.info, .agent, "restart: after register, status=\(service.status.displayName)")
    }

    /// The label launchd knows the agent by — same as the bundle id, used
    /// in both the embedded plist's ``Label`` key and ``launchctl`` calls.
    static let agentLabel = "com.nuclearcyborg.drews-socialmedia-scheduler"

    /// `launchctl bootout gui/<uid>/<label>` — force-unload the job so
    /// launchd stops respawning it. Logs PID-level detail before/after so
    /// the user can see what changed in the Activity Log.
    static func launchctlBootout(label: String) async {
        let domain = "gui/\(getuid())"
        let target = "\(domain)/\(label)"

        // Inspect launchd's view BEFORE we touch anything — this gets us
        // the PID launchd thinks is running the job, plus its loaded/
        // pending state, so we can see whether bootout actually changed
        // anything.
        let preState = launchctlPrintSummary(target: target)
        ActivityLog.shared.log(.info, .agent,
            "restart: launchd pre-bootout for \(target): \(preState)")

        guard preState != "not loaded" else {
            ActivityLog.shared.log(.info, .agent,
                "restart: launchd already shows \(target) as not loaded — skipping bootout")
            return
        }

        let proc = Process()
        proc.executableURL = URL(fileURLWithPath: "/bin/launchctl")
        proc.arguments = ["bootout", target]
        let outPipe = Pipe()
        let errPipe = Pipe()
        proc.standardOutput = outPipe
        proc.standardError = errPipe
        do {
            try proc.run()
            proc.waitUntilExit()
            let stderr = String(
                data: errPipe.fileHandleForReading.readDataToEndOfFile(),
                encoding: .utf8) ?? ""
            ActivityLog.shared.log(
                proc.terminationStatus == 0 ? .info : .warn,
                .agent,
                "restart: launchctl bootout \(target) exited \(proc.terminationStatus)" +
                    (stderr.isEmpty ? "" : "; stderr=\(stderr.trimmingCharacters(in: .whitespacesAndNewlines))"))
        } catch {
            ActivityLog.shared.log(.warn, .agent,
                "restart: launchctl bootout threw: \(error.localizedDescription)")
            return
        }

        // Wait up to ~3s for launchd to fully release the job.
        for _ in 0..<6 {
            try? await Task.sleep(nanoseconds: 500_000_000)
            if launchctlPrintSummary(target: target) == "not loaded" {
                ActivityLog.shared.log(.info, .agent,
                    "restart: launchd released \(target)")
                return
            }
        }
        ActivityLog.shared.log(.warn, .agent,
            "restart: launchd still reports \(target) loaded after bootout " +
            "— register() may fail. Post-state: \(launchctlPrintSummary(target: target))")
    }

    /// Run ``launchctl print <target>`` and reduce to a one-word summary:
    /// "not loaded" when the target doesn't exist, "loaded pid=N" when it
    /// does, or "loaded" when it's loaded but no PID was reported.
    private static func launchctlPrintSummary(target: String) -> String {
        let proc = Process()
        proc.executableURL = URL(fileURLWithPath: "/bin/launchctl")
        proc.arguments = ["print", target]
        let pipe = Pipe()
        proc.standardOutput = pipe
        proc.standardError = Pipe()
        do {
            try proc.run()
            proc.waitUntilExit()
        } catch {
            return "unknown"
        }
        if proc.terminationStatus != 0 {
            // Non-zero typically means "Could not find service" — i.e.
            // the job isn't loaded. Treat that as the success state.
            return "not loaded"
        }
        let data = pipe.fileHandleForReading.readDataToEndOfFile()
        guard let text = String(data: data, encoding: .utf8) else {
            return "loaded"
        }
        for line in text.split(whereSeparator: \.isNewline) {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            if trimmed.hasPrefix("pid = ") {
                return "loaded \(trimmed)"
            }
        }
        return "loaded"
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
