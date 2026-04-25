import Foundation
import ServiceManagement
import AppKit

/// Headless diagnostic mode: ``--self-test`` argument runs this. Performs
/// a deterministic register → probe → dump cycle and exits in ~20 seconds.
/// Output goes to stdout. We start NSApplication (without showing UI) so
/// SMAppService's XPC machinery has a runloop to live on.
enum SelfTest {
    static func run() {
        // SMAppService uses XPC; XPC needs a runloop. NSApplication.shared
        // initializes one without requiring activation policy regular.
        let app = NSApplication.shared
        app.setActivationPolicy(.accessory)

        // Hard timeout — even if we wedge somewhere.
        DispatchQueue.global().asyncAfter(deadline: .now() + 20) {
            FileHandle.standardOutput.write(Data("\n[self-test] HARD TIMEOUT — exiting\n".utf8))
            exit(2)
        }

        // Kick off the test once the runloop is up.
        DispatchQueue.main.async {
            Task {
                await runAsync()
                exit(0)
            }
        }

        // Run until exit() is called.
        app.run()
    }

    private static func say(_ s: String) {
        FileHandle.standardOutput.write(Data((s + "\n").utf8))
    }

    private static func runAsync() async {
        let bundle = BuildInfoReader.bundle
        say("====== self-test ======")
        say("bundle:    \(Bundle.main.bundlePath)")
        say("bundleId:  \(Bundle.main.bundleIdentifier ?? "?")")
        say("kind:      \(bundle.kind)")
        say("version:   \(bundle.version) (#\(bundle.buildNumber))")
        say("build_id:  \(bundle.buildId)")
        say("")

        let plistName = "\(Bundle.main.bundleIdentifier ?? "").plist"
        let agent = SMAppService.agent(plistName: plistName)
        let loginItem = SMAppService.mainApp

        say("[step 1] SMAppService initial status:")
        say("  agent (\(plistName)): rawValue=\(agent.status.rawValue) (\(agent.status.displayName))")
        say("  loginItem:            rawValue=\(loginItem.status.rawValue) (\(loginItem.status.displayName))")
        say("")

        // Truncate the boot log so we only see this run's output.
        let bootLogPath = "/tmp/\(Bundle.main.bundleIdentifier ?? "").boot.log"
        try? "".write(toFile: bootLogPath, atomically: true, encoding: .utf8)
        say("[truncated boot log so step 4 shows only this run's output]")
        say("")

        say("[step 2] force-replace registration (unregister → sleep → register)…")
        if agent.status != .notRegistered {
            do {
                try await agent.unregister()
                say("  unregister() OK; status=\(agent.status.rawValue) (\(agent.status.displayName))")
            } catch {
                say("  unregister() threw (continuing): \(error.localizedDescription)")
            }
            try? await Task.sleep(nanoseconds: 800_000_000)
        }
        do {
            try agent.register()
            say("  register() OK; status=\(agent.status.rawValue) (\(agent.status.displayName))")
        } catch {
            let nsErr = error as NSError
            say("  register() THREW")
            say("    domain: \(nsErr.domain)  code: \(nsErr.code)")
            say("    desc:   \(nsErr.localizedDescription)")
            say("    info:   \(nsErr.userInfo)")
        }
        say("  post-register status: \(agent.status.rawValue) (\(agent.status.displayName))")
        say("")

        say("[step 3] waiting up to 12s for /api/build to respond…")
        for attempt in 1...12 {
            try? await Task.sleep(nanoseconds: 1_000_000_000)
            let r = await BuildInfoReader.probeServer(port: 8008, timeout: 0.8)
            switch r {
            case .ok(let info):
                say("  attempt \(attempt): OK  build_id=\(info.buildId) (server)")
                if info.buildId == bundle.buildId {
                    say("  ✓ matches bundle build_id")
                } else {
                    say("  ⚠ MISMATCH bundle=\(bundle.buildId)")
                }
                break
            case .responseError(let msg):
                say("  attempt \(attempt): responseError: \(msg)")
            case .connectionRefused(let msg):
                say("  attempt \(attempt): no listener (\(msg))")
            }
            if case .ok = r { break }
        }
        say("")

        say("[step 4] /tmp boot log (last 40 lines):")
        let bootLog = "/tmp/\(Bundle.main.bundleIdentifier ?? "").boot.log"
        if let data = try? Data(contentsOf: URL(fileURLWithPath: bootLog)),
           let text = String(data: data, encoding: .utf8) {
            let lines = text.split(separator: "\n", omittingEmptySubsequences: false).suffix(40)
            for line in lines { say("  | \(line)") }
        } else {
            say("  (boot log empty or missing: \(bootLog))")
        }
        say("")

        say("[step 5] server.log (last 20 lines):")
        let serverLog = (FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent("Library/Logs/\(Bundle.main.bundleIdentifier ?? "")/server.log")).path
        if let data = try? Data(contentsOf: URL(fileURLWithPath: serverLog)),
           let text = String(data: data, encoding: .utf8) {
            let lines = text.split(separator: "\n", omittingEmptySubsequences: false).suffix(20)
            for line in lines { say("  | \(line)") }
        } else {
            say("  (server log missing: \(serverLog))")
        }
        say("")

        say("====== self-test end ======")
    }
}
