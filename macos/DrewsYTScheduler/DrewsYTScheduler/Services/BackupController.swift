import AppKit
import Combine
import Foundation

/// Drives the "Export Data…" / "Import Data…" actions in Settings by shelling
/// out to the bundled Python (`yt-scheduler export-all` / `import-all`).
///
/// The Python CLI is the real implementation; this class only orchestrates the
/// macOS bits: the save/open panels, the passphrase prompt, and — for import —
/// stopping the launch-agent server before the restore and restarting it after.
@MainActor
final class BackupController: ObservableObject {
    @Published private(set) var busy = false

    // --- public entry points (wired to the Settings buttons) ---

    func beginExport() {
        guard !busy else { return }

        let panel = NSSavePanel()
        panel.title = "Export Backup"
        panel.prompt = "Export"
        panel.nameFieldStringValue = "drews-scheduler-backup-\(Self.dateStamp).dysbak"
        panel.allowsOtherFileTypes = true
        panel.canCreateDirectories = true
        guard panel.runModal() == .OK, let url = panel.url else { return }

        guard let passphrase = promptSecret(
            title: "Choose a passphrase",
            message: "This passphrase encrypts the backup file. You'll need it to import on the other Mac. There's no way to recover it."
        ), !passphrase.isEmpty else { return }
        guard let again = promptSecret(
            title: "Re-enter the passphrase",
            message: "Type the same passphrase again to confirm."
        ) else { return }
        guard again == passphrase else {
            showAlert(style: .warning, title: "Passphrases didn't match", message: "Nothing was exported. Try again.")
            return
        }

        busy = true
        Task {
            let (status, output) = await Self.runCLI(["export-all", url.path], passphrase: passphrase)
            busy = false
            if status == 0 {
                showAlert(style: .informational, title: "Backup created",
                          message: "Saved to \(url.path)\n\n\(output)")
            } else {
                showAlert(style: .critical, title: "Export failed", message: output.isEmpty ? "Unknown error." : output)
            }
        }
    }

    func beginImport() {
        guard !busy else { return }

        let panel = NSOpenPanel()
        panel.title = "Import Backup"
        panel.prompt = "Choose"
        panel.canChooseFiles = true
        panel.canChooseDirectories = false
        panel.allowsMultipleSelection = false
        guard panel.runModal() == .OK, let url = panel.url else { return }

        let confirm = NSAlert()
        confirm.alertStyle = .warning
        confirm.messageText = "Replace all data on this Mac?"
        confirm.informativeText = """
        Importing replaces the current database, templates, uploaded media, and stored \
        credentials with the contents of this backup. A copy of your current data is kept \
        alongside it (named “…pre-import-<timestamp>”). The server is stopped during the \
        import and restarted afterward.
        """
        confirm.addButton(withTitle: "Import")
        confirm.addButton(withTitle: "Cancel")
        guard confirm.runModal() == .alertFirstButtonReturn else { return }

        guard let passphrase = promptSecret(
            title: "Backup passphrase",
            message: "Enter the passphrase this backup was created with."
        ), !passphrase.isEmpty else { return }

        busy = true
        Task {
            ActivityLog.shared.log(.info, .ui, "Backup import: stopping the server")
            try? await LaunchAgentController.shared.unregisterAgent()
            await LaunchAgentController.launchctlBootout(label: LaunchAgentController.agentLabel)
            await LaunchAgentController.killProcessOnPort(AppPaths.serverPort)

            let (status, output) = await Self.runCLI(["import-all", url.path], passphrase: passphrase)

            ActivityLog.shared.log(.info, .ui, "Backup import: restarting the server")
            var restarted = true
            do {
                try await LaunchAgentController.shared.registerAgent()
            } catch {
                restarted = false
                ActivityLog.shared.log(.error, .ui, "Backup import: server restart failed: \(error.localizedDescription)")
            }
            let serverNote = restarted
                ? "The server has been restarted."
                : "⚠️ The server could NOT be restarted automatically — open the Server Monitor and use “Restart server”."

            busy = false
            if status == 0 {
                showAlert(style: .informational, title: "Import complete",
                          message: "\(output)\n\n\(serverNote)")
            } else {
                showAlert(style: .critical, title: "Import failed",
                          message: "\(output.isEmpty ? "Unknown error." : output)\n\nYour existing data was left in place. \(serverNote)")
            }
        }
    }

    // --- helpers ---

    private static var dateStamp: String {
        let f = DateFormatter()
        f.locale = Locale(identifier: "en_US_POSIX")
        f.dateFormat = "yyyy-MM-dd"
        return f.string(from: Date())
    }

    /// Run the bundled Python entry point with the given args. Returns the exit
    /// status and the combined stdout/stderr text. Never throws.
    private static func runCLI(_ extraArgs: [String], passphrase: String) async -> (Int32, String) {
        await Task.detached(priority: .userInitiated) {
            let launcher = Bundle.main.bundleURL
                .appendingPathComponent("Contents/Resources/python/bin/yt_scheduler_launcher.sh")
            let proc = Process()
            proc.executableURL = launcher
            proc.arguments = ["-m", "yt_scheduler.main"] + extraArgs

            var env = ProcessInfo.processInfo.environment
            env["DYS_BUNDLE_PASSPHRASE"] = passphrase   // kept out of argv (not visible in `ps`)
            env["PATH"] = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
            env.removeValue(forKey: "DYS_REDIRECT_LOGS")  // we want output on the pipe, not the log file
            proc.environment = env

            let pipe = Pipe()
            proc.standardOutput = pipe
            proc.standardError = pipe
            do {
                try proc.run()
            } catch {
                return (Int32(-1), "Couldn't launch the bundled helper: \(error.localizedDescription)")
            }
            let data = pipe.fileHandleForReading.readDataToEndOfFile()
            proc.waitUntilExit()
            let text = (String(data: data, encoding: .utf8) ?? "")
                .trimmingCharacters(in: .whitespacesAndNewlines)
            return (proc.terminationStatus, text)
        }.value
    }

    private func promptSecret(title: String, message: String) -> String? {
        let alert = NSAlert()
        alert.messageText = title
        alert.informativeText = message
        alert.addButton(withTitle: "OK")
        alert.addButton(withTitle: "Cancel")
        let field = NSSecureTextField(frame: NSRect(x: 0, y: 0, width: 260, height: 24))
        alert.accessoryView = field
        alert.window.initialFirstResponder = field
        return alert.runModal() == .alertFirstButtonReturn ? field.stringValue : nil
    }

    private func showAlert(style: NSAlert.Style, title: String, message: String) {
        let alert = NSAlert()
        alert.alertStyle = style
        alert.messageText = title
        alert.informativeText = message
        alert.addButton(withTitle: "OK")
        alert.runModal()
    }
}
