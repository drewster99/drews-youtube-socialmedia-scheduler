import AppKit

/// Entry point. Two modes:
///   * ``--self-test`` — headless diagnostic, exits in ~20s.
///   * default — full app, run via plain ``NSApplication.shared.run()``.
///
/// We do NOT use SwiftUI's ``App`` protocol because its synthesised menu bar
/// (driven by the Scenes we declare) overrides AppDelegate's manual
/// ``NSApp.mainMenu`` assignment, dropping File/Edit/Tools menus we add by
/// hand. Running as an NSApplication leaves the menu bar entirely under our
/// control.
@main
enum DrewsYTSchedulerEntry {
    static func main() {
        if CommandLine.arguments.contains("--self-test") {
            SelfTest.run()
            return
        }
        let app = NSApplication.shared
        let delegate = AppDelegate()
        app.delegate = delegate
        app.run()
    }
}
