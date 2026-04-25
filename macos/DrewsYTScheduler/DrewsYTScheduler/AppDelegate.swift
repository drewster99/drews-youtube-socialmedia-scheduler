import AppKit
import SwiftUI
import ServiceManagement

/// Window-based AppDelegate. The .app does NOT spawn the Python server;
/// SMAppService manages the embedded launch agent which owns that lifecycle.
/// On first launch we present the Welcome window which mandates installing
/// the agent before the user can dismiss it.
@MainActor
final class AppDelegate: NSObject, NSApplicationDelegate, NSMenuDelegate {
    private static let firstRunDoneKey = "DYSFirstRunCompleted"
    private static let menuBarVisibleKey = "DYSMenuBarVisible"
    fileprivate static let restartMenuItemTag = 9001

    private let state = ServerStateModel.shared

    private var welcomeWindow: NSWindow?
    private var settingsWindow: NSWindow?
    private var monitorWindow: NSWindow?
    private var activityLogWindow: NSWindow?

    private var statusItem: NSStatusItem?

    func applicationDidFinishLaunching(_ notification: Notification) {
        let bundle = BuildInfoReader.bundle
        ActivityLog.shared.log(.info, .lifecycle,
            "app launched — kind=\(bundle.kind) version=\(bundle.version) #\(bundle.buildNumber) build_id=\(bundle.buildId.prefix(8))")

        NSApp.setActivationPolicy(.regular)

        buildMainMenu()

        if UserDefaults.standard.bool(forKey: Self.menuBarVisibleKey) {
            installMenuBarItem()
        }

        state.refresh()

        // Auto-register the agent on every launch when it isn't already
        // enabled. This handles three cases without any user clicks:
        //   * fresh install (.notRegistered)
        //   * stale registration was booted out manually
        //   * approval revoked in System Settings (.requiresApproval)
        // SMAppService.register() is idempotent when already enabled.
        if state.agentStatus != .enabled {
            ActivityLog.shared.log(.info, .lifecycle,
                "agent status=\(state.agentStatus.displayName) at launch — auto-registering")
            state.registerAgent()
        }

        if !UserDefaults.standard.bool(forKey: Self.firstRunDoneKey) {
            ActivityLog.shared.log(.info, .lifecycle, "first run — showing Welcome")
            showWelcomeWindow()
        } else {
            // Subsequent launches: open Settings so the user has somewhere to
            // land. Without this, the .app launches into "no window" and the
            // dock icon appears with nothing to click on.
            ActivityLog.shared.log(.info, .lifecycle, "subsequent launch — showing Settings")
            showSettingsWindow()
        }
    }

    func applicationShouldHandleReopen(_ sender: NSApplication, hasVisibleWindows flag: Bool) -> Bool {
        if !flag {
            showSettingsWindow()
        }
        return true
    }

    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
        // Keep the .app running so the menu bar item stays alive when the
        // last window closes. The launch agent's lifecycle is independent
        // anyway — quitting just removes our windows + menu bar item.
        false
    }

    // MARK: Menus

    private func buildMainMenu() {
        let mainMenu = NSMenu()

        // App menu — title is replaced by AppKit with the app name.
        let appMenuItem = NSMenuItem()
        appMenuItem.title = "Drew's YT Scheduler"
        let appMenu = NSMenu()
        appMenu.addItem(NSMenuItem(title: "About Drew's YT Scheduler", action: #selector(NSApplication.orderFrontStandardAboutPanel(_:)), keyEquivalent: ""))
        appMenu.addItem(.separator())
        let settingsItem = NSMenuItem(title: "Settings…", action: #selector(showSettingsAction), keyEquivalent: ",")
        settingsItem.target = self
        appMenu.addItem(settingsItem)
        appMenu.addItem(.separator())
        appMenu.addItem(NSMenuItem(title: "Hide Drew's YT Scheduler", action: #selector(NSApplication.hide(_:)), keyEquivalent: "h"))
        let hideOthers = NSMenuItem(title: "Hide Others", action: #selector(NSApplication.hideOtherApplications(_:)), keyEquivalent: "h")
        hideOthers.keyEquivalentModifierMask = [.command, .option]
        appMenu.addItem(hideOthers)
        appMenu.addItem(NSMenuItem(title: "Show All", action: #selector(NSApplication.unhideAllApplications(_:)), keyEquivalent: ""))
        appMenu.addItem(.separator())
        appMenu.addItem(NSMenuItem(title: "Quit Drew's YT Scheduler", action: #selector(NSApplication.terminate(_:)), keyEquivalent: "q"))
        appMenuItem.submenu = appMenu
        mainMenu.addItem(appMenuItem)

        // File menu
        let fileItem = NSMenuItem()
        fileItem.title = "File"
        let fileMenu = NSMenu(title: "File")
        let openWebUI = NSMenuItem(title: "Open Web UI", action: #selector(openWebUI), keyEquivalent: "o")
        openWebUI.target = self
        fileMenu.addItem(openWebUI)
        fileMenu.addItem(.separator())
        let closeItem = NSMenuItem(title: "Close Window", action: #selector(NSWindow.performClose(_:)), keyEquivalent: "w")
        fileMenu.addItem(closeItem)
        fileItem.submenu = fileMenu
        mainMenu.addItem(fileItem)

        // Edit menu (so cut/copy/paste work in any text fields)
        let editItem = NSMenuItem()
        editItem.title = "Edit"
        let editMenu = NSMenu(title: "Edit")
        editMenu.addItem(NSMenuItem(title: "Undo", action: Selector(("undo:")), keyEquivalent: "z"))
        editMenu.addItem(NSMenuItem(title: "Redo", action: Selector(("redo:")), keyEquivalent: "Z"))
        editMenu.addItem(.separator())
        editMenu.addItem(NSMenuItem(title: "Cut", action: #selector(NSText.cut(_:)), keyEquivalent: "x"))
        editMenu.addItem(NSMenuItem(title: "Copy", action: #selector(NSText.copy(_:)), keyEquivalent: "c"))
        editMenu.addItem(NSMenuItem(title: "Paste", action: #selector(NSText.paste(_:)), keyEquivalent: "v"))
        editMenu.addItem(NSMenuItem(title: "Select All", action: #selector(NSText.selectAll(_:)), keyEquivalent: "a"))
        editItem.submenu = editMenu
        mainMenu.addItem(editItem)

        // View menu
        let viewItem = NSMenuItem()
        viewItem.title = "View"
        let viewMenu = NSMenu(title: "View")
        let viewLogs = NSMenuItem(title: "View Logs", action: #selector(viewLogsAction), keyEquivalent: "l")
        viewLogs.target = self
        viewMenu.addItem(viewLogs)
        let activityLog = NSMenuItem(title: "Activity Log", action: #selector(showActivityLogAction), keyEquivalent: "L")
        activityLog.target = self
        viewMenu.addItem(activityLog)
        viewItem.submenu = viewMenu
        mainMenu.addItem(viewItem)

        // Tools menu — restart is disabled until the agent is running.
        let toolsItem = NSMenuItem()
        toolsItem.title = "Tools"
        let toolsMenu = NSMenu(title: "Tools")
        toolsMenu.autoenablesItems = false
        toolsMenu.delegate = self
        let monitor = NSMenuItem(title: "Monitor Server", action: #selector(showMonitorAction), keyEquivalent: "m")
        monitor.target = self
        toolsMenu.addItem(monitor)
        toolsMenu.addItem(.separator())
        let restart = NSMenuItem(title: "Restart Server", action: #selector(restartServerAction), keyEquivalent: "r")
        restart.target = self
        restart.tag = AppDelegate.restartMenuItemTag
        toolsMenu.addItem(restart)
        toolsItem.submenu = toolsMenu
        mainMenu.addItem(toolsItem)

        // Window menu — populated by AppKit
        let windowItem = NSMenuItem()
        windowItem.title = "Window"
        let windowMenu = NSMenu(title: "Window")
        windowItem.submenu = windowMenu
        mainMenu.addItem(windowItem)
        NSApp.windowsMenu = windowMenu

        NSApp.mainMenu = mainMenu
    }

    // MARK: Window helpers

    private func makeWindow<Content: View>(
        title: String,
        size: CGSize,
        styleMask: NSWindow.StyleMask = [.titled, .closable, .miniaturizable, .resizable],
        @ViewBuilder content: () -> Content
    ) -> NSWindow {
        let window = NSWindow(
            contentRect: NSRect(origin: .zero, size: size),
            styleMask: styleMask,
            backing: .buffered,
            defer: false
        )
        window.title = title
        window.center()
        window.isReleasedWhenClosed = false
        window.contentView = NSHostingView(rootView: content())
        return window
    }

    private func showWelcomeWindow() {
        if welcomeWindow == nil {
            // No close/minimize buttons: the user can't escape this window
            // without explicitly completing the flow (enable agent + open in
            // browser). The "Open in browser" button is the only exit.
            welcomeWindow = makeWindow(
                title: "Welcome",
                size: CGSize(width: 650, height: 462),
                styleMask: [.titled]
            ) {
                WelcomeView(state: self.state) { [weak self] in
                    UserDefaults.standard.set(true, forKey: Self.firstRunDoneKey)
                    self?.welcomeWindow?.close()
                    self?.welcomeWindow = nil
                }
            }
        }
        bringWindowToFront(welcomeWindow)
    }

    private func showSettingsWindow() {
        if settingsWindow == nil {
            settingsWindow = makeWindow(title: "Settings", size: CGSize(width: 1120, height: 756)) {
                SettingsView(state: self.state)
            }
            settingsWindow?.delegate = self.windowReleaseDelegate
        }
        bringWindowToFront(settingsWindow)
    }

    private func showMonitorWindow() {
        if monitorWindow == nil {
            monitorWindow = makeWindow(title: "Server Monitor", size: CGSize(width: 720, height: 480)) {
                MonitorView(state: self.state)
            }
            monitorWindow?.delegate = self.windowReleaseDelegate
        }
        bringWindowToFront(monitorWindow)
    }

    func showActivityLogWindow() {
        ActivityLog.shared.log(.info, .ui, "showActivityLogWindow() called")
        if activityLogWindow == nil {
            activityLogWindow = makeWindow(title: "Activity Log", size: CGSize(width: 900, height: 560)) {
                ActivityLogView(state: self.state)
            }
            activityLogWindow?.delegate = self.windowReleaseDelegate
        }
        bringWindowToFront(activityLogWindow)
    }

    /// Reliably surface a window in three different timing scenarios:
    ///
    ///   1. A button click while the .app is already foregrounded → the
    ///      synchronous block handles it on this runloop tick.
    ///   2. ``applicationDidFinishLaunching`` showing a window before AppKit's
    ///      own activation pass completes → the deferred block re-applies on
    ///      the next tick after AppKit has settled.
    ///   3. ``open`` against an already-running .app from elsewhere → the
    ///      reopen handler routes through here too.
    ///
    /// ``orderFrontRegardless()`` ensures the window appears even if our
    /// activation request is denied (e.g. another app refused to yield).
    private func bringWindowToFront(_ window: NSWindow?) {
        guard let window else { return }
        let activate: () -> Void = {
            if #available(macOS 14.0, *) {
                NSApp.activate()
            } else {
                NSApp.activate(ignoringOtherApps: true)
            }
            window.makeKeyAndOrderFront(nil)
            window.orderFrontRegardless()
        }
        activate()
        DispatchQueue.main.async(execute: activate)
    }

    /// Shared delegate that nils out the strong reference when the window
    /// closes, so re-showing rebuilds fresh SwiftUI state.
    private lazy var windowReleaseDelegate: WindowReleaseDelegate = {
        WindowReleaseDelegate { [weak self] window in
            guard let self else { return }
            if window === self.settingsWindow { self.settingsWindow = nil }
            if window === self.monitorWindow { self.monitorWindow = nil }
            if window === self.activityLogWindow { self.activityLogWindow = nil }
        }
    }()

    // MARK: Menu actions

    @objc private func showSettingsAction() { showSettingsWindow() }
    @objc private func showMonitorAction() { showMonitorWindow() }
    @objc private func showActivityLogAction() { showActivityLogWindow() }

    @objc private func openWebUI() {
        NSWorkspace.shared.open(AppPaths.serverWebURL)
    }

    @objc private func viewLogsAction() {
        NSWorkspace.shared.activateFileViewerSelecting([AppPaths.serverLogFile])
    }

    @objc private func restartServerAction() {
        state.restartAgent()
    }

    @objc private func quitAction() {
        NSApp.terminate(nil)
    }

    // MARK: Menu bar item

    private func installMenuBarItem() {
        guard statusItem == nil else { return }
        let item = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
        if let button = item.button {
            button.image = NSImage(systemSymbolName: "play.rectangle.fill", accessibilityDescription: "Drew's YT Scheduler")
            button.image?.size = NSSize(width: 18, height: 18)
        }
        let menu = NSMenu()
        menu.delegate = self
        menu.autoenablesItems = false
        item.menu = menu
        statusItem = item
        rebuildMenuBarMenu()
    }

    private func removeMenuBarItem() {
        if let item = statusItem {
            NSStatusBar.system.removeStatusItem(item)
            statusItem = nil
        }
    }

    private func rebuildMenuBarMenu() {
        guard let menu = statusItem?.menu else { return }
        menu.removeAllItems()

        let statusLine = NSMenuItem(title: menuBarStatusText(), action: nil, keyEquivalent: "")
        statusLine.isEnabled = false
        menu.addItem(statusLine)

        menu.addItem(.separator())

        let openWebUI = NSMenuItem(title: "Open Web UI", action: #selector(openWebUI), keyEquivalent: "")
        openWebUI.target = self
        menu.addItem(openWebUI)

        let restart = NSMenuItem(title: "Restart Server", action: #selector(restartServerAction), keyEquivalent: "")
        restart.target = self
        restart.isEnabled = state.agentStatus == .enabled
        menu.addItem(restart)

        let viewLogs = NSMenuItem(title: "View Logs", action: #selector(viewLogsAction), keyEquivalent: "")
        viewLogs.target = self
        menu.addItem(viewLogs)

        let monitor = NSMenuItem(title: "Monitor Server…", action: #selector(showMonitorAction), keyEquivalent: "")
        monitor.target = self
        menu.addItem(monitor)

        menu.addItem(.separator())

        let settings = NSMenuItem(title: "Settings…", action: #selector(showSettingsAction), keyEquivalent: "")
        settings.target = self
        menu.addItem(settings)

        menu.addItem(.separator())

        let quit = NSMenuItem(title: "Quit", action: #selector(NSApplication.terminate(_:)), keyEquivalent: "")
        menu.addItem(quit)
    }

    private func menuBarStatusText() -> String {
        switch state.agentStatus {
        case .enabled:
            return "Running on port \(AppPaths.serverPort)"
        case .requiresApproval:
            return "Needs approval (System Settings)"
        case .notRegistered:
            return "Background service not installed"
        case .notFound:
            return "Background service not found"
        @unknown default:
            return "Unknown"
        }
    }

    // NSMenuDelegate — refresh on every open so the user sees fresh status.
    func menuWillOpen(_ menu: NSMenu) {
        state.refresh()
        if menu === statusItem?.menu {
            rebuildMenuBarMenu()
        } else {
            // Other menus (Tools): gate Restart by agent state.
            for item in menu.items where item.tag == Self.restartMenuItemTag {
                item.isEnabled = state.agentStatus == .enabled
            }
        }
        updateMenuBarIcon()
    }

    /// Tints the menu-bar icon orange when the running server's build_id
    /// disagrees with this .app's bundled identity.
    private func updateMenuBarIcon() {
        guard let button = statusItem?.button else { return }
        let mismatched = state.buildMismatch != nil
        let symbol = mismatched ? "exclamationmark.triangle.fill" : "play.rectangle.fill"
        let image = NSImage(systemSymbolName: symbol, accessibilityDescription: "Drew's YT Scheduler")
        image?.isTemplate = !mismatched
        button.image = image
        button.image?.size = NSSize(width: 18, height: 18)
        button.contentTintColor = mismatched ? .systemOrange : nil
    }

    // MARK: External controls

    /// Called from SettingsView (via UserDefaults observation if we had one)
    /// or from the menu-bar toggle if we add one. For now toggling visibility
    /// is wired directly through user defaults; we expose this for completeness.
    func setMenuBarVisible(_ visible: Bool) {
        UserDefaults.standard.set(visible, forKey: Self.menuBarVisibleKey)
        if visible {
            installMenuBarItem()
        } else {
            removeMenuBarItem()
        }
    }
}

/// Bridges NSWindowDelegate's close notification to a closure so we can drop
/// our strong reference and let SwiftUI rebuild on next open.
@MainActor
private final class WindowReleaseDelegate: NSObject, NSWindowDelegate {
    private let onClose: (NSWindow) -> Void
    init(onClose: @escaping (NSWindow) -> Void) {
        self.onClose = onClose
    }
    nonisolated func windowWillClose(_ notification: Notification) {
        guard let window = notification.object as? NSWindow else { return }
        Task { @MainActor in
            self.onClose(window)
        }
    }
}
