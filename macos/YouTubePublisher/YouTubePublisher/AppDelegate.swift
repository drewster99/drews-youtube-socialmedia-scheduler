import AppKit
import SwiftUI

class AppDelegate: NSObject, NSApplicationDelegate {
    private var statusItem: NSStatusItem!
    private var serverManager = ServerManager.shared
    private var popover: NSPopover?

    func applicationDidFinishLaunching(_ notification: Notification) {
        // Hide dock icon — menubar only
        NSApp.setActivationPolicy(.accessory)

        setupMenuBar()
        serverManager.startServer()
    }

    func applicationWillTerminate(_ notification: Notification) {
        serverManager.stopServer()
    }

    private func setupMenuBar() {
        statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)

        if let button = statusItem.button {
            button.image = NSImage(systemSymbolName: "play.rectangle.fill", accessibilityDescription: "YouTube Publisher")
            button.image?.size = NSSize(width: 18, height: 18)
        }

        let menu = NSMenu()

        let statusMenuItem = NSMenuItem(title: "Starting...", action: nil, keyEquivalent: "")
        statusMenuItem.tag = 100
        menu.addItem(statusMenuItem)

        menu.addItem(NSMenuItem.separator())

        let openItem = NSMenuItem(title: "Open Web UI", action: #selector(openWebUI), keyEquivalent: "o")
        openItem.target = self
        menu.addItem(openItem)

        menu.addItem(NSMenuItem.separator())

        let restartItem = NSMenuItem(title: "Restart Server", action: #selector(restartServer), keyEquivalent: "r")
        restartItem.target = self
        menu.addItem(restartItem)

        let logsItem = NSMenuItem(title: "View Logs...", action: #selector(openLogs), keyEquivalent: "l")
        logsItem.target = self
        menu.addItem(logsItem)

        menu.addItem(NSMenuItem.separator())

        let quitItem = NSMenuItem(title: "Quit YouTube Publisher", action: #selector(quitApp), keyEquivalent: "q")
        quitItem.target = self
        menu.addItem(quitItem)

        statusItem.menu = menu

        // Update status in menu when server state changes
        serverManager.onStatusChange = { [weak self] status in
            DispatchQueue.main.async {
                self?.updateStatus(status)
            }
        }
    }

    private func updateStatus(_ status: ServerManager.ServerStatus) {
        guard let menu = statusItem.menu,
              let statusItem = menu.item(withTag: 100) else { return }

        switch status {
        case .starting:
            statusItem.title = "⏳ Starting..."
            self.statusItem.button?.image = NSImage(systemSymbolName: "play.rectangle", accessibilityDescription: nil)
        case .running(let port):
            statusItem.title = "✅ Running on port \(port)"
            self.statusItem.button?.image = NSImage(systemSymbolName: "play.rectangle.fill", accessibilityDescription: nil)
        case .stopped:
            statusItem.title = "⛔ Stopped"
            self.statusItem.button?.image = NSImage(systemSymbolName: "stop.circle", accessibilityDescription: nil)
        case .error(let message):
            statusItem.title = "❌ Error: \(message)"
            self.statusItem.button?.image = NSImage(systemSymbolName: "exclamationmark.triangle", accessibilityDescription: nil)
        }
    }

    @objc private func openWebUI() {
        if case .running(let port) = serverManager.status {
            NSWorkspace.shared.open(URL(string: "http://127.0.0.1:\(port)")!)
        } else {
            // Try default port
            NSWorkspace.shared.open(URL(string: "http://127.0.0.1:8008")!)
        }
    }

    @objc private func restartServer() {
        serverManager.stopServer()
        DispatchQueue.main.asyncAfter(deadline: .now() + 1) {
            self.serverManager.startServer()
        }
    }

    @objc private func openLogs() {
        let logDir = ServerManager.dataDir.appendingPathComponent("logs")
        NSWorkspace.shared.open(logDir)
    }

    @objc private func quitApp() {
        serverManager.stopServer()
        NSApp.terminate(nil)
    }
}
