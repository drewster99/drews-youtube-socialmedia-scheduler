import Foundation
import ServiceManagement

/// In-memory activity log for the .app shell. Captures every SMAppService
/// action, every status query, every server probe, and every error with a
/// timestamp + source tag. Surfaced via Settings → "Activity log…".
@MainActor
final class ActivityLog: ObservableObject {
    static let shared = ActivityLog()

    enum Level: String {
        case info, warn, error
    }

    enum Source: String {
        case agent          // SMAppService.agent (background server)
        case loginItem      // SMAppService.mainApp
        case probe          // HTTP probe against /api/build
        case ui             // UI-driven action (button, menu)
        case lifecycle      // app launch / activation
    }

    struct Entry: Identifiable {
        let id = UUID()
        let timestamp: Date
        let level: Level
        let source: Source
        let message: String
    }

    @Published private(set) var entries: [Entry] = []
    private let maxEntries = 500

    private init() {}

    func log(_ level: Level, _ source: Source, _ message: String) {
        let entry = Entry(timestamp: Date(), level: level, source: source, message: message)
        entries.append(entry)
        if entries.count > maxEntries {
            entries.removeFirst(entries.count - maxEntries)
        }
        // Also emit to NSLog so it lands in Console.app + the launchd-captured
        // stderr — useful when debugging without the .app window open.
        NSLog("[%@/%@] %@", source.rawValue, level.rawValue, message)
    }

    func clear() {
        entries.removeAll()
    }

    func formattedDump() -> String {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:mm:ss.SSS"
        return entries.map { entry in
            "\(formatter.string(from: entry.timestamp))  [\(entry.source.rawValue)/\(entry.level.rawValue)]  \(entry.message)"
        }.joined(separator: "\n")
    }
}
