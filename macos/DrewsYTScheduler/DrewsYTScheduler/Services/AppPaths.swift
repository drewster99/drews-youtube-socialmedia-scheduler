import Foundation

/// Apple-standard paths for the embedded Python server. Mirrors the
/// resolution done in ``yt_scheduler/config.py`` — the server reads these
/// env vars when the Swift launcher passes them; otherwise it derives the
/// same paths itself, so both processes converge on identical locations.
enum AppPaths {
    static let bundleId = "com.nuclearcyborg.drews-socialmedia-scheduler"

    /// ``~/Library`` — looked up via FileManager so sandboxed paths and
    /// non-default user dirs resolve correctly.
    static var userLibrary: URL {
        if let url = try? FileManager.default.url(
            for: .libraryDirectory, in: .userDomainMask,
            appropriateFor: nil, create: false
        ) {
            return url
        }
        // FileManager always succeeds for libraryDirectory under userDomainMask
        // on real macOS; the fallback exists only to satisfy the optional.
        return FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent("Library", isDirectory: true)
    }

    /// ``~/Library/Application Support/<bundle_id>/``
    static var dataDirectory: URL {
        if let url = try? FileManager.default.url(
            for: .applicationSupportDirectory, in: .userDomainMask,
            appropriateFor: nil, create: true
        ) {
            return url.appendingPathComponent(bundleId, isDirectory: true)
        }
        return userLibrary
            .appendingPathComponent("Application Support", isDirectory: true)
            .appendingPathComponent(bundleId, isDirectory: true)
    }

    /// ``~/Library/Logs/<bundle_id>/`` — there's no FileManager.SearchPathDirectory
    /// for Logs, so we anchor on userLibrary (also FileManager-derived).
    static var logDirectory: URL {
        userLibrary
            .appendingPathComponent("Logs", isDirectory: true)
            .appendingPathComponent(bundleId, isDirectory: true)
    }

    /// Server log written by the Python entry point (see ``yt_scheduler/main.py``).
    static var serverLogFile: URL {
        logDirectory.appendingPathComponent("server.log")
    }

    /// Pre-redirect output captured directly by launchd. This is where
    /// EX_CONFIG / LWCR mismatch / Python ImportError messages show up, since
    /// they happen before our entry point can dup stdout/stderr into the
    /// user-Library log file. Path is wired in ``macos/build.sh``'s embedded
    /// LaunchAgent plist.
    static var bootLogFile: URL {
        URL(fileURLWithPath: "/tmp/\(bundleId).boot.log")
    }

    /// Server bind port. Single source of truth — must match the Python
    /// ``DYS_PORT`` default in ``config.py``.
    static let serverPort: Int = 8008

    static var serverWebURL: URL {
        URL(string: "http://127.0.0.1:\(serverPort)/")!
    }
}
