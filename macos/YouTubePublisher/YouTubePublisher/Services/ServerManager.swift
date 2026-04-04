import Foundation

/// Manages the embedded Python server lifecycle.
/// Python runtime + all dependencies are bundled inside the app.
class ServerManager {
    static let shared = ServerManager()

    /// Default server port — single source of truth for Swift side.
    /// Must match YTP_PORT passed to the Python process below.
    static let defaultPort = 8008

    static let dataDir: URL = {
        let dir = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".youtube-publisher")
        do {
            try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        } catch {
            // Fatal during startup — can't run without a data directory
            fatalError("Failed to create data directory at \(dir.path): \(error)")
        }
        return dir
    }()

    enum ServerStatus: Equatable {
        case starting
        case running(port: Int)
        case stopped
        case error(String)
    }

    private(set) var status: ServerStatus = .stopped
    var onStatusChange: ((ServerStatus) -> Void)?

    private var process: Process?
    private var outputPipe: Pipe?
    private var logHandle: FileHandle?
    private let port: Int = ServerManager.defaultPort

    private init() {}

    /// Path to the bundled Python binary inside the app
    private var pythonPath: URL? {
        guard let resourcePath = Bundle.main.resourceURL else { return nil }
        return resourcePath
            .appendingPathComponent("python")
            .appendingPathComponent("bin")
            .appendingPathComponent("python3")
    }

    /// Path to the bundled Python source code
    private var pythonSourcePath: URL? {
        guard let resourcePath = Bundle.main.resourceURL else { return nil }
        return resourcePath.appendingPathComponent("youtube_publisher_src")
    }

    /// Path to the bundled Python site-packages
    private var sitePackagesPath: URL? {
        guard let resourcePath = Bundle.main.resourceURL else { return nil }
        return resourcePath.appendingPathComponent("python").appendingPathComponent("lib")
    }

    func startServer() {
        guard process == nil else { return }

        guard let python = pythonPath, FileManager.default.fileExists(atPath: python.path) else {
            setStatus(.error("Bundled Python not found"))
            return
        }

        guard let sourcePath = pythonSourcePath else {
            setStatus(.error("Python source not found in bundle"))
            return
        }

        setStatus(.starting)

        let logDir = Self.dataDir.appendingPathComponent("logs")
        do {
            try FileManager.default.createDirectory(at: logDir, withIntermediateDirectories: true)
        } catch {
            setStatus(.error("Failed to create log directory: \(error.localizedDescription)"))
            return
        }

        let proc = Process()
        proc.executableURL = python
        proc.arguments = ["-m", "youtube_publisher.main"]

        // Whitelist safe environment variables instead of inheriting everything.
        // This prevents leaking unrelated sensitive env vars from the parent process.
        let parentEnv = ProcessInfo.processInfo.environment
        var env: [String: String] = [:]

        // System essentials
        let safeKeys = ["HOME", "USER", "LANG", "LC_ALL", "LC_CTYPE", "TMPDIR", "SHELL", "TERM"]
        for key in safeKeys {
            if let value = parentEnv[key] {
                env[key] = value
            }
        }

        // Pass through app-specific vars (YTP_*, ANTHROPIC_*) and network proxy vars
        let passThroughPrefixes = ["YTP_", "ANTHROPIC_"]
        let passThroughExact: Set = [
            "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY",
            "http_proxy", "https_proxy", "no_proxy",
            "SSL_CERT_FILE", "REQUESTS_CA_BUNDLE",
        ]
        for (key, value) in parentEnv {
            if passThroughExact.contains(key) || passThroughPrefixes.contains(where: { key.hasPrefix($0) }) {
                env[key] = value
            }
        }
        env["PYTHONHOME"] = python.deletingLastPathComponent().deletingLastPathComponent().path
        env["PYTHONPATH"] = sourcePath.path
        env["YTP_HOST"] = "127.0.0.1"
        env["YTP_PORT"] = String(port)
        env["YTP_DATA_DIR"] = Self.dataDir.path
        // Ensure FFmpeg is findable
        let extraPaths = ["/opt/homebrew/bin", "/usr/local/bin"]
        env["PATH"] = (extraPaths + [parentEnv["PATH"] ?? "/usr/bin:/bin"]).joined(separator: ":")
        proc.environment = env

        // Capture output for logging
        let pipe = Pipe()
        proc.standardOutput = pipe
        proc.standardError = pipe
        outputPipe = pipe

        // Open log file handle once and keep it open for the server's lifetime
        let logFile = logDir.appendingPathComponent("server.log")
        if !FileManager.default.fileExists(atPath: logFile.path) {
            FileManager.default.createFile(atPath: logFile.path, contents: nil)
        }
        do {
            let handle = try FileHandle(forWritingTo: logFile)
            handle.seekToEndOfFile()
            self.logHandle = handle
        } catch {
            // Non-fatal: server can run without logging
            NSLog("WARNING: Could not open log file at \(logFile.path): \(error)")
        }

        pipe.fileHandleForReading.readabilityHandler = { [weak self] handle in
            let data = handle.availableData
            guard !data.isEmpty else { return }

            // Write to log file (handle kept open)
            self?.logHandle?.write(data)

            // Check for server ready message
            if let output = String(data: data, encoding: .utf8) {
                if output.contains("Uvicorn running") || output.contains("Application startup complete") {
                    DispatchQueue.main.async {
                        self?.setStatus(.running(port: self?.port ?? ServerManager.defaultPort))
                    }
                }
            }
        }

        proc.terminationHandler = { [weak self] process in
            DispatchQueue.main.async {
                // Close the log file handle
                self?.logHandle?.closeFile()
                self?.logHandle = nil

                if process.terminationStatus != 0 {
                    self?.setStatus(.error("Server exited with code \(process.terminationStatus)"))
                } else {
                    self?.setStatus(.stopped)
                }
                self?.process = nil
            }
        }

        do {
            try proc.run()
            process = proc

            // If server doesn't report ready within 10 seconds, verify via HTTP health check
            DispatchQueue.main.asyncAfter(deadline: .now() + 10) { [weak self] in
                guard let self, case .starting = self.status else { return }
                self.performHealthCheck()
            }
        } catch {
            setStatus(.error("Failed to start: \(error.localizedDescription)"))
        }
    }

    func stopServer() {
        guard let proc = process, proc.isRunning else {
            process = nil
            return
        }

        proc.interrupt()  // SIGINT — graceful shutdown

        // Give it 5 seconds to shut down gracefully, then force kill
        DispatchQueue.global().asyncAfter(deadline: .now() + 5) {
            if proc.isRunning {
                proc.terminate()  // SIGTERM
            }
        }
    }

    /// Verify the server is actually responding before marking it as running.
    private func performHealthCheck() {
        let url = URL(string: "http://127.0.0.1:\(port)/")
        guard let url else {
            setStatus(.error("Invalid health check URL"))
            return
        }

        let task = URLSession.shared.dataTask(with: url) { [weak self] _, response, error in
            DispatchQueue.main.async {
                guard let self, case .starting = self.status else { return }

                if let httpResponse = response as? HTTPURLResponse, httpResponse.statusCode < 500 {
                    self.setStatus(.running(port: self.port))
                } else if let error {
                    self.setStatus(.error("Server not responding: \(error.localizedDescription)"))
                } else {
                    self.setStatus(.error("Server returned unexpected response"))
                }
            }
        }
        task.resume()
    }

    private func setStatus(_ newStatus: ServerStatus) {
        status = newStatus
        onStatusChange?(newStatus)
    }
}
