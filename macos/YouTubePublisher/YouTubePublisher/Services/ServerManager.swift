import Foundation

/// Manages the embedded Python server lifecycle.
/// Python runtime + all dependencies are bundled inside the app.
class ServerManager {
    static let shared = ServerManager()

    static let dataDir: URL = {
        let dir = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".youtube-publisher")
        try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }()

    enum ServerStatus {
        case starting
        case running(port: Int)
        case stopped
        case error(String)
    }

    private(set) var status: ServerStatus = .stopped
    var onStatusChange: ((ServerStatus) -> Void)?

    private var process: Process?
    private var outputPipe: Pipe?
    private let port: Int = 8008

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
        try? FileManager.default.createDirectory(at: logDir, withIntermediateDirectories: true)

        let proc = Process()
        proc.executableURL = python
        proc.arguments = ["-m", "youtube_publisher.main"]

        // Set up environment
        var env = ProcessInfo.processInfo.environment
        env["PYTHONHOME"] = python.deletingLastPathComponent().deletingLastPathComponent().path
        env["PYTHONPATH"] = sourcePath.path
        env["YTP_HOST"] = "127.0.0.1"
        env["YTP_PORT"] = String(port)
        env["YTP_DATA_DIR"] = Self.dataDir.path
        // Ensure FFmpeg is findable
        let extraPaths = ["/opt/homebrew/bin", "/usr/local/bin"]
        env["PATH"] = (extraPaths + [env["PATH"] ?? ""]).joined(separator: ":")
        proc.environment = env

        // Capture output for logging
        let pipe = Pipe()
        proc.standardOutput = pipe
        proc.standardError = pipe
        outputPipe = pipe

        // Log server output to file
        let logFile = logDir.appendingPathComponent("server.log")
        let logHandle = try? FileHandle(forWritingTo: logFile)
        if logHandle == nil {
            FileManager.default.createFile(atPath: logFile.path, contents: nil)
        }

        pipe.fileHandleForReading.readabilityHandler = { [weak self] handle in
            let data = handle.availableData
            guard !data.isEmpty else { return }

            // Write to log file
            if let logHandle = try? FileHandle(forWritingTo: logFile) {
                logHandle.seekToEndOfFile()
                logHandle.write(data)
                logHandle.closeFile()
            }

            // Check for server ready message
            if let output = String(data: data, encoding: .utf8) {
                if output.contains("Uvicorn running") || output.contains("Application startup complete") {
                    DispatchQueue.main.async {
                        self?.setStatus(.running(port: self?.port ?? 8008))
                    }
                }
            }
        }

        proc.terminationHandler = { [weak self] process in
            DispatchQueue.main.async {
                if process.terminationStatus != 0 && self?.status != nil {
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

            // If server doesn't report ready within 10 seconds, assume it's running
            DispatchQueue.main.asyncAfter(deadline: .now() + 10) { [weak self] in
                if case .starting = self?.status {
                    self?.setStatus(.running(port: self?.port ?? 8008))
                }
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

    private func setStatus(_ newStatus: ServerStatus) {
        status = newStatus
        onStatusChange?(newStatus)
    }
}
