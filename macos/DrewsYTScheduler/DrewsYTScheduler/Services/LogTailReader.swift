import Foundation

/// Polling-based file tailer for the server log. Polls every 500ms because
/// ``DispatchSource.makeFileSystemObjectSource`` doesn't fire on file
/// truncation/rotation reliably, and the server log is rotated occasionally.
@MainActor
final class LogTailReader: ObservableObject {
    @Published private(set) var content: String = ""

    private let url: URL
    private var task: Task<Void, Never>?
    private var fileHandle: FileHandle?
    private var lastInode: UInt64?
    private let maxBytes: Int = 256 * 1024  // keep the last 256 KiB in memory

    init(url: URL) {
        self.url = url
    }

    deinit {
        task?.cancel()
        try? fileHandle?.close()
    }

    func start() {
        guard task == nil else { return }
        task = Task { [weak self] in
            await self?.runLoop()
        }
    }

    func stop() {
        task?.cancel()
        task = nil
        try? fileHandle?.close()
        fileHandle = nil
        lastInode = nil
    }

    private func runLoop() async {
        // Initial load — read up to maxBytes from the end so we don't dump
        // gigabytes of history into the UI.
        await reopenAndSeekToTail()
        while !Task.isCancelled {
            await pollForChanges()
            try? await Task.sleep(nanoseconds: 500_000_000)
        }
    }

    private func currentInode() -> UInt64? {
        guard let attrs = try? FileManager.default.attributesOfItem(atPath: url.path) else { return nil }
        return (attrs[.systemFileNumber] as? NSNumber)?.uint64Value
    }

    private func reopenAndSeekToTail() async {
        try? fileHandle?.close()
        fileHandle = nil
        lastInode = currentInode()

        guard FileManager.default.fileExists(atPath: url.path) else {
            content = "(server log does not exist yet — \(url.path))"
            return
        }
        guard let handle = try? FileHandle(forReadingFrom: url) else {
            content = "(could not open server log)"
            return
        }
        fileHandle = handle
        let size = (try? handle.seekToEnd()) ?? 0
        let start = size > UInt64(maxBytes) ? size - UInt64(maxBytes) : 0
        try? handle.seek(toOffset: start)
        if let data = try? handle.readToEnd() {
            content = String(data: data, encoding: .utf8) ?? content
        }
    }

    private func pollForChanges() async {
        // Detect rotation/truncation: inode changed, or current size < our
        // file offset.
        let inode = currentInode()
        if inode != lastInode {
            await reopenAndSeekToTail()
            return
        }
        guard let handle = fileHandle else {
            await reopenAndSeekToTail()
            return
        }
        let attrs = try? FileManager.default.attributesOfItem(atPath: url.path)
        let fileSize = (attrs?[.size] as? NSNumber)?.uint64Value ?? 0
        let offset = (try? handle.offset()) ?? 0
        if fileSize < offset {
            // Truncated in place.
            await reopenAndSeekToTail()
            return
        }
        if fileSize == offset {
            return
        }
        if let data = try? handle.readToEnd(), !data.isEmpty {
            let chunk = String(data: data, encoding: .utf8) ?? ""
            var combined = content + chunk
            if combined.utf8.count > maxBytes {
                let bytes = Array(combined.utf8.suffix(maxBytes))
                combined = String(decoding: bytes, as: UTF8.self)
            }
            content = combined
        }
    }
}
