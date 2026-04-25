import Foundation

/// Build identity for both the .app shell and the running server. The two
/// must match — when they don't, ``Settings`` shows a banner and the web UI
/// shows a banner of its own (see ``static/js/app.js``).
struct BuildInfo: Equatable {
    let kind: String
    let version: String
    let buildNumber: String
    let buildDate: String
    let buildId: String

    static let unknown = BuildInfo(
        kind: "unknown", version: "0.0.0", buildNumber: "0",
        buildDate: "", buildId: "unknown"
    )
}

/// Result of probing the local server. Three meaningful states:
///
/// - ``connectionRefused`` — TCP refused. The agent is not listening on the
///   port. Either the agent isn't registered, or it's registered but the
///   process crashed / is between restarts.
/// - ``responseError`` — TCP accepted, HTTP layer returned something we
///   couldn't parse. The process is running but not yet healthy.
/// - ``ok`` — full success, server returned its build identity.
enum ServerProbeResult {
    case connectionRefused(String)
    case responseError(String)
    case ok(BuildInfo)
}

enum BuildInfoReader {
    /// Identity baked into the .app via ``Info.plist`` (set by build.sh).
    static var bundle: BuildInfo {
        let info = Bundle.main.infoDictionary ?? [:]
        return BuildInfo(
            kind: info["DYSBuildKind"] as? String ?? "unknown",
            version: info["CFBundleShortVersionString"] as? String ?? "0.0.0",
            buildNumber: info["DYSBuildNumber"] as? String ?? (info["CFBundleVersion"] as? String ?? "0"),
            buildDate: info["DYSBuildDate"] as? String ?? "",
            buildId: info["DYSBuildId"] as? String ?? "unknown"
        )
    }

    /// Hits the local server's ``/api/build`` endpoint. Categorises the
    /// outcome so the UI can show distinct registered/running/reachable
    /// signals rather than collapsing to a single yes/no.
    static func probeServer(port: Int, timeout: TimeInterval = 1.0) async -> ServerProbeResult {
        let url = URL(string: "http://127.0.0.1:\(port)/api/build")!
        var request = URLRequest(url: url)
        request.timeoutInterval = timeout

        do {
            let (data, response) = try await URLSession.shared.data(for: request)
            guard let http = response as? HTTPURLResponse else {
                return .responseError("non-HTTP response")
            }
            guard http.statusCode == 200 else {
                return .responseError("HTTP \(http.statusCode)")
            }
            struct Wire: Decodable {
                let kind: String
                let version: String
                let build_number: String
                let build_date: String
                let build_id: String
            }
            let wire = try JSONDecoder().decode(Wire.self, from: data)
            return .ok(BuildInfo(
                kind: wire.kind,
                version: wire.version,
                buildNumber: wire.build_number,
                buildDate: wire.build_date,
                buildId: wire.build_id
            ))
        } catch let error as URLError {
            switch error.code {
            case .cannotConnectToHost, .networkConnectionLost, .notConnectedToInternet,
                 .cannotFindHost, .timedOut:
                return .connectionRefused(error.localizedDescription)
            default:
                return .responseError(error.localizedDescription)
            }
        } catch {
            return .responseError(error.localizedDescription)
        }
    }
}
