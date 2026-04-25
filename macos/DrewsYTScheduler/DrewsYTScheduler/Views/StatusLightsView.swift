import SwiftUI

/// Three independent indicators for the background server's health. Each
/// answers a distinct question, and a green light requires the previous
/// light to also be green:
///
///   1. **Registered** — SMAppService has the agent loaded.
///   2. **Running**    — a process is listening on port 8008 (any HTTP
///                       response counts, even errors).
///   3. **Reachable**  — the server returned ``/api/build`` AND its
///                       ``build_id`` matches this .app shell.
struct StatusLightsView: View {
    @ObservedObject var state: ServerStateModel

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            row(title: "Registered", on: state.isRegistered, detail: registeredDetail)
            row(title: "Running",    on: state.isRunning,    detail: runningDetail)
            row(title: "Reachable",  on: state.isReachable,  detail: reachableDetail)
        }
    }

    private func row(title: String, on: Bool, detail: String) -> some View {
        HStack(spacing: 8) {
            Circle()
                .fill(on ? Color.green : Color.gray.opacity(0.5))
                .frame(width: 10, height: 10)
            Text(title).font(.subheadline.weight(.medium))
                .frame(width: 90, alignment: .leading)
            Text(detail).font(.footnote).foregroundStyle(.secondary)
                .lineLimit(1).truncationMode(.tail)
            Spacer(minLength: 0)
        }
    }

    private var registeredDetail: String {
        state.agentStatus.displayName
    }

    private var runningDetail: String {
        switch state.reachability {
        case .unknown: return "probing…"
        case .connectionRefused: return "no listener on port \(AppPaths.serverPort)"
        case .responseError(let msg): return "responding (\(msg))"
        case .ok: return "listening on port \(AppPaths.serverPort)"
        }
    }

    private var reachableDetail: String {
        switch state.reachability {
        case .unknown: return ""
        case .connectionRefused: return "—"
        case .responseError(let msg): return "no /api/build (\(msg))"
        case .ok(let info):
            return state.serverMatchesBundle
                ? "build \(info.kind) \(info.version) (#\(info.buildNumber))"
                : "build mismatch — server is #\(info.buildNumber), app is #\(BuildInfoReader.bundle.buildNumber)"
        }
    }
}
