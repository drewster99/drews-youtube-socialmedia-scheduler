import SwiftUI

/// Surfaces a build_id mismatch between this .app shell and the running
/// server. Used in Welcome, Settings, and Monitor windows.
struct BuildMismatchBanner: View {
    let bundle: BuildInfo
    let server: BuildInfo

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("Build mismatch — restart the server")
                .font(.subheadline.weight(.semibold))
            Text("This app was built as \(bundle.kind) \(bundle.version) (#\(bundle.buildNumber)), but the running server reports \(server.kind) \(server.version) (#\(server.buildNumber)). Restart the server from Tools → Restart Server (or use the menu bar) to bring them in sync.")
                .font(.footnote)
                .fixedSize(horizontal: false, vertical: true)
            Text("app build_id: \(bundle.buildId.prefix(8))…   server build_id: \(server.buildId.prefix(8))…")
                .font(.footnote.monospaced())
                .foregroundStyle(.secondary)
        }
        .padding(10)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(RoundedRectangle(cornerRadius: 6).fill(Color.orange.opacity(0.15)))
        .overlay(RoundedRectangle(cornerRadius: 6).stroke(Color.orange, lineWidth: 1))
    }
}
