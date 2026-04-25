import SwiftUI
import AppKit

/// Live, color-coded activity log for this .app launch — captures every
/// SMAppService action, status query, server probe, and error so the user
/// can see exactly what's been tried and what came back.
struct ActivityLogView: View {
    @ObservedObject var state: ServerStateModel
    @ObservedObject private var log = ActivityLog.shared

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            HStack {
                Text("\(log.entries.count) entries since app launch")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                Spacer()
                Button("Copy all") {
                    NSPasteboard.general.clearContents()
                    NSPasteboard.general.setString(log.formattedDump(), forType: .string)
                }
                Button("Clear") {
                    log.clear()
                }
            }
            .padding(12)

            Divider()

            ScrollViewReader { proxy in
                ScrollView {
                    LazyVStack(alignment: .leading, spacing: 2) {
                        ForEach(log.entries) { entry in
                            row(entry).id(entry.id)
                        }
                        Color.clear.frame(height: 1).id("logBottom")
                    }
                    .padding(.vertical, 6)
                }
                .background(Color(NSColor.textBackgroundColor))
                .onChange(of: log.entries.count) { _, _ in
                    proxy.scrollTo("logBottom", anchor: .bottom)
                }
                .onAppear {
                    proxy.scrollTo("logBottom", anchor: .bottom)
                }
            }
        }
        .frame(minWidth: 720, minHeight: 400)
    }

    private func row(_ entry: ActivityLog.Entry) -> some View {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:mm:ss.SSS"
        return HStack(alignment: .top, spacing: 8) {
            Text(formatter.string(from: entry.timestamp))
                .foregroundStyle(.secondary)
                .frame(width: 96, alignment: .leading)
            Text(entry.source.rawValue)
                .foregroundStyle(.secondary)
                .frame(width: 80, alignment: .leading)
            Text(entry.level.rawValue.uppercased())
                .foregroundStyle(color(for: entry.level))
                .frame(width: 52, alignment: .leading)
            Text(entry.message)
                .foregroundStyle(color(for: entry.level))
                .textSelection(.enabled)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
        .font(.system(.body, design: .monospaced))
        .padding(.horizontal, 12)
    }

    private func color(for level: ActivityLog.Level) -> Color {
        switch level {
        case .info:  return .primary
        case .warn:  return .orange
        case .error: return .red
        }
    }
}
