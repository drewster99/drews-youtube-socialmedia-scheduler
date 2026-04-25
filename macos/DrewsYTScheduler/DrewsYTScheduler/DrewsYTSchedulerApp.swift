import SwiftUI

@main
struct DrewsYTSchedulerApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) var appDelegate

    var body: some Scene {
        // No main window — this is a menubar app
        Settings {
            EmptyView()
        }
    }
}
