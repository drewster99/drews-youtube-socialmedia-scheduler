// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "DrewsYTScheduler",
    platforms: [.macOS(.v14)],
    targets: [
        .executableTarget(
            name: "DrewsYTScheduler",
            path: "DrewsYTScheduler",
            resources: [
                .copy("Resources"),
            ]
        ),
    ]
)
