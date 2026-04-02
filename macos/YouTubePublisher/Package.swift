// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "YouTubePublisher",
    platforms: [.macOS(.v14)],
    targets: [
        .executableTarget(
            name: "YouTubePublisher",
            path: "YouTubePublisher",
            resources: [
                .copy("Resources"),
            ]
        ),
    ]
)
