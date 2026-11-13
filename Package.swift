// swift-tools-version: 6.3
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "fp-swift-pipe",
    platforms: [
        .macOS(.v26), .iOS(.v26), .tvOS(.v26), .watchOS(.v26), .visionOS(.v26),
    ],
    products: [
        // Products define the executables and libraries a package produces, making them visible to other packages.
        .library(
            name: "FPPipe",
            targets: ["FPPipe"]
        )
    ],
    dependencies: [
        // 4.4.1 is the first release with the concurrent-map deadlock fix.
        .package(url: "https://github.com/velocityzen/fp-swift", from: "4.4.1"),
        // Property-based tests. Test targets only, so FPPipe's users never
        // fetch it.
        .package(url: "https://github.com/velocityzen/hegel-swift", from: "0.51.5"),
    ],
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .target(
            name: "FPPipe",
            dependencies: [
                .product(name: "FP", package: "fp-swift")
            ],
            swiftSettings: [
                .enableUpcomingFeature("NonisolatedNonsendingByDefault")
            ]
        ),
        .testTarget(
            name: "FPPipeTests",
            dependencies: ["FPPipe"]
        ),
        // Hegel's engine is built for macOS and Linux only, so on the other
        // platforms this target is empty: every file is behind
        // `#if os(macOS) || os(Linux)`.
        .testTarget(
            name: "FPPipePropertyTests",
            dependencies: [
                "FPPipe",
                .product(
                    name: "HegelTesting",
                    package: "hegel-swift",
                    condition: .when(platforms: [.macOS, .linux])
                ),
            ]
        ),
    ],
    swiftLanguageModes: [.v6]
)
