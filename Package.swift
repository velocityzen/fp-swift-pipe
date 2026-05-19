// swift-tools-version: 6.2
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "fp-swift-pipe",
    platforms: [.macOS(.v15), .iOS(.v18)],
    products: [
        // Products define the executables and libraries a package produces, making them visible to other packages.
        .library(
            name: "FPPipe",
            targets: ["FPPipe"]
        ),
    ],
    dependencies: [
        .package(url: "https://github.com/velocityzen/fp-swift", from: "2.2.0"),
    ],
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .target(
            name: "FPPipe",
            dependencies: [
                .product(name: "FP", package: "fp-swift"),
            ]
        ),
        .testTarget(
            name: "FPPipeTests",
            dependencies: ["FPPipe"]
        ),
    ],
    swiftLanguageModes: [.v6]
)
