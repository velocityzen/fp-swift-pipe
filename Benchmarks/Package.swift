// swift-tools-version: 6.3

import PackageDescription

// Same isolation settings as the FPPipe target.
let swiftSettings: [SwiftSetting] = [
    .enableUpcomingFeature("NonisolatedNonsendingByDefault")
]

// A separate package, so the library does not depend on it. Run `swift package benchmark`
// from this folder.
let package = Package(
    name: "fp-swift-pipe-benchmarks",
    platforms: [.macOS(.v26)],
    dependencies: [
        .package(name: "fp-swift-pipe", path: ".."),
        .package(
            url: "https://github.com/ordo-one/benchmark",
            .upToNextMajor(from: "1.38.1")
        ),
    ],
    targets: [
        .target(
            name: "BenchmarkSupport",
            dependencies: [
                .product(name: "Benchmark", package: "benchmark")
            ],
            swiftSettings: swiftSettings
        )
    ]
)

// Each suite is an executable in Benchmarks/, where the plugin finds it.
for suite in ["StageBenchmarks", "AsyncStageBenchmarks", "PipeBenchmarks"] {
    package.targets.append(
        .executableTarget(
            name: suite,
            dependencies: [
                "BenchmarkSupport",
                .product(name: "FPPipe", package: "fp-swift-pipe"),
                .product(name: "Benchmark", package: "benchmark"),
            ],
            path: "Benchmarks/\(suite)",
            swiftSettings: swiftSettings,
            plugins: [
                .plugin(name: "BenchmarkPlugin", package: "benchmark")
            ]
        )
    )
}
