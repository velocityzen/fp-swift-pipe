import Benchmark

/// The metrics every benchmark records, per run.
public let pipeMetrics: [BenchmarkMetric] = [
    .wallClock,
    .cpuTotal,
    .throughput,
    .instructions,
    .mallocCountTotal,
    .mallocBytesCount,
    .memoryLeakedBytes,
    .objectAllocCount,
    .retainCount,
    .releaseCount,
]

/// The metrics `baseline check` fails on. Times change with machine load, so they are
/// not in this list.
public let checkedMetrics: [BenchmarkMetric] = [
    .mallocCountTotal,
    .objectAllocCount,
    .retainCount,
    .releaseCount,
]

/// How much, in percent, checked metrics may change before `baseline check` fails.
public enum CountTolerance: Sendable {
    /// 5%, for a run that does the same work every time.
    case standard

    /// 15%, for a run where a producer task races the reader, such as a flat-map stage with
    /// `bufferSize` above 1. Scheduling moves its counts by up to 13%. One extra retain or
    /// allocation per element moves them by 30% or more, so that is still caught.
    case racing

    var percent: Double {
        switch self {
            case .standard: 5
            case .racing: 15
        }
    }
}

func pipeThresholds(_ tolerance: CountTolerance) -> [BenchmarkMetric: BenchmarkThresholds] {
    Dictionary(
        uniqueKeysWithValues: pipeMetrics.map { metric in
            let thresholds =
                checkedMetrics.contains(metric)
                ? BenchmarkThresholds(relative: [.p50: tolerance.percent])
                : BenchmarkThresholds()
            return (metric, thresholds)
        }
    )
}

public extension Benchmark.Configuration {
    /// The configuration every FPPipe benchmark uses.
    ///
    /// Use `scaling: .kilo` for runs under a millisecond, and `.one` for longer ones. Use
    /// `tolerance: .racing` when a producer task races the reader.
    static func pipe(
        _ tags: [String: String] = [:],
        scaling: BenchmarkScalingFactor = .kilo,
        tolerance: CountTolerance = .standard
    ) -> Self {
        .init(
            metrics: pipeMetrics,
            tags: tags,
            warmupIterations: 5,
            scalingFactor: scaling,
            maxDuration: .seconds(1),
            maxIterations: 10_000,
            thresholds: pipeThresholds(tolerance)
        )
    }
}

public struct BenchError: Error, Equatable, Sendable {
    public init() {}
}

/// For stages that change the failure type.
public struct OtherError: Error, Equatable, Sendable {
    public init() {}
}

public typealias IntResult = Result<Int, BenchError>

/// Reads `sequence` to the end and returns the element count.
@inlinable
public func drain<S: AsyncSequence>(_ sequence: S) async -> Int where S.Failure == Never {
    var count = 0
    for await _ in sequence {
        count += 1
    }
    return count
}

/// An async `0..<count` that never suspends or allocates, so only the reader is measured.
public struct CountingSequence: AsyncSequence, Sendable {
    public typealias Element = Int
    public typealias Failure = Never

    public let count: Int

    @inlinable
    public init(_ count: Int) {
        self.count = count
    }

    @inlinable
    public func makeAsyncIterator() -> Iterator {
        Iterator(count: count)
    }

    public struct Iterator: AsyncIteratorProtocol {
        @usableFromInline let count: Int
        @usableFromInline var current = 0

        @inlinable
        init(count: Int) {
            self.count = count
        }

        @inlinable
        public mutating func next() async -> Int? {
            guard current < count else { return nil }
            defer { current += 1 }
            return current
        }
    }
}

/// Like `CountingSequence`, but gives results, and every third one is a failure.
public struct ResultSequence: AsyncSequence, Sendable {
    public typealias Element = IntResult
    public typealias Failure = Never

    public let count: Int

    @inlinable
    public init(_ count: Int) {
        self.count = count
    }

    @inlinable
    public func makeAsyncIterator() -> Iterator {
        Iterator(count: count)
    }

    public struct Iterator: AsyncIteratorProtocol {
        @usableFromInline let count: Int
        @usableFromInline var current = 0

        @inlinable
        init(count: Int) {
            self.count = count
        }

        @inlinable
        public mutating func next() async -> IntResult? {
            guard current < count else { return nil }
            defer { current += 1 }
            return current.isMultiple(of: 3) ? .failure(BenchError()) : .success(current)
        }
    }
}
