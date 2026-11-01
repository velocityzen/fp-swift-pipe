import Benchmark

/// Metrics recorded by every FPPipe benchmark: time, instructions, heap
/// traffic and ARC traffic. Every value is reported per run of the pipeline
/// under test.
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

/// The metrics `baseline check` fails on. Allocation and ARC counts barely
/// move between runs of the same code, while times swing with the machine's
/// load, so times are reported but never fail a check.
public let checkedMetrics: [BenchmarkMetric] = [
    .mallocCountTotal,
    .objectAllocCount,
    .retainCount,
    .releaseCount,
]

/// How far, in percent, a checked metric's median may move before
/// `baseline check` reports it. Where a race between tasks decides how much
/// a run allocates, two runs of the same code differ by a few percent; one
/// allocation more per element is far more.
let checkedTolerance = 5.0

var pipeThresholds: [BenchmarkMetric: BenchmarkThresholds] {
    Dictionary(
        uniqueKeysWithValues: pipeMetrics.map { metric in
            let thresholds =
                checkedMetrics.contains(metric)
                ? BenchmarkThresholds(relative: [.p50: checkedTolerance])
                : BenchmarkThresholds()
            return (metric, thresholds)
        }
    )
}

public extension Benchmark.Configuration {
    /// The configuration every FPPipe benchmark uses.
    ///
    /// Each sample runs `scaling` runs in a `benchmark.scaledIterations`
    /// loop, and the scaled metrics are divided back to a single run. Use
    /// `.kilo` for runs in the micro- or nanosecond range, so timer and
    /// sampling overhead stays out of the numbers, and `.one` for runs that
    /// already take a millisecond or so.
    static func pipe(
        _ tags: [String: String] = [:],
        scaling: BenchmarkScalingFactor = .kilo
    ) -> Self {
        .init(
            metrics: pipeMetrics,
            tags: tags,
            warmupIterations: 5,
            scalingFactor: scaling,
            maxDuration: .seconds(1),
            maxIterations: 10_000,
            thresholds: pipeThresholds
        )
    }
}

/// The failure every failing benchmark element carries.
public struct BenchError: Error, Equatable, Sendable {
    public init() {}
}

/// A second failure type, for stages that change the failure channel.
public struct OtherError: Error, Equatable, Sendable {
    public init() {}
}

public typealias IntResult = Result<Int, BenchError>

/// Iterates `sequence` to its end and returns how many elements it produced,
/// the cheapest way to run a pipeline.
@inlinable
public func drain<S: AsyncSequence>(_ sequence: S) async -> Int where S.Failure == Never {
    var count = 0
    for await _ in sequence {
        count += 1
    }
    return count
}

/// An async sequence of `0..<count` that never suspends or allocates, so a
/// benchmark measures what consumes it rather than its source.
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

/// An async sequence of `count` results, every third a failure, that never
/// suspends or allocates: the source for stages that act on failures.
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
