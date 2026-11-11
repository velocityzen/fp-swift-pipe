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

/// How much checked metrics may change before `baseline check` fails.
public enum CountTolerance: Sendable {
    /// 5%, for a run that does the same work every time.
    case standard

    /// For a run where tasks race the reader, on fp-swift's concurrent map: a stage at
    /// `concurrency` above 1, which starts up to `workers` worker tasks, or a flat-map stage
    /// with `bufferSize` above 1, whose read-ahead task starts none. Its counts move with
    /// scheduling, so each one gets an absolute allowance (see `racingAllowance`).
    case racing(workers: Int, elements: Int)
}

// A racing run starts a worker for an element only when no worker is idle, so it starts
// anywhere from one to `min(workers, elements)` of them, and its results wait for the reader
// in buffers that grow when they fill. How many workers start and how far the buffers grow
// depend on how the reader and the tasks interleave, and move the run's counts by more than
// any percentage that would still catch a regression.

/// The checked counts one more worker adds to a run: its task and its closure, and their
/// retains and releases. Between two runs of the same code, `AsyncMatchError` at
/// `concurrency: 8` moved from 25 to 27 object allocations and from 43 to 47 mallocs.
let workerCost: [BenchmarkMetric: Int] = [
    .objectAllocCount: 2,
    .mallocCountTotal: 4,
    .retainCount: 2,
    .releaseCount: 4,
]

/// How far the retains and releases of one element may move with the path it takes: to an
/// idle worker or a new one, its result to a waiting reader or a buffer. Runs of the same
/// code have moved by a few hundredths per element locally, and the flat-map stages' retains
/// by 7% on CI, about a fifth per element; one more retain or release per element is 1.
let elementPathTolerance = 0.5

/// The most a checked count of one racing run can move between runs of the same code.
///
/// Besides the workers, the results buffer doubles when it fills. In fp-swift 4.3.0 it
/// starts empty and keeps its size between uses, so a run allocates it from 0 to
/// log2(elements) + 1 times.
func racingAllowance(_ metric: BenchmarkMetric, workers: Int, elements: Int) -> Int {
    let workerSwing = max(min(workers, elements) - 1, 0) * (workerCost[metric] ?? 0)
    switch metric {
        case .objectAllocCount, .mallocCountTotal:
            let growths = Int.bitWidth - elements.leadingZeroBitCount
            return workerSwing + growths
        default:
            return workerSwing + Int(elementPathTolerance * Double(elements))
    }
}

func pipeThresholds(
    _ tolerance: CountTolerance,
    scaling: BenchmarkScalingFactor
) -> [BenchmarkMetric: BenchmarkThresholds] {
    Dictionary(
        uniqueKeysWithValues: pipeMetrics.map { metric in
            guard checkedMetrics.contains(metric) else {
                return (metric, BenchmarkThresholds())
            }

            switch tolerance {
                case .standard:
                    return (metric, BenchmarkThresholds(relative: [.p50: 5]))
                case let .racing(workers, elements):
                    // Absolute thresholds apply to raw samples, which cover `scaling` runs
                    // each.
                    let allowance =
                        racingAllowance(metric, workers: workers, elements: elements)
                        * scaling.rawValue
                    return (metric, BenchmarkThresholds(absolute: [.p50: allowance]))
            }
        }
    )
}

public extension Benchmark.Configuration {
    /// The configuration every FPPipe benchmark uses.
    ///
    /// Use `scaling: .kilo` for runs under a millisecond, and `.one` for longer ones. Use
    /// `tolerance: .racing` when tasks race the reader.
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
            thresholds: pipeThresholds(tolerance, scaling: scaling)
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
