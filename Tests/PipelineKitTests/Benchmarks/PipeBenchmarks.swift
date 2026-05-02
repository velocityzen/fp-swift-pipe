import Testing
import FP
import Synchronization
@testable import PipelineKit

// Informational benchmarks. These run as part of the regular test suite and
// validate behavior at scale; timings are printed for inspection but not asserted
// against fixed thresholds (which would be flaky across hardware). To skip them
// during fast iteration, filter with `swift test --skip Benchmark`.

private enum E: Error, Equatable, Sendable { case bad }

@Test func benchmarkLongChainOver10kElements() async {
    let count = 10_000
    let pipe = Pipe<Int, Never> {
        From(0..<count)
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n * 2 }
        Filter { (n: Int) in n.isMultiple(of: 4) }
        Map { (n: Int) in n &- 3 }
        CompactMap { (n: Int) -> Int? in n > 0 ? n : nil }
        Map { (n: Int) in n / 2 }
        Map { (n: Int) in String(n) }
        Map { (s: String) in s.count }
        Map { (n: Int) in n &+ 1 }
    }

    let clock = ContinuousClock()
    let start = clock.now
    let result = await pipe.toResult()
    let elapsed = start.duration(to: clock.now)
    let collected = result.getOrElse([]).count

    print("[bench] long-chain 10K → \(collected) elements in \(elapsed)")
    #expect(collected > 0)
    if case .failure = result { Issue.record("expected success") }
}

@Test func benchmarkShortCircuitAtScaleSkipsDownstreamWork() async {
    // 100K-element source that fails on the first element. Downstream Map closure
    // must not run for the remaining 99,999 — proves short-circuit works at scale.
    let mapHits = Mutex<Int>(0)
    let pipe = Pipe<Int, E> {
        From(0..<100_000)
        FlatMap { (n: Int) -> Result<Int, E> in
            n == 0 ? .failure(.bad) : .success(n)
        }
        Map { (n: Int) -> Int in
            mapHits.withLock { $0 += 1 }
            return n
        }
    }

    let clock = ContinuousClock()
    let start = clock.now
    let result = await pipe.toResult()
    let elapsed = start.duration(to: clock.now)

    print("[bench] short-circuit 100K → \(elapsed), Map closure hits = \(mapHits.withLock { $0 })")
    #expect(result == .failure(.bad))
    #expect(mapHits.withLock { $0 } == 0)
}

@Test func benchmarkAsyncMapKeepOrderParallelizesLatentWork() async {
    // 20 elements, each transform takes ~10ms. Sequential would be ~200ms;
    // parallel keep-order should be bounded by the slowest element (~10ms +
    // task scheduling overhead).
    let count = 20
    let perElementSleepNs: UInt64 = 10_000_000  // 10ms

    let pipe = Pipe<Int, Never> {
        From(0..<count)
        AsyncMapKeepOrder(concurrency: count) { (n: Int) async -> Int in
            try? await Task.sleep(nanoseconds: perElementSleepNs)
            return n
        }
    }

    let clock = ContinuousClock()
    let start = clock.now
    let result = await pipe.toResult()
    let elapsed = start.duration(to: clock.now)

    let values = result.getOrElse([])
    print("[bench] keep-order 20×10ms → \(elapsed) (sequential would be ~\(count * 10)ms)")
    #expect(values == Array(0..<count))

    // Conservative: parallel must finish in well under the sequential bound.
    let sequentialBoundNs = UInt64(count) * perElementSleepNs
    let observedNs =
        UInt64(elapsed.components.attoseconds / 1_000_000_000) + UInt64(elapsed.components.seconds)
        * 1_000_000_000
    #expect(
        observedNs < sequentialBoundNs / 2,
        "expected parallel keep-order to be at least 2× faster than sequential"
    )
}

@Test func benchmarkReiterationCostIsStable() async {
    let pipe = Pipe<Int, Never> {
        From(0..<1_000)
        Map { (n: Int) in n + 1 }
        Filter { (n: Int) in n.isMultiple(of: 2) }
        Map { (n: Int) in n * 3 }
    }

    let clock = ContinuousClock()
    var samples: [Duration] = []
    for _ in 0..<50 {
        let start = clock.now
        _ = await pipe.toResult()
        samples.append(start.duration(to: clock.now))
    }

    let total = samples.reduce(.zero, +)
    let avg = total / samples.count
    let min = samples.min() ?? .zero
    let max = samples.max() ?? .zero

    print("[bench] re-iter 50× over 1K → avg \(avg), min \(min), max \(max)")

    // Re-iteration should be O(1) per pass — max should not be wildly larger
    // than min. Allow 50× headroom for cold-cache effects on the first pass.
    #expect(max < min * 50, "re-iteration cost should be roughly constant")
}
