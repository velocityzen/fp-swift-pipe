import Testing
import FP
import Synchronization
@testable import FPPipe

// Benchmarks that run with the normal tests and check their time against generous limits.
// If one fails only on CI, raise its limit. Do not remove the check. Simulators can stall
// on CI, so time checks run on macOS and Linux only. `.serialized` keeps the benchmarks
// from slowing each other down.

private enum E: Error, Equatable, Sendable { case bad }

@Suite(.serialized)
struct PipeBenchmarks {
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
        // About 25ms locally.
        #if os(macOS) || os(Linux)
        #expect(elapsed < .milliseconds(250), "10-stage chain over 10K elements took \(elapsed)")
        #endif
    }

    @Test func benchmarkShortCircuitAtScaleSkipsDownstreamWork() async {
        // `toResult()` stops at the first failure, so `Map` never runs.
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

        let hits = mapHits.withLock { $0 }
        print("[bench] short-circuit 100K → \(elapsed), Map closure hits = \(hits)")
        #expect(result == .failure(.bad))
        #expect(hits == 0)
        // About 1ms locally.
        #if os(macOS) || os(Linux)
        #expect(
            elapsed < .milliseconds(100),
            "short-circuit on first failure shouldn't iterate the source: \(elapsed)",
        )
        #endif
    }

    @Test func benchmarkAsyncMapOrderedParallelizesLatentWork() async {
        // One at a time takes about 500ms. In parallel, it takes about one sleep (100ms). The
        // sleep is long, so task start-up time on CI does not matter.
        let count = 5
        let perElementSleepNs: UInt64 = 100_000_000  // 100ms

        let pipe = Pipe<Int, Never> {
            From(0..<count)
            AsyncMap(concurrency: count, ordered: true) { (n: Int) async -> Int in
                try? await Task.sleep(nanoseconds: perElementSleepNs)
                return n
            }
        }

        let clock = ContinuousClock()
        let start = clock.now
        let result = await pipe.toResult()
        let elapsed = start.duration(to: clock.now)

        let values = result.getOrElse([])
        print("[bench] ordered 5×100ms → \(elapsed) (sequential would be ~\(count * 100)ms)")
        #expect(values == Array(0..<count))

        #if os(macOS) || os(Linux)
        let sequentialBoundNs = UInt64(count) * perElementSleepNs
        let observedNs =
            UInt64(elapsed.components.seconds) * 1_000_000_000
            + UInt64(elapsed.components.attoseconds / 1_000_000_000)
        #expect(
            observedNs < sequentialBoundNs / 2,
            "expected parallel ordered to be at least 2× faster than sequential"
        )
        #endif
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
        let median = samples.sorted()[samples.count / 2]
        let min = samples.min() ?? .zero
        let max = samples.max() ?? .zero

        print("[bench] re-iter 50× over 1K → avg \(avg), median \(median), min \(min), max \(max)")

        // Check the median, not the max: CI can pause a single run for 100ms or more.
        #if os(macOS) || os(Linux)
        #expect(median < min * 50, "re-iteration cost should be roughly constant")
        // About 50ms locally.
        #expect(total < .seconds(1), "50× re-iteration over 1K took \(total)")
        #endif
    }
}
