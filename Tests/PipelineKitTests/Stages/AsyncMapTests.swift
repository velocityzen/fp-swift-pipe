@testable import PipelineKit
import FP
import Testing

@Test
func asyncMapTransformsSequentially() async {
    // Default concurrency: 1 → sequential, order preserved.
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        AsyncMap { (n: Int) async -> Int in
            try? await Task.sleep(nanoseconds: 1_000)
            return n * 100
        }
    }

    let result = await pipe.toResult()
    #expect(result == .success([100, 200, 300]))
}

@Test
func asyncMapConcurrentEmitsUnordered() async {
    // Smaller numbers finish faster. With concurrency > 1, AsyncMap emits as ready;
    // the result set is the same but the order matches completion, not source.
    let count = 5
    let pipe = Pipe<Int, Never> {
        From([5, 3, 1, 4, 2])
        AsyncMap(concurrency: count) { (n: Int) async -> Int in
            try? await Task.sleep(nanoseconds: UInt64(n) * 5_000_000)
            return n
        }
    }

    let result = await pipe.toResult()
    let values = result.getOrElse([])

    // Same elements, possibly different order.
    #expect(Set(values) == Set([1, 2, 3, 4, 5]))
    // Smaller sleeps complete first → first emitted is the smallest.
    #expect(values.first == 1)
}

@Test
func asyncMapConcurrentParallelizesWork() async {
    // 10 elements × 20ms each. Sequential bound: 200ms. Concurrent: ~20ms.
    let count = 10
    let perElementMs: UInt64 = 20

    let pipe = Pipe<Int, Never> {
        From(0..<count)
        AsyncMap(concurrency: count) { (n: Int) async -> Int in
            try? await Task.sleep(nanoseconds: perElementMs * 1_000_000)
            return n
        }
    }

    let clock = ContinuousClock()
    let start = clock.now
    _ = await pipe.toResult()
    let elapsed = start.duration(to: clock.now)
    let observedMs =
        Double(elapsed.components.seconds) * 1_000
        + Double(elapsed.components.attoseconds) / 1e15

    // Conservative: parallel must finish in well under sequential bound.
    let sequentialMs = Double(count) * Double(perElementMs)
    #expect(observedMs < sequentialMs / 2)
}
