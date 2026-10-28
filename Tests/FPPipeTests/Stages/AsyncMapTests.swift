@testable import FPPipe
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
    // Smaller sleeps complete first → first emitted is the smallest. Completion order
    // is scheduler-timing dependent and simulators can stall under CI, so assert it on
    // macOS/Linux only.
    #if os(macOS) || os(Linux)
    #expect(values.first == 1)
    #endif
}

@Test
func asyncMapConcurrentParallelizesWork() async {
    // Every transform waits until all `count` are running at once — reachable only if the
    // stage runs them in parallel. No wall-clock bound, so a loaded CI runner can't flake it.
    let count = 5
    let rendezvous = Rendezvous(expected: count)
    let pipe = Pipe<Int, Never> {
        From(0..<count)
        AsyncMap(concurrency: count) { (n: Int) async -> Int in
            await rendezvous.enter()
            return n
        }
    }

    _ = await pipe.toResult()
    #expect(rendezvous.peak == count)
}
