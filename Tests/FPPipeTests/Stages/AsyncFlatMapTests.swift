@testable import FPPipe
import Testing

private enum AppError: Error, Equatable { case empty }

@Test
func asyncFlatMapShortCircuits() async {
    let pipe = Pipe<Int, AppError> {
        From([2, 4, 5, 6])
        AsyncFlatMap { (n: Int) async -> Result<Int, AppError> in
            n.isMultiple(of: 2) ? .success(n * 10) : .failure(.empty)
        }
    }

    let result = await pipe.toResult()
    #expect(result == .failure(.empty))
}

@Test
func asyncFlatMapConcurrentEmitsUnordered() async {
    let pipe = Pipe<Int, AppError> {
        From([5, 3, 1, 4, 2])
        AsyncFlatMap(concurrency: 5) { (n: Int) async -> Result<Int, AppError> in
            try? await Task.sleep(nanoseconds: UInt64(n) * 5_000_000)
            return .success(n)
        }
    }

    let elements = await pipe.toArray()
    let values = elements.successes()
    #expect(Set(values) == Set([1, 2, 3, 4, 5]))
    #expect(values.first == 1)  // smallest sleep completes first
}

@Test
func asyncFlatMapConcurrentParallelizesWork() async {
    // 10 elements × 20ms each. Sequential bound: 200ms. Concurrent: ~20ms.
    let count = 10
    let perElementMs: UInt64 = 20
    let pipe = Pipe<Int, AppError> {
        From(0..<count)
        AsyncFlatMap(concurrency: count) { (n: Int) async -> Result<Int, AppError> in
            try? await Task.sleep(nanoseconds: perElementMs * 1_000_000)
            return .success(n)
        }
    }

    let clock = ContinuousClock()
    let start = clock.now
    _ = await pipe.toResult()
    let elapsed = start.duration(to: clock.now)
    let observedMs =
        Double(elapsed.components.seconds) * 1_000
        + Double(elapsed.components.attoseconds) / 1e15

    // CI runners (often 2 vCPUs, oversubscribed) can't reliably hit 2×; require ≥10%.
    let sequentialMs = Double(count) * Double(perElementMs)
    #expect(observedMs < sequentialMs * 9 / 10)
}
