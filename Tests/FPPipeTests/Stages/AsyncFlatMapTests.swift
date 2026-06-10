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
    // Smallest sleep completes first — completion order is scheduler-timing dependent
    // and simulators can stall under CI, so assert it on macOS/Linux only.
    #if os(macOS) || os(Linux)
    #expect(values.first == 1)
    #endif
}

@Test
func asyncFlatMapConcurrentParallelizesWork() async {
    // 5 elements × 100ms each. Sequential bound: 500ms. Concurrent: ~100ms.
    // Per-element work dominates scheduler overhead on shared CI runners.
    let count = 5
    let perElementMs: UInt64 = 100
    let pipe = Pipe<Int, AppError> {
        From(0..<count)
        AsyncFlatMap(concurrency: count) { (n: Int) async -> Result<Int, AppError> in
            try? await Task.sleep(nanoseconds: perElementMs * 1_000_000)
            return .success(n)
        }
    }

    // Simulators can stall ~10s warming up under CI, so the wall-clock bound runs on
    // macOS/Linux only.
    #if os(macOS) || os(Linux)
    let clock = ContinuousClock()
    let start = clock.now
    #endif
    _ = await pipe.toResult()
    #if os(macOS) || os(Linux)
    let elapsed = start.duration(to: clock.now)
    let observedMs =
        Double(elapsed.components.seconds) * 1_000
        + Double(elapsed.components.attoseconds) / 1e15

    let sequentialMs = Double(count) * Double(perElementMs)
    #expect(observedMs < sequentialMs / 2)
    #endif
}
