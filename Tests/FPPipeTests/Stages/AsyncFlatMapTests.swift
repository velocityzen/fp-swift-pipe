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
    // Every transform waits until all `count` are running at once — reachable only if the
    // stage runs them in parallel. No wall-clock bound, so a loaded CI runner can't flake it.
    let count = 5
    let rendezvous = Rendezvous(expected: count)
    let pipe = Pipe<Int, AppError> {
        From(0..<count)
        AsyncFlatMap(concurrency: count) { (n: Int) async -> Result<Int, AppError> in
            await rendezvous.enter()
            return .success(n)
        }
    }

    _ = await pipe.toResult()
    #expect(rendezvous.peak == count)
}
