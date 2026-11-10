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
    // Timing can vary on CI simulators, so check this on macOS and Linux only.
    #if os(macOS) || os(Linux)
    #expect(values.first == 1)
    #endif
}

@Test
func asyncFlatMapConcurrentParallelizesWork() async {
    // Passes only if all `count` closures run at once. It measures no time, so slow CI is fine.
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
