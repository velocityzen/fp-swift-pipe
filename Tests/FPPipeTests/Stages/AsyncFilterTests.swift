@testable import FPPipe
import FP
import Testing

private enum AppError: Error, Equatable { case bad }

@Test
func asyncFilterKeepsMatchingSuccessesAndPassesFailures() async {
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3, 4, 5])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n == 3 ? .failure(.bad) : .success(n)
        }
        AsyncFilter { (n: Int) async in
            try? await Task.sleep(nanoseconds: 1_000)
            return n.isMultiple(of: 2)
        }
    }

    var observed: [Result<Int, AppError>] = []
    for await element in pipe {
        observed.append(element)
    }

    #expect(observed == [.success(2), .failure(.bad), .success(4)])
}

@Test
func asyncFilterConcurrentRetainsExpectedElements() async {
    // With concurrency > 1, order is not guaranteed, but the set of survivors must match.
    let pipe = Pipe<Int, Never> {
        From(0..<20)
        AsyncFilter(concurrency: 8) { (n: Int) async in n.isMultiple(of: 3) }
    }

    let result = await pipe.toResult()
    let values = result.getOrElse([])
    #expect(Set(values) == Set([0, 3, 6, 9, 12, 15, 18]))
}

@Test
func asyncFilterConcurrentParallelizesWork() async {
    // 5 elements × 100ms predicate. Sequential bound: 500ms. Concurrent: ~100ms.
    // Per-element work dominates scheduler overhead on shared CI runners.
    let count = 5
    let perElementMs: UInt64 = 100
    let pipe = Pipe<Int, Never> {
        From(0..<count)
        AsyncFilter(concurrency: count) { (_: Int) async in
            try? await Task.sleep(nanoseconds: perElementMs * 1_000_000)
            return true
        }
    }

    let clock = ContinuousClock()
    let start = clock.now
    _ = await pipe.toResult()
    let elapsed = start.duration(to: clock.now)
    let observedMs =
        Double(elapsed.components.seconds) * 1_000
        + Double(elapsed.components.attoseconds) / 1e15

    let sequentialMs = Double(count) * Double(perElementMs)
    #expect(observedMs < sequentialMs / 2)
}
